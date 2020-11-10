from greent import node_types
from greent.util import LoggingUtil,Text
from collections import defaultdict, deque
from greent.export_type_graph import ExportGraph
from greent.synonymization import Synonymizer
from greent.graph_components import LabeledID
import logging


logger = LoggingUtil.init_logging(__name__, logging.DEBUG)


class BufferedWriter:
    """Buffered writer accepts individual nodes and edges to write to neo4j.
    It doesn't write the node/edge if it has already been written in its lifetime (it maintains a record)
    It then accumulates nodes/edges by label/type until a buffersize has been reached, at which point it does
    an intelligent update/write to the batch of nodes and edges.
    
    The correct way to use this is 
    with BufferedWriter(rosetta) as writer:
        writer.write_node(node)
        ...

    Doing this as a context manager will make sure that the different queues all get flushed out.
    """

    def __init__(self, rosetta):
        self.rosetta = rosetta
        self.merge_edges = rosetta.service_context.config.get('MERGE_EDGES') is not None
        self.written_nodes = set()
        self.written_edges = defaultdict(lambda: defaultdict( set ) )
        self.node_queues = defaultdict(dict)
        self.edge_queues = []
        self.node_buffer_size = 100
        self.edge_buffer_size = 100
        self.driver = self.rosetta.type_graph.driver
        self.maxWrittenNodes = 100000
        self.maxWrittenEdges = 100000
        self.missed_curies = {}
        # Variable to tell if we should avoid synonym map construction when processing edges.
        # Useful when processing kgx files, and cord19 files
        self.normalized = False
        # temporary cache of ids , this will be kept around till we hit maxWrittenNodes
        self.synonym_map = {}

    def __enter__(self):
        return self

    def write_node(self,node):
        if node.id in self.written_nodes:
            return
        if node.name is None or node.name == '':
            logger.warning(f"Node {node.id} is missing a label")
        self.written_nodes.add(node.id)
        typednodes = self.node_queues[frozenset(node.export_labels)]
        typednodes.update({node.id: node})
        if len(typednodes) >= self.node_buffer_size:
            self.flush()

    def write_edge(self,edge, force_create=False):
        if edge.original_predicate.identifier in self.written_edges[edge.source_id][edge.target_id] and not force_create:
            return
            # Need to only maintain the predicate id.
            # When flushing we are going to Standardize predicates.
            # Somethings might change. but not original predicates.
        self.written_edges[edge.source_id][edge.target_id].add(edge.original_predicate.identifier)
        # Append the edge in the edge queue. It will be standardized in a batch when flushing
        self.edge_queues.append(edge)
        if len(self.edge_queues) >= self.edge_buffer_size:
            self.flush()

    def flush_nodes(self, session):
        for node_type in self.node_queues:
            # Condition # 1. Handling nodes that could not be synonymized.
            # This could happen among other reasons,
            # maybe the synonymization service doesn't know about it.
            # ExportGraph.addlabels should add some types for the ones missed too, if
            # a service has decided on the node type ( Eg if gwas says node is sequence
            # variant export and nodenormalization service doesnt know it would be labled by exportgraph)

            node_queue = self.node_queues[node_type]
            node_curies = list(node_queue.keys())
            # Update the node queue for a type with results from normalization call
            if node_types.SEQUENCE_VARIANT in node_type:
                # treating S.V. nodes differently
                normalized_nodes= Synonymizer.batch_normalize_sequence_variants(node_curies)
            else:
                normalized_nodes = Synonymizer.batch_normalize_nodes(node_curies)

            # lets make a list of mappings to use it to update the edges source and target ids
            self.synonym_map.update({k: normalized_nodes[k].id for k in normalized_nodes})

            # filter out the missed ones first, i.e calling synonymization on these nodes
            # returned nothing, maybe the normalization service doesn't know about them...

            missed_nodes = {curie: node_queue[curie] for curie in node_curies if curie not in normalized_nodes}
            # previous named_thing labels won't help us catch these so saving them to file
            for k in missed_nodes:
                self.missed_curies[k] = list(node_type)
            self.write_missed_curies_to_file()
            if missed_nodes:
                for missed_node_id in missed_nodes:
                    self.synonym_map.update({missed_node_id: missed_node_id})
                session.write_transaction(
                    export_node_chunk,
                    missed_nodes,
                    node_type
                )
            # Condition # 2
            # bucket out  normalized node into chunks by their type and do something similar
            by_type = {}
            for curie in normalized_nodes:
                original_node = node_queue[curie]
                normalized_node = normalized_nodes[curie]
                # to preserve original node properties copy original's properties
                normalized_node.properties = original_node.properties

                # use the types we get from normalized node to write to graph
                types = frozenset(normalized_node.export_labels)
                typed_nodes = by_type.get(types, {})
                # add normalized node with properties from original node
                typed_nodes.update({curie: normalized_node})
                by_type[types] = typed_nodes
            for n_type in by_type:
                session.write_transaction(
                    export_node_chunk,
                    by_type[n_type],
                    n_type
                )
            self.node_queues[node_type] = {}

    def flush_edges(self, session):
        # batch normalize edges
        # and group them by their standardized labels
        standard_predicates = {}
        synonym_map = {}
        edge_by_predicate_id = {}
        if not self.normalized:
            # get the predicate ids
            original_predicates = set(map(lambda edge: edge.original_predicate.identifier, self.edge_queues))

            # batch Normalize them
            standard_predicates = Synonymizer.batch_normalize_edges(original_predicates)
            # group edges by labels and also update their standard predicates
            synonym_map = self.synonym_map
            ids = [edge.source_id for edge in self.edge_queues if edge.source_id not in synonym_map]
            ids += [edge.target_id for edge in self.edge_queues if edge.target_id not in synonym_map]
            nodes = Synonymizer.batch_normalize_nodes(ids)
            # # could this be an sv node
            ids = [i for i in ids if i not in nodes]
            nodes.update(Synonymizer.batch_normalize_sequence_variants(ids))
            synonym_map.update({curie: nodes[curie].id for curie in nodes})
        for edge in self.edge_queues:
            # update standard predicate if it's mapped.
            edge.standard_predicate = standard_predicates.get(
                edge.original_predicate.identifier,
                edge.standard_predicate if edge.standard_predicate else LabeledID(identifier='GAMMA:0', label='Unmapped_Relation')
            )
            # also need to know if source_id and target_id have changed.
            # This could happen if node was synonymized to a different ID that
            # is different from what a service returned. This implies that we need to update the
            # edge that goes along with it.
            edge.source_id = synonym_map.get(edge.source_id, edge.source_id)
            edge.target_id = synonym_map.get(edge.target_id, edge.target_id)

            predicate_id = edge.standard_predicate.identifier
            if predicate_id not in edge_by_predicate_id:
                edge_by_predicate_id[predicate_id] = []
            edge_by_predicate_id[predicate_id].append(edge)

        for predicate_id in edge_by_predicate_id:
            session.write_transaction(export_edge_chunk, edge_by_predicate_id[predicate_id], predicate_id, self.merge_edges)
        self.edge_queues = []

    def flush(self):
        with self.driver.session() as session:
            # flush the nodes and capture any id changes
            self.flush_nodes(session)

            # flush edges
            self.flush_edges(session)

            # clear the memory on a threshold boundary to avoid using up all memory when
            # processing large data sets
            if len(self.written_nodes) > self.maxWrittenNodes:
                self.synonym_map = {}
                self.written_nodes.clear()

            if len(self.written_edges) > self.maxWrittenEdges:
                self.written_edges.clear()

    def write_missed_curies_to_file(self):
        """ When node normalization is not working write the missed curies to file."""
        if not len(self.missed_curies.keys()):
            return
        with open('missed_curies.lst', 'a') as f:
            for curie in self.missed_curies:
                f.write(f'{curie}\t {",".join(self.missed_curies[curie])}\n')
        self.missed_curies = {}

    def __exit__(self,*args):
        self.flush()


def sort_edges_by_label(edges):
    el = defaultdict(list)
    deque(map(lambda x: el[Text.snakify(x[2]['object'].standard_predicate.label)].append(x), edges))
    return el


def export_edge_chunk(tx,edgelist,edgelabel, merge_edges):
    """The approach of updating edges will be to erase an old one and replace it in whole.   There's no real
    reason to worry about preserving information from an old edge.
    What defines the edge are the identifiers of its nodes, and the source.function that created it."""
    cypher = f"""UNWIND $batches as row            
            MATCH (a:{node_types.ROOT_ENTITY} {{id: row.source_id}}),(b:{node_types.ROOT_ENTITY} {{id: row.target_id}})
            MERGE (a)-[r:`{edgelabel}` {{id: apoc.util.md5([a.id, b.id, '{edgelabel}']), predicate: row.standard_id}}]->(b)
            SET r.provided_by = row.provided_by
            SET r.relation_label = row.original_predicate_label
            SET r.source_database= row.database
            SET r.ctime= row.ctime            
            SET r.publications=row.publications
            SET r.relation = row.original_predicate_id
            SET r.predicate = row.standard_id     
            SET r.source_id = a.id
            SET r.target_id = b.id       
            SET r += row.properties
            """
    if merge_edges:
        cypher = f"""UNWIND $batches as row
                MATCH (a:{node_types.ROOT_ENTITY} {{id: row.source_id}}),(b:{node_types.ROOT_ENTITY} {{id: row.target_id}})
                MERGE (a)-[r:`{edgelabel}` {{id: apoc.util.md5([a.id, b.id, '{edgelabel}']), predicate: row.standard_id}}]->(b)
                ON CREATE SET r.edge_source = [row.provided_by]
                ON CREATE SET r.relation_label = [row.original_predicate_label]
                ON CREATE SET r.source_database=[row.database]
                ON CREATE SET r.ctime=[row.ctime]
                ON CREATE SET r.publications=row.publications
                ON CREATE SET r.relation = [row.original_predicate_id]
                ON CREATE SET r.source_id = a.id
                ON CREATE SET r.target_id = b.id   
                // FOREACH mocks if condition
                FOREACH (_ IN CASE WHEN row.provided_by in r.edge_source THEN [] ELSE [1] END |
                SET r.edge_source = CASE WHEN EXISTS(r.edge_source) THEN r.edge_source + [row.provided_by] ELSE [row.provided_by] END
                SET r.ctime = CASE WHEN EXISTS (r.ctime) THEN r.ctime + [row.ctime] ELSE [row.ctime] END
                SET r.relation_label = CASE WHEN EXISTS(r.relation_label) THEN r.relation_label + [row.original_predicate_label] ELSE [row.original_predicate_label] END
                SET r.source_database = CASE WHEN EXISTS(r.source_database) THEN r.source_database + [row.database] ELSE [row.database] END
                SET r.predicate_id = row.standard_id
                SET r.relation = CASE WHEN EXISTS(r.relation) THEN r.relation + [row.original_predicate_id] ELSE [row.original_predicate_id] END
                SET r.publications = [pub in row.publications where not pub in r.publications ] + r.publications
                )
                SET r += row.properties
                """

    batch = [ {'source_id': edge.source_id,
               'target_id': edge.target_id,
               'provided_by': edge.provided_by,
               'database': edge.provided_by.split('.')[0],
               'ctime': edge.ctime,
               'standard_id': edge.standard_predicate.identifier,
               'original_predicate_id': edge.original_predicate.identifier,
               'original_predicate_label': edge.original_predicate.label,
               'publication_count': len(edge.publications),
               'publications': edge.publications[:1000],
               'properties': edge.properties if edge.properties != None else {}
               }
              for edge in edgelist]

    tx.run(cypher,{'batches': batch})

    for edge in edgelist:
        if edge.standard_predicate.identifier == 'GAMMA:0':
            logger.warn(f"Unable to map predicate for edge {edge.original_predicate}  {edge}")

def sort_nodes_by_label(nodes):
    nl = defaultdict(list)
    deque( map( lambda x: nl[x.type].append(x), nodes ) )
    return nl


def export_node_chunk(tx,nodelist,labels):
    cypher = f"""UNWIND $batches as batch
                MERGE (a:{node_types.ROOT_ENTITY} {{id: batch.id}})\n"""
    for label in labels:
        cypher += f"set a:`{label}`\n"
    cypher += """set a += batch.properties\n"""

    batch = []
    for node_id in nodelist:
        n = nodelist[node_id]
        n.properties['equivalent_identifiers'] = [s.identifier for s in n.synonyms]
        n.properties['category'] = list(labels)
        if n.name is not None:
            n.properties['name'] = n.name
        nodeout = {'id': n.id, 'properties': n.properties}
        batch.append(nodeout)
    tx.run(cypher,{'batches': batch})
