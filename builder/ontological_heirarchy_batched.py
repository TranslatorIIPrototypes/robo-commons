from greent.service import Service
from greent.triplestore import TripleStore
from greent.util import Text
from greent.graph_components import KEdge, KNode, LabeledID
from greent import node_types
from functools import reduce
from greent.export_delegator import WriterDelegator
from greent.rosetta import Rosetta

class OntologicalHeirarchy(Service):
    """
    Service that makes call to uberongraph to resolve subclass relationships between ontological terms
    """
    def __init__(self):
        self.url = "https://stars-app.renci.org/uberongraph/sparql"
        self.triplestore = TripleStore(self.url)
        self.prefix_set = {
            node_types.DISEASE_OR_PHENOTYPIC_FEATURE: ['HP','MONDO'],
            node_types.CELLULAR_COMPONENT: ['CL'],
            node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY: ['GO'],
            node_types.ANATOMICAL_ENTITY: ['UBERON'],
            node_types.CHEMICAL_SUBSTANCE: ['CHEBI']
        }
        self.root_uris = {
            node_types.ANATOMICAL_ENTITY:"<http://purl.obolibrary.org/obo/UBERON_0001062>",
            node_types.DISEASE: "<http://purl.obolibrary.org/obo/MONDO_0000001>",
            node_types.MOLECULAR_ACTIVITY: "<http://purl.obolibrary.org/obo/GO_0003674>",
            node_types.BIOLOGICAL_PROCESS: "<http://purl.obolibrary.org/obo/GO_0008150>",
            node_types.CHEMICAL_SUBSTANCE: "<http://purl.obolibrary.org/obo/CHEBI_24431>",
            node_types.PHENOTYPIC_FEATURE: "<http://purl.obolibrary.org/obo/HP_0000118>",
            node_types.CELL: "<http://purl.obolibrary.org/obo/CL_0000000>",
            node_types.CELLULAR_COMPONENT: "<http://purl.orolibrary.org/obo/GO_0005575>"
        }
        obo_prefixes = '\n'.join([
            f'PREFIX {pref}: <http://purl.obolibrary.org/obo/{pref}_>'
            for pref in set(reduce(lambda x, y: x + y, self.prefix_set.values(),[]))
        ])
        self.query = f"""
                    {obo_prefixes}
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
                    select  ?parent_id ?parent_label ?child_id ?child_label
                    where {{                        
                        ?parent_id rdfs:subClassOf $root_uri .
                        ?child_id rdfs:subClassOf ?parent_id.
                   OPTIONAL {{
                    ?parent_id rdfs:label ?parent_label.
                    ?child_id rdfs:label ?child_label.
                    }}                      
                    }}
                        """
        rosetta = Rosetta()
        self.wdg = WriterDelegator(rosetta)

    def runner(self):
        for node_type, root_iri in self.root_uris.items():
            nodes, edges = self.term_get_ancestors(node_type, root_iri)
            for index, n in enumerate(nodes):
                self.wdg.write_node(n,annotate=False)
                if( (index/len(nodes)) * 100 ) % 10 == 0:
                    print((index/len(nodes)) * 100, '% complete')
            for index, e in enumerate(edges):
                self.wdg.write_edge(e)
                if index % 100 == 0:
                    self.wdg.flush()
                if ((index / len(edges)) * 100) % 10 == 0:
                    print((index / len(edges)) * 100, '% complete')
        return



    def term_get_ancestors(self, node_type, root_iri):
        results = self.triplestore.query_template(
            template_text=self.query,
            inputs={ 'root_uri': root_iri},
            outputs=['parent_id', 'parent_label', 'child_id', 'child_label']
        )
        print('found total ', len(results), ' results.')
        nodes = set()
        edges = set()
        for index, row in enumerate(results):
            # Output type would be same as input type?
            ancestor_node = KNode(Text.obo_to_curie(row['parent_id']), name=row['parent_label'], type=node_type)
            child_node = KNode(Text.obo_to_curie(row['child_id']), name=row['child_label'], type=node_type)
            if ancestor_node.id == child_node.id:
                # refrain from adding edge to the node itself
                continue
            predicate = LabeledID(identifier='rdfs:subClassOf', label='subclass of')
            edge = self.create_edge(
                source_node=child_node,
                target_node=ancestor_node,
                predicate=predicate,
                provided_by='uberongraph.term_get_ancestors',
                input_id=child_node.id
            )
            nodes.add(child_node)
            nodes.add(ancestor_node)
            edges.add(edge)
        return nodes, edges


if __name__== '__main__':
    r = OntologicalHeirarchy()
    r.runner()
