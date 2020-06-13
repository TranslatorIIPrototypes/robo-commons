from greent.service import Service
from greent.triplestore import TripleStore
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KEdge, KNode, LabeledID
from greent import node_types
from functools import reduce

class OntologicalHeirarchy(Service):
    """
    Service that makes call to uberongraph to resolve subclass relationships between ontological terms
    """
    def __init__(self, context):
        super(OntologicalHeirarchy, self).__init__("ontological_hierarchy", context)
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
            node_types.CELL: "http://purl.obolibrary.org/obo/CL_0000000",
            node_types.CELLULAR_COMPONENT: "http://purl.orolibrary.org/obo/GO_0005575"
        }
        obo_prefixes = '\n'.join([
            f'PREFIX {pref}: <http://purl.obolibrary.org/obo/{pref}_>'
            for pref in set(reduce(lambda x, y: x + y, self.prefix_set.values(),[]))
        ])
        self.query = f"""
                    {obo_prefixes}
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
                    select distinct ?parent_id ?label
                    where {{
                      graph <http://reasoner.renci.org/ontology/closure> {{
                        $child_curie  rdfs:subClassOf ?parent_id .
                        ?parent_id rdfs:subClassOf $root_uri .
                      }}
                      graph <http://reasoner.renci.org/ontology>{{
                      ?parent_id rdfs:label ?label.
                      }}
                    }}
                    """

    def term_get_ancestors(self, child_node):
        root_uri = self.root_uris.get(child_node.type, None)
        if not root_uri:
            return []
        ###
        # Query does have an upper bound so for ontologies that start from
        #
        # Step 1 get prefixes that are supported for input node
        curie_set = set()
        for node_type in  child_node.export_labels:
            ps = self.prefix_set.get(node_type, [])
            for prefix in ps:
                synonyms = child_node.get_synonyms_by_prefix(prefix)
                curie_set.update(synonyms)
        # Step 2 get parents for those curies we support from uberon graph
        outputs = []
        for curie in curie_set:
            results = self.triplestore.query_template(
                template_text=self.query,
                inputs={'child_curie': curie, 'root_uri': root_uri},
                outputs=['parent_id', 'label']
            )

            for row in results:
                # Output type would be same as input type?
                ancestor_node = KNode(Text.obo_to_curie(row['parent_id']), name=row['label'], type=child_node.type)
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
                outputs.append((edge, ancestor_node))
        return outputs