import asyncio
from greent.annotators.util import async_client, async_sparql_client
from greent import node_types
from builder.question import LabeledID
from greent.graph_components import KNode, KEdge


class Chebi_resolver: 
    # Parent class for every hierarchy resolver.
    def __init__ (self, url, node_type, rosetta):
        self.url = url
        self.url = 'https://stars-app.renci.org/uberongraph/sparql'
        self.type = node_type
        self.async_triple_store = async_sparql_client.TripleStoreAsync(self.url)


            
    async def __get_ancestors(self, curie):
        text = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        SELECT  ?id ?label
        from <http://reasoner.renci.org/ontology>
        WHERE {
            $chebi_id rdfs:subClassOf ?id.
            ?id rdfs:label ?label.
        }
        """
        results  = await self.async_triple_store.async_query_template(
            inputs = {'chebi_id': curie},
            outputs = [ 'id', 'label' ],
            template_text = text
        )
        return results
    async def __get_parents(self, curie):
        text = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        SELECT  ?id ?label
        from <http://reasoner.renci.org/ontology>
        WHERE {
            $chebi_id rdfs:subClassOf ?id.
            ?id rdfs:label ?label.
        }
        """
        results  = await self.async_triple_store.async_query_template(
            inputs = {'chebi_id': curie},
            outputs = [ 'id', 'label' ],
            template_text = text
        )
        return results
        

    async def __get_children(self, curie):
        text = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        SELECT  ?id ?label
        from <http://reasoner.renci.org/ontology>
        WHERE {
            ?id rdfs:subClassOf $chebi_id.
            ?id rdfs:label ?label.
        }
        """
        results  = await self.async_triple_store.async_query_template(
            inputs = {'chebi_id': curie},
            outputs = [ 'id', 'label' ],
            template_text = text
        )
        return results


    async def __get_decendents(self, curie):
        text = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        SELECT  ?id ?label
        from <http://reasoner.renci.org/ontology>
        from <http://reasoner.renci.org/closure>
        WHERE {
            ?id rdfs:subClassOf $chebi_id.
            ?id rdfs:label ?label.
        }
        """
        results  = await self.async_triple_store.async_query_template(
            inputs = {'chebi_id': curie},
            outputs = [ 'id', 'label' ],
            template_text = text
        )
        return results


    async def get_children(self, curie) :         
        children = await self.__get_children(curie)       
        children_lids =  list(map(lambda x: LabeledID(identifier = x['id'], label= x['label']), children))
        return self.make_nodes(children_lids)

    async def get_parents(self, curie):
        parents = await self.__get_parents(curie)
        parents_lids = list(map(lambda x: LabeledID(identifier = x['id'], label= x['label']), parents))
        return self.make_nodes(parents_lids)
    
    async def get_descendants(self, curie):
        descendants = await self.__get_decendents(curie)
        descendants_lids = list(map(lambda x: LabeledID(identifier = x['id'], label= x['label']), descendants))
        return self.make_nodes(descendants_lids)
    
    async def get_ancestors(self, curie):
        ancestors = await self.__get_ancestors(curie)
        ancestors_lids = list(map(lambda x: LabeledID(identifier = x['id'], label= x['label']), ancestors))
        return self.make_nodes(ancestors_lids)

    def make_nodes(self, labelID_list):
        nodes = []
        for labeledId in labelID_list:
            nodes.append(KNode(id=labeledId.identifier, label = labeledId.label, type= self.type))                
        return nodes

    async def __get_raw_response(self, url, headers = {}):
        return await async_client.async_get_response(url, headers= headers)
    async def __get_json_response(self, url ,headers = {}):
        return await async_client.async_get_json(url, headers= headers)

    


