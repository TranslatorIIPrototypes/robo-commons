##################
# This annotator will be used for all nodes regardless of thier type 
# Things like literary synonymization (collecting different names for node)
# adding names 
# or anything we'd like to apply in the general sense.
################

from greent.annotators.annotator import Annotator
from builder.question import LabeledID
import asyncio
from greent.util import Text, LoggingUtil
import logging
import traceback
from greent import node_types
import requests

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG, format='medium')

class GenericAnnotator(Annotator):
    """
    Singleton class to perform our generic annotation tasks.
    """
    instance = None

    def __init__(self, rosetta):
        if not self.instance:
            self.instance = GenericAnnotator.__GenericAnnotator(rosetta)
    def __getattr__(self, name):
        return getattr(self.instance, name)

    def annotate(self, node):
        # Overriding this method with the generic way
        self.instance.annotate(node)

    class __GenericAnnotator(Annotator):
        def __init__(self, rosetta):
            super().__init__(rosetta)
            self.onto_url = rosetta.core.onto.url
            self.concepts = rosetta.type_graph.concept_model
            # instead of querying onto blindly we can ask it for list of curies it supports
            response = requests.get(f'{self.onto_url}/curie_uri_map')
            self.supported_prefixes = list(response.json().keys())
            logger.debug(f'generic annotator active for {self.supported_prefixes}')

        def annotate(self, node):
            ## sequence variants are unique for now
            if node.type == node_types.SEQUENCE_VARIANT:
                return
            ## override this and and other steps here aswell. For now we just grab literary synonyms
            self.get_literary_synonyms(node)
            return node

        def get_literary_synonyms(self, node):
            # synonym curies
            synonym_curies = list(map(lambda x: x.identifier if isinstance(x, LabeledID) else x, node.synonyms))
            syns = self.event_loop.run_until_complete(self.fetch_literary_syns(synonym_curies))
            node.properties.update({
                'synonyms': syns
            })

        def get_good_name(self, node):
            logger.debug(f'getting good name for node: {node} ({node.name})')
            key = f"node_name({node.id})"
            cached_name = self.rosetta.cache.get(key)
            if not cached_name or cached_name == '':
                self.event_loop.run_until_complete(self.set_name(node))
            elif cached_name:
                logger.debug(f'Setting good name from cache {cached_name} -- {node.id}')
                node.name = cached_name
                return
            if node.name != '':
                # set if name is not empty
                self.rosetta.cache.set(key, node.name)
                cached_name = node.name

            logger.debug(f'got good name: {node}({node.name})')

        async def resolve_name_by_curie(self, node_curie):
            url = f'{self.onto_url}/label/{node_curie}'
            label = ''
            if self.curie_is_supported(node_curie):
                return {node_curie: ''}
            try:
                response = await self.async_get_json(url)
                label = response['label']
            except Exception as e:
                logger.error(f'Exception {e} raised calling {url}')
            return {node_curie: label}

        async def fetch_literary_syns(self, synonym_curies):
            tasks = []
            for curie in synonym_curies:
                tasks.append(self.get_syns_from_cache(curie))
            results = await asyncio.gather(*tasks, return_exceptions=False)
            response = []
            for r in filter(lambda x: x and len(x), results):
                desc = map(lambda x: x['desc'], r)
                for name in desc:
                    if name not in response:
                        response.append(name)
            return response

        async def get_syns_from_cache(self, curie):
            key = f"literal_synonyms({curie})"
            logger.debug(f"[x] Get literal synonyms for {curie}")
            if not self.curie_is_supported(curie):
                return None
            cached = self.rosetta.cache.get(key)
            if cached == None:
                logger.info(f" cache miss: {key}")
                response = await self.async_get_json(f'{self.onto_url}/synonyms/{curie}')
                logger.debug(response)
                self.rosetta.cache.set(key, response)
                return response
            else:
                logger.debug(f"cache hit: {key} ")
                return cached

        def curie_is_supported(self, node_curie):
            prefix = Text.get_curie(node_curie)
            if prefix not in self.supported_prefixes:
                logger.debug(f'prefix {prefix} not supported by {self.onto_url}')
                return False
            return True