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

        def annotate(self, node):     
            ## override this and and other steps here aswell. For now we just grab literary synonyms
            self.get_literary_synonyms(node)
            
        def get_literary_synonyms(self, node):
            #synonym curies 
            synonym_curies = list(map(lambda x : x.identifier if isinstance(x, LabeledID) else x , node.synonyms))
            event_loop = asyncio.new_event_loop()
            syns = event_loop.run_until_complete(self.fetch_literary_syns(synonym_curies))
            event_loop.close()
            node.properties.update({
                'synonyms': syns
            })


        async def fetch_literary_syns(self,synonym_curies):
            tasks = []
            for curie in synonym_curies:
                tasks.append(self.get_syns_from_cache(curie))
            results = await asyncio.gather(*tasks, return_exceptions= False)
            logger.error(f'### {results} ###')
            response = []
            for r in filter(lambda x: x and len(x), results):
                desc = map(lambda x: x['desc'], r)
                for name in desc:
                    if name not in response:
                        response.append(name)   
            logger.error(f'$$$ {response} $$$')
            return response

        async def get_syns_from_cache(self, curie):
            key = f"literal_synonyms({curie})"
            logger.warn(f"[x] Get literal synonyms for {curie}")
            cached = self.rosetta.cache.get(key)
            if cached == None:
                logger.info(f" cache miss: {key}")
                response = await self.async_get_json(f'{self.onto_url}/synonyms/{curie}')
                logger.info(response)
                self.rosetta.cache.set(key, response)
                return response
            else :
                logger.info(f"cache hit: {key} ")
                return cached