from greent.util import Text, LoggingUtil
import logging
import aiohttp
import asyncio
import greent.annotators.util.async_client as async_client
logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG, format='medium')

class Annotator:
    """
    Super class for all annotators. Defines common methods that should be implemented among the annotators.
    """
    def __init__ (self, rosetta):
        self.rosetta = rosetta
        """
        variable as a that would be used as a mapper between the curie prefix and
        a function that will get annotation data in child class implementions. 
        """
        self.prefix_source_mapping = {}

    def annotate(self, node, synonyms={}):
        """
        Makes an event loop and fires off the annotator to get data concurrently, blocks 
        until all results are done.
        """
        logger.debug(f"Annotating {node.id}")
        synonym_basket = {prefix : node.get_synonyms_by_prefix(prefix) for prefix in self.prefix_source_mapping.keys()}
        synonym_basket.update(synonyms)
        loop = asyncio.new_event_loop()
        logger.debug(f'Got the event loop')
        loop.set_debug(True) # 
        node.properties = loop.run_until_complete(self.merge_property_data(synonym_basket))
        loop.close()
        logger.debug(f"Updated node {node} : added {len(node.properties.keys())} properties")
        return node


    async def merge_property_data(self, synonym_basket):
        """
        Creates tasks that each will get part of the node property based on node id and synonyms.
        """
        properties = {}
        tasks = []
        for prefix in synonym_basket:
            for synonym in synonym_basket[prefix]:
                tasks.append(self.get_from_cache(synonym))
        results = await asyncio.gather(*tasks,return_exceptions= True)  
        for result in results:
            properties.update(result)     
        return properties


    async def get_from_cache(self, node_curie):
        """
        Trys to get from redis or else it will make necessary call to fetch data and 
        add it to cache.
        Calling the cache is still a blocking call.
        """
        key = f"annotation({Text.upper_curie(node_curie)})"
        logger.info(f"Getting attribute: {key}")
        # also here it might be helpful to make it async
        cached_data = None #self.rosetta.cache.get(key)
        if cached_data == None:

            logger.info(f"cache miss: {key}")
    
            annotation_data = await self.get_curie_annotation(node_curie)
            if annotation_data != {}:
                self.insert_to_cache(node_curie, annotation_data)
            return annotation_data            
        else:

            logger.info(f"cache hit: {key} - found")

            return cached_data

    def insert_to_cache(self, node_curie, annotation):
        """
        inserts into redis cache, this might be a blocker of the event loop 
        might consider adding aioredis for sending data to redis async also.
        """
        key = f"annotation({Text.upper_curie(node_curie)})"
        logger.info(f'inserting into cache {key}')
        self.rosetta.cache.set(key, annotation)
        return annotation

    async def get_curie_annotation(self, node_curie):
        """
        Gets a single annotation based on a curie. Sources will differ on each annotators implemntation.
        """
        prefix = node_curie.split(':')[0]
        logger.info(f"going to fetch {node_curie}")
        annotation_fetcher_function = self.prefix_source_mapping.get(prefix)

        if annotation_fetcher_function == None :
            logger.info(f'No annotators for {prefix}')
            return {}
        result = annotation_fetcher_function(node_curie)
        if asyncio.iscoroutine(result):
            # if its an async function schedule it 
            # else no choice... wait for it.
            logger.info(f'found coroutine for {prefix}')
            return await result
        
        return result

    async def async_get_json(self, url):
        await async_client.async_get_json(url)
    
    async def async_get_text(self, url):
        await async_client.async_get_text(url)