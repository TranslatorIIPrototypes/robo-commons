import logging
from greent.graph_components import KNode
from greent.util import  LoggingUtil
from builder.question import LabeledID
import requests
import asyncio
from greent.annotators.util.async_client import async_get_json

logger = LoggingUtil.init_logging(__name__, level=logging.INFO, format='medium')


class Synonymizer:
    NODE_NORMALIZATION_URL = 'https://nodenormalization-sri.renci.org/get_normalized_nodes'
    EDGE_NORMALIZATION_URL = 'https://edgenormalization-sri.renci.org/resolve_predicate'
    BIOLINK_VERSION = 'custom'
    CHUNK_SIZE = 1000

    # Although Edge resolution takes up multiple keys at once
    # it's probably good to send 1 at a time
    # reason being if we send 100 predicates types that it needs to resolve from leaves of uberon graph
    # that don't have direct entry in Biolink model, it would take a while to comeback with results, as it's going
    # fetch them syncronously.
    # We can parallelize it here by sending multiple request each containing one predicate to resolve.
    EDGE_CHUNK_SIZE = 1

    @staticmethod
    def synonymize(node):
        normalization_url = f'{Synonymizer.NODE_NORMALIZATION_URL}?curie={node.id}'
        response = requests.get(normalization_url)
        if response.status_code == 200:
            response = response.json()[node.id]
            main_id = LabeledID(**response['id'])
            node.id = main_id.identifier
            node.name = main_id.label
            node.add_synonyms(map(lambda synonym: LabeledID(**synonym), response['equivalent_identifiers']))
            node.add_export_labels(frozenset(response['type']))
            if node.name == '':
                for syns in response['equivalent_identifiers']:
                    # if main_id didn't have label look for the first occurance in the eq' ids.
                    if 'label' in syns:
                        node.name = syns['label']
                        break
            node.type = response['type']
        else:
            logger.error(f'failed to normalize node {node.id} on {normalization_url}')
            logger.error(f'{response.content.decode()}')

    @staticmethod
    def batch_normalize_nodes(node_curies: list):
        """
        given list of curies returns a map of curies to KNodes.
        If the node could not be normalized it would not be inside the returned result.
        :param node_curies: List of curies
        :return: {
            'curie_string': Knode()
        }
        """
        # Batch into 1000 per url
        chunk_size = Synonymizer.CHUNK_SIZE
        chunks = [node_curies[start: start + chunk_size] for start in range(0, len(node_curies), chunk_size)]

        # make urls
        urls = map(lambda chunk:
                   f"{Synonymizer.NODE_NORMALIZATION_URL}"
                   f"?{'&'.join(map(lambda curie: f'curie={curie}', chunk))}",
                   chunks
                   )
        results_array = Synonymizer.async_get_json_wrapper(urls)
        results_dict = {}
        for chunked_response in results_array:
            # Node normalization returns None with the key for some keys that were hit and missed.
            # Convert each result dict to a KNode Mixin.
            parsed = {
                curie: Synonymizer.parse_dict_to_knode(chunked_response[curie])
                for curie in chunked_response if chunked_response[curie]
            }
            results_dict.update(parsed)
        return results_dict

    @staticmethod
    def batch_normalize_edges(edge_predicates: list):
        # shorten edge predicates list if possible
        edge_predicates = list(set(edge_predicates))
        chunk_size = Synonymizer.EDGE_CHUNK_SIZE
        # If called from the buffered_writer.flush we are gauranteed at most we have buffered_writer.edge_queue_size predicates
        chunks = [edge_predicates[start: start + chunk_size] for start in range(0, len(edge_predicates), chunk_size)]
        urls = map(lambda chunk:
                   f"{Synonymizer.EDGE_NORMALIZATION_URL}"
                   f"?version={Synonymizer.BIOLINK_VERSION}&{'&'.join(map(lambda predicate: f'predicate={predicate}', chunk))}",
                   chunks
                   )
        results = Synonymizer.async_get_json_wrapper(urls)
        response = {}
        for chunked_response in results:
            parsed = {
                predicate: Synonymizer.parse_dict_to_kedge(chunked_response[predicate])
                for predicate in chunked_response
            }
            response.update(parsed)
        return response

    @staticmethod
    def parse_dict_to_knode(nn_dict: dict) -> KNode:
        node = KNode(
            id=nn_dict.get('id', {}).get('identifier', ''),
            name=nn_dict.get('id', {}).get('label', ''),
            type=nn_dict.get('type', ['named_thing'])[0],
        )
        node.add_synonyms(set(map(lambda x: LabeledID(**x), nn_dict.get('equivalent_identifiers', []))))
        node.add_export_labels(nn_dict.get('type', ['named_thing']))
        return node

    @staticmethod
    def parse_dict_to_kedge(en_dict: dict) -> LabeledID:
        return LabeledID(**en_dict)

    @staticmethod
    def async_get_json_wrapper(urls):
        # make async req,
        event_loop = asyncio.get_event_loop()
        tasks = []
        for url in urls:
            tasks.append(async_get_json(url))
        results_array = event_loop.run_until_complete(
            asyncio.gather(*tasks)
        )
        return results_array
