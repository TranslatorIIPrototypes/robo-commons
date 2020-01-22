import logging
from collections import defaultdict

from greent import node_types
from greent.util import Text, LoggingUtil
from builder.question import LabeledID
import requests

logger = LoggingUtil.init_logging(__name__, level=logging.INFO, format='medium')

class Synonymizer:
    def synonymize(self, node):
        normalization_url = f'https://nodenormalization-sri.renci.org/get?key={node.id}'
        logger.debug(f'getting synonyms from {normalization_url}')
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
