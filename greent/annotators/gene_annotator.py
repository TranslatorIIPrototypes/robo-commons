from greent import node_types
import requests
import time
import logging
from greent.annotators.annotator import Annotator
import re

logger = logging.getLogger(__name__)
class GeneAnnotator(Annotator):

    def __init__(self, rosetta):
        super().__init__(rosetta)
        self.url = 'http://rest.genenames.org/fetch'
        self.prefix_source_mapping = {
            'HGNC': self.get_hgnc_annotations
        }
    
    def get_hgnc_annotations(self, node_curie):
        """
        Trying to mitigate for hgnc's request limit of 10 / sec 
        Cannot be async.
        """
        time.sleep(0.10001)
        identifier_parts = node_curie.split(':')
        id = identifier_parts[1]
        headers = {'Accept':'application/json'}
        #@todo maybe add retries like hgnc service.
        # and also use common config for URL and stuff, 
        url = f"{self.url}/hgnc_id/{id}"
    
        r = requests.get(url, headers = headers).json()
        try:
            docs = r['response']['docs']
        except:
            #didn't get anything useful
            logger.error("No good return")
            return {}
        annotations = {}
        logger.debug(f"Number of docs: {docs}")
        for doc in docs:
            extract = self.extract_annotation_from_hgnc(doc)
            annotations.update(extract)
        return annotations

    def extract_annotation_from_hgnc(self, raw):
        """
        Exracts certain parts of  the HGNC gene data.
        """
        keys_of_interest = [
            'gene_family',
            'gene_family_id',
            'location',
            'locus_group'
        ]
        new  = { key : raw[key] for key in keys_of_interest if key in raw }
        #sanity check
        if len(new.keys()) != len(keys_of_interest):
            logger.warning(f"found data less than expected for {raw['hgnc_id']} ")
        if new['location'] != None:
            # Cytogenetic location, I think first digit denotes Chromosome number. 
            regex = re.compile(r'\d+|\D+')
            match = regex.search(new['location'])[0]
            new['chromosome'] = match
        return new