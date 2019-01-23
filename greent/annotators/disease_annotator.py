from greent.annotators.annotator import Annotator
import logging

logger = logging.getLogger(name = __name__)

class DiseaseAnnotator(Annotator):

    def __init__(self, rosetta):
        super().__init__(rosetta)
        self.prefix = ['MONDO']
        self.urls = {
            'ONTO': 'https://onto.renci.org'
        }
        self.prefix_source_mapping = {
            'MONDO': self.get_mondo_properties
        }

        self.mondo_dict = {
            'MONDO:0020683': 'acute disease',
            'MONDO:0000839': 'congenital abnormality',
            'MONDO:0024236': 'degenerative disorder',
            'MONDO:0042489': 'disease susceptibility',
            'MONDO:0043543': 'iatrogenic disease',
            'MONDO:0021178': 'injury',
            'MONDO:0024297': 'nutritional or metabolic disease',
            'MONDO:0021669': 'post-infectious disorder',
            'MONDO:0002025': 'psychiatric disorder',
            'MONDO:0045028': 'radiation of chemically induced disorder',
            'MONDO:0021200': 'rare disease',
            'MONDO:0002254': 'syndromic disease',
            'MONDO:0020012': 'systemic or rheumatic disease',
            'MONDO:0021683': 'transmissible disease',
            'MONDO:0000275': 'monogenic disease',
            'MONDO:0000428': 'Y-linked disease',
            'MONDO:0000429': 'autosomal genetic disease',
            'MONDO:0000426': 'autosomal dominant disease',
            'MONDO:0006025': 'autosomal recessive disease',
            'MONDO:0000425': 'X-linked disease',
            'MONDO:0020604': 'X-linked dominant disease',
            'MONDO:0020605': 'X-linked recessive disease'
            }

    async def get_mondo_properties(self, mondo_curie):
        """
        Gets the ascestors from onto and maps them to the ones we are intereseted in.
        """
        ancestors_url = f"{self.urls['ONTO']}/ancestors/{mondo_curie}"
        response = await self.async_get_json(ancestors_url)
        ancestors = response['ancestors']
        properties = { self.mondo_dict[x] : True for x in ancestors if x in self.mondo_dict.keys()}
        
        return properties

