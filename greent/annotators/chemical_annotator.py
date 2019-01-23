import requests 
import logging
from greent.annotators.annotator import Annotator

logger = logging.getLogger(__name__)

class ChemicalAnnotator(Annotator):
    def __init__(self, rosetta):
        super().__init__(rosetta)
        self.urls = {
            'CHEMBL': 'https://www.ebi.ac.uk/chembl/api/data/molecule/',
            'ONTO': 'https://onto.renci.org/all_properties/',
            'KEGG': 'http://rest.kegg.jp/get/',
            'PUBCHEM': 'https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/'
        }
        self.prefix_source_mapping = {
            'CHEMBL': self.get_chembl_data, 
            'CHEBI' : self.get_chebi_data,
            'KEGG' : self.get_kegg_data
        }
        

    async def get_chembl_data(self, chembl_id):
        """
        Fetches chembl data from ebi.ac.uk
        """
        key = 'CHEMBL'
        
        id_parts = chembl_id.split(':')
        suffix = id_parts[-1]
        url_part = f'{suffix}.json'
        response_json = await self.async_get_json(self.urls[key] + url_part)
        return self.extract_chembl_data(response_json)
        
    def extract_chembl_data(self, chembl_raw):
        """
        Extracts interesting data from chembl raw response.
        """
        keys_of_interest = [
            'molecule_properties', # same as physical properites ?? 
            'molecule_type',
            'natural_product',
            'oral',
            'parenteral',
            'topical',
            'prodrug',
            'therapeutic_flag',
            'withdrawn_flag'
        ]
        extracted = {key : str(chembl_raw[key]) for key in keys_of_interest if key in chembl_raw.keys()}
        
        if len(keys_of_interest) != len(extracted.keys()):
            logger.warn(f"All keys were not annotated for {chembl_raw['molecule_chembl_id']}")
        
        return extracted

    async def get_chebi_data(self, chebi_id):
        """
        Gets cebi data from onto.renci.org 
        """
        url = self.urls['ONTO'] + chebi_id
        chebi_raw = await self.async_get_json(url)
        return self.extract_chebi_data(chebi_raw)

    def extract_chebi_data(self, chebi_raw):
        """
        restructures chebi raw data
        """
        extract = {}
        for prop in chebi_raw['all_properties']['property_value']:
            prop_parts = prop.split(' ')
            prop_name = prop_parts[0].split('/')[-1]
            prop_value = prop_parts[1].strip('"')
            extract[prop_name] = prop_value
        return extract
          
    async def get_kegg_data(self, kegg_id):
        kegg_id_parts = kegg_id.split(':')  #KEGG.COMPOUND:C14850
        kegg_c_id = kegg_id_parts[-1]
        url = self.urls['KEGG'] + kegg_c_id 
        response = await self.async_get_text(url)
        kegg_dict = self.parse_flat_file_to_dict(response)
        return self.extract_kegg_data(kegg_dict)
    
    def extract_kegg_data(self, kegg_dict):
        keys_of_interest = [
            'NAME',
            'FORMULA',
            'EXACT_MASS',
            'MOL_WEIGHT'
        ]
        extracted = {key : kegg_dict[key] for key in keys_of_interest if key in kegg_dict.keys()}
        if len(keys_of_interest) != len(extracted.keys()):
            logger.warn(f"All keys were not annotated for {kegg_dict['ENTRY']}")
        return extracted

    def parse_flat_file_to_dict(self, raw):
        new_dict = {}
        lines = raw.split('\n')
        current_key = ''
        for line in lines:
            if line == '///':
                break # last line break
            if line and len(line) > 0 and line.startswith(' ') :
                line.strip()
                new_dict[current_key].append(line)
            else:
                words = line.split(' ')
                current_key = words[0].strip(' ')
                new_dict[current_key] = [' '.join(words[1:]).strip()]
        return new_dict

