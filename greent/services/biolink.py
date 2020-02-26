import requests
import urllib
from greent.service import Service
from greent.ontologies.mondo2 import Mondo2
from greent.ontologies.go2 import GO2
from greent.util import Text
from greent.graph_components import KNode, LabeledID
from greent import node_types
from builder.question import LabeledID
from datetime import datetime as dt
import logging
import time

#TODO: include pagination

class Biolink(Service):
    """ Preliminary interface to Biolink. Will move to automated Translator Registry invocation over time. """

    def __init__(self, context):
        super(Biolink, self).__init__("biolink", context)
        self.checker = context.core.mondo
        self.go = context.core.go
        self.label2id = {'colocalizes_with': 'RO:0002325', 'contributes_to': 'RO:0002326'}


    def page_calls(self,url):
        rows = 100
        start = 0
        allassociations = []
        startchar = '?'
        if '?' in url:
            startchar = '&'
        while True:
            page_url = url+f'{startchar}rows={rows}&start={start}'
            response = self.query(page_url)
            if response is None:
                break
            if 'associations' not in response:
                break
            if len(response['associations']) == 0:
                break
            allassociations += response['associations']
            if len(response['associations']) < rows:
                #got back a partial page, must be the end
                break
            start += rows
        return allassociations


    #TODO: share the retry logic in Service?
    def query(self,url):
        """The biolink functions mostly work nicely - if the identifier is unresolvable, they return
        a valid json with no results.   However, gene/{id}/function throws a 500 (yuck).  So if there's a 500 from
        any endpoint, don't try again."""
        done = False
        num_tries = 0
        max_tries = 10
        wait_time = 5 # seconds
        while num_tries < max_tries:
            try:
                r = requests.get(url)
                if r.status_code == 500 or r.status_code == 404:
                    return None
                #Anything else, it's either good or we want to retry on exception.
                return r.json()
            except Exception as e:
                num_tries += 1
                time.sleep(wait_time)
        return None
 
        
    def process_associations(self, associations, relationship_id, function, target_node_type, input_identifier, url, input_node, reverse=False):
        """Given a response from biolink, create our edge and node structures.
        Sometimes (as in pathway->Genes) biolink returns the query as the object, rather
        than the subject.  reverse=True will handle this case, bringing back the subject
        of the response, rather than the object.  Fortunately, it looks like this is just per-function.
        We could instead try to see if the subject id matched our input id, etc... if the same
        function sometimes spun things around."""
        edge_nodes = []
        for association in associations:
            # We would like to include edges that are direct links, if we have entity A we've queried for we also get other subjects that have (New_subject)-is_a-> A and relations returned for those,
            # so we end up having direct relations of subclasses  being pushed up to parent classes, so check to see if subject is actually the one we asked for
            # if association['subject']['id'] != input_node.id:
                # continue
            pubs = []
            if 'publications' in association and association['publications'] is not None:
                for pub in association['publications']:
                    # Sometimes, we get back something like "uniprotkb" instead of a PMID.  We don't want it.
                    pubid_prefix = pub['id'][:4].upper()
                    if pubid_prefix == 'PMID':
                        # Sometimes, there is something like: 'id': 'PMID:9557891PMID:9557891' !?
                        # Oh, and even better, sometimes there is this: 'id': 'PMID:12687501:PMID:17918734'
                        # I will refrain from cursing in code.
                        ids = pub['id'].split('PMID:')
                        for n in ids[1:]:
                            while n.endswith(':'):
                                n = n[:-1]
                            pubs.append(f'PMID:{n}')
            inverse = False 
            if 'relation' in association:
                inverse = association['relation'].get('inverse', False)
            if reverse or inverse:
                source_node = KNode(association['object']['id'], type=target_node_type, name=association['object']['label'])
                target_node = input_node
                newnode = source_node
            else:
                target_node = KNode(association['object']['id'], type=target_node_type, name=association['object']['label'])
                source_node = input_node
                newnode = target_node
            #Deal with biolink's occasional propensity to return Null relations
            # This basically happens only with the gene_get_function call, so if that gets fixed, we might be
            # able to make this a little nicer
            predicate_id = association['relation']['id']
            if (predicate_id is None):
                predicate_id = relationship_id
            elif (':' not in predicate_id):
                if predicate_id in self.label2id:
                    predicate_id = self.label2id[predicate_id]
                else:
                    logging.getLogger('application').error(f'Relationship Missing: {predicate_id}')
                    predicate_id = relationship_id
            predicate_label= association['relation']['label']
            if predicate_label is None:
                predicate_label = relationship_id
            #now back to the show
            predicate = LabeledID(identifier=predicate_id, label=predicate_label)
            try:
                edge = self.create_edge(source_node, target_node, f'biolink.{function}',  input_identifier, predicate,  publications = pubs, url = url)
            except Exception as e:
                print(e)
                print(association['publications'])
                print( pubs)
                raise e
            edge_nodes.append((edge, newnode))
        return edge_nodes


    def gene_get_disease(self, gene_node):
        """Given a gene specified as a curie, return associated diseases."""
        #Biolink is pretty forgiving on gene inputs, and our genes should have HGNC as their identifiers nearly always
        ehgnc = urllib.parse.quote_plus(gene_node.id)
        logging.getLogger('application').debug('          biolink: %s/bioentity/gene/%s/diseases' % (self.url, ehgnc))
        urlcall = '%s/bioentity/gene/%s/diseases' % (self.url, ehgnc)
        r = self.page_calls(urlcall)
        #r = requests.get(urlcall).json()
        return self.process_associations(r, 'gene_get_disease', node_types.DISEASE, ehgnc, urlcall, gene_node)

    def disease_get_phenotype(self, disease):
        #Biolink should understand any of our disease inputs here.
        url = "{0}/bioentity/disease/{1}/phenotypes/".format(self.url, disease.id)
        response = self.page_calls(url)
        #response = requests.get(url).json()
        return self.process_associations(response, 'disease_get_phenotype', node_types.PHENOTYPIC_FEATURE, disease.id, url, disease)

    def phenotype_get_disease(self,phenotype):
        url = "{0}/bioentity/phenotype/{1}/diseases/".format(self.url, phenotype.id)
        response = self.page_calls(url)
        #response = requests.get(url).json()
        return self.process_associations(response, 'phenotype_get_disease', node_types.DISEASE, phenotype.id, url, phenotype, reverse= True)


    def gene_get_go(self, gene):
        # This biolink function should be able to take an HGNC or other gene id, and convert to UniProtKB in the
        # backend.  This somewhat works, but it's not 100%.  For instance, there is a test in test_biolink model
        # that tries to look up HGNC for KIT, and it returns an empty result.
        # And furthermore, there are often many UniProt ids for a gene.  Many of them will return a 500 (unrecognized)
        # for the function.  So: we need to send UniProt (until we can be sure that the mappings are solid)
        # and we need to send them all.
        # But if, for some reason we don't have any UNIPROTKB, we might as well give the gene ID a shot.
        uniprot_ids = gene.get_synonyms_by_prefix('UNIPROTKB')
        if len(uniprot_ids) == 0:
            gene_id = gene.id
            url = "{0}/bioentity/gene/{1}/function/".format(self.url, gene_id)
            response = self.page_calls(url)
            return response,url,gene_id
        else:
            for uniprot_id in uniprot_ids:
                gene_id = 'UniProtKB:{0}'.format(Text.un_curie(uniprot_id))
                url = "{0}/bioentity/gene/{1}/function/".format(self.url, gene_id)
                response = self.page_calls(url)
                if response is not None and len(response) > 0:
                    return response,url,gene_id
            return None,None,None

    def gene_get_process_or_function(self,gene):
        response,url,input_id = self.gene_get_go(gene)
        if response is None:
            return []
        # default relationship Gene - [involved in (RO:0002331)] -> pathway
        edges_nodes = self.process_associations(response, 'RO:0002331', 'gene_get_process_or_function', node_types.BIOLOGICAL_PROCESS_OR_ACTIVITY, input_id, url,gene)
        process_or_function_results = list(filter(lambda x: self.go.is_biological_process(x[1]) or
                                                  self.go.is_molecular_function(x[1]), edges_nodes))
        return process_or_function_results

    def gene_get_pathways(self, gene):
        url = "{0}/bioentity/gene/{1}/pathways/".format(self.url, gene.id)
        #response = requests.get(url).json()
        response = self.page_calls(url)
        # default relationship Gene - [involved in (RO:0002331)] -> pathway
        return self.process_associations(response, 'RO:0002331', 'gene_get_pathways', node_types.PATHWAY, gene.id, url,gene)

    def pathway_get_gene(self, pathway):
        url = "{0}/bioentity/pathway/{1}/genes/".format(self.url, pathway.id)
        #response = requests.get(url).json()
        response = self.page_calls(url)
        # default relationship Gene - [involved in (RO:0002331)] -> pathway
        return self.process_associations(response, 'RO:0002331', 'pathway_get_genes', node_types.GENE, url, pathway.id, pathway, reverse=True)

    def sequence_variant_get_phenotype(self, variant_node):
        results = []
        clinvarsyns = variant_node.get_synonyms_by_prefix('CLINVARVARIANT')
        for clinvarsyn in clinvarsyns:
            clinvar_url_curie = f'ClinVarVariant:{Text.un_curie(clinvarsyn)}'
            url = f'{self.url}/bioentity/variant/{clinvar_url_curie}/phenotypes/'
            response = self.page_calls(url)
            # 2/26/2020 seems like we don't have any thing coming back here on rkg. but letting the process_associa..
            # logic handle the predicate type from service.
            results.extend(self.process_associations(response, '', 'sequence_variant_get_phenotype', node_types.DISEASE_OR_PHENOTYPIC_FEATURE, clinvarsyn, url, variant_node))
        return results
        
    def disease_get_gene(self, disease):
        url = "{0}/bioentity/disease/{1}/genes/".format(self.url, disease.id)
        response = self.page_calls(url)
        # not defaulting here to any predicate type, let the service call logic do it.
        # @TODO check if any predicates where being missed.
        return self.process_associations(response, '', 'disease_get_gene', node_types.GENE, disease.id, url, disease)

    def gene_get_phenotype(self, gene):
        url = f"{self.url}/bioentity/gene/{gene.id}/phenotypes/"
        response = self.page_calls(url)
        # not defaulting here to any predicate type, let the service call logic do it.
        return self.process_associations(response, '', 'gene_get_phenotype', node_types.PHENOTYPIC_FEATURE, gene.id, url, gene)

    def phenotype_get_gene(self, phenotype):
        url = f"{self.url}/bioentity/phenotype/{phenotype.id}/genes/"
        response = self.page_calls(url)
        return self.process_associations(response, '', 'phenotype_get_gene', node_types.GENE, phenotype.id, url, phenotype)

