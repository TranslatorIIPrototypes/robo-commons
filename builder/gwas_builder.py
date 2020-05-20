import requests
from ftplib import FTP
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json,pickle,re,os,sys
from collections import defaultdict
from robokop_genetics.genetics_normalization import GeneticsNormalizer
from greent.export_delegator import WriterDelegator
from greent.rosetta import Rosetta

logger = LoggingUtil.init_logging("robo-commons.builder.gwascatalog", logging.DEBUG, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

class GWASCatalog(Service):
    def __init__(self, rosetta):
        self.is_cached_already = False
        self.genetics_normalizer = GeneticsNormalizer()
        self.rosetta = rosetta
        self.writer = WriterDelegator(rosetta)
        self.version = '2020/05/04'
        self.sequence_variant_export_labels = None
        self.get_sequence_variant_export_labels()

    def get_sequence_variant_export_labels(self):
        """
        Gets a set of labels for seqence variant
        :return:
        """
        if not self.sequence_variant_export_labels:
            bl_url = f"https://bl-lookup-sri.renci.org/bl/{node_types.SEQUENCE_VARIANT}/ancestors?version=latest"
            with requests.session() as client:
                response = client.get(bl_url)
                if response.status_code == 200:
                    self.sequence_variant_export_labels =  set(response.json() + [node_types.SEQUENCE_VARIANT])
                else:
                    raise RuntimeError(f'Could not resolve export labels for type {node_types.SEQUENCE_VARIANT}')
        return self.sequence_variant_export_labels

    def process_gwas(self):
        # main entry point
        gwas_file = self.get_gwas_file()
        self.parse_gwas_file(gwas_catalog=gwas_file)

    def get_gwas_file(self):
        """
        Get the gwas file
        :return: Array of lines in the `gwas-catalog-associations_ontology-annotated.tsv` file
        """
        # adding a specific version instead of latest to help track things
        self.query_url = f'ftp.ebi.ac.uk/pub/databases/gwas/releases/{self.version}/' \
                         f'gwas-catalog-associations_ontology-annotated.tsv'
        ftpsite = 'ftp.ebi.ac.uk'
        ftpdir = f'/pub/databases/gwas/releases/{self.version}'
        ftpfile = 'gwas-catalog-associations_ontology-annotated.tsv'
        ftp = FTP(ftpsite)
        ftp.login()
        ftp.cwd(ftpdir)
        gwas_catalog = []
        ftp.retrlines(f'RETR {ftpfile}', gwas_catalog.append)
        ftp.quit()
        return gwas_catalog

    def parse_gwas_file(self, gwas_catalog):

        try:
            # get column headers
            file_headers = gwas_catalog[0].split('\t')
            pub_med_index = file_headers.index('PUBMEDID')
            p_value_index = file_headers.index('P-VALUE')
            snps_index = file_headers.index('SNPS')
            trait_ids_index = file_headers.index('MAPPED_TRAIT_URI')
        except (IndexError, ValueError) as e:
            logger.error(f'GWAS Catalog failed to prepopulate_cache ({e})')
            return []

        corrupted_lines = 0
        missing_variant_ids = 0
        missing_phenotype_ids = 0
        variant_to_pheno_cache = defaultdict(set)
        progress_counter = 0
        total_lines = len(gwas_catalog)
        trait_uri_pattern = re.compile(r'[^,\s]+')
        snp_pattern = re.compile(r'[^,;x*\s]+')
        for line in gwas_catalog[1:]:

            line = line.split('\t')
            try:
                # get pubmed id
                pubmed_id = line[pub_med_index]
                # get p-value
                p_value = float(line[p_value_index])
                if p_value == 0:
                    p_value = sys.float_info.min
                # get all traits (possible phenotypes)
                trait_uris = trait_uri_pattern.findall(line[trait_ids_index])
                # find all sequence variants
                snps = snp_pattern.findall(line[snps_index])
            except (IndexError, ValueError) as e:
                corrupted_lines += 1
                logger.warning(f'GWASCatalog corrupted line: {e}')
                continue

            if not (trait_uris and snps):
                corrupted_lines += 1
                logger.warning(f'GWASCatalog corrupted line: {line}')
                continue
            else:
                traits = []
                for trait_uri in trait_uris:
                    try:
                        trait_id = trait_uri.rsplit('/', 1)[1]
                        # ids show up like EFO_123, Orphanet_123, HP_123
                        if trait_id.startswith('EFO'):
                            curie_trait_id = f'EFO:{trait_id[4:]}'
                        elif trait_id.startswith('Orp'):
                            curie_trait_id = f'ORPHANET:{trait_id[9:]}'
                        elif trait_id.startswith('HP'):
                            curie_trait_id = f'HP:{trait_id[3:]}'
                        elif trait_id.startswith('NCIT'):
                            curie_trait_id = f'NCIT:{trait_id[5:]}'
                        elif trait_id.startswith('MONDO'):
                            curie_trait_id = f'MONDO:{trait_id[6:]}'
                        elif trait_id.startswith('GO'):
                            # Biological process or activity
                            # 5k+ of these
                            missing_phenotype_ids += 1
                            continue
                        else:
                            missing_phenotype_ids += 1
                            logger.warning(f'{trait_uri} not a recognized trait format')
                            continue

                        traits.append(curie_trait_id)

                    except IndexError as e:
                        logger.warning(f'trait uri index error:({trait_uri}) not splittable')

                variant_nodes = set()
                for n, snp in enumerate(snps):
                    if snp.startswith('rs'):
                        dbsnp_curie = f'DBSNP:{snp}'
                        main_curie, main_label, synonyms = self.genetics_normalizer.get_sequence_variant_normalization(
                            {dbsnp_curie}
                        )
                        variant_node = KNode(
                            main_curie,
                            name=main_label,
                            type=node_types.SEQUENCE_VARIANT
                        )
                        variant_node.add_synonyms(synonyms)
                        variant_node.add_export_labels(self.sequence_variant_export_labels)
                        variant_nodes.add(variant_node)
                    else:
                        missing_variant_ids += 1
                        pass

                if traits and variant_nodes:
                    props = {'p_value' : p_value}
                    for variant_node in variant_nodes:
                        self.writer.write_node(variant_node)
                        for trait_id in traits:
                            # variant_to_pheno_cache[variant_node].add(self.create_variant_to_phenotype_components(
                            #                                                 variant_node,
                            #                                                 trait_id,
                            #                                                 None,
                            #                                                 pubmed_id=pubmed_id,
                            #                                                 properties=props))
                            #
                            variant_to_pheno_edge, phenotype_node = self.create_variant_to_phenotype_components(
                                                                            variant_node,
                                                                            trait_id,
                                                                            None,
                                                                            pubmed_id=pubmed_id,
                                                                            properties=props)
                            self.writer.write_node(phenotype_node)
                            self.writer.write_edge(variant_to_pheno_edge)
            progress_counter += 1
            if progress_counter % 1000 == 0:
                percent_complete = (progress_counter / total_lines) * 100
                logger.info(f'GWASCatalog progress: {int(percent_complete)}%')


    def create_variant_to_phenotype_components(self, variant_node, phenotype_id, phenotype_label, pubmed_id=None, properties={}):
        phenotype_node = KNode(phenotype_id, name=phenotype_label, type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
        pubs = []
        if pubmed_id:
            pubs.append(f'PMID:{pubmed_id}')

        predicate = LabeledID(identifier=f'RO:0002200',label=f'has_phenotype')
        edge = self.create_edge(
            variant_node,
            phenotype_node,
            'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature',
            variant_node.id,
            predicate,
            url=self.query_url,
            properties=properties,
            publications=pubs)
        return (edge, phenotype_node)

    def create_phenotype_to_variant_components(self, query_url, phenotype_node, variant_id, variant_label, pubmed_id=None, properties={}):
        variant_id, variant_label, variant_synonyms = self.genetics_normalizer({variant_id})

        variant_node = KNode(variant_id, name=variant_label, type=node_types.SEQUENCE_VARIANT)
        pubs = []
        # add pubmeds to edge
        if pubmed_id:
            pubs.append(f'PMID:{pubmed_id}')
        # define predicate
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        # create edge
        edge = self.create_edge(
            phenotype_node,
            variant_node,
            'gwascatalog.disease_or_phenotypic_feature_to_sequence_variant',
            phenotype_node.id,
            predicate,
            url=query_url,
            properties=properties,
            publications=pubs)
        return edge, variant_node



if __name__=="__main__":
    rosetta = Rosetta()
    gwas_builder = GWASCatalog(rosetta)
    gwas_builder.process_gwas()
