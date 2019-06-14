from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode
from greent.export import BufferedWriter
from greent.util import LoggingUtil
from greent.util import Text
from greent.services.gtex import GTExUtils

from builder.question import LabeledID

import csv
import pickle

# declare a logger and initialize it.
import logging
logger = LoggingUtil.init_logging(__name__, logging.INFO, format='medium')


##############
# Class: GTExBuilder
# By: Phil Owen
# Date: 5/21/2019
# Desc: Class that pre-loads significant GTEx data elements into the redis cache and neo4j graph database.
##############
class GTExBuilder:
    #######
    # Constructor
    #######
    def __init__(self, rosetta: Rosetta):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.ensembl = rosetta.core.ensembl

        # create static edge labels for variant/gtex and gene/gtex edges
        self.variant_gtex_label = LabeledID(identifier=f'GTEx:affects_expression_in', label=f'affects expression in')
        self.gene_gtex_label = LabeledID(identifier=f'gene_to_expression_site_association', label=f'gene to expression site association')

        # get a ref to the util class
        self.gtu = GTExUtils(self.rosetta)

    #######
    # create_gtex_graph - Parses the CSV file(s) and inserts the data into the graph DB
    #
    # Ex. The row header, and an example row of data:
    # tissue_name,          tissue_uberon,  hgvs,                       variant_id,         gene_id,            tss_distance,   ma_samples,
    # Adipose Subcutaneous, 0002190,        NC_000001.10:g.753865G>C,   1_753865_G_C_b37,   ENSG00000237683.5,  614486,         12,
    # (cont.)
    # ma_count, maf,        pval_nominal,   slope,    slope_se,   pval_nominal_threshold, min_pval_nominal,   pval_beta
    # 12,       0.0159151,  4.94712e-05,    0.914962, 0.222374,   0.000132768,            4.94712e-05,        0.0448675
    #######
    def create_gtex_graph(self, data_directory: str, file_names: list, analysis_id: str = None):
        # for each file to parse
        for file_name in file_names:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            logger.info(f'Creating GTEx graph data elements in file: {full_file_path}')

            # open a pipe to the redis cache DB
            with BufferedWriter(self.rosetta) as graph_writer, self.cache.redis.pipeline() as redis_pipe:
                # create arrays to bucket the unchached variants and thier annotations
                uncached_variants = []
                uncached_variant_annotation_nodes = []

                # init a progress counters
                line_counter = 0
                redis_counter = 0

                try:
                    # loop through the variants
                    # open the file and start reading
                    with open(full_file_path, 'r') as inFH:
                        # open up a csv reader
                        csv_reader = csv.reader(inFH)

                        # read the header
                        header_line = next(csv_reader)

                        # index into the array to the variant id position
                        tissue_name_index = header_line.index('tissue_name')
                        tissue_uberon_index = header_line.index('tissue_uberon')
                        hgvs_index = header_line.index('HGVS')
                        variant_id_index = header_line.index('variant_id')
                        ensembl_id_index = header_line.index('gene_id')
                        pval_nominal_index = header_line.index('pval_nominal')
                        pval_slope_index = header_line.index('slope')

                        # for the rest of the lines in the file
                        for line in csv_reader:
                            # increment the counter
                            line_counter += 1

                            # get the data elements
                            tissue_name = line[tissue_name_index]
                            uberon = line[tissue_uberon_index]
                            hgvs = line[hgvs_index]
                            variant_id = line[variant_id_index]
                            ensembl = line[ensembl_id_index].split(".", 1)[0]
                            pval_nominal = line[pval_nominal_index]
                            slope = line[pval_slope_index]

                            # create curies for the various id values
                            curie_hgvs = f'HGVS:{hgvs}'
                            curie_uberon = f'UBERON:{uberon}'
                            curie_ensembl = f'ENSEMBL:{ensembl}'

                            # create variant, gene and GTEx nodes with the HGVS, ENSEMBL or UBERON expression as the id and name
                            variant_node = KNode(curie_hgvs, name=curie_hgvs, type=node_types.SEQUENCE_VARIANT)
                            gene_node = KNode(curie_ensembl, type=node_types.GENE)
                            gtex_node = KNode(curie_uberon, name=tissue_name, type=node_types.ANATOMICAL_ENTITY)

                            # call to load the each node with synonyms
                            self.rosetta.synonymizer.synonymize(variant_node)
                            self.rosetta.synonymizer.synonymize(gene_node)
                            self.rosetta.synonymizer.synonymize(gtex_node)

                            # get the SequenceVariant object filled in with the sequence location data
                            seq_var_data = self.gtu.get_sequence_variant_obj(variant_id)

                            # add properties to the variant node
                            variant_node.properties['sequence_location'] = [seq_var_data.build, str(seq_var_data.chrom), str(seq_var_data.pos)]

                            # for now insure that the gene node has a name after synonymization
                            # this can happen if gene is not currently in the graph DB
                            if gene_node.name is None:
                                gene_node.name = curie_ensembl

                            # get the polarity of slope to get the direction of expression.
                            # positive value increases expression, negative decreases
                            label_id, label_name = self.gtu.get_expression_direction(slope)

                            # create the edge label predicate for the gene/variant relationship
                            predicate = LabeledID(identifier=label_id, label=label_name)

                            # get a MD5 hash int of the composite hyper edge ID
                            hyper_egde_id = self.gtu.get_hyper_edge_id(uberon, ensembl, Text.un_curie(variant_node.id))

                            # set the properties for the edge
                            edge_properties = [ensembl, pval_nominal, slope, analysis_id]

                            ##########################
                            # data details are ready. write all edges and nodes to the graph DB.
                            ##########################

                            # write out the sequence variant node
                            graph_writer.write_node(variant_node)

                            # write out the gene node
                            graph_writer.write_node(gene_node)

                            # write out the anatomical gtex node
                            graph_writer.write_node(gtex_node)

                            # associate the sequence variant node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, variant_node, gtex_node, self.variant_gtex_label, hyper_egde_id, None, True)

                            # associate the gene node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, gene_node, gtex_node, self.gene_gtex_label, hyper_egde_id, None)

                            # associate the sequence variant node with an edge to the gene node. also include the GTEx properties
                            self.gtu.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_egde_id, edge_properties, True)

                            ##########################
                            # put the data into the redis cache (if not already)
                            ##########################

                            # look up the variant by the HGVS expresson
                            if self.cache.get(f'synonymize(HGVS:{hgvs})') is None:
                                uncached_variants.append(hgvs)

                            # if there is enough in the batch process them and empty the array
                            if len(uncached_variants) == 10000:
                                self.gtu.process_variant_synonymization_cache(uncached_variants)
                                uncached_variants = []

                            # check if myvariant key exists in cache, otherwise add it to buffer for batch processing
                            if self.cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                                uncached_variant_annotation_nodes.append(variant_node)

                            # if there is enough in the variant annotation batch process them and empty the array
                            if len(uncached_variant_annotation_nodes) == 10000:
                                self.gtu.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)
                                uncached_variant_annotation_nodes = []

                            # ensembl cant handle batches, and for now NEEDS to be pre-cached individually here
                            # (the properties on the nodes needed by ensembl wont be available to the runner)
                            nearby_cache_key = f'ensembl.sequence_variant_to_gene({variant_node.id})'

                            # grab the nearby genes
                            cached_nearby_genes = self.cache.get(nearby_cache_key)

                            # were there any nearby genes not cached
                            if cached_nearby_genes is None:
                                # get the data for the nearby genes
                                nearby_genes = self.ensembl.sequence_variant_to_gene(variant_node)

                                # add the key and data to the list to execute
                                redis_pipe.set(nearby_cache_key, pickle.dumps(nearby_genes))

                                # increment the counter
                                redis_counter += 1

                            # do we have enough to process
                            if redis_counter > 500:
                                # execute the redis load
                                redis_pipe.execute()

                                # reset the counter
                                redis_counter = 0

                            # output some feedback for the user
                            if (line_counter % 100000) == 0:
                                logger.info(f'Processed {line_counter} variants.')

                        #  if there are remainder variants entries left to process
                        if uncached_variants:
                            self.gtu.process_variant_synonymization_cache(uncached_variants)

                        # if there are remainder ensemble entries left to process
                        if redis_counter > 0:
                            redis_pipe.execute()

                        # if there are remainder variant node entries left to process
                        if uncached_variant_annotation_nodes:
                            self.gtu.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)

                except Exception as e:
                    logger.error(f'Exception caught trying to process variant: {curie_hgvs}-{curie_uberon}-{curie_ensembl}. Exception: {e}')
                    logger.error('Continuing...')

                # output some final feedback for the user
                logger.info(f'Building complete. Processed {line_counter} variants.')
        return 0

#######
# Main - Stand alone entry point
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # directory with GTEx data to process
    gtex_data_directory = 'C:/Phil/Work/Informatics/GTEx/GTEx_data/'
    # gtex_data_directory = '/projects/stars/var/GTEx/stage/smartBag/example/GTEx/bag/data/'

    # assign the name of the GTEx data file
    # available test files:
    # 'signif_variant_gene_pairs',
    # 'test_signif_Adipose_Subcutaneous_all', 'test_signif_Adipose_Subcutaneous_100k', 'test_signif_Adipose_Subcutaneous_10k', 'test_signif_Adipose_Subcutaneous_100', 'test_signif_Adipose_Subcutaneous_6'
    # 'test_signif_Stomach_all', 'test_signif_Stomach_100k', 'test_signif_Stomach_10k', 'test_signif_Stomach_100', 'test_signif_Stomach_6'
    # 'hypertest_1-var_2-genes_1-tissue', 'hypertest_1-var_2-tissues_1-gene'
    associated_file_names = ['test_signif_Stomach_10k.csv']

    # call the GTEx builder to load the cache and graph database
    gtb.create_gtex_graph(gtex_data_directory, associated_file_names, 'GTEx')
