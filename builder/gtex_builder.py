from greent.rosetta import Rosetta
from greent import node_types
from greent.graph_components import KNode
from greent.export_delegator import WriterDelegator
from greent.util import LoggingUtil
from greent.util import Text
from builder.gtex_utils import GTExUtils
from builder.question import LabeledID
import csv
import os

# declare a logger and initialize it.
import logging
logger = LoggingUtil.init_logging("robo-commons.builder.GTExBuilder", logging.INFO, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')


##############
# Class: GTExBuilder
#
# By: Phil Owen
# Date: 5/21/2019
# Desc: Class that pre-loads significant GTEx data elements into a neo4j graph database.
##############
class GTExBuilder:
    #######
    # Constructor
    # param rosetta : Rosetta - project object for shared objects
    #######
    def __init__(self, rosetta: Rosetta):
        self.rosetta = rosetta
        self.written_anatomical_entities = set()
        self.written_genes = set()
        self.max_nodes = 100_000

        # create static labels for the edge predicates
        self.variant_anatomy_predicate = LabeledID(identifier=f'biolink:affects_expression_of', label=f'affects_expression_in')
        self.gene_anatomy_predicate = LabeledID(identifier=f'biolink:gene_to_expression_site_association', label=f'gene_to_expression_site_association')
        self.variant_gene_sqtl_predicate = LabeledID(identifier=f'biolink:affects_splicing_of', label=f'affects_splicing_of')
        self.increases_expression_predicate = LabeledID(identifier='biolink:increases_expression_of', label='increases_expression_of')
        self.decreases_expression_predicate = LabeledID(identifier='biolink:decreases_expression_of', label='decreases_expression_of')

        # get a ref to the util class
        self.gtu = GTExUtils(self.rosetta)

    #####################
    # load - Processes the gtex data and loads the redis cache and graph databases with it
    #
    # param data_directory: str - the name of the directory that GTEx files will be processed in
    # param out_file_name: str - the name of the target file
    # param process_raw_data: bool - flag to gather and process raw GTEx data files
    # param process_for_graph: bool - flag to process the GTEx file and load the neo4j graph with it
    # param is_sqtl: bool - flag to indicate if we're talking about sqtl or eqtl, default to eqtl
    # param gtex_version: int - the version of gtex data to load
    # returns: object, pass if it is None, otherwise an exception object to indicate what failed
    #####################
    def load(self, data_directory: str, out_file_name: str = None, process_raw_data: bool = True, process_for_graph: bool = True, is_sqtl: bool = False, gtex_version: int = 8) -> object:
        # init the return value
        ret_val = None

        #set default output file names if not provided
        if not out_file_name:
            if is_sqtl:
                out_file_name = 'sqtl_signif_pairs.csv'
            else:
                out_file_name = 'eqtl_signif_pairs.csv'


        # does the output directory exist
        if not os.path.isdir(data_directory):
            ret_val = Exception("Working directory does not exist. Aborting.")
        else:
            # ensure the working directory ends with a '/' in order to properly append a data file name
            if data_directory[-1] != '/':
                data_directory = f'{data_directory}/'

            # process the GTEx tissue files if requested
            if process_raw_data is True:
                ret_val: object = self.gtu.process_gtex_files(data_directory, out_file_name, gtex_version=gtex_version, is_sqtl=is_sqtl)
            else:
                logger.info("Raw GTEx data processing not selected.")

            # does the processed file exist
            if os.path.isfile(f'{data_directory}{out_file_name}'):
                # was the raw GTEx data processed
                if ret_val is None:
                    if process_for_graph is True:
                        # call the GTEx builder to load the cache and graph database
                        ret_val: object = self.create_gtex_graph(data_directory, out_file_name, f'GTEx.v{gtex_version}', is_sqtl=is_sqtl)
                    else:
                        logger.info("Graph node/edge processing not selected.")
                else:
                    ret_val = Exception('Error detected in GTEx file creation. Aborting.', ret_val)
            else:
                ret_val = Exception("Error detected no processed GTEx file found. Aborting.", ret_val)

        # return to the caller
        return ret_val

    # a wrapper function to load sqtl instead of eqtl
    def load_sqtl(self,
                  data_directory: str,
                  out_file_name: str = 'sqtl_signif_pairs.csv',
                  process_raw_data: bool = True,
                  process_for_graph: bool = True,
                  gtex_version: int = 8):
        return self.load(data_directory,
                         out_file_name,
                         process_raw_data=process_raw_data,
                         process_for_graph=process_for_graph,
                         is_sqtl=True,
                         gtex_version=gtex_version)

    #######
    # create_gtex_graph - Parses the CSV file(s) and inserts the data into the graph DB
    #
    # param data_directory: str - the name of the directory the file is in
    # param associated_file_names: list - list of file names to process
    # param namespace: str - the name of the data source
    # returns: object, pass if it is none, otherwise an exception object
    #######
    def create_gtex_graph(self, data_directory: str, file_name: str, namespace: str, is_sqtl: bool=False) -> object:
        # init the return value
        ret_val: object = None

        # init a progress counter
        line_counter = 0

        try:
            # get the full path to the input file
            full_file_path = f'{data_directory}{file_name}'

            logger.info(f'Creating GTEx graph data elements from file: {full_file_path}')

            # walk through the gtex data file and create/write nodes and edges to the graph
            with WriterDelegator(self.rosetta) as graph_writer:
                # init these outside of try catch block
                curie_hgvs = None
                curie_uberon = None
                curie_ensembl = None

                # open the file and start reading
                with open(full_file_path, 'r') as inFH:
                    # open up a csv reader
                    csv_reader = csv.reader(inFH)

                    # read the header
                    header_line = next(csv_reader)

                    # find relevant indices
                    tissue_name_index = header_line.index('tissue_name')
                    tissue_uberon_index = header_line.index('tissue_uberon')
                    hgvs_index = header_line.index('HGVS')
                    ensembl_id_index = header_line.index('gene_id')
                    pval_nominal_index = header_line.index('pval_nominal')
                    pval_slope_index = header_line.index('slope')

                    try:
                        # for the rest of the lines in the file
                        for line in csv_reader:
                            # increment the counter
                            line_counter += 1

                            # get the data elements
                            tissue_name = line[tissue_name_index]
                            uberon = line[tissue_uberon_index]
                            hgvs = line[hgvs_index]
                            ensembl = line[ensembl_id_index].split(".", 1)[0]
                            pval_nominal = line[pval_nominal_index]
                            slope = line[pval_slope_index]

                            # create curies for the various id values
                            curie_hgvs = f'HGVS:{hgvs}'
                            curie_uberon = f'UBERON:{uberon}'
                            curie_ensembl = f'ENSEMBL:{ensembl}'
                            # create variant, gene and GTEx nodes with the HGVS, ENSEMBL or UBERON expression as the id and name
                            variant_node = KNode(curie_hgvs, name=hgvs, type=node_types.SEQUENCE_VARIANT)
                            variant_node.add_export_labels([node_types.SEQUENCE_VARIANT])
                            gene_node = KNode(curie_ensembl, name=ensembl, type=node_types.GENE)
                            gene_node.add_export_labels([node_types.GENE])
                            gtex_node = KNode(curie_uberon, name=tissue_name, type=node_types.ANATOMICAL_ENTITY)

                            if is_sqtl:
                                # sqtl variant to gene always uses the same predicate
                                predicate = self.variant_gene_sqtl_predicate
                            else:
                                # for eqtl use the polarity of slope to get the direction of expression.
                                # positive value increases expression, negative decreases
                                try:
                                    if float(slope) > 0.0:
                                        predicate = self.increases_expression_predicate
                                    else:
                                        predicate = self.decreases_expression_predicate
                                except ValueError as e:
                                    logger.error(f"Error casting slope to a float on line {line_counter} (slope - {slope}) {e}")
                                    continue

                            # get a MD5 hash int of the composite hyper edge ID
                            hyper_edge_id = self.gtu.get_hyper_edge_id(uberon, ensembl, hgvs)

                            # set the properties for the edge
                            edge_properties = [ensembl, pval_nominal, slope, namespace]

                            ##########################
                            # data details are ready. write all edges and nodes to the graph DB.
                            ##########################

                            # write out the sequence variant node
                            graph_writer.write_node(variant_node)

                            # write out the gene node
                            if gene_node.id not in self.written_genes:
                                graph_writer.write_node(gene_node)
                                self.written_genes.add(gene_node.id)

                            # write out the anatomical gtex node
                            if gtex_node.id not in self.written_anatomical_entities:
                                graph_writer.write_node(gtex_node)
                                self.written_anatomical_entities.add(gtex_node.id)

                            # associate the sequence variant node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, variant_node, gtex_node, self.variant_anatomy_predicate, hyper_edge_id, None, True)

                            # associate the gene node with an edge to the gtex anatomy node
                            self.gtu.write_new_association(graph_writer, gene_node, gtex_node, self.gene_anatomy_predicate, 0, None, False)

                            # associate the sequence variant node with an edge to the gene node. also include the GTEx properties
                            self.gtu.write_new_association(graph_writer, variant_node, gene_node, predicate, hyper_edge_id, edge_properties, True)

                            # output some feedback for the user
                            if (line_counter % 250000) == 0:
                                logger.info(f'Processed {line_counter} variants.')

                            # reset written nodes list to avoid memory overflow
                            if len(self.written_anatomical_entities) == self.max_nodes:
                                self.written_anatomical_entities = set()
                            if len(self.written_genes) == self.max_nodes:
                                self.written_genes = set()
                    except (KeyError, IndexError) as e:
                        logger.error(f'Exception caught trying to process variant: {curie_hgvs}-{curie_uberon}-{curie_ensembl} at data line: {line_counter}. Exception: {e}, Line: {line}')

        except Exception as e:
            logger.error(f'Exception caught: Exception: {e}')
            ret_val = e

        # output some final feedback for the user
        logger.info(f'Building complete. Processed {line_counter} variants.')

        # return to the caller
        return ret_val

#######
# Main - Stand alone entry point for testing
#######
if __name__ == '__main__':
    # create a new builder object
    gtb = GTExBuilder(Rosetta())

    # directory to write/read GTEx data to process
    working_data_directory = '.'

    # load up the eqtl GTEx data with default settings
    rv = gtb.load(working_data_directory)

    # or use some optional parameters
    # out_file_name specifies the name of the combined and processed gtex cvs (eqtl_signif_pairs.csv)
    # process_raw_data creates that file - specify the existing file name and set to False if one exists
    # rv = gtb.load(working_data_directory,
    #              out_file_name='example_eqtl_output.csv',
    #              process_raw_data=True,
    #              process_for_graph=True,
    #              gtex_version=8)

    # check the return, output error if found
    if rv is not None:
        logger.error(rv)

    # or load the sqtl data (you can use the same optional parameters)
    rv = gtb.load_sqtl(working_data_directory)

    # check the return, output error if found
    if rv is not None:
        logger.error(rv)
