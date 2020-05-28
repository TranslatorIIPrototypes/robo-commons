from robokop_genetics.genetics_services import GeneticsServices, ALL_VARIANT_TO_GENE_SERVICES, ENSEMBL, MYVARIANT
from greent.export_delegator import WriterDelegator
from greent.graph_components import KNode, KEdge, node_types, LabeledID
from greent.util import LoggingUtil
from greent.rosetta import Rosetta
from neo4j import GraphDatabase
import argparse

logger = LoggingUtil.init_logging('genetics_builder')

class GeneticsBuilder:
    def __init__(self, sv_neo4j_credentials, crawl_for_service, recreate_sv_node):
        self.rosetta = Rosetta()
        self.writerDelegator = WriterDelegator(rosetta=self.rosetta)
        self.sv_neo4j_credentials = sv_neo4j_credentials
        self.crawl_for_service = crawl_for_service
        self.genetics_services = GeneticsServices()
        self.recreate_sv_node = recreate_sv_node
        self.written_genes = set()
        self.written_max_size = 100_000


    def get_all_variants_and_synonymns(self):
        driver = GraphDatabase.driver(**self.sv_neo4j_credentials)
        with driver.session() as session:
            results = session.run("MATCH (c:sequence_variant) RETURN c.id as id, c.equivalent_identifiers as synonyms")
        response = []
        for row in results:
            response.append((row['id'], row['synonyms']))
        return response


    def start_build(self) -> list:
        # Entry point
        variant_list = self.get_all_variants_and_synonymns()
        if not variant_list:
            logger.info('No Sequence variant nodes found from graph.')
        variant_subset = []
        with self.writerDelegator as writer:
            # for each variant
            for var in variant_list:
                # check to see if we have all the data elements we need. element [0] is the ID, element [1] is the synonym list
                if len(var) == 2:
                    # create a variant node
                    variant_curie = var[0]

                    # get the synonym data from the graph DB call
                    variant_syn_set = set(var[1])

                    variant_node = KNode(variant_curie, type=node_types.SEQUENCE_VARIANT)
                    variant_node.add_synonyms(variant_syn_set)
                    variant_node.add_export_labels([node_types.SEQUENCE_VARIANT])

                    variant_subset.append(variant_node)
                    if len(variant_subset) == 1000:
                        self.process_variant_to_gene_relationships(variant_nodes=variant_subset, writer=writer)
                        variant_subset = []
            if variant_subset:
                # for left overs
                self.process_variant_to_gene_relationships(variant_nodes=variant_subset, writer=writer)

    def process_variant_to_gene_relationships(self, variant_nodes: list, writer: WriterDelegator):
        # reset written nodes every max size to avoid memory overflow
        if len(self.written_genes) == self.written_max_size:
            self.written_genes = set()
        all_results = self.genetics_services.get_variant_to_gene(self.crawl_for_service, variant_nodes)
        for source_node_id, results in all_results.items():
            # convert the simple edges and nodes to rags objects and write them to the graph
            for (edge, node) in results:
                gene_node = KNode(node.id, type=node.type, name=node.name, properties=node.properties)
                if self.recreate_sv_node:
                    variant_node = KNode(source_node_id, type= node_types.SEQUENCE_VARIANT)
                    variant_node.add_export_labels([node_types.SEQUENCE_VARIANT])
                    writer.write_node(variant_node)
                if gene_node.id not in self.written_genes:
                    writer.write_node(gene_node)
                    self.written_genes.add(gene_node.id)

                predicate = LabeledID(identifier=edge.predicate_id, label=edge.predicate_label)
                gene_edge = KEdge(source_id=source_node_id,
                                  target_id=gene_node.id,
                                  provided_by=edge.provided_by,
                                  ctime=edge.ctime,
                                  original_predicate=predicate,
                                  # standard_predicate=predicate,
                                  input_id=edge.input_id,
                                  properties=edge.properties)
                writer.write_edge(gene_edge)
            logger.info(f'added {len(results)} variant relationships for {source_node_id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    help_string = """Builds a graph on ensemble or/and myvariant services.    
    """
    parser = argparse.ArgumentParser(description=help_string,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-P', '--provider', help='Can be 0 for both, 1 for ensemble, 2 for myvariant')
    parser.add_argument('-n', '--neo4j_uri', help='Uri of neo4j host to get Sequence variant list eg bolt://<your_host>:<bolt_port>', required=True)
    parser.add_argument('-u', '--username', help='User name for neo4j', required=True)
    parser.add_argument('-p', '--password', help='Password for neo4j', required=True)
    parser.add_argument('-c', '--recreate_sv_node', help='If source if the variant nodes is different and not sure if it exists in the graph use this', action='store_true', default=False)
    args = parser.parse_args()
    neo4j_credentials = {
        'uri': args.neo4j_uri,
        'auth': (
            args.username,
            args.password
        )
    }
    provider = {
        '0': ALL_VARIANT_TO_GENE_SERVICES,
        '1': ENSEMBL,
        '2': MYVARIANT
    }.get(args.provider, None)
    if not provider:
        raise(f'Invalid provider options are 0, 1 or 2')
    genetics_builder = GeneticsBuilder(neo4j_credentials, provider, args.recreate_sv_node)
    genetics_builder.start_build()
