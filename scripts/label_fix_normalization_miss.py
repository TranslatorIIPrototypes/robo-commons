from neo4j import GraphDatabase
import os

class DBDriver:
    def __init__(self):
        neo4j_host = os.environ['NEO4J_HOST']
        neo4j_bolt = os.environ['NEO4J_BOLT_PORT']
        neo4j_user = 'neo4j'
        neo4j_password = os.environ['NEO4J_PASSWORD']
        self.driver = GraphDatabase.driver(
            uri=f'bolt://{neo4j_host}:{neo4j_bolt}',
            auth = (neo4j_user, neo4j_password)
        )

    def run(self, query):
        with self.driver.session() as session:
            return session.run(query)

def run_fix():
    db_driver = DBDriver()
    # fix map is curie prefix to labels
    fix_map = {
        'PANTHER.PATHWAY': ':biological_entity:biological_process:biological_process_or_activity:pathway',
        'CARO': ':anatomical_entity:biological_entity:organismal_entity',
        'OMIM': ':biological_entity:disease:disease_or_phenotypic_feature',
        'DOID': ':biological_entity:disease:disease_or_phenotypic_feature',
        'CP': ':anatomical_entity:cellular_component',
        'NCBIGene': ':gene:gene_or_gene_product:biological_entity:genomic_entity:macromolecular_machine:molecular_entity',
        'UniProtKB': ':gene:gene_or_gene_product:biological_entity:genomic_entity:macromolecular_machine:molecular_entity',
        'CHEMBL.COMPOUND': ':biological_entity:chemical_substance:molecular_entity',
        'GTOPDB': ':biological_entity:chemical_substance:molecular_entity',
        'HGNC': ':gene:gene_or_gene_product:biological_entity:genomic_entity:macromolecular_machine:molecular_entity',
        'HMDB': ':biological_entity:chemical_substance:molecular_entity',
        'PANTHER.FAMILY': ':gene_family',
        'NCBITaxon': ':organism_taxon',
        'NCBIGENE': ':gene:gene_or_gene_product:biological_entity:genomic_entity:macromolecular_machine:molecular_entity',
        'UNIPROTKB': ':gene:gene_or_gene_product:biological_entity:genomic_entity:macromolecular_machine:molecular_entity'
    }

    make_query = lambda prefix, labels: f"MATCH (a) " \
        f"WHERE 1 = length(labels(a)) " \
        f"AND NOT a:Concept " \
        f"AND a.id STARTS WITH '{prefix}' " \
        f"SET a{labels} RETURN COUNT(a) as count"

    for prefix, labels in fix_map.items():
        query = make_query(prefix, labels)
        print(f'running query \n {query}')
        count = db_driver.run(query).single()['count']
        print(f'changed {count} nodes.')

    print('done.')

if __name__=='__main__':
    run_fix()