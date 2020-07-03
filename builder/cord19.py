import csv
import os
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.export_delegator import WriterDelegator
from greent.rosetta import Rosetta


class Cord19Service(Service):

    def __init__(self):
        self.cord_dir = os.environ.get('CORD_DIR')
        self.rosetta = Rosetta()
        self.writer = WriterDelegator(rosetta=self.rosetta)
        # line counts for reporting
        self.num_edges = self.count_lines_in_file('edges.txt')
        self.num_nodes = self.count_lines_in_file('nodes.txt')

    def count_lines_in_file(self, file_name):
        count = -1  # don't count headers
        with open(os.path.join(self.cord_dir, file_name)) as nodes_file:
            for line in nodes_file:
                count += 1
        return count

    def load_nodes_only(self):
        print('Writing nodes')
        for index, node in self.parse_nodes():
            index += 1
            self.writer.write_node(node)
            if index % 100 == 0:
                print(f'~~~~~~~~~{(index/self.num_nodes)* 100}% complete')

    def load(self, provided_by, limit=0):
        print('writing to graph')
        print('writing nodes')
        self.writer.normalized = True
        for index, node in self.parse_nodes():
            self.writer.write_node(node)
            if index % 1000 == 0:
                print(f'~~~~~~~~~{(index / self.num_edges) * 100} % complete')
        for index, edge in self.parse_edges(provided_by=provided_by, limit=limit):
            source_node = KNode(edge.source_id)
            target_node = KNode(edge.target_id)
            self.writer.write_node(source_node)
            self.writer.write_node(target_node)
            self.writer.write_edge(edge)
            if index % 10000 == 0:
                print(f'~~~~~~~~~{(index/self.num_edges)* 100} % complete')
        self.writer.flush()
        print('done writing edges')

    def parse_nodes(self, limit=0):
        """
        Parse nodes.
        :param limit: for testing reads first n nodes from file
        :return: dict with node_id as key and KNode as value
        """
        print('parsing nodes...')
        limit_counter = 0
        with open(os.path.join(self.cord_dir, 'nodes.txt')) as nodes_file:
            reader = csv.DictReader(nodes_file, delimiter='\t')
            for raw_node in reader:
                # transform headers to knode attrbutes
                labels = raw_node.get('semantic_type')
                labels = labels.replace(']', '').replace('[', '').replace('\\', '').replace("'", '')
                labels = labels.split(',')
                node = KNode({
                    'id': raw_node.get('normalized_curie'),
                    'type': labels[0],
                    'name': raw_node.get('name'),
                    'properties': {
                        'input_term': raw_node.get('input_term')
                    }
                })
                node.add_export_labels(labels)
                limit_counter += 1
                if limit and limit_counter > limit:
                    break
                yield limit_counter -1, node

    def parse_edges(self, provided_by, limit=0):
        """ Construct KEdges"""
        if not provided_by:
            raise RuntimeError('Error edge property provided by is not specified')
        limit_counter = 0
        with open(os.path.join(self.cord_dir,'edges.txt')) as edges_file:
            reader = csv.DictReader(edges_file, delimiter='\t')
            for edge_raw in reader:
                predicate = LabeledID(identifier='SEMMEDDB:ASSOCIATED_WITH', label='related_to')
                source_node = KNode(edge_raw['Term1'])
                target_node = KNode(edge_raw['Term2'])
                edge = self.create_edge(
                    source_node=source_node,
                    target_node=target_node,
                    input_id=edge_raw['Term1'],
                    provided_by=provided_by,
                    predicate=predicate,
                    publications=[],
                    properties={
                        'num_publications': float(edge_raw['Effective_Pubs']),
                        'enrichment_p': float(edge_raw['Enrichment_p'])
                    }
                )
                edge.standard_predicate = predicate
                limit_counter += 1
                if limit and limit_counter > limit:
                    break
                yield limit_counter - 1, edge

    def parse_covid_pheno(self, phenotypes_file):
        items = []
        self.writer.normalized = True
        with open(phenotypes_file) as csf_file:
            data = csv.DictReader(csf_file, delimiter=',')
            for row in data:
                items.append(row)
        ids = []
        for n in items:
            if n['HP']:
                ids.append(n['HP'])
        import requests
        url = 'https://nodenormalization-sri.renci.org/get_normalized_nodes?'
        curies = '&'.join(list(map(lambda x: f'curie={x}', ids)))
        url += curies
        phenotypes = requests.get(url).json()
        knodes = []
        for n in phenotypes:
            node_data = phenotypes[n]
            i = node_data['id']
            knodes.append(KNode(i['identifier'], type=node_data['type'][0]))

        covid_node = requests.get(
            'https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=MONDO:0100096').json()
        covid_node = KNode(covid_node['MONDO:0100096']['id']['identifier'], type=covid_node['MONDO:0100096']['type'][0])
        predicate = LabeledID(identifier='RO:0002200', label='has_phenotype')
        self.writer.write_node(covid_node)
        for node,edge_data in zip(knodes, items):
            self.writer.write_node(node)
            property = {}
            if 'Note' in edge_data:
                property = {
                    'notes': edge_data['Note']
                }
            edge = self.create_edge(
                source_node=covid_node,
                target_node=node,
                input_id=covid_node.id,
                provided_by='covid_phenotypes_csv',
                predicate=predicate,
                publications=[],
                properties=property
            )
            edge.standard_predicate = predicate
            self.writer.write_edge(edge)
        self.writer.flush()

    def parse_drug_bank_items(self):
        import requests
        from contextlib import closing
        drug_bank_parsed_tsv = 'https://raw.githubusercontent.com/TranslatorIIPrototypes/CovidDrugBank/master/trials.txt'
        items = []
        tsv_file = requests.get(drug_bank_parsed_tsv,).text.split('\n')
        reader = csv.DictReader(tsv_file, delimiter="\t")
        for row in reader:
            items.append(row)
        drug_ids = '&'.join([f"curie={item['source']}" for item in items])
        normalize_url = f"https://nodenormalization-sri.renci.org/get_normalized_nodes?{drug_ids}"
        response = requests.get(normalize_url).json()
        nodes = []
        export_labels_fallback = requests.get('https://bl-lookup-sri.renci.org/bl/chemical_substance/ancestors?version=latest').json()
        export_labels_fallback.append('chemical_substance')
        for drug_id in response:
            node = None
            if response[drug_id] == None:
                node = KNode(drug_id, type='chemical_substance')
                node.add_export_labels(export_labels_fallback)
            else:
                # else use synonimized id so edges are merged
                prefered_curie = response[drug_id]['id']['identifier']
                node = KNode(prefered_curie, type="chemical_substance")
            nodes.append(node)
            self.writer.write_node(node)
        self.writer.flush()
        ## 'manually write in_clinical_trial_for edges
        query =lambda source_id, target_id, count :f"""
        MATCH (a:chemical_substance{{id: '{source_id}'}}) , (b:disease{{id:'{target_id}'}})
        Merge (a)-[e:in_clinical_trial_for{{id: apoc.util.md5([a.id, b.id, 'ROBOKOVID:in_clinical_trial_for']), predicate_id: 'ROBOKOVID:in_clinical_trial_for'}}]->(b)
        SET e.edge_source = "https://www.drugbank.ca/covid-19"
        SET e.relation_label = "in_clinical_trial_for"
        SET e.source_database = "drugbank"
        SET e.predicate_id = "ROBOKOVID:in_clinical_trial_for"
        SET e.relation = "in_clinical_trial_for"
        SET e.count = {count}
        """
        with self.rosetta.type_graph.driver.session() as session:
            for source_node, row in zip(nodes, items):
                q = query(source_node.id, row['object'], row['count'])# assuming  MONDO:0100096 is in there
                session.run(q)
    @staticmethod
    def convert_dict_to_neo4j_dict(d, exclude=[]):
        lines = []
        for k in d:
            if k in exclude:
                continue
            value = d[k]
            if isinstance(value, str):
                value = f"'{value}'"
            lines.append(f"{k}: {value}")
        lines.append('rectified: true')
        return f"{{{','.join(lines)}}}"
    @staticmethod
    def write_edge_copy(session, source_id, row, reverse,):
        if reverse:
            target_id = source_id
            source_id = row['other_id']
        else:
            target_id = row['other_id']
        edge_type = row['edge_type']
        edge_properties = Cord19Service.convert_dict_to_neo4j_dict(row['e'], ['id'])
        edge = row['e']
        session.run(f"""
        MATCH (a:named_thing{{id:'{source_id}'}}), (b:named_thing{{id:'{target_id}'}})
        WHERE not (a)-[:{edge_type}]-(b)
        MERGE (a)-[e:{edge_type}{{id: apoc.util.md5([a.id, b.id, '{edge['predicate_id']}']), predicate_id: '{edge['predicate_id']}'}}]->(b)
         
        SET e += {edge_properties}        
                """)

    def rectify_relationships(self):
        """
        Gets edges for NCBITaxon:2697049(Covid-19 virus) and links them to MONDO:0100096(Covid-19 disease
        :return:
        """
        disease_id = "MONDO:0100096"
        taxon_id = "NCBITaxon:2697049"
        as_source_query = lambda source_id, other_id: f"""        
        MATCH (a:named_thing{{id:'{source_id}'}})-[e]->(b)
        WHERE b.id <> '{other_id}'
        return e, b.id as other_id , type(e) as edge_type
        """
        as_target_query = lambda target_id, other_id: f"""        
        MATCH (a)-[e]->(b:named_thing{{id:'{target_id}'}})
        WHERE b.id <> '{other_id}'
        return e, a.id as other_id, type(e) as edge_type
        """
        driver = self.rosetta.type_graph.driver
        with self.rosetta.type_graph.driver.session() as session:
            disease_to_things = [dict(**row) for row in session.run(as_source_query(disease_id, taxon_id))]
        with driver.session() as session:
            things_to_disease = [dict(**row) for row in session.run(as_target_query(disease_id, taxon_id))]
        with driver.session() as session:
            taxon_to_things = [dict(**row) for row in session.run(as_source_query(taxon_id, disease_id))]
        with driver.session() as session:
            things_to_taxon = [dict(**row) for row in session.run(as_target_query(taxon_id, disease_id))]

        for row in disease_to_things:
            with driver.session() as session:
                session.write_transaction(Cord19Service.write_edge_copy, taxon_id, row, False)
        for row in things_to_disease:
            with driver.session() as session:
                session.write_transaction(Cord19Service.write_edge_copy,taxon_id, row, True)
        for row in taxon_to_things:
            with driver.session() as session:
                session.write_transaction(Cord19Service.write_edge_copy, disease_id, row, False)
        for row in things_to_taxon:
            with driver.session() as session:
                session.write_transaction(Cord19Service.write_edge_copy, disease_id, row, True)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
        Parse edges and nodes file to graph.
        """, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--provided_by',
                        help='Provided by attribute to be used on edges.', default=None)
    parser.add_argument('-n', '--nodes_only',
                        help='Parse nodes only', action='store_true')
    parser.add_argument('-ph', '--phenotypes',
                        help="add phenotypes, phenotypes will be linked to `MONDO:0100096` expects file to have "
                             "`HP` column header, optionally provide `Note` header to add notes on the edges.", default=None)
    parser.add_argument('-d', '--drug_bank',
                        help="parse drug bank extract from https://raw.githubusercontent.com/TranslatorIIPrototypes/CovidDrugBank/master/trials.txt",
                        action='store_true')
    parser.add_argument('-r', '--rectify',
                        help="assigns every edge from covid-19 disease node to covid-19 taxon "
                             "and vice versa",
                        action='store_true')
    args = parser.parse_args()
    svc = Cord19Service()
    if args.nodes_only:
        svc.load_nodes_only()
    if args.provided_by:
        svc.load(provided_by=args.provided_by)
    if args.phenotypes:
        svc.parse_covid_pheno(args.phenotypes)
    if args.drug_bank:
        svc.parse_drug_bank_items()
    if args.rectify:
        svc.rectify_relationships()

