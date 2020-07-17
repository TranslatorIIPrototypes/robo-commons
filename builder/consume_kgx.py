import csv
from greent.graph_components import KNode, LabeledID
from greent.rosetta import Rosetta
from greent.export_delegator import WriterDelegator
from greent.service import Service
import requests

import traceback

class BL_lookup:
    """
    Robokop uses relation_id to figure out type of edge,
    KGX outputs relation label (the type) but not the curie,
    so grab a relevant curie from BL_Lookup
    """
    label_to_curie_map = {
        'similar_to': 'SO:similar_to'
    }
    instance = None
    def __init__(self):
        if BL_lookup.instance == None:
            BL_lookup.instance = BL_lookup._bl_lookup()

    class _bl_lookup:
        def __init__(self):
            self.bl_url = lambda edge_type: f"https://bl-lookup-sri.renci.org/bl/{edge_type}"
        def resolve_curie(self, edge_type):
            if edge_type in BL_lookup.label_to_curie_map:
                return BL_lookup.label_to_curie_map[edge_type]
            lookup_url = self.bl_url(edge_type)
            try:
                with requests.session() as session:
                    response = session.get(lookup_url)
                    if response.status_code == 200:
                        result = response.json()
                        BL_lookup.label_to_curie_map[edge_type] = result.get('slot_uri')
                        return BL_lookup.label_to_curie_map[edge_type]
            except :
                traceback.format_exc()
            print(f'error resolving edge curie for edge type :{edge_type}')
            return ''


    def __getattr__(self, name):
        return getattr(BL_lookup.instance, name)


class KGX_File_parser(Service):
    def __init__(self):
        pass


    def get_nodes_from_file(self, file_name, delimiter: str):
        if not file_name:
            return

        with open(file_name) as nodes_file:
            reader = csv.DictReader(nodes_file, delimiter=delimiter)
            for raw_node in reader:
                labels = list(filter(lambda x : x , raw_node['category'].split('|')))
                if not len(labels):
                    labels = ['named_thing']
                id = raw_node['id']
                name = raw_node['name']
                node = KNode(
                    id,
                    type=labels[0],
                    name=name
                )
                node.add_export_labels(labels)
                yield node

    def get_edges_from_file(self, file_name, provided_by, delimiter):
        """
        All is stuff is till we get kgx to merge edges. For now creating
        a pattern looking like a robokopservice and let writer handle it.
        :param file_name:
        :return:
        """
        if not file_name:
            return

        bl_resolver = BL_lookup()
        with open(file_name) as edge_file:
            reader = csv.DictReader(edge_file, delimiter=delimiter)
            for raw_edge in reader:
                edge_label = raw_edge['edge_label']
                predicate = LabeledID(
                    identifier=bl_resolver.resolve_curie(edge_label),
                    label=edge_label
                )
                source_node = KNode(raw_edge['subject'])
                target_node = KNode(raw_edge['object'])
                edge = self.create_edge(
                    source_node=source_node,
                    target_node=target_node,
                    input_id=source_node.id,
                    provided_by=provided_by,
                    predicate=predicate,
                )
                edge.standard_predicate = predicate
                yield edge

    def run(self, nodes_file_name, edges_file_name, provided_by, delimiter):
        self.rosetta = Rosetta()
        self.wdg = WriterDelegator(rosetta)
        self.wdg.normalized = True

        for node in self.get_nodes_from_file(nodes_file_name, delimiter):
            self.wdg.write_node(node, annotate=False)

        for edge in self.get_edges_from_file(edges_file_name, provided_by=provided_by, delimiter=delimiter):
            self.wdg.write_edge(edge)
        self.wdg.flush()




if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
    Parse kgx files to a graph and merge them 
    """)
    parser.add_argument('-n', '--nodes_file', help="Nodes file")
    parser.add_argument('-e', '--edges_file', help="Edges file")
    parser.add_argument('-p', '--provided_by', help="provided by", required=True)
    parser.add_argument('-d', '--delimiter', required=True, help="Data column delimiter (c=comma, t=tab)")
    args = parser.parse_args()

    # check for the data record character delimiter
    if args.delimiter == 'c':
        delimiter = ','
    elif args.delimiter == 't':
        delimiter = '\t'
    else:
        print('Invalid record column delimiter.')
        exit()

    rosetta = Rosetta()
    kgx_loader =KGX_File_parser()
    if not args.nodes_file and not args.edges_file:
        print('Nothing to parse exiting')
        exit()

    kgx_loader.run(args.nodes_file, args.edges_file, args.provided_by, delimiter)
    exit(0)






