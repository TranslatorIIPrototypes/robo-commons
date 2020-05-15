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
    label_to_curie_map = {}
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


    def get_nodes_from_file(self, file_name):
        if not file_name:
            return
        with open(file_name) as nodes_file:
            reader = csv.DictReader(nodes_file, delimiter=',')
            for raw_node in reader:
                labels = raw_node['category'].split('|')
                id = raw_node['id']
                name = raw_node['name']
                node = KNode(
                    id,
                    type=labels[0],
                    name=name
                )
                node.add_export_labels(labels)
                yield node

    def get_edges_from_file(self, file_name, provided_by):
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
            reader = csv.DictReader(edge_file, delimiter=',')
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
                    predicate=predicate
                )
                yield edge

    def run(self, nodes_file_name, edges_file_name):
        self.rosetta = Rosetta()
        self.wdg = WriterDelegator(rosetta)
        for node in self.get_nodes_from_file(nodes_file_name):
            self.wdg.write_node(node)
        for edge in self.get_edges_from_file(edges_file_name, provided_by='https://github.com/TranslatorIIPrototypes/ViralProteome'):
            self.wdg.write_edge(edge)
        self.wdg.flush()




if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
    Parse kgx files to a graph and merge them 
    """)
    parser.add_argument('-n', '--nodes_file', help="Nodes file")
    parser.add_argument('-e', '--edge_file', help="Edges file")
    args = parser.parse_args()
    rosetta = Rosetta()
    kgx_loader =KGX_File_parser()
    if not args.nodes_file and not args.edges_file:
        print('Nothing to parse exiting')
        exit()

    kgx_loader.run(args.nodes_file, args.edges_file)
    exit(0)






