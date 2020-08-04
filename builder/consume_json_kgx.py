import csv
import json
from greent.graph_components import KNode, LabeledID
from greent.rosetta import Rosetta
from greent.export_delegator import WriterDelegator
from greent.service import Service


class KGX_JSON_File_parser(Service):
    def __init__(self):
        pass

    def get_nodes_from_file(self, file_name):
        if not file_name:
            return

        with open(file_name) as nodes_file:
            for line in nodes_file:
                try:
                    json_node = json.loads(line.strip().rstrip(","))
                except ValueError as e:
                    print(f'Invalid json for node: {line}({e})')
                    continue

                try:
                    labels = json_node['category']
                    node = KNode(json_node['id'], type='named_thing', name=json_node['name'])
                    node.add_synonyms(json_node['equivalent_identifiers'])
                    node.add_export_labels(labels)
                    yield node
                except KeyError as e:
                    print(f'Missing required properties for node: {line.strip().rstrip(",")}({e})')
                    yield None

    def get_edges_from_file(self, file_name, provided_by = None):
        """
        All is stuff is till we get kgx to merge edges. For now creating
        a pattern looking like a robokopservice and let writer handle it.
        :param provided_by:
        :param file_name:
        :return:
        """
        if not file_name:
            return

        desired_edge_properties = ["distance",
                                   "p-value",
                                   "slope",
                                   "expressed_in"]

        unmapped_predicates = set()
        with open(file_name) as edges_file:
            for i, line in enumerate(edges_file, start=1):

                if i % 1_500_000 == 0:
                    print(f'Still writing edges.. {i} written')

                try:
                    json_edge = json.loads(line.strip().rstrip(","))
                except ValueError:
                    print(f'Invalid json for edge: {line}')
                    continue

                try:
                    source_node = KNode(json_edge['subject'])
                    target_node = KNode(json_edge['object'])

                    if 'relation' in json_edge:
                        original_predicate = LabeledID(
                            identifier=json_edge['relation'],
                            label=json_edge['relation'].split(':')[-1])
                    elif 'predicate' in json_edge:
                        unmapped_predicates.add(json_edge['predicate'])
                        original_predicate = LabeledID(
                            identifier=json_edge['predicate'],
                            label=json_edge['predicate'].split(':')[-1])

                    normalized_predicate = LabeledID(
                        identifier=json_edge['edge_label'],
                        label=json_edge['edge_label'].split(':')[-1])

                    if 'provided_by' in json_edge:
                        provided_by = json_edge['provided_by']

                    props = {}
                    for key in json_edge:
                        if key in desired_edge_properties:
                            props[key] = json_edge[key]

                    # TODO this input id is not necessarily correct
                    edge = self.create_edge(
                        source_node=source_node,
                        target_node=target_node,
                        input_id=source_node.id,
                        provided_by=provided_by,
                        predicate=original_predicate,
                        properties=props
                    )
                    edge.standard_predicate = normalized_predicate
                    yield edge
                except KeyError as e:
                    print(f'Missing properties for edge: {line}({e})')
                    continue

    def run(self, nodes_file_name, edges_file_name, provided_by):
        self.rosetta = Rosetta()
        self.wdg = WriterDelegator(rosetta)
        self.wdg.normalized = True

        for node in self.get_nodes_from_file(nodes_file_name):
            self.wdg.write_node(node, annotate=False)

        for edge in self.get_edges_from_file(edges_file_name, provided_by):
            self.wdg.write_edge(edge)


        self.wdg.flush()


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
    Parse kgx json files to a graph and merge them 
    """)
    parser.add_argument('-n', '--nodes_file', help="Nodes file")
    parser.add_argument('-e', '--edges_file', help="Edges file")
    parser.add_argument('-p', '--provided_by', help="provided by", required=True)
    args = parser.parse_args()

    rosetta = Rosetta()
    kgx_loader = KGX_JSON_File_parser()
    if not args.nodes_file and not args.edges_file:
        print('Nothing to parse exiting')
        exit()

    kgx_loader.run(args.nodes_file, args.edges_file, args.provided_by)
    exit(0)






