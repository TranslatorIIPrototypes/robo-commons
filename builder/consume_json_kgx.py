import time
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

        # declare a set of properties that should/may be in all datasets
        baseline_node_properties = ['id', 'name', 'category', 'equivalent_identifiers', '']

        with open(file_name) as nodes_file:
            for line in nodes_file:
                try:
                    json_node = json.loads(line.strip().rstrip(","))
                except ValueError as e:
                    print(f'Invalid json for node: {line}({e})')
                    continue

                try:
                    labels = json_node['category']

                    # init some storage for any non-default properties on the node
                    props: dict = {}

                    # find the properties that are non-standard and add them
                    for key in json_node:
                        if key not in baseline_node_properties:
                            props[key] = json_node[key]

                    node = KNode(json_node['id'], type='named_thing', name=json_node['name'], properties=props)
                    node.add_synonyms(json_node['equivalent_identifiers'])
                    node.add_export_labels(labels)

                    yield node
                except KeyError as e:
                    print(f'Missing required properties for node: {line.strip().rstrip(",")}({e})')
                    yield None

    def get_edges_from_file(self, file_name, kgx_provided_by=None):
        """
        All is stuff is till we get kgx to merge edges. For now creating
        a pattern looking like a robokopservice and let writer handle it.
        :param provided_by:
        :param file_name:
        :return:
        """
        if not file_name:
            return

        # declare a set of properties that should/may be in all datasets
        baseline_edge_properties = ['relation', 'predicate', 'subject', 'object', 'provided_by', 'edge_label', 'source_database', 'publications']

        print(f'({time.ctime()}) Starting Edges...')

        unmapped_predicates = set()

        with open(file_name) as edges_file:
            for i, line in enumerate(edges_file, start=1):

                if i % 1_000_000 == 0:
                    print(f'({time.ctime()}) Still writing edges.. {i} written')

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

                    # get the data source. this is in priority order
                    if 'provided_by' in json_edge:
                        provided_by = json_edge['provided_by']
                    elif 'source_database' in json_edge:
                        provided_by = json_edge['source_database'].replace('.', '_')
                    else:
                        provided_by = kgx_provided_by

                    # if there are publications add them in
                    if 'publications' in json_edge:
                        publications: list = [json_edge['publications']]
                    else:
                        publications: list = []

                    props = {}

                    for key in json_edge:
                        if key not in baseline_edge_properties:
                            props[key] = json_edge[key]

                    # create the edge
                    edge = self.create_edge(
                        source_node=source_node,
                        target_node=target_node,
                        input_id=source_node.id,
                        provided_by=provided_by,
                        predicate=original_predicate,
                        publications=publications,
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

        nodes = self.get_nodes_from_file(nodes_file_name)

        counter = 0

        for node in nodes:
            print(node)
            counter += 1
            if counter > 10:
                break
            self.wdg.write_node(node, annotate=False)

        edges = self.get_edges_from_file(edges_file_name, provided_by)

        counter = 0

        for edge in edges:
            print(edge)
            counter += 1
            if counter > 10:
                break
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






