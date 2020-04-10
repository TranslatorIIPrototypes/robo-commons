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
                limit_counter += 1
                if limit and limit_counter > limit:
                    break
                yield limit_counter - 1, edge


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
        Parse edges and nodes file to graph.
        """, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--provided_by',
                        help='Provided by attribute to be used on edges.', default=None)
    parser.add_argument('-n', '--nodes_only',
                        help='Parse nodes only', action='store_true')
    args = parser.parse_args()
    svc = Cord19Service()
    if args.nodes_only:
        svc.load_nodes_only()
        exit(1)
    if args.provided_by:
        svc.load(provided_by=args.provided_by)



