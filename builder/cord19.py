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

    def load(self, provided_by, limit=0):
        print('writing to graph')
        nodes_dict = self.parse_nodes()
        edges = self.parse_edges(nodes_dict=nodes_dict, provided_by=provided_by, limit=limit)
        print(f'found {len(edges)}')
        for index, edge in enumerate(edges):
            source_node = nodes_dict.get(edge.source_id)
            target_node = nodes_dict.get(edge.target_id)
            self.writer.write_node(source_node)
            self.writer.write_node(target_node)
            self.writer.write_edge(edge)
            if index % 10000 == 0:
                print(f'~~~~~~~~~{(index/len(edges))* 100}% complete')
        self.writer.flush()
        print('done writing edges')

    def parse_nodes(self, limit=0):
        """
        Parse nodes.
        :param limit: for testing reads first n nodes from file
        :return: dict with node_id as key and KNode as value
        """
        nodes = {}
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
                nodes[node.id] = node
                limit_counter += 1
                if limit and limit_counter > limit:
                    break
            print(f'done parsing nodes {len(nodes)}')
            return nodes

    def parse_edges(self, nodes_dict, provided_by, limit=0):
        """ Construct KEdges"""
        edges = []
        limit_counter = 0
        with open(os.path.join(self.cord_dir,'edges.txt')) as edges_file:
            reader = csv.DictReader(edges_file, delimiter='\t')
            for edge_raw in reader:
                predicate = LabeledID(identifier='SEMMEDDB:ASSOCIATED_WITH', label='related_to')
                source_node = nodes_dict.get(edge_raw['Term1'])
                target_node = nodes_dict.get(edge_raw['Term2'])
                edge = self.create_edge(
                    source_node=source_node,
                    target_node=target_node,
                    input_id=edge_raw['Term1'],
                    provided_by=provided_by,
                    predicate=predicate,
                    publications=[],
                    properties={
                        'num_publications': edge_raw['Effective_Pubs'],
                        'enrichment_p': edge_raw['Enrichment_p']
                    }
                )
                limit_counter += 1
                if limit and limit_counter > limit:
                    break
                edges.append(edge)
        return edges


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
        Parse edges and nodes file to graph.
        """, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--provided_by',
                        help='Provided by attribute to be used on edges.',)
    args = parser.parse_args()
    if not args.provided_by:
        print('provided by arg is required')
        exit(-1)
    svc = Cord19Service()
    svc.load(provided_by=args.provided_by)



