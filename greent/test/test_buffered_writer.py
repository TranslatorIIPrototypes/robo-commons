from greent.export import BufferedWriter, export_edge_chunk, export_node_chunk
import pytest
from unittest.mock import Mock
from greent.graph_components import KNode, node_types, KEdge, LabeledID



rosetta_mock = Mock()
rosetta_mock.service_context = Mock()
rosetta_mock.service_context.config = {}


def test_can_initialize():
    assert BufferedWriter(rosetta_mock)

def test_write_node_with_export_lables():
    # assert that a node will be queued to its export types
    node = KNode('CURIE:1', type=node_types.NAMED_THING)
    all_types = [node_types.CHEMICAL_SUBSTANCE, node_types.NAMED_THING]
    node.add_export_labels(all_types)
    bf = BufferedWriter(rosetta_mock)
    bf.write_node(node)
    assert node.id in bf.written_nodes
    key = node.export_labels
    assert key in bf.node_queues
    queue = bf.node_queues[key]
    assert node.id in queue

def test_write_node_with_out_export_labels():
    # if no we send a node with type and its not normalizable, then it should be in the queue with empty frozen set key
    node = KNode('CURIE:1', type=node_types.CHEMICAL_SUBSTANCE)

    bf = BufferedWriter(rosetta_mock)
    bf.write_node(node)
    assert node == bf.node_queues[frozenset()][node.id]



def test_flush_nodes_non_changing_node():
    # test if nodes are sent to export function if they are already assigned primary id
    node = KNode('CHEBI:15347', type=node_types.CHEMICAL_SUBSTANCE)
    properties = {
        'a': 'some prop'
    }
    node.properties = properties

    bf = BufferedWriter(rosetta_mock)
    # label_by_export_graph = ['this_should_be_overridden']
    # def mock_add_labels(node): node.add_export_labels(label_by_export_graph)
    # # patch export_graph.add_type_labels and see if its called
    # bf.export_graph.add_type_labels = mock_add_labels

    # we add the node
    bf.write_node(node)

    def write_transaction_mock(export_func, nodes, types):
        print(types)
        # make sure this is the right function
        assert export_func == export_node_chunk
        # make sure we have out node id in there
        assert node.id in nodes
        # get the node and see if the properties are preserved
        assert nodes[node.id].properties == properties
        # see if the types are expected
        assert nodes[node.id].export_labels == types == frozenset([
      "chemical_substance",
      "named_thing",
      "biological_entity",
      "molecular_entity"
    ])
    session = Mock()
    session.write_transaction = write_transaction_mock
    # pass the mock tester to bf and let it rip
    synonym_map = bf.flush_nodes(session)

    # make sure the synonym map we get here to be used for edge correction is sane
    assert 'CHEBI:15347' in synonym_map
    assert synonym_map['CHEBI:15347'] == 'CHEBI:15347'

def test_flush_nodes_changing_node():
    # exact same test as non changing node except have to get different synonym map from flush_nodes
    node = KNode('MESH:D000096', type=node_types.CHEMICAL_SUBSTANCE)
    properties = {
        'a': 'some prop'
    }
    node.properties = properties

    bf = BufferedWriter(rosetta_mock)

    bf.write_node(node)

    def write_transaction_mock(export_func, nodes, types):
        print(types)
        # make sure this is the right function
        assert export_func == export_node_chunk
        # make sure we have out node id in there
        assert node.id in nodes
        # get the node and see if the properties are preserved
        assert nodes[node.id].properties == properties
        # see if the types are expected
        assert nodes[node.id].export_labels == types == frozenset([
            "chemical_substance",
            "named_thing",
            "biological_entity",
            "molecular_entity"
        ])

    session = Mock()
    session.write_transaction = write_transaction_mock
    # pass the mock tester to bf and let it rip
    synonym_map = bf.flush_nodes(session)

    # make sure the synonym map we get here to be used for edge correction is sane
    assert 'MESH:D000096' in synonym_map
    assert synonym_map['MESH:D000096'] == 'CHEBI:15347'


def test_flush_nodes_non_normilizable():
    # exact same test as non changing node except have to get different synonym map from flush_nodes
    node = KNode('SOME:curie', type=node_types.CHEMICAL_SUBSTANCE)
    properties = {
        'a': 'some prop'
    }
    node.properties = properties

    bf = BufferedWriter(rosetta_mock)


    bf.write_node(node)

    def write_transaction_mock(export_func, nodes, types):
        print(types)
        # make sure this is the right function
        assert export_func == export_node_chunk
        # make sure we have out node id in there
        assert node.id in nodes
        # get the node and see if the properties are preserved
        assert nodes[node.id].properties == properties
        # see if the types are expected
        assert nodes[node.id].export_labels == []
        assert types == frozenset()

    session = Mock()
    session.write_transaction = write_transaction_mock
    # pass the mock tester to bf and let it rip
    synonym_map = bf.flush_nodes(session)

    # make sure the synonym map we get here to be used for edge correction is sane
    assert 'SOME:curie' in synonym_map
    assert synonym_map['SOME:curie'] == 'SOME:curie'

def test_write_edges():
    bf = BufferedWriter(rosetta_mock)
    edge = KEdge({
        'source_id': 'source:1',
        'target_id': 'target:1',
        'provided_by': 'test_write_edges'
    })
    # edge.source_id = 'source:1'
    # edge.target_id = 'target:1'
    # edge.provided_by = 'test_write_edges'
    edge.original_predicate = LabeledID(identifier='SEMMEDDB:CAUSES', label='semmed:causes')
    bf.write_edge(edge)
    assert bf.written_edges[edge.source_id][edge.target_id] == set([edge.original_predicate.identifier])
    assert len(bf.edge_queues) == 1
    # try to write it twice and it should be keeping edge queues as 1
    bf.write_edge(edge)
    assert len(bf.edge_queues) == 1
    bf.write_edge(edge, force_create=True)
    assert len(bf.edge_queues) == 2


