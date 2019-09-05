from neo4j import GraphDatabase
import os
import pytest
import json
from functools import reduce
from greent import node_types
import yaml
import pickle

DB1_CONF = {
    'uri': "bolt://robokopdb2.renci.org:7687",
    'auth': (
        'neo4j',
        os.environ['NEO4J_PASSWORD']
    )
}
DB2_CONF = {
    'uri': "bolt://robokopdev.renci.org:7687",
    'auth': (
        'neo4j',
        os.environ['NEO4J_PASSWORD']
    )
}



def write_report_to_file (report, file_name):
    with open(f'reports/{file_name}', 'w') as report_file:
        json.dump(report, report_file, indent= 4 )
@pytest.fixture()
def db1_driver(rosetta):
    return GraphDatabase.driver(uri = DB1_CONF['uri'], auth = DB1_CONF['auth'])
@pytest.fixture()
def db2_driver(rosetta):
    return GraphDatabase.driver(uri = DB2_CONF['uri'],auth =  DB2_CONF['auth'])

EXECLUDED_NODE_TYPE= [
    node_types.SEQUENCE_VARIANT
]


def query(driver, q):
    with driver.session() as session:
        return session.run(q)

def sorted_list_diff(list_1, list_2):
    big_list = list_1
    small_list =list_2
    in_big = []
    in_small = []
    i_s = 0
    i_b = 0
    while i_s < len(small_list) and i_b < len(big_list):
        # first lets see if they are different
        if big_list[i_b] > small_list[i_s]:
            in_small += [small_list[i_s]]
            i_s +=1
            # oh they are now 
        elif big_list[i_b] < small_list[i_s]:
            in_big += [big_list[i_b]]
            i_b += 1
        else:
            i_b += 1
            i_s += 1
    in_big += big_list[i_b:]
    in_small += small_list[i_s:]
    return in_big, in_small


PERCENTAGE = 0.01

def test_random_node_property_check(db1_driver, db2_driver):
    # get count of types from each 
    q = lambda x : f"MATCH (c:{x}) return count(c) as c "
    counts = {
        ty: {
            'db1': query(db1_driver, q(ty)).single()['c'] or 0,
            'db2': query(db2_driver, q(ty)).single()['c'] or 0
        } for ty in node_types.node_types
    }

    # now we use the counts to set limits and as for some samples

    q = lambda x, y : f"MATCH (c:{x}) return collect(c) as nodes limit {int(y * PERCENTAGE)}"
    print(json.dumps(counts, indent= 4))
    sample_nodes = {
        ty: {
            # grab some from db1 now use those ids for db2 
            'db1': query(db1_driver, q(ty, counts[ty]['db1'])).single()['nodes'],            
        }
        for ty in counts
    }
    with open('pickle.jar', 'w') as jar:
        pickle.dump(sample_nodes, jar)
    # maybe save pickle this part so we can repeat sample tests 

    #actual test is to go over this nodes and comparing things with in 

    report  = {}
    for ty in sample_nodes:
        if ty not in report:
            report[ty] = {}
            sample_nodes[ty]['db2'] = []
        for node in sample_nodes[ty]['db1']:
            i = node['id']
            result =  query(db2_driver, f"MATCH (c:{ty}{{'id':{i}}}) return c").single() 
            db2_node =  result['c']  if result else None
            if db2_node == None:
                report[ty][i] = {
                    'missing': True
                }
            # now we make sure that the nodes are good 
            db1_node_keys = list(node.keys())
            db2_node_keys = list(db2_node.keys())
            db1_node_keys.sort()
            db2_node_keys.sort()
            db1_diff, db2_diff = sorted_list_diff(db1_node_keys, db2_node_keys)
            if len(db1_diff) != len(db2_diff):
                report[ty][i] = {
                    'db1_props': db1_diff,
                    'db2_props': db2_diff
                }
    assert reduce(lambda x, y: x and len(report[y])== 0, report, True)
            







