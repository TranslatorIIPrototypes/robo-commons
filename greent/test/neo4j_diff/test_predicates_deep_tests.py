
from neo4j import GraphDatabase
import os
import pytest
import json
from functools import reduce
from greent import node_types
import yaml

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

#maybe this is a bad Idea but ... 
def parse_rosetta(rosetta_path= f"{os.environ.get('ROBOKOP_HOME')}/robokop-interfaces/greent/rosetta.yml"):
    with open(rosetta_path) as rosetta_yml:
        ro = yaml.load(rosetta_yml)

    return ro 




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

def test_predicates_types_between_concepts(db1_driver, db2_driver, rosetta):
    rosetta_conf = parse_rosetta()
    ops = rosetta_conf['@operators']
    main_db, other_db = db1_driver, db2_driver
    report = {}
    # #premitive pairing
    pairs = []
    for node_type in ops:
        if node_type in EXECLUDED_NODE_TYPE:
            continue
        for key in ops[node_type]:
            if key in EXECLUDED_NODE_TYPE:
                continue
            pairs.append((node_type, key))    
    # now we ask for predicates for each pair 
    for n1, n2 in pairs:
        q = f""" MATCH (n1:{n1})-[e]-(n2:{n2}) return distinct type(e) as type, count(e) as count ORDER BY type"""
        db1_type_list = {r['type']: r['count'] for r in query(main_db, q)}
        db2_type_list = {r['type']: r['count'] for r in query(other_db, q)}
        # Do they both have same type list ? 
        db1_type_diff, db2_type_diff = sorted_list_diff(
            [x for x in db1_type_list.keys()], 
            [x for x in db2_type_list.keys()])
        report[f"{n1}<->{n2}"] = {
            'types_only_in_db1': db1_type_diff,
            'types_only_in_db2': db2_type_diff
        }
        # now we go over thier common types and make sure that their count match
        common_types = [x for x in db1_type_list if x not in db1_type_diff]
        count_diffs = {}
        for t in common_types:
            if db1_type_list[t] != db2_type_list[t]:
                count_diffs[t] = {
                    'counts': db1_type_list[t] - db2_type_list[t]
                }
                sample_size = 10
                sample_ids = []
                counts = count_diffs[t]['counts']
                if counts != 0: 
                    q = f"MATCH(n1:{n1})-[e:{t}]-(n2:{n2}) return collect(distinct e.id) as ids"
                    edge_ids_db1 = query(main_db, q).single()['ids']
                    edge_ids_db2 = query(other_db, q).single()['ids']
                    
                    if counts > 0 :# means db1 has more
                        for i in edge_ids_db1:
                            if i not in edge_ids_db2:
                                sample_ids.append(
                                    f"""MATCH p = (:{n1})-[:{t}{{id:'{i}'}}]-(:n2) return p"""
                                )
                            if len(sample_ids) >=  sample_size or len(sample_ids) == counts:
                                break
                    else:
                        for i in edge_ids_db2:
                            if i not in edge_ids_db1:
                                sample_ids.append(
                                    f"""MATCH p = (:{n1})-[:{t}{{id:'{i}'}}]-(:n2) return p"""
                                )
                            if len(sample_ids) >=  sample_size  or len(sample_ids) == counts:
                                break
                count_diffs[t]['samples'] = sample_ids
        report[f"{n1}<->{n2}"]['count_diffs'] = count_diffs
    # now for the test if everylist in the report is empty we are :)
    write_report_to_file(report, 'predicate_diff.json')
    assert reduce(
        lambda x, y: x and (reduce (
            lambda accu, key: accu and len(report[y][key]) == 0, 
            report[y].keys(), 
            True)           
            ), report , True), json.dumps(report, indent=4)