import pytest
from neo4j import GraphDatabase
from functools import reduce
import json
from greent.node_types import node_types
import os

DB1_CONF = {
    'uri': "bolt://robokopdev.renci.org:7687",
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


def assert_report(report):
    ## This collects all the test values and reduces them to a single value with `and`. 
    return reduce(
        lambda x , y: 
            x and reduce(
                lambda f, j: 
                    f and report[y][j]['value']
                , 
                report[y],
            True)
    , report, 
    True)



def test_concept_type_nodes_counts(db1_driver, db2_driver):
    q = """
    MATCH (c:Concept) return labels(c) as labels,  count(c) as count
    """
    db1_results = {', '.join(x['labels']): x['count'] for x in query(db1_driver, q)}
    db2_results = {', '.join(x['labels']): x['count'] for x in query(db2_driver, q)}
    report = {
        'db1_results': {},
        'db2_results': {}
    } 
    report['equal_length']= {'db1_vs_db2': {'value': len(db1_results) == len(db2_results)}}
    for i in db1_results:        
        report['db1_results'][i] = {'value': i in db2_results, 'diff': db1_results[i] - db2_results[i] if i in db2_results else -1 }
    for i in db2_results:
        report['db2_results'][i] = {'value': i in db1_results, 'diff': db2_results[i] - db1_results[i] if i in db1_results else -1 }
    write_report_to_file(report, 'concept_type_node_counts.json')
    assert assert_report(report), json.dumps(report, indent= 4)

def get_concepts(driver):
    q = """
    MATCH (c:Concept) return c.name as name ORDER BY c.name
    """
    return [r['name'] for r in query(driver, q)]



def test_node_type_counts(db1_driver, db2_driver):
    main_concepts = get_concepts(db1_driver)
    other_concepts = get_concepts(db2_driver)
    in_db1_only, in_db2_only = sorted_list_diff(main_concepts, other_concepts)
    report = {
        'results': {},
        'equal_length': {
            'db1_vs_db2':{
                'value' : len(in_db1_only) == len(in_db2_only) and len(in_db1_only) == 0,
                'diff': {
                    f"in_{DB1_CONF['uri']}_only": in_db1_only,
                    f"in_{DB2_CONF['uri']}_only": in_db2_only,
                },
                'comment': 'If any concepts are exclusive to one DB it would be listed here.'
                }
            }
    }
    exists_in_both = list(filter(lambda x : x not in in_db1_only, main_concepts))

    for concept in exists_in_both:
        q = f"""
        MATCH (c:{concept}) return count(c) as count 
        """
        db1_count = query(db1_driver, q).single()['count'] 
        db2_count = query(db2_driver, q).single()['count'] 
        report['results'][concept] = {
            'value': db1_count == db2_count,
            'diff': db1_count - db2_count
        }
    write_report_to_file(report,'count_of_all_concepts.json')
    assert assert_report(report)        
 


def xtes_properties_containing_space(main_db):
    concepts = get_concepts(main_db)
    report = {} 
    for c in concepts:
        q = f""" MATCH (c:{c}) 
        WHERE ANY(
            x in keys(c) 
            where x contains ' '
            ) 
        return collect(c.id) as ids"""
        ids = query(main_db, q).single()['ids']
        if len(ids) :
            report[c] = ids
    assert len(report) == 0