import requests
from neo4j import GraphDatabase
import os
import pytest
import json
from functools import reduce


DB1_CONF = {
    'uri': "bolt://robokopdev.renci.org:7687",
    'auth': (
        'neo4j',
        os.environ['NEO4J_PASSWORD']
    )
}

ANSWER_SOURCE_URL = True

answers = [
    '442ef339-b3eb-4260-9c77-a5559bbb21cc_ca2effb1-3938-400d-8ea9-2feb3fd2f980'
]


def write_report_to_file (report, file_name):
    with open(f'reports/{file_name}', 'w') as report_file:
        json.dump(report, report_file, indent= 4 )



@pytest.fixture()
def db1_driver():
    return GraphDatabase.driver(uri = DB1_CONF['uri'], auth = DB1_CONF['auth'])

def query(graph_driver, q):
    with graph_driver.session() as session:
        return session.run(q)


def get_answer_from_file(answer_file_name):
    with open(answer_file_name) as answer_file:
        return json.load(answer_file)

def get_answer_from_url(answer_url):
    return requests.get(f'https://robokop.renci.org/api/a/{answer_url}').json()

def get_answer(resouce_path):
    if ANSWER_SOURCE_URL:
        return get_answer_from_url(resouce_path)
    else:
        return get_answer_from_file(resouce_path)
# this test will run and look into a db if any of the answers have changed
def test_question_node_types_exist(db1_driver):
    # first we need to make sure that all we are asking for is not missing
    for answer in answers:
        a = get_answer(answer)
        # lets look for the nodes
        question_nodes = a['question_graph']['nodes']       
        make_query = lambda node : f"MATCH (n:{node['type']}) return count(n) as count" 
        report = {
            f"""{node['type']}{f"({node['id'] if 'id' in node else ''})"}""": {
                'exists' : query(db1_driver, make_query(node)).single()['count'] > 0
            } for node in question_nodes
        }
    assert reduce(lambda x,y : x and report[y]['exists'], report , True)
    
def test_question_node_curies_exist(db1_driver):
    for answer in answers: 
        question_nodes = get_answer(answer)['question_graph']['nodes']
        with_curies = filter(lambda node: 'curie' in node, question_nodes)
        map_type_vs_curies = {}
        for node in with_curies:
            if node['type'] not in map_type_vs_curies:
                map_type_vs_curies[node['type']] = []
            map_type_vs_curies[node['type']] += node['curie']
        report = {}
    
        for node_type in map_type_vs_curies:
            report[node_type] = []
            for curies in map_type_vs_curies[node_type]:
                exists = query(db1_driver, f"MATCH (c:{node_type}{{id: '{curies}'}}) return count(c) as count").single()['count'] > 0
                if not exists:
                    report[node_type].append(curies)
        assert reduce(lambda x, y: x and len(report[y]) == 0, report, True )

def test_answer(db1_driver):
    # lets say we have a list of resource somewhere,
    for answer in answers :
        answer = get_answer(answer)
        q_nodes = answer['question_graph']['nodes']
        q_edges = answer['question_graph']['edges']
        answer_set = answer['answers']
        map_node_id_to_type = {node['id'] : node['type'] for node in q_nodes}
        map_edge_source_target = {
            edge['id'] : {
                'source_type': map_node_id_to_type[edge['source_id']] ,
                'target_type': map_node_id_to_type[edge['target_id']],
                'source_id': edge['source_id'],
                'target_id': edge['target_id']
            }
            for edge in q_edges
            }
        node_bindings = {}
        edge_bindings = {}
        for ans in answer_set:
            for node in ans['node_bindings']:
                tp = map_node_id_to_type[node]
                if tp not in node_bindings:
                    node_bindings[tp] =  []
                node_bindings[tp] += ans['node_bindings'][node]
            for edge_id in ans['edge_bindings']:
                # we don't care of support edges here
                
                if edge_id[0] == 'e':
                    source_node = map_edge_source_target[edge_id]['source_id']
                    target_node = map_edge_source_target[edge_id]['target_id']
                    if edge_id not in edge_bindings :
                        edge_bindings[edge_id] = []
                    edge_bindings[edge_id] += [{
                        'source_curies': ans['node_bindings'][source_node],
                        'target_curies': ans['node_bindings'][target_node],
                        'edge_ids': ans['edge_bindings'][edge_id]
                    }]
        # now that we have our nodes organized with types we can query neo4j 
        report = {
            'nodes': {},
            'edges': {},
        }
        for ty in node_bindings:
            for curie in node_bindings[ty]:
                exists = query(db1_driver, f"MATCH (c:{ty}{{id: '{curie}'}}) return c ").single() != None
                if not exists:
                    # report it 
                    if ty not in report['nodes']:
                        report['nodes'][ty] = []
                    report['nodes'][ty].append(curie)
        for edge_id in edge_bindings:            
            source_target = map_edge_source_target[edge_id]
            for e_binds in edge_bindings[edge_id]:
               
                target_curies = e_binds['target_curies']
                for s_curie, e_neo_ids  in zip(e_binds['source_curies'], e_binds['edge_ids']):                    
                    q = f"""MATCH (:{source_target['source_type']}{{id: '{s_curie}'}})-[e{{id : '{e_neo_ids}'}}]->(t:{source_target['target_type']}) RETURN collect(t.id) as target_ids, e""" 
                    # ?? should this always be of one value,  sure ... 
                    query_result =  query(db1_driver,q).single()
                    target_id = query_result['target_ids'] if query_result else []
                    edge_meta =  {
                        'edge_source': query_result['e']['edge_source'],
                        'predicate_id': query_result['e']['predicate_id']
                    } if query_result else {}                             
                    exists = len(target_id) == 1 and  target_id[0] in target_curies
                    if not exists:
                        if edge_id not in report['edges']:
                            report['edges'][edge_id] = {
                                'source_target': source_target,
                                'missing_edge_ids': []
                            }
                        if 'queries' not in e_binds:
                            e_binds['queries'] = []
                        e_binds['queries'].append(q)
                        e_binds['edge_meta'] = edge_meta
                        report['edges'][edge_id]['missing_edge_ids'].append(e_binds)
        no_missing_nodes = reduce(lambda x, y: x and len(report['nodes'][y]) == 0, report['nodes'], True)
        no_missing_edges = reduce(lambda x, y: x and len(report['edges'][y]['missing_edge_ids']) == 0,report['edges'], True )

        write_report_to_file(report, 'answer_set_checks.json')
        assert no_missing_edges and no_missing_nodes, json.dumps(report, indent= 4)
                
        






