import pytest
from neo4j import GraphDatabase
from functools import reduce
import json
from greent.node_types import node_types

@pytest.fixture()
def main_db(rosetta):
    return GraphDatabase.driver(uri="bolt://robokopdb1.renci.org:7687", auth=('neo4j', 'ncatsgamma'))

@pytest.fixture()
def other_db(rosetta):
    return GraphDatabase.driver(uri="bolt://robokopdev.renci.org:7687", auth=('neo4j', 'ncatsgamma'))

def query(driver, q):
    with driver.session() as session:
        return session.run(q)

def test_concept_counts(main_db, other_db):
    q = """
    MATCH (c:Concept) return count(c) as count
    """
    main_result = query(main_db, q).single()['count']
    other_result = query(other_db, q).single()['count']
    assert main_result == other_result

def get_concepts(driver):
    q = """
    MATCH (c:Concept) return c.name as name
    """
    return [r['name'] for r in query(driver, q)]



def test_node_type_counts(main_db, other_db):
    main_concepts = get_concepts(main_db)
    other_concepts = get_concepts(other_db)
    assert len(main_concepts) == len(other_concepts)
    for x in main_concepts:
        assert x in other_concepts   
    concept_diff = {}
    for x in main_concepts:
        q = f"""
        MATCH (c:{x}) return count(c) as count
        """
        concept_diff[x] = query(main_db, q).single()['count'] == query(other_db, q).single()['count']
    
    assert reduce(lambda x, y: x and concept_diff[y] , concept_diff, True), json.dumps(concept_diff, indent= 4)
    
def test_predicates_types_between_concepts(main_db, other_db):
    report = {}
    #premitive pairing
    pairs = []
    for t in node_types:
        for t1 in node_types:
            pairs.append((t, t1))    
    # now we ask for predicates for each pair 
    for n1, n2 in pairs:
        q = f""" MATCH (n1:{n1})-[e]-(n2:{n2}) return distinct type(e) as type, count(e) as count ORDER BY type"""
        db1_type_list = {r['type']: r['count'] for r in query(main_db, q)}
        db2_type_list = {r['type']: r['count'] for r in query(main_db, q)}
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
                count_diffs[t] = db1_type_list[t] - db2_type_list[t]
        report[f"{n1}<->{n2}"]['count_diffs'] = count_diffs
    # now for the test if everylist in the report is empty we are :)
    assert reduce(
        lambda x, y: x and (reduce (
            lambda accu, key: accu and len(report[y][key]) == 0, 
            report[y].keys(), 
            True)           
            ), report , True), json.dumps(report, indent=4)


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



# def test_all_concepts_count(rosetta, main_db, other_db):


def test_properties_containing_space(main_db):
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