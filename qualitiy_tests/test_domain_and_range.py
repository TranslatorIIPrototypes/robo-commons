"""
Gets the types of edges that are in the database.
And lookup the edge in the biolink lookup service(https://bl-lookup-sri.renci.org).
There it will try to look for the domain and range of the edge definition,
if those are defined. Those will be used to check if proper
directionality is used for the types in the built database.
"""
import json
import os
import pytest
import requests
from neo4j import GraphDatabase

BL_LOOKUP_URL = 'https://bl-lookup-sri.renci.org'
BL_VERSION = 'custom'
REPORT_FILE_NAME = 'domain_range_report.json'

NEO4J_HOST = 'robokopdev.renci.org' #os.environ.get("NEO4J_HOST")
NEO4J_BOLT_PORT = '7689' #os.environ.get("NEO4J_BOLT_PORT")
NEO4J_USER = 'neo4j' #os.environ.get('NEO4J_USER'),
NEO4J_PASSWORD = 'ncatsgamma' #os.environ.get('NEO4J_PASSWORD')

@pytest.fixture()
def neo4j_driver():
    return GraphDatabase.driver(
        uri=f'bolt://{NEO4J_HOST}:{NEO4J_BOLT_PORT}',
        auth=(
            NEO4J_USER,
            NEO4J_PASSWORD
        )
    )



def test_domain_range_match(neo4j_driver):
    query = """
    MATCH (a)-[e]->(b) 
        WHERE NOT a:Concept AND NOT b:Concept 
    RETURN DISTINCT 
        LABELS(a) AS subject, 
        TYPE(e) AS predicate, 
        LABELS(b) AS object
    """
    report = {
        'errors': {},
        'warnings': [],
        'lookup_errors': []
    }
    with neo4j_driver.session() as session:
        rows = session.run(query)
    for triplet in rows:
        # get the full definition of the predicate from bl-lookup
        source_labels = triplet['subject']
        target_labels = triplet['object']
        predicate = triplet['predicate']
        full_url = f'{BL_LOOKUP_URL}/bl/{predicate}?version={BL_VERSION}'
        response = requests.get(full_url)
        # make sure we have a successful call to bl-lookup
        if response.status_code != 200:
            error = f'failed to get data for url {full_url} for triplet {(source_labels, predicate, target_labels)}.'
            if error not in report['lookup_errors']:
                report['lookup_errors'].append(error)
            continue
        response = response.json()
        # check if we have something to work with
        if 'domain' not in response or 'range' not in response:
            error = f'could not find domain and range definition for type {predicate} on url {full_url}.'
            if error not in report['warnings']:
                report['warnings'].append(error)
            continue
        # check if we have valid associations
        snakify = lambda x: x.replace(' ', '_')
        domain = snakify(response['domain'])
        range = snakify(response['range'])
        report['errors'][predicate] = []
        errors = report['errors'][predicate]
        if domain not in source_labels:
            error = f'Domain({domain}) is expected to be in subject labels from db({source_labels})'
            if error not in errors:
                errors.append(error)
        if range not in target_labels:
            error = f'Range({range}) is expected to be in object labels from db({target_labels})'
            if error not in errors:
                errors.append(error)

    to_remove = []
    for error in report['errors']:
        if not report['errors'][error]:
            to_remove.append(error)

    for remove in to_remove:
        del report['errors'][remove]

    with open(REPORT_FILE_NAME, 'w') as report_file:
        json.dump(report, report_file, indent=2)
    for k in report:
        assert not report[k], f'errors/warnings found check {REPORT_FILE_NAME}'
