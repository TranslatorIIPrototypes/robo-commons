from crawler.service_based_crawler import get_supported_types
from crawler.program_runner import load_all
import argparse
from builder.question import LabeledID
from greent.rosetta import Rosetta
import sqlite3

"""
Uses sqllite db file output from edge_id_based_diff_test.py to sync db. 
"""

def bake_programs(triplet, rosetta, identifier_list=[]):
    """
    So here we have (source, predicate op , target)
    if we filter ops with our collection we should be fine
    """
    source, operations, target = triplet
    load_all(source,target, rosetta, 10, op_list = operations, identifier_list=identifier_list)


def run(id_list, service):
    rosy = Rosetta()
    triplets = get_supported_types(service_name=service, rosetta=rosy)

    for triplet in triplets:
        # here a triplet contains something like
        # 'gene' or 'disease' coming from the name attr of concept graph
        # this mini 'crawl' should run for a type that exists in the keys
        # of the grouped types. The keys look something like
        # `gene:gene_or_gene_product:macromolecular ...`
        key = list(filter(lambda b: triplet[0] in b, id_list.keys()))
        if not len(key):
            # if there is no match continue for others
            continue
        key = key[0]
        identifiers = [LabeledID(identifier=y) for y in id_list[key]]
        print(f'running {triplet[0]} --> {triplet[2]}')
        bake_programs(triplet,rosy, identifier_list=identifiers)


def group_by_type(file_name):
    db = sqlite3.connect(file_name)
    cur = db.cursor()
    result = cur.execute(f"""
    SELECT source_id, target_id , edge_source, source_labels, target_labels from edge
    """
    )
    ids_by_type = {}
    missing = []
    for source_id, target_id, edge_source, source_labels, target_labels in result:
        missing.append({
            'source': source_id,
            'target': target_id,
            'edge_source': edge_source,
            'source_labels': source_labels,
            'target_labels': target_labels
        })
    for m in missing:
        source_type = m['source_labels']
        target_type = m['target_labels']
        if source_type not in ids_by_type:
            ids_by_type[source_type] = []
        if target_type not in ids_by_type:
            ids_by_type[target_type] = []
        ids_by_type[source_type].append(m['source'])
        ids_by_type[target_type].append(m['target'])
    return ids_by_type





if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--service', help="service to run")
    args = parser.parse_args()
    if not args.service:
        print('service is required arg')
        exit()
    try:
        f = open(args.service)
    except IOError:
        print(f'Make sure sqllite file {args.service} exists!')
        exit(1)
    finally:
        f.close()
    id_list = group_by_type(args.service)
    run(id_list, args.service)







