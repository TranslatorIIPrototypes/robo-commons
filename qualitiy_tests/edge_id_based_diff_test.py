from neo4j import GraphDatabase
import argparse, sqlite3

"""
These test assumes that the graphs to compare are normalized. 
This is important because edge id is computed using 
`md5(source_id, target_id, predicate_id)`.

This test helps identify edge that exist in one of the databases being inspected and not in the other.

Differences will be written in sqllite3 dump file. 

@TODO: Hookup as a post built script to Jenkins to notify if test fails. 
"""

def create_sqllite_db(db_name):
    connection = sqlite3.connect(f'{db_name}')
    cur = connection.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS edge
    """)
    connection.commit()
    cur.execute("""
        CREATE TABLE edge (
        edge_id VARCHAR,
        source_id VARCHAR,
        target_id VARCHAR,
        edge_source VARCHAR,
        source_labels VARCHAR,
        target_labels VARCHAR,
        edge_type VARCHAR, 
        missing_from type VARCHAR,
        PRIMARY KEY (edge_id)
        )
        """)
    connection.commit()
    return connection


def write_edge_info_to_sqlite3(edge_id, edge_info, missing_from, sql_connection):
    cursor = sql_connection.cursor()
    insert_statement = """
    INSERT INTO edge (edge_id, source_id, target_id, edge_source, source_labels, target_labels, edge_type, missing_from)
    VALUES (?,?,?,?,?,?,?,?)        
    """
    edge_source = [edge_info['edge_source']] if isinstance(edge_info['edge_source'], str) else edge_info['edge_source']
    edge_source = ','.join(edge_source)
    values = (
        edge_id,
        edge_info['source'],
        edge_info['target'],
        edge_source,
        ':'.join(edge_info['source_labels']),
        ':'.join(edge_info['target_labels']),
        edge_info['tp'],
        missing_from
    )
    cursor.execute(insert_statement, values)
    sql_connection.commit()



def setup_drivers(db_conf_1, db_conf_2):
    return GraphDatabase.driver(**db_conf_1), GraphDatabase.driver(**db_conf_2)


def get_edges(driver, filter = None):
    if filter:
        filter = f"AND {filter}"
    else:
        filter = ''
    query  = f"""
    MATCH (a)-[e]->(b) 
    WHERE 
        exists(e.id) 
        {filter}
    RETURN 
    e.id AS edge_id, 
    {{
        source: a.id,
        target: b.id,
        edge_source: e.edge_source,
        source_labels: labels(a),
        target_labels: labels(b),
        tp: type(e)
    }}
     AS edge_info
    """
    with driver.session() as session:
        return session.run(query)

def parse_edge_info(neo4j_result_set):
    result = {}
    for row in neo4j_result_set:
        result[row['edge_id']] = row['edge_info']
    return result

def make_filter(source_db):
    return f"""
    (
    e.source_database = "{source_db}" 
     OR 
    any(x in e.source_database WHERE x = "{source_db}" )
    )
    """

def write_diff_to_sql(edge_set1, edge_set2,db1_uri , db2_uri,  sql_connection):
    "Writes differences in sql lite3 db"
    different = False
    edge_ids_in_db1 =set(edge_set1.keys())
    edge_ids_in_db2 =set(edge_set2.keys())

    edges_in_db1_only = [i for i in edge_ids_in_db1 if i not in edge_ids_in_db2]
    edges_in_db2_only = [i for i in edge_ids_in_db2 if i not in edge_ids_in_db1]

    for ids in edges_in_db1_only:
        different = True
        write_edge_info_to_sqlite3(
            edge_id=ids,
            edge_info=edge_set1[ids],
            sql_connection=sql_connection,
            missing_from=db2_uri
        )
    for ids in edges_in_db2_only:
        different = True
        write_edge_info_to_sqlite3(
            edge_id=ids,
            edge_info=edge_set2[ids],
            sql_connection=sql_connection,
            missing_from=db1_uri
        )
    return different



def run_tests(driver_1, driver_2, source_db):
    sql_lite_conn = create_sqllite_db(source_db)
    db1_uri = driver_1.address[0] + str(driver_1.address[1])
    db2_uri = driver_2.address[0] + str(driver_2.address[1])
    print(f"Getting edges... This might take a bit time....")
    print("----- from database {db1_uri}")
    db1_edges = get_edges(driver_1, make_filter(source_db))
    print(f"----- from database {db2_uri}")
    db2_edges = get_edges(driver_2, make_filter(source_db))
    print("Parsing edges ---- parsing neo4j results")
    db1_edges = parse_edge_info(db1_edges)
    db2_edges = parse_edge_info(db2_edges)
    print(f"found {len(db1_edges)} VS {len(db2_edges)} for source -- {source_db}")
    print(f"Computing differences in edge list...")
    has_difference = write_diff_to_sql(
        edge_set1=db1_edges,
        edge_set2=db2_edges,
        db1_uri=db1_uri,
        db2_uri=db2_uri,
        sql_connection=sql_lite_conn
    )
    if has_difference:
        print(f'[!!!!!] Difference detected between {db1_uri} and {db2_uri}; Results are in sqlite db named `{source_db}`')
    else:
        print(f'[-] NO Difference detected between {db1_uri} and {db2_uri}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""
    Run edge id comparision among databases
    """)
    parser.add_argument('--uri1', help="""
    Neo4j bolt Uri of first database
    """)
    parser.add_argument("--pass1", help="""
    password of first database
    """)
    parser.add_argument("--user1", help="""
    User name of first database
    """)
    parser.add_argument('--uri2', help="""
        Neo4j bolt Uri of second database
        """)
    parser.add_argument("--pass2", help="""
        password of second database
        """)
    parser.add_argument("--user2", help="""
        User name of second database
        """)
    parser.add_argument("--source_db", help="""
        Source database we want to focus on, eg. ctd, uberongraph etc... 
    """)
    args = parser.parse_args()
    config_1 = {
        "uri": args.uri1,
        "auth": (
            args.user1,
            args.pass1
        )
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            }
    config_2 = {
        "uri": args.uri2,
        "auth": (
            args.user2,
            args.pass2
        )
    }
    driver_1, driver_2 = setup_drivers(config_1, config_2)
    run_tests(driver_1, driver_2, args.source_db)
