from neo4j import GraphDatabase
import requests, json

class DB:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri=uri, auth=auth)

    def run(self, query):
        results = []
        with self.driver.session() as session:
            response = session.run(query)
        for row in response:
            results.append(dict(**row))
        return results


def get_graph_summary_raw(driver: DB):
    query = """
    MATCH (source)-[edge]->(target) 
    WITH COUNT(source) AS source_count,  
         LABELS(source) AS source_labels,
         LABELS(target) AS target_labels,
         TYPE(edge) AS edge_type,
         edge.source_database AS source_dbs,
         count(*) as path_count
    UNWIND( source_dbs) AS source_db 
    RETURN source_count, 
           source_labels, 
           target_labels,
           edge_type,
           source_db, 
           path_count

    """
    return driver.run(query)


def get_automat_graph_summary():
    """
    Grab the graph summary for a kg
    """
    registry = 'https://automat.renci.org/registry'
    db_names = requests.get(registry).json()
    result = {}
    for kg_name in db_names:
        url = f'https://automat.renci.org/{kg_name}/graph/summary'
        response = requests.get(url)
        if response.status_code != 200:
            print(f'failed to get response for {url}', response.status_code, response.text)
            continue
        result[kg_name] = response.json()
    return result


def organize_by_source_db(g_summary):
    """
    returns a dictionary of
    {
    source_db_name: {
     'sourcelabels': {
       'targetlabels': {
           'edge_type': count
       }
     }
    }
    }


    """
    organized = {}
    for item in g_summary:
        db_name = item['source_db']
        db_summary = organized.get(db_name, {})
        s_lbs = ':'.join(item['source_labels'])
        t_lbs = ':'.join(item['target_labels'])
        # initialize if null else use values
        #  {slbs: {
        #   tlbs: {
        #   edge_type:
        ##}
        # }}
        #
        s_summary = db_summary.get(s_lbs, {})
        db_summary[s_lbs] = s_summary
        t_summary = s_summary.get(t_lbs, {})
        s_summary[t_lbs] = t_summary
        edge_type = item['edge_type']
        edge_count = item['path_count']
        t_summary[edge_type] = edge_count
        organized[db_name] = db_summary
    return organized


def get_diff(new, old, new_db_name, old_db_name):
    diff = {}
    valid = True
    # find none existing keys
    # elminate node_counts
    new_node_types = list(filter(lambda x: x != 'nodes_count', new.keys()))
    old_node_types = list(filter(lambda x: x != 'nodes_count', old.keys()))
    diff[f'types_in_{old_db_name}_only'] = [node_type for node_type in old_node_types if
                                            node_type not in new_node_types]
    diff[f'types_in_{new_db_name}_only'] = [node_type for node_type in new_node_types if
                                            node_type not in old_node_types]
    disjoin = diff[f'types_in_{old_db_name}_only'] + diff[f'types_in_{new_db_name}_only']
    print(new_node_types, old_node_types)
    if disjoin:
        valid = False
    for node_type in new:
        diff_per_node_type = {}
        if node_type in disjoin:
            continue
        old_target_set = old[node_type]
        new_target_set = new[node_type]
        # compare targets like before
        diff_per_node_type[f'target_nodes_in_{old_db_name}_only'] = [node_type for node_type in old_target_set
                                                                     if node_type not in new_target_set]
        diff_per_node_type[f'target_nodes_in_{new_db_name}_only'] = [node_type for node_type in new_target_set
                                                                     if node_type not in old_target_set]
        per_type_disjoin = diff_per_node_type[f'target_nodes_in_{old_db_name}_only'] + \
                           diff_per_node_type[f'target_nodes_in_{new_db_name}_only']
        # Compare edgesets
        if per_type_disjoin:
            valid = False
        for target_node_type in new_target_set:
            if target_node_type in per_type_disjoin or target_node_type == 'nodes_count':
                continue
            # compare edges
            diff_per_edge_set = {
                'edge_count_diff': []
            }
            new_build_edge_set = new_target_set[target_node_type]
            old_build_edge_set = old_target_set[target_node_type]
            edges_in_previous_build_only = [x for x in old_build_edge_set if x not in new_build_edge_set]
            edges_in_current_build_only = [x for x in new_build_edge_set if x not in old_build_edge_set]
            if edges_in_previous_build_only:
                diff_per_edge_set[f'edges_in_{old_db_name}_only'] = {
                    'description': [node_type, target_node_type],
                    'edges': edges_in_previous_build_only
                }
            if edges_in_current_build_only:
                diff_per_edge_set[f'edges_in_{new_db_name}_only'] = {
                    'description': [node_type, target_node_type],
                    'edges': edges_in_current_build_only
                }
            edges_disjoin = edges_in_current_build_only + edges_in_previous_build_only
            if edges_disjoin:
                valid = False
            diff_per_edge_set['edge_count_diff'] = {}
            for edge, new_build_edge_count in new_build_edge_set.items():
                if edge in edges_disjoin:
                    continue
                edge_count_diff = old_build_edge_set[edge] - new_build_edge_count
                diff_message = {
                    old_db_name: old_build_edge_set[edge],
                    new_db_name: new_build_edge_count,
                    'diff': edge_count_diff
                }
                diff_per_edge_set['edge_count_diff'].update({edge: diff_message})

            diff_per_node_type[target_node_type] = diff_per_edge_set
        # add node type diff to main diff
        diff[node_type] = diff_per_node_type
    diff['meta_data'] = {
        'db1': old_db_name,
        'db2': new_db_name
    }
    return diff, valid


def run_diff_test(connection_params):
    local_graph_db = DB(**connection_params)
    local_graph_summary = organize_by_source_db(get_graph_summary_raw(local_graph_db))
    automat_graph_summary = get_automat_graph_summary()

    # ---- rename some kp's to the source_database we can lookup in big graph

    rename_map = {
        'uberon': 'uberongraph'
    }
    for kg in rename_map:
        if kg in automat_graph_summary:
            automat_graph_summary[rename_map[kg]] = automat_graph_summary[kg]
            del automat_graph_summary[kg]

    # ----- end rename
    files = []
    for kg in local_graph_summary:
        if kg in automat_graph_summary:
            # calculate edge diffs
            difference = get_diff(old=local_graph_summary[kg],
                                  old_db_name=f'robokop_{kg}',
                                  new=automat_graph_summary[kg],
                                  new_db_name=f'automat_{kg}'
                                  )
            # positive values means local kg has more, negative automat version has more.

            with open(f'{kg}_diff.json', 'w') as kg_diff_file:
                json.dump(difference, kg_diff_file, indent=4)
                files.append(f'{kg}_diff.json')
    return files


if __name__ == '__main__':
    import os
    connection_params = {
        'uri': f'bolt://{os.environ.get("NEO4J_HOST")}:{os.environ.get("NEO4J_BOLT_PORT")}',
        'auth': (
            'neo4j', os.environ.get('NEO4J_PASSWORD')
        )
    }
    files = run_diff_test(connection_params)

