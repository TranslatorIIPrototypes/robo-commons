import redis
import pickle
import os
from collections import defaultdict
from neo4j import GraphDatabase
from functools import reduce
import yaml

with open(f"{os.environ.get('ROBOKOP_HOME')}/robokop-interfaces/greent/greent.conf") as conf_file:
    bad_ids = yaml.load(conf_file, Loader = yaml.FullLoader)['bad_identifiers']

redis_credenticals = {
    'host': os.environ.get('CACHE_HOST', 'localhost'),
    'port': os.environ.get('CACHE_PORT', '6380'),
    'password': os.environ.get('CACHE_PASSWORD')
}
neo4j_credentials = {
    'uri': f"bolt://{os.environ.get('NEO4J_HOST')}:{os.environ.get('NEO4J_BOLT_PORT')}",
    'auth': (
        'neo4j',
        os.environ.get('NEO4J_PASSWORD')
    )
}


def get_service_cached_counts(service_name):
    r = redis.StrictRedis(**redis_credenticals)
    keys = r.keys(pattern=f'*{service_name}*')
    key_chunks = [keys[start: start + 100000] for start in range(0, len(keys), 100000)]
    counts = defaultdict(int)
    for key_chunk in key_chunks:
        data = r.mget(key_chunk)
        for key, v in zip(key_chunk, data):
            x = pickle.loads(v)
            for edge, node in x:
                counts[key.decode('utf-8')] += 1
    return counts


def unwrap(ftext):
    """We know that there will only be nested parens in the first argument"""
    l = ftext.index('(')
    fname = ftext[:l]
    argstring = ftext[l + 1:-1]
    if ')' in argstring:
        r = argstring.rindex(')')
        arg0 = argstring[:r + 1]
        args = [arg0] + argstring[r + 2:].split(',')
    else:
        args = argstring.split(',')
    return fname, args


def get_real_function_and_curie(functiontext):
    parts = functiontext.split('(')
    curie = parts[-1].strip(')')
    functiontext = '('.join(parts[1:-1])
    functiontext = get_function_name(functiontext)
    return functiontext, curie


def get_function_name(functiontext):
    if '(' in functiontext:
        fname, args = unwrap(functiontext)
        if fname == 'output_filter':
            newargs = get_function_name(args[0])
            return newargs
        elif fname == 'upcast':
            newargs = get_function_name(args[0])
            return newargs
        elif fname == 'input_filter':
            newargs = get_function_name(args[0])
            if len(args) == 3:
                newargs = get_function_name(args[0])
            return newargs
    else:
        fname = '.'.join(functiontext.split(',')[0].split('~'))
        return fname


def process_possible_caster_calls(service_name, missing_ops, restructured_neo4j, prefix_map):
    """hopefully missing_ops wont be large to hang redis """
    missing_ops_lookup = set(map(lambda x: f"caster.*{x.replace('.', '~')}*", missing_ops))
    redis_keys = []
    redis_conn = redis.Redis(**redis_credenticals)
    for lookup in missing_ops_lookup:
        redis_keys += redis_conn.keys(lookup)
    # chunk up redis_keys for lookup
    print(f'found {len(redis_keys)} : caster*{"|".join(missing_ops)}*')
    chunked = [redis_keys[start: 100_000] for start in range(0, len(redis_keys), 100_000)]
    responses = {}
    for chunks in chunked:
        redis_responses = redis_conn.mget(chunks)
        for key, val in zip(chunks, redis_responses):
            responses[key.decode('utf-8')] = val
    # convert responses to a something that makes sense,

    ## 1. convert each key to main op only
    ## 2. grab curie and put it inside inner for the key
    ## 3. unpickle value and insert into value for inner curie key (make sure these are unique)
    ## 3.1 so essential caster(input.filter...)(curie) and caster(output.filter...)(curie) might
    ## have same entries so this step will make a union of those
    ## 4. compare against neo4j counts
    redis_structured_with_values = {}
    for key in responses:
        value = pickle.loads(responses[key])
        func, curie = get_real_function_and_curie(key)
        inner = redis_structured_with_values.get(func, {})
        inner_inner = inner.get(curie, set())
        # merge them into a set
        for v in value:
            inner_inner.add(v)
        inner[curie] = inner_inner
        redis_structured_with_values[func] = inner
    errors = {}
    for op in missing_ops:
        redis_values = redis_structured_with_values.get(op, None)
        if redis_values == None:
            print(f'oh boy still missing! {op} ... continue')
            continue
        for curie in redis_values:
            r_values = redis_values[curie]
            redis_count = len(r_values)
            neo4j_count = restructured_neo4j[op].get(curie, 0)
            if redis_count != neo4j_count:
                prefix = curie.split(':')[0]
                actual_prefix = prefix_map.get(prefix, prefix)
                curie = curie.replace(prefix, actual_prefix)
                neo_links = grab_all_relation_with_curie(service_name, curie)
                neo4j_all_eq_ids = set(reduce(lambda x, y: x + y['node_ids'], neo_links, []))
                neo4j_all_eq_ids_upper = [x.upper() for x in neo4j_all_eq_ids]
                redis_all_ids = set([x[1].id.upper() for x in r_values])
                not_in_neo = [x for x in redis_all_ids if x not in neo4j_all_eq_ids_upper and x not in bad_ids]
                if len(not_in_neo):
                    errors[f'{op}({curie})'] = {
                        'missing_in_neo4j': not_in_neo,
                        'curie': curie
                    }

    return errors


def get_node_edge_data(service_name):
    """
    Returns an edge between source and target,
    Note that this needs further processing
    EG:
     a service may be called for a gene
    """

    query = f"""
    MATCH (a:named_thing)-[e]->(b:named_thing) 
    WHERE any(x in e.edge_source WHERE x starts with '{service_name}.' )
    RETURN COLLECT ({{
    source_id: a.id, 
    source_eq: a.equivalent_identifiers,
    op: [e_s in e.edge_source where e_s starts with '{service_name}' | e_s  ], 
    target_id: b.id,
    target_eq: b.equivalent_identifiers
    }}) AS results
    """
    driver = GraphDatabase.driver(**neo4j_credentials)
    with driver.session() as s:
        results = s.run(query).single()['results']
    return results


def grab_all_relation_with_curie(service_name, curie):
    query = f"""
    MATCH (a:named_thing)-[e]-(b)
    USING INDEX a:named_thing(id)
    WHERE any(x in e.edge_source WHERE x starts with '{service_name}' )
    AND a.id = '{curie}'
    RETURN collect({{edge: e , node_ids: b.equivalent_identifiers}}) as edges
    """
    driver = GraphDatabase.driver(**neo4j_credentials)
    with driver.session() as s:
        results = s.run(query).single()['edges']
    return results


def inspect_errors(errors, service_name, prefix_map, size=0, chunk_size=1000):
    """
    This tries to cover cases where mistmatch could happen
    """
    # 1. Duplicate ids
    mismatches = errors.get('count_mismatch')
    if not mismatches:
        return {}
    keys = list(mismatches.keys())
    chunks = [keys[start: start + chunk_size] for start in range(0, size or len(keys), chunk_size)]
    r = redis.Redis(**redis_credenticals)
    pipeline = r.pipeline()
    still_has_errors = {}
    for chunk in chunks:
        for key in chunk:
            pipeline.get(key)
        redis_results = pipeline.execute()
        for key, value in zip(chunk, redis_results):
            curie = mismatches[key]['curie']
            value = pickle.loads(value)
            curie_prefix = curie.split(':')[0]
            actual_prefix = prefix_map[curie_prefix]
            curie = curie.replace(curie_prefix, actual_prefix)
            neo4j_connections = grab_all_relation_with_curie(service_name, curie)
            # merge all node ids from neo4j equivalent ids
            neo4j_all_eq_ids = set(reduce(lambda x, y: x + y['node_ids'], neo4j_connections, []))
            # converting to upper , found that test was reporting things like UniProtKB:P23141 in eqid ven though
            # they are present
            neo4j_all_eq_ids_upper = list(map(lambda x: x.upper(), neo4j_all_eq_ids))
            # get all redis ids
            redis_all_ids = set([x[1].id.upper() for x in value])
            not_in_neo = [x for x in redis_all_ids if x not in neo4j_all_eq_ids_upper and x not in bad_ids]
            if not_in_neo:
                still_has_errors[key] = {
                    "curie": curie,
                    "missing_ids": not_in_neo
                }
    return still_has_errors


def compare_neo4j_with_cache(cache_results, neo4j_results):
    """
    Things get messy here.
    potential pitfalls are:
    1. when parsing neo4j there is no garantee that the it's equivalent redis key is <opname>(source_id)
    for instance, if there was a neo4j result such as
        {  'op': ['chembio.graph_pubchem_to_ncbigene'],
          'target_id': 'NCBIGene:2939',
          'source_id': 'CHEBI:2244'}
    we may need to check redis for both
    chembio.graph_pubchem_to_ncbigene(CHEBI:2244)
    and chembio.graph_pubchem_to_ncbigene(NCBIGENE:2939)
    """

    # restructring data for easier analysis
    # we are going to format them as {op: {curie: count}}
    restructured_neo4j = {}
    prefix_map = {}
    for x in neo4j_results:
        ops = x['op']
        # redis keys are always upper case curies. To match that upper case here
        # use this to replace prefix
        prefixes = x['source_id'].split(':')[0], x['target_id'].split(':')[0]
        for p in prefixes:
            prefix_map[p.upper()] = prefix_map.get(p.upper(), p)
        curies = [x['source_id'].upper(), x['target_id'].upper()]
        # here we essentially are abstracting away direction of edges
        for op in ops:
            inner = restructured_neo4j.get(op, {})
            for curie in curies:
                count = inner.get(curie, 0)
                count += 1
                inner[curie] = count
            restructured_neo4j[op] = inner
    restructured_redis = {}
    # sometimes some services are called through the caster and are stored in redis
    # like `caster.input_filter(pharos~disease_get_gene,disease,typecheck~is_disease)(CURIE)` etc...
    # so capture those too
    neo4j_potential_op_to_redis_caster_function_key_map = {}
    for x in cache_results:
        key_parts = x.split('(')
        op_org = '('.join(key_parts[:-1])
        curie = key_parts[-1].strip(')')
        inner = restructured_redis.get(op_org, {})
        count = cache_results[x]
        inner[curie] = count
        restructured_redis[op_org] = inner
        extract_func_name = lambda x: x.split('(')[-1].split(',')[0].replace('~', '.')
    #         if op_org.startswith('caster.'):
    #             op = extract_func_name(op_org)
    #             redis_ops = neo4j_potential_op_to_redis_caster_function_key_map.get(op, set())
    #             redis_ops.add(op_org)
    #             neo4j_potential_op_to_redis_caster_function_key_map[op] = redis_ops
    # errors bucket
    from collections import OrderedDict
    errors = OrderedDict()

    # stage 1 check if all op's are in one or the other
    ops_redis = set(map(lambda x: extract_func_name(x) if x.startswith('caster.') else x, restructured_redis.keys()))

    ops_neo4j = set(restructured_neo4j.keys())

    print("comparing services are all present in redis and ")
    r_only, n_only = ops_redis.difference(ops_neo4j), ops_neo4j.difference(ops_redis)
    print(" redis only : ", r_only)
    print("neo4j only : ", n_only)
    if len(r_only) or len(n_only):
        errors['Some op keys are totally missing from either neo4j or redis '] = {
            'neo4j_only': n_only,
            'redis_only': r_only
        }
    print('##################################')
    print('stage 1: Check for `op missing totally` complete ')
    print(f'\t errors:  {errors}')
    print('##################################')
    # stage 2
    # check for counts
    # we have to avoid getting ops we already know not to exist in redis
    #     return restructured_neo4j, restructured_redis
    matches = set()
    skipped_op_missing_redis_entry = set()
    for op in [x for x in restructured_neo4j if x not in n_only]:
        for curie in restructured_neo4j[op]:
            # not all curies in the neo4j data will have a corresponding redis key
            # but if it does exist in redis we expect it to have equal count of edges

            redis_count_set = restructured_redis.get(op, None)
            if redis_count_set == None:
                print(f'Skipping check for missing Redis Key  {op} ' )
                skipped_op_missing_redis_entry.add(op)
                break
            redis_count = 0

            redis_count = redis_count_set.get(curie, None)
            # errors['missing_redis_things'] = errors.get('missing_redis_things', defaultdict(set))
            # errors['missing_redis_things'][op].add(curie)

            if redis_count:
                neo4j_count = restructured_neo4j[op].get(curie)
                if redis_count != neo4j_count:
                    count_mismatch = errors.get('count_mismatch', {})
                    count_mismatch[f'{op}({curie})'] = {
                        'neo4j_count': neo4j_count,
                        'redis_count': redis_count,
                        'curie': curie
                    }
                    errors['count_mismatch'] = count_mismatch
                else:
                    matches.add(f'{op}({curie})')
    print('##################################')
    print('stage 2: Check for count mismatch complete')
    total_redis_keys = len(cache_results)
    error_count = len(errors.get('count_mismatch', []))
    if error_count == 0:
        print('All seems fine exiting...')
        if len(skipped_op_missing_redis_entry) == 0:
            return
    print(
        f'Some counts were not matching : {error_count}/{total_redis_keys} ({(error_count / total_redis_keys) * 100}%)')
    print('##################################')
    #     return errors, neo4j_potential_op_to_redis_caster_function_key_map, restructured_neo4j, restructured_redis
    # stage 3 diagnosis
    # here we try to explain why we have mistmatch
    print('##################################')
    print('stage 3: Fetching mismatching keys and corresponding neo4j relationships')
    still_has_errors = inspect_errors(errors, service_name, prefix_map)
    errors['count_mismatch'] = [x for x in errors.get('count_mismatch', []) if x in still_has_errors]
    if len(errors['count_mismatch']) == 0:
        print('Neo4j data has been compared with redis results all seem to exist in neo4j.')
        if len(skipped_op_missing_redis_entry)== 0:
            print('exiting ...')
            return
    if errors.get('count_mismatch'):
        print(f"""found errors some keys still have mismatch counts after inspecting neo4j...
                  {len(errors['count_mismatch'])} / {error_count} ({(len(errors.get('count_mismatch')) / error_count) * 100}%)
              """)

    # stage 4 if there were skipped redis keys go back and get caster things
    if len(skipped_op_missing_redis_entry):
        print('#####################')
        print('starting stage 4, checking back missing redis ops for caster')
        e = process_possible_caster_calls(service_name=service_name,
                                      missing_ops=skipped_op_missing_redis_entry,
                                      restructured_neo4j= restructured_neo4j,
                                      prefix_map=prefix_map)
        if e != {}:
            print('stage 4 completed with errors ... dumping errors')
            errors['stage_4_still_missing'] = e

    for op in errors.get('missing_redis_things', {}):
        # convert these to list for reporting
        errors['missing_redis_things'][op] = list(errors['missing_redis_things'][op])
    return {'neo4j_errors': still_has_errors,
            'redis_errors': errors.get('missing_redis_things', {}),
            'stage4_still_missing': errors.get('stage_4_still_missing', {})

    }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
            Compare neo4j with cache.
            """, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--service', required=True)
    args = parser.parse_args()
    service_name = args.service
    neo4j_edge_node_data = get_node_edge_data(service_name)
    redis_edge_node_data = get_service_cached_counts(service_name)
    errors = compare_neo4j_with_cache(redis_edge_node_data, neo4j_edge_node_data)
    with open(f'{service_name}.cache_compare.json', 'w') as f:
        import json
        json.dump(errors, f, indent=4)
