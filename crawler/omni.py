import pickle
import datetime
import psycopg2
from collections import defaultdict
from functools import lru_cache

bad_ids = defaultdict(list)
bad_ids['CL'].append('CL:0000000')
bad_ids['HP'].append('HP:0000001')
max_piped = 1000000


def create_omnicache(rosetta):
    redis = rosetta.cache.redis
    key = 'OmnicorpPrefixes'
    value = redis.get(key)
    if value is None:
        pairset = set()
    else:
        pairset = pickle.loads(value)
    p=['CHEBI','CHEMBL.COMPOUND','CL','DRUGBANK','ECTO','EFO','ENVO','FOODON','GO','HANCESTRO','HP','MONDO','NCBIGene','NCBITaxon','PUBCHEM','UBERON','UMLS']
    sizes={}
    for i,prefix_i in enumerate(p):
        for prefix_j in p[i:]:
            conn = create_connection(rosetta)
            print(prefix_i,prefix_j)
            #cacheit(prefix_i, prefix_j, conn, redis)
            cacheit2(prefix_i, prefix_j, conn, redis, pairset,sizes)
            conn.close()

def update_omnicache(rosetta,p1,p2):
    """Use this one to add a single pair to the cache.  But if you find yourself doing this,
    add the pair of interest to the build method above."""
    conn = create_connection(rosetta)
    redis = rosetta.cache.redis
    cacheit(p1,p2, conn, redis)

def create_connection(rosetta):
    context = rosetta.service_context
    db = context.config['OMNICORP_DB']
    user = context.config['OMNICORP_USER']
    port = context.config['OMNICORP_PORT']
    host = context.config['OMNICORP_HOST']
    pw = context.config['OMNICORP_PASSWORD']
    #print(db)
    #print(user)
    #print(port)
    #print(host)
    #print(pw)
    return psycopg2.connect(dbname=db, user=user, host=host, port=port,password=pw)


def dump(k, v, pipe):
    if len(k) == 0:
        return
    key = f'OmnicorpSupport({k[0]},{k[1]})'
    # outf.write(f'SET {key} {pickle.dumps(v)}\n')
    pipe.set(key, pickle.dumps(v))

def dump_count(k,v,pipe):
    if len(k) == 0:
        return
    key = f'OmnicorpSupport_count({k[0]},{k[1]})'
    # outf.write(f'SET {key} {pickle.dumps(v)}\n')
    pipe.set(key, pickle.dumps(v))

def update_prefixes(p1, p2, redis):
    key = 'OmnicorpPrefixes'
    value = redis.get(key)
    if value is None:
        pairset = set()
    else:
        pairset = pickle.loads(value)
    pairset.add((p1, p2))
    redis.set(key, pickle.dumps(pairset))

def cacheit2(p1,p2,conn,redis,pairset,sizes):
    start = datetime.datetime.now()
    p1,p2 = sorted([p1,p2])
    if (p1,p2) in pairset:
        print('Already computed')
        return
    if p1 not in sizes:
        sizes[p1] = get_curie_count(p1,conn)
    if p2 not in sizes:
        sizes[p2] = get_curie_count(p2,conn)
    if sizes[p1] > sizes[p2]:
        small_prefix = p2
        large_prefix = p1
    else:
        small_prefix = p1
        large_prefix = p2
    print('Go!')
    print('get dicts')
    curie_1_to_pmid = get_curie_to_pmid(small_prefix,conn)
    pmid_to_curie_2 = get_pmid_to_curie(large_prefix,conn)
    num_piped=0
    print('start')
    n=0
    with redis.pipeline() as pipe:
        for curie_1 in curie_1_to_pmid:
            pmids = curie_1_to_pmid[curie_1]
            shareds = { p:pmid_to_curie_2[p] for p in pmids }
            inv = defaultdict(list)
            for p,cs in shareds.items():
                for c in cs:
                    inv[c].append(p)
            for curie_2,pubs in inv.items():
                curie_1,curie_2 = sorted( [curie_1, curie_2] )
                ckey = (curie_1, curie_2)
                n+=1
                dump(ckey, pubs, pipe)
                dump_count(ckey, len(pubs), pipe)
                num_piped += 1
                if num_piped >= max_piped:
                    pipe.execute()
                    num_piped = 0
        pipe.execute()
    end = datetime.datetime.now()
    print(f'Wrote {n} entries in {end-start}')
    update_prefixes(p1, p2, redis)

def cacheit(p1, p2, conn, redis):
    p1, p2 = sorted([p1, p2])
    print(p1,p2)
    start = datetime.datetime.now()
    query, values = generate_query(p1, p2)
    a_curies = get_curies(p1, conn)
    if p1 == p2:
        b_curies = a_curies
    else:
        b_curies = get_curies(p2, conn)
    print(len(a_curies), len(b_curies), len(a_curies) * len(b_curies))
    done_pairs = set()
    ckey = ()
    pubs = []
    n = 0
    num_piped = 0
    # with open(f'{p1}_{p2}.redis','w') as outf:
    with redis.pipeline() as pipe:
        with conn.cursor(name='cache') as cursor:
            cursor.execute(query, tuple(values))
            while True:
                records = cursor.fetchmany(size=2000)
                if not records:
                    break
                print(len(records))
                for r in records:
                    curie_1 = r[0]
                    curie_2 = r[1]
                    if p1 == p2:
                        #have to sort the curies in this case
                        curie_1,curie_2 = sorted( [curie_1, curie_2] )
                    if (curie_1, curie_2) != ckey:
                        n += 1
                        dump(ckey, pubs, pipe)
                        dump_count(ckey, len(pubs), pipe)
                        num_piped += 1
                        if num_piped >= max_piped:
                            pipe.execute()
                            num_piped = 0
                        ckey = (curie_1, curie_2)
                        done_pairs.add(ckey)
                        pubs = []
                    pubmed = r[2]
                    pubs.append(f'PMID:{pubmed}')
                    # do something with record here
        # Can't do this, at least on my local
        #        for ac in a_curies:
        #            for bc in b_curies:
        #                if (ac,bc) not in done_pairs:
        #                    dump( (ac,bc), [], pipe )
        #                    n += 1
        #                    num_piped += 1
        #                    if num_piped >= max_piped:
        #                        pipe.execute()
        #                        num_piped = 0
        pipe.execute()
    end = datetime.datetime.now()
    print(f'Wrote {n} entries in {end-start}')
    update_prefixes(p1, p2, redis)


def get_curie_count(prefix,conn):
    pf = ''.join(prefix.split('.'))
    query = f'SELECT COUNT(DISTINCT curie) FROM omnicorp.{pf}'
    with conn.cursor() as cursor:
        cursor.execute(query, ())
        records = cursor.fetchall()
        count = records[0][0]
    print(prefix,count)
    return count

def get_curies(prefix, conn):
    pf = ''.join(prefix.split('.'))
    query = f'SELECT DISTINCT curie FROM omnicorp.{pf}'
    with conn.cursor() as cursor:
        cursor.execute(query, ())
        records = cursor.fetchall()
        curies = {r[0] for r in records}
        for bad in bad_ids[prefix]:
            curies.remove(bad)
    return curies

#@lru_cache(maxsize=2)
def get_curie_to_pmid(prefix, conn):
    pf = ''.join(prefix.split('.'))
    query = f'SELECT DISTINCT curie,pubmedid FROM omnicorp.{pf}'
    c2p = defaultdict(list)
    with conn.cursor() as cursor:
        cursor.execute(query, ())
        records = cursor.fetchall()
        for r in records:
            if r[0] not in bad_ids[prefix]:
                c2p[r[0]].append(r[1])
    return c2p

#@lru_cache(maxsize=2)
def get_pmid_to_curie(prefix, conn):
    pf = ''.join(prefix.split('.'))
    query = f'SELECT DISTINCT curie,pubmedid FROM omnicorp.{pf}'
    p2c = defaultdict(list)
    with conn.cursor() as cursor:
        cursor.execute(query, ())
        records = cursor.fetchall()
        for r in records:
            if r[0] not in bad_ids[prefix]:
                p2c[r[1]].append(r[0])
    return p2c

def generate_query(p1, p2):
    query = f'SELECT a.curie, b.curie, a.pubmedid FROM omnicorp.{p1} a JOIN omnicorp.{p2} b ON a.pubmedid = b.pubmedid'
    values = []
    first = True
    if p1 == p2:
        if first:
            query += '\nWHERE a.curie < b.curie'
            first = False
        else:
            query += '\nAND a.curie < b.curie'
    if p1 in bad_ids:
        for bid in bad_ids[p1]:
            if first:
                query += '\nWHERE a.curie <> %s'
                first = False
            else:
                query += '\nAND a.curie <> %s'
            values.append(bid)
    if p2 in bad_ids:
        for bid in bad_ids[p2]:
            if first:
                query += '\nWHERE b.curie <> %s'
                first = False
            else:
                query += '\nAND b.curie <> %s'
            values.append(bid)
    query += '\nORDER BY a.curie, b.curie'
    return query, values
