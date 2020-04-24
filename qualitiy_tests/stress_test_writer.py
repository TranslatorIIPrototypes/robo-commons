from greent.graph_components import KNode, KEdge, LabeledID, node_types
from greent.export_delegator import WriterDelegator
from greent.rosetta import Rosetta
from pika.channel import Channel
from builder.writer import start_consuming
from functools import partial
import time
import traceback

def generate_graph(size: int):
    """ generate triplets of length size"""
    for i in range(0, size):
        source_curie = f'SOURCE:{i}'
        target_curie = f'TARGER:{i}'
        source_node = KNode(source_curie, type=node_types.CHEMICAL_SUBSTANCE)

        target_node = KNode(target_curie, type=node_types.CHEMICAL_SUBSTANCE)
        edge = KEdge(source_id=source_node.id,
                     target_id=target_node.id,
                     provided_by='stress_tester',
                     ctime='now',
                     original_predicate=LabeledID('RO:0000052', 'affects'),
                     input_id=source_node.id,
                     )
        yield (source_node, edge, target_node)


def write_to_queue(source_node_length, wdg):
    for s, p, o in generate_graph(source_node_length):
        wdg.write_node(s)
        wdg.write_node(o)
        wdg.write_edge(p)


def process_queue(pool_id=0, errors={}):
    rosetta = Rosetta()
    wdg = WriterDelegator(rosetta, push_to_queue=True)
    print('starting consumer')
    # send a 'close' message to stop consumer consumer at the end assuming that this will go at the end of the nodes and edges.
    wdg.flush()
    wdg.close()
    start_consuming(max_retries=-1)


def check_queue(size):
    rosetta = Rosetta()
    wdg = WriterDelegator(rosetta, push_to_queue=True)
    import time
    # wait a bit before reading the queue

    time.sleep(1)
    res = wdg.channel.queue_declare(
        queue="neo4j",
        passive=True
    )
    return res.method.message_count == size


def check_neo4j(source_node_size):
    import os
    from neo4j import GraphDatabase

    query  = "MATCH (a)-[e]->(c) RETURN count(a) AS source_count, count(e) AS edge_count, count(c) AS target_count"
    driver = GraphDatabase.driver(auth= ('neo4j', os.environ['NEO4J_PASSWORD']),
        uri= f"bolt://{os.environ['NEO4J_HOST']}:{os.environ['NEO4J_BOLT_PORT']}")
    with driver.session() as s:
        response = dict(**s.run(query).single())

    assert response['source_count'] == source_node_size
    assert response['edge_count'] == source_node_size
    assert response['target_count'] == source_node_size

def write_error_to_file(pid, exception:Exception):
    with open(f'{pid}_error.log', 'w') as f:
        f.write(traceback.format_exc())

def write_termination(pid, v):
    if not v:
        return
    with open(f'{pid}.log', 'w') as f:
        f.write(v)



def start_multiple_consumers(num_consumers, errors: dict):
    from multiprocessing import Pool
    pool = Pool(processes=num_consumers)
    finished = []
    for r in range(0, num_consumers):
        p_q_partial = partial(process_queue, r, errors)
        error_call_back_partial = partial(write_error_to_file, r)
        success_call_back_partial = partial(write_termination, r)
        pp = pool.apply_async(p_q_partial, [], callback=success_call_back_partial, error_callback=error_call_back_partial)
        finished.append(pp)
    [x.wait() for x in finished]
    pool.close()
    pool.join()


if __name__ == '__main__':
    rosetta = Rosetta()
    wdg = WriterDelegator(rosetta, push_to_queue=True)
    wdg.flush()
    wdg.close()
    # # clear out the queue
    wdg.channel.queue_purge('neo4j')
    # # # # # source nodes len
    source_node_length = 100
    write_to_queue(source_node_length, wdg)
    # # # # expect node_length * 3 in queue
    assert check_queue(source_node_length*3) == True
    errors = {}
    # start consumer(s)
    start_multiple_consumers(1, errors={})
    # process_queue(1, {})
    print('checking neo4j')
    check_neo4j(source_node_length)

