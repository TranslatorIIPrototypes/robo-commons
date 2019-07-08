import pytest
from greent.conftest import rosetta
import asyncio
from greent.edge_inheritance.chebi_resolver import Chebi_resolver

@pytest.fixture()
def chemical_resolver(rosetta):
    return rosetta.heirarchy_resolver.chemical_substance
    

@pytest.fixture()
def resolver(rosetta):
    return rosetta.heirarchy_resolver

@pytest.fixture(scope='function', autouse= True)
def event_loop():
    event_loop =  asyncio.new_event_loop()
    yield event_loop
    event_loop.close()
    

def test_none_existing(resolver):
    with pytest.raises(AttributeError):
        resolver.resolver_i_dont_have

def test_init_chemical_resolver(chemical_resolver):
    assert  chemical_resolver != None
    assert  len(chemical_resolver) > 0

def test_chebi_resolver_init(chemical_resolver):
    assert 'CHEBI' in chemical_resolver
    assert type(chemical_resolver['CHEBI']) == Chebi_resolver

def test_chebi_resolver_get_parent(chemical_resolver, event_loop):
    chebi_resolver = chemical_resolver['CHEBI']
    results = event_loop.run_until_complete(chebi_resolver.get_parents('CHEBI:35176'))
    # event_loop.close()
    assert len(results) == 2

def test_chebi_resolver_get_child(chemical_resolver, event_loop):
    chebi_resolver = chemical_resolver['CHEBI']
    results = event_loop.run_until_complete(chebi_resolver.get_children('CHEBI:51336'))
    assert len(results) == 28


def test_chebi_resolver_get_ancestor(chemical_resolver, event_loop):
    chebi_resolver = chemical_resolver['CHEBI']
    results = event_loop.run_until_complete(chebi_resolver.get_ancestors('CHEBI:35176'))
    # @TODO check length of results for that CHEBI id
    assert len(results) > 2

def test_chebi_resolver_get_descendants(chemical_resolver, event_loop):
    chebi_resolver = chemical_resolver['CHEBI']
    results = event_loop.run_until_complete(chebi_resolver.get_descendants('CHEBI:51336'))
    #@todo check len
    assert len(results) > 28







