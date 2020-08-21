import pytest
from greent.graph_components import KNode, LabeledID
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta

@pytest.fixture()
def pharos(rosetta):
    pharos = rosetta.core.pharos
    return pharos

def xtest_string_to_info(pharos):
    r = pharos.drugname_string_to_pharos_info('CELECOXIB')
    assert len(r) == 1
    assert r[0][0] == 'CHEMBL:CHEMBL118' #first result is a tuple (curie,name)

def xtest_string_to_info_wackycap(pharos):
    r = pharos.drugname_string_to_pharos_info('CeLEcOXIB')
    assert len(r) == 1
    assert r[0][0] == 'CHEMBL.COMPOUND:CHEMBL118' #first result is a tuple (curie,name)

def test_drug_get_gene(pharos):
    #pharos should find chembl in the synonyms
    node = KNode('DB:FakeyName', type=node_types.CHEMICAL_SUBSTANCE)
    node.add_synonyms([LabeledID(identifier='CHEMBL.COMPOUND:CHEMBL118', label='blahbalh')])
    results = pharos.drug_get_gene(node)
    #we get results
    assert len(results) > 0
    #They are gene nodes:
    ntypes = set([n.type for e,n in results])
    assert node_types.GENE in ntypes
    assert len(ntypes) == 1
    #All of the ids should be HGNC
    identifiers = [n.id for e,n in results]
    prefixes = set([ Text.get_curie(i) for i in identifiers])
    assert 'HGNC' in prefixes
    assert len(prefixes) == 1
    #PTGS2 (COX2) (HGNC:9605) should be in there
    assert 'HGNC:9605' in identifiers

def xtest_gene_get_drug_long(pharos,rosetta):
    gene_node = KNode('HGNC:6871', type=node_types.GENE)
    rosetta.synonymizer.synonymize(gene_node)
    print(gene_node.synonyms)
    #output = pharos.gene_get_drug(gene_node)
    #identifiers = [ output_i[1].id for output_i in output ]
    #assert 'CHEMBL:CHEMBL118'in identifiers
    #assert False

def test_gene_get_drug(pharos,rosetta):
    gene_node = KNode('HGNC:9605', type=node_types.GENE)
    rosetta.synonymizer.synonymize(gene_node)
    print(gene_node.synonyms)
    
    output = pharos.gene_get_drug(gene_node)
    identifiers = [ output_i[1].id for output_i in output ]
    assert 'CHEMBL.COMPOUND:CHEMBL118'in identifiers

def test_disease_get_gene(pharos,rosetta):
    disease_node = KNode('DOID:4325', type=node_types.DISEASE, name="ebola")
    output = pharos.disease_get_gene(disease_node)
    identifiers = [ output_i[1].id for output_i in output ]
    assert 'HGNC:7897' in identifiers

def test_disease_gene_mondo(pharos,rosetta):
    d_node = KNode('MONDO:0008903', type=node_types.DISEASE)
    rosetta.synonymizer.synonymize(d_node)
    output = pharos.disease_get_gene(d_node)
    assert len(output) > 0

def test_disease_gene_direction(pharos, rosetta):
    d_node = KNode('MONDO:0008903', type=node_types.DISEASE)
    rosetta.synonymizer.synonymize(d_node)
    output = pharos.disease_get_gene(d_node)
    edge_exists = False
    for edge, node in output:
        if edge.original_predicate.identifier == 'WD:P2293':
            edge_exists = True
            assert edge.target_id == d_node.id
    assert edge_exists

def test_gene_disease_direction(pharos, rosetta):
    gene_node = KNode('NCBIGene:11176')
    rosetta.synonymizer.synonymize(gene_node)
    output = pharos.gene_get_disease(gene_node)
    for edge , node in output:
        assert edge.source_id == gene_node.id