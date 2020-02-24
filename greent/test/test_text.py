
from greent.util import Text



def test_obo_to_curie():
    test_set = {
        'http://purl.obolibrary.org/obo/prefix_suffix': 'prefix:suffix',
        'http://purl.obolibrary.org/obo/prefix_suffix_suffix': 'prefix:suffix_suffix',
        'http://purl.obolibrary.org/obo/path/prefix_suffix': 'path_prefix:suffix',
        'http://purl.obolibrary.org/obo/path/prefix#suffix': 'path_prefix:suffix',
        'http://purl.obolibrary.org/obo/path/prefix#suffix_more_suffix': 'path_prefix:suffix_more_suffix'
    }
    for obo, curie in test_set.items():
        assert Text.obo_to_curie(obo) == curie
    # could not find a way to smartly go back to urls containing # back
    do = 3
    for obo, curie in test_set.items():
        if do == 1:
            break
        do -= 1
        assert Text.curie_to_obo(curie) == f'<{obo}>'


def test_normalize_predicates():
    test_set = {
        "CTD:affects^expression": "CTD:affects_expression",
        "CTD:affects^metabolic processing": "CTD:affects_metabolic_processing",
        "CTD:affects^ADP-ribosylation": "CTD:affects_ADP-ribosylation",
        "GAMMA:other/unknown": "GAMMA:other_unknown",
        "CTD:affects^N-linked glycosylation": "CTD:affects_N-linked_glycosylation"
    }
    for unformatted, formatted in test_set.items():
        result = Text.normalize_predicate(unformatted)
        assert result == formatted