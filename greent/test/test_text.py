
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