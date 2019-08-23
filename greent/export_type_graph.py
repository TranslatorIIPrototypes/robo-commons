import os
import itertools
from greent.util import Resource
from collections import defaultdict

#The biolink model is what we use to really understand types, and to figure out whether we can call
# services and expect to get results back.
#But, turns out that we want to have a slightly different model for exporting to neo4j.  This is mostly to make
# querying the db more straightforward.  The type graph to understand is conf/neo4j_types.yaml
class ExportGraph:

    def __init__(self, rosetta):
        self.rosetta = rosetta
        self.supers = defaultdict(list)
        self.subs = defaultdict(list)
        model_path = os.path.join (os.path.dirname (__file__), "conf", f"neo4j_types.yaml")
        model_obj = Resource.load_yaml (model_path)
        for child,stuff in model_obj.items():
            if 'is_a' in stuff:
                parents = stuff['is_a']
                for parent in parents:
                    self.supers[child].append(parent)
                    self.subs[parent].append(child)

    def add_type_labels(self,node):
        """Starting at a node, find the most child-ended type for it, then add all superclasses"""
        leaf_type = self.get_leaf_type(node,node.node_type)
        supers = set([leaf_type])
        self.get_superclasses(leaf_type,supers)
        node.add_export_labels(supers)

    def nodeistype(self,node,ctype):
        if ctype in self.rosetta.type_checks:
            return self.rosetta.get_ops(self.rosetta.type_checks[ctype])(node)
        #we don't have a direct check.  So we're going to check all the children and say if this is one of the children,
        # then it's also this type.  This is because some types are defined as union types, and don't have much
        # definition of their own.
        grandchild_types = self.subs[ctype]
        if len(grandchild_types) == 0:
            raise Exception(f"missing definitions for {ctype}")
        for gctype in grandchild_types:
            if self.nodeistype(node,gctype):
                return True
        return False

    def get_leaf_type(self,node,current_type):
        child_types = self.subs[current_type]
        trues = []
        for possible_child_type in child_types:
            if self.nodeistype(node,possible_child_type):
                trues.append(possible_child_type)
        list_of_ltypes = [self.get_leaf_type(node,t) for t in trues ]
        ltypes = list(set(list(itertools.chain.from_iterable(list_of_ltypes))))
        if len(ltypes) == 0:
            return [current_type]
        return ltypes
        #There are more than one possibility.  Not 100% sure what to do here
        #Option 1: Stop & return current
        #Option 2: pick one / force this not to occur upstream somehow
        #Option 3: Allow both children to be true (end up with things that are disease and phenotype, in a single node)
        #Both options 1 and 2 are failures because of multiple inheritance.
        #chemical is a named thing
        #gene_or_gene_product is a named_thing
        #gene_product is a chemical
        #gene_product is a gene_or_gene_product
        #There are 2 paths going down to gene_product, and option 1 halts at named_thing
        # option 2 pretends this doesn't happen
        #only leaves 3.

    def get_superclasses(self, ctype, collected_superclasses):
        for sup in self.supers[ctype]:
            collected_superclasses.add(sup)
            self.get_superclasses(sup,collected_superclasses)
