import requests
from greent.services.onto import Onto
from greent.service import Service
from greent.graph_components import KNode
from greent.util import LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__name__)

#TODO: LOOKUP all the terms that map to this... or use an ancestor call that doesn't require such stuff (i.e. that handles this)
ANATOMICAL_ENTITY='UBERON:0001062'

class Uberon(Onto):
    
    """ Query MONDO """
    def __init__(self, context):
        super(Uberon, self).__init__("uberon", context)
    
    def get_label(self,identifier):
        return super(Uberon,self).get_label(identifier)

    def get_uber_id(self,obj_id):
        uber_id, label = self.get_uber_id_and_label(obj_id)
        return uber_id

    def get_uber_id_and_label(self,obj_id):
        result = []
        label = super(Uberon,self).get_label(obj_id)
        #if label and 'label' in label and label['label'] is not None:
        if label is not None:
            result.append (obj_id)
        else:
            result = super(Uberon,self).lookup(obj_id)
        return result,label

    def has_ancestor(self,obj, terms):
        """ Is is_a(obj,t) true for any t in terms ? """
        ids = self.get_uber_id(obj.id)
        results = [ i for i in ids for candidate_ancestor in terms if super(Uberon,self).is_a(i, candidate_ancestor) ] \
                 if terms else []
        return len(results) > 0, results

    def is_anatomical_entity(self,obj):
        """Checks uberon to find whether the subject has UBERON:0001602 as an ancestor"""
        return self.has_ancestor(obj, [ANATOMICAL_ENTITY])[0]

