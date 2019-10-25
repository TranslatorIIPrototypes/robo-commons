from greent import node_types
from greent.annotators.gene_annotator import GeneAnnotator
from greent.annotators.chemical_annotator import ChemicalAnnotator
from greent.annotators.disease_annotator import DiseaseAnnotator
from greent.annotators.generic_annotator import GenericAnnotator
import logging

logger = logging.getLogger(name= __name__)
annotator_class_list = {
    node_types.GENE : GeneAnnotator,
    node_types.CHEMICAL_SUBSTANCE: ChemicalAnnotator,
    node_types.DISEASE: DiseaseAnnotator,
    node_types.NAMED_THING: GenericAnnotator # Maybe tie this to namedThing, although our genericAnnotator is type neutral.
}
annotator_instances = {}


def make_annotator(node, rosetta):
    """
    Factory of annotators. Maintains instances so data can be cached. 
    """
    if node.type not in annotator_instances:
        annotator_class = annotator_class_list.get(node.type)
        if annotator_class :
            annotator_instances[node.type] = annotator_class(rosetta)
        else :
            annotator_instances[node.type] =  None
    return annotator_instances[node.type]

def annotate_shortcut(node, rosetta):
    """
    Shortcut to calling the annotator, basically does making the annotator
    using the factory and calling it on the node. Returns none if no annotator
    was found.
    """

    #generic annotation
    if annotator_instances.get(node_types.NAMED_THING, None) == None :
        annotator_instances[node_types.NAMED_THING] = GenericAnnotator(rosetta)
    generic_annotator = annotator_instances.get(node_types.NAMED_THING)
    generic_annotator.annotate(node) 

    # typed annotation
    typed_annotator = make_annotator(node, rosetta)    

    if typed_annotator != None:
        return typed_annotator.annotate(node)
    return None