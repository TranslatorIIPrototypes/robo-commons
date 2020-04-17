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
    Some times we might have nodes that span several annotators,
    eg a Chemical_substance can also be a gene
    so we want to annotate it as both ??
    if so we can return all the annotators for all the types associated with the node.
    """
    annotators = []
    for node_type in node.type:
        if node_type not in annotator_instances:
            annotator_class = annotator_class_list.get(node_type)
            if annotator_class :
                annotator_instances[node_type] = annotator_class(rosetta)
            else :
                annotator_instances[node_type] =  None
        annotators.append(annotator_instances.get(node_type))
    return annotators

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
    annotated = None
    typed_annotators = make_annotator(node, rosetta)
    for annotator in typed_annotators:
        if annotator != None:
            annotator.annotate(node)
        annotated = True
    return annotated
