from greent.service import Service
from greent.util import Text, LoggingUtil
import logging

logger = LoggingUtil.init_logging(__name__, logging.INFO)

class TypeCheck(Service):
    """Service that has the ability to determine whether a given ID corresponds to a particular class or not.
    Returning True from any of these functions means that the identifier is one of the given class.
    Returning False means that the function was not able to determine that ID is an instance of the class.
    Sometimes that will be because the identifier is not part of the class, and in other cases the function
    just won't be able to tell.  For instance, if all we have is meddra and umls, there's no way to know if that's
    a disease of a phenotype."""

    def __init__(self, context, greent,rosetta):
        super(TypeCheck, self).__init__("typecheck", context)
        self.greent = greent
        self.synonymizer = rosetta.synonymizer

    def is_cell(self, node):
        """This is a very cheesy approach.  Once we have a generic ontology browser hooked in, we can reformulate"""
        for pref in ['CL']:
            if len(node.get_synonyms_by_prefix(pref)) > 0:
                return True
        return False

    #The way caster works, these nodes won't necessarily be synonymized yet.  So it may just
    # have e.g. a Meddra ID or something
    def is_disease(self,node):
        #If this thing can be converted to DOID or MONDO then I'm calling it a disease
        self.synonymizer.synonymize(node)
        mondos = node.get_synonyms_by_prefix('MONDO')
        if len(mondos) > 0:
            return True
        doids = node.get_synonyms_by_prefix('DOID')
        if len(doids) > 0:
            return True
        return False

    def is_phenotypic_feature(self,node):
        #If this thing can be converted to HP, then it's a phenotype
        self.synonymizer.synonymize(node)
        hps = node.get_synonyms_by_prefix('HP')
        if len(hps) > 0:
            return True
        efos = node.get_synonyms_by_prefix('EFO')
        if len(efos) > 0:
            return True
        return False

    def is_gene_product(self,node):
        #If this thing can be converted to HP, then it's a phenotype
        self.synonymizer.synonymize(node)
        uniprot = node.get_synonyms_by_prefix('UniProtKB')
        if len(uniprot) > 0:
            return True
        return False

    def is_gene_family(self,node):
        if node.id.startswith('HGNC.FAMILY'):
            return True
        if node.id.startswith('PANTHER.FAMILY'):
            return True
        return False

    def is_gene(self,node):
        hgncs = node.get_synonyms_by_prefix('HGNC')
        if len(hgncs) > 0:
            return True
        ensembls = node.get_synonyms_by_prefix('ENSEMBL')
        if len(ensembls) > 0:
            return True
        ncbis = node.get_synonyms_by_prefix('NCBIGENE')
        if len(ncbis) > 0:
            return True
        return False

    def is_sequence_variant(self,node):
        return False

    def is_pathway(self,node):
        return False

    def is_chemical(self,node):
        for pref in ['CHEBI', 'CHEMBL', 'UniProtKB', 'KEGG.COMPOUND']:
            if len(node.get_synonyms_by_prefix(pref)) > 0:
                return True
        return False
