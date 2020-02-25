import requests
import json
from greent.service import Service
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KNode
from greent import node_types
from builder.question import LabeledID
import re
from collections import defaultdict

logger = LoggingUtil.init_logging (__name__)

class KEGG(Service):
    """ Access HMDB via the beacon """

    def __init__(self, context):
        super(KEGG, self).__init__("kegg", context)

    def parse_raw_results(self, raw_results, results,prefix):
        rawlines = raw_results.text.split('\n')
        lines = list(filter(lambda y: len(y) == 2, [x.split('\t') for x in rawlines]))
        for line in lines:
            rn = line[1]
            if not rn.startswith(prefix):
                continue
            results.append(rn)

    def parse_chemicals(self, line):
        n = line.split()
        return set(map(lambda x : x.split('(')[0], filter( lambda x: x.startswith('C'), n)))      

    def chemical_get_reaction(self,chemnode):
        identifiers = chemnode.get_synonyms_by_prefix('KEGG.COMPOUND')
        results = []
        for cid in identifiers:
            url = f'{self.url}/link/reaction/{Text.un_curie(cid)}'
            raw_results = requests.get(url)
            self.parse_raw_results(raw_results, results,'rn')
        return results

    def enzyme_get_reaction(self,enzymenode):
        identifiers = enzymenode.get_synonyms_by_prefix('EC')
        results = []
        for cid in identifiers:
            url = f'{self.url}/link/reaction/{Text.un_curie(cid)}'
            raw_results = requests.get(url)
            self.parse_raw_results(raw_results, results,'rn')
        return results


    def reaction_get_chemicals(self,reaction_id):
        results=[]
        rid = reaction_id.split(':')[1]
        url = f'{self.url}/link/cpd/{rid}'
        raw_results = requests.get(url)
        self.parse_raw_results(raw_results, results, 'cpd')
        return results


    def get_rp_from_enzyme(self,enzyme_id):
        url = f'{self.url}/get/ec:{enzyme_id}'
        reaction = {}
        raw_results = requests.get(url)
        substrates=set()
        products=set()
        mode = 'looking'
        for line in raw_results.text.split('\n'):
            if line.startswith('SUBSTRATE'):
                mode = 'parsing'
                values = substrates
            if line.startswith('PRODUCT'):
                mode = 'parsing'
                values = products
            if mode == 'parsing':
                l = line.strip()
                if ':' in l:
                    cid = l.split('[')[-1].split(']')[0].split(':')[1]
                    values.add(cid)
                if not l.endswith(';'):
                    mode = 'looking'
        return substrates, products

    def hsa2ncbi(self,hsaid):
        url = f'{self.url}/conv/ncbi-geneid/hsa:{hsaid}'
        raw_results = requests.get(url)
        ncbis = []
        if raw_results.status_code == 200:
            for line in raw_results.text.split('\n'):
                try:
                    ncbis.append( line.strip().split()[-1] )
                except:
                    pass
        ncbi = [f'NCBIGene:{x.split(":")[-1]}' for x in ncbis]
        return ncbi

    def get_human_genes(self,ko):
        """Given a KEGG Orthology ID, get Human Genes that have an HGNC identifier"""
        url = f'{self.url}/get/{ko}'
        raw_results = requests.get(url)
        for line in raw_results.text.split('\n'):
            if not line.startswith(' '):
                if len(line.strip()) > 0:
                    mode = line.split()[0]
            if 'HSA:' in line and mode == 'GENES':
                i = line.index('HSA:')
                hsas = line[i+4:].strip().split()
                hsaids = [h.split('(')[0] for h in hsas]
                genes = set()
                for h in hsaids:
                    genes.update( self.hsa2ncbi(h) )
                return genes
        return set()

    def get_reaction(self,reaction_id):
        #It's complicated to get the direction and the gene out of this.   The left/right of the reaction is arbitrary
        # The reactants/products are in the EC listing.  But not everything has a fully qualified EC listing under
        # ENZYME.   But, there are usually fully qualified EC under orthology.  But they might not match perfectly
        # because they're usually higher order reactions. So grab anything we can.  Once we've parsed it all, we can
        # step through all the enzymes collected looking for matches to orient the rxn.  Once we find one, we go
        # with it.
        # For gene, we really have to use the orthology section, not the EC.  And it may return multiple values.
        url = f'{self.url}/get/{reaction_id}'
        reactions = []
        raw_results = requests.get(url)
        #estring = None
        #elist = []
        ko2eclist = {}
        ko2genes = {}
        last = ''
        for line in raw_results.text.split('\n'):
            if line.startswith(' '):
                sw = last
            elif len(line) > 2:
                sw = line.split()[0]
                last = sw
            if sw == 'EQUATION':
                parts = line[9:].split('=')
                left = self.parse_chemicals(parts[0])
                right = self.parse_chemicals(parts[1])
            elif sw == 'ORTHOLOGY':
                elist = []
                if '[' in line:
                    ni = line.index('[')
                    nj = line.index(']',ni+1)
                    ecs = line[ni+1:nj].split()
                    elisto = [ x.split(':')[-1] for x in ecs ]
                    elist = list(filter( lambda z: '-' not in z, elisto))
                if line.startswith(sw):
                    ko = line.split()[1]
                else:
                    ko = line.split()[0]
                ko2eclist[ko] = elist
                ko2genes[ko] = self.get_human_genes(ko)
        #Now, see if we can get substrates from anywhere
        # But we need to be careful: different EC might give us different genes, and we need to check them all
        # It's possible that the different ECs will also go different directions, so we need to return a list of
        # reactions here.
        for ko in ko2genes:
            reaction = {}
            if len(ko2genes[ko]) == 0:
                continue
            if len(ko2eclist[ko]) == 0:
                continue
            #I believe that I don't need to check every enzyme.  They all point the same way.  So if I find one,
            # I can move to the next ko
            reaction['enzyme'] = ko2genes[ko]
            for ec in ko2eclist[ko]:
                substrates,products = self.get_rp_from_enzyme(ec)
                if len(left.intersection(substrates)) > 0:
                    reaction['reactants'] = left
                    reaction['products'] = right
                    break
                elif len(right.intersection(substrates)) > 0:
                    reaction['reactants'] = right
                    reaction['products'] = left
                    break
            if 'reactants' not in reaction:
                #Cant establish direction?
                reaction['reactants']=set()
                reaction['products']=set()
            reactions.append(reaction)
        return reactions

    def chemical_get_enzyme(self,chemnode):
        """To get an enzyme from chemicals, we first look up the reactions for the chemical.
        Then we pull the reaction which gives us (1) the enzyme and (2) whether the chemical
        is a reactant or a product."""
        reactions = self.chemical_get_reaction(chemnode)
        chemids = set([Text.un_curie(x) for x in chemnode.get_synonyms_by_prefix('KEGG.COMPOUND')])
        results = []
        for reaction_id in reactions:
            rxns = self.get_reaction(reaction_id)
            for rxn in rxns:
                if 'enzyme' in rxn:
                    for gene_id in rxn['enzyme']:
                        enzyme = KNode(gene_id, type=node_types.GENE)
                        if len(chemids.intersection(rxn['reactants'])) > 0:
                            predicate = LabeledID('CTD:increases_degradation_of', label='increases degradation of')
                            #predicate = LabeledID('RO:0002449','negatively regulates, entity to entity')
                            input_identifier = chemids.intersection(rxn['reactants']).pop()
                        elif len(chemids.intersection(rxn['products'])) > 0:
                            predicate = LabeledID('CTD:increases_synthesis_of', label='increases synthesis of')
                            #predicate = LabeledID('RO:0002450','positively regulates, entity to entity')
                            input_identifier = chemids.intersection(rxn['products']).pop()
                        else:
                            logger.error(f"Mismatch between query and answer: {rxn} {chemids}")
                            continue
                        edge = self.create_edge(enzyme, chemnode, f'kegg.chemical_get_enzyme',  input_identifier, predicate)
                        results.append( (edge, enzyme))
        return results

    def chemical_get_chemical(self,chemnode):
        """One chemical might be produced from the metabolism of another or it may produce another
        as a metabolite. We first look up the reactions for the input chemical.
        Then we pull the reaction which gives us the other chemicals and the relationship"""
        reactions = self.chemical_get_reaction(chemnode)
        chemids = set([Text.un_curie(x) for x in chemnode.get_synonyms_by_prefix('KEGG.COMPOUND')])
        results = []
        for reaction_id in reactions:
            rxns = self.get_reaction(reaction_id)
            for rxn in rxns:
                #Only rxns with enzymes are directional I think.
                if 'enzyme' in rxn and len(rxn['enzyme']) > 0:
                    if len(chemids.intersection(rxn['reactants'])) > 0:
                        predicate = LabeledID('RO:0001001','derives into')
                        input_identifier = chemids.intersection(rxn['reactants']).pop()
                        other_chems = rxn['products']
                        forward = True
                    elif len(chemids.intersection(rxn['products'])) > 0:
                        predicate = LabeledID('RO:0001001','derives into')
                        input_identifier = chemids.intersection(rxn['products']).pop()
                        other_chems = rxn['reactants']
                        forward = False
                    else:
                        logger.error(f"Mismatch between query and answer: {rxn} {chemids}")
                        continue
                    for chem in other_chems:
                        output = KNode(f'KEGG.COMPOUND:{chem}', type=node_types.METABOLITE)
                        if forward:
                            subj = chemnode
                            obj = output
                        else:
                            subj = output
                            obj = chemnode
                        edge = self.create_edge(subj, obj, f'kegg.chemical_get_chemical',  input_identifier, predicate)
                        results.append( (edge, output))
        return results


    def add_chem_results(self,chem_ids, predicate, enzyme_node, input_identifier, results, rset):
        for chem_id in chem_ids:
            if chem_id not in rset:
                chem_node = KNode(f'KEGG.COMPOUND:{chem_id}', type=node_types.CHEMICAL_SUBSTANCE)
                edge = self.create_edge(enzyme_node, chem_node, f'kegg.enzyme_get_chemicals',  input_identifier, predicate)
                results.append( (edge, chem_node))
                rset.add(chem_id)

    # KEGG has a set of compounds that have a 'sequence' tag
    # according to https://www.genome.jp/kegg/compound/:
    #   Peptide entries in KEGG COMPOUND are designated with "Peptide"
    #   in the first Entry line (see example here). They are always
    #   represented as sequence information using the three-letter
    #   amino acid codes, but they may or may not contain the full
    #   atomic structure representation. Small bioactive peptides are
    #   categorized in the BRITE hierarchy file shown below.
    # Following the referenced link leads one to
    # https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=
    # Which can be parsed to find the KEGG compounds that have a sequence.
    # As for crawling them and pulling the sequence, should we be going through the KEGG client? probably?
    def pull_sequences(self):
        kegg_sequences = defaultdict(set)
        r=requests.get('https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=')
        j = r.json()
        identifiers = []
        self.handle_kegg_list(j['children'],identifiers)
        for i,kid in enumerate(identifiers):
            s = self.get_sequence(kid)
            kegg_sequences[s].add(f'KEGG.COMPOUND:{kid}')
        return kegg_sequences

    def handle_kegg_list(self,childlist,names):
        for child in childlist:
            if 'children' in child:
                self.handle_kegg_list(child['children'],names)
            else:
                n = child['name'].split()
                names.append(n[0])

    #fairly clunky
    def get_sequence(self,compound_id):
        onetothree={'A':'Ala' ,'B':'Asx' ,'C':'Cys' ,'D':'Asp' ,'E':'Glu' ,'F':'Phe' ,'G':'Gly' ,
               'H':'His' ,'I':'Ile' ,'K':'Lys' ,'L':'Leu' ,'M':'Met' ,'N':'Asn' ,'P':'Pro' ,
               'Q':'Gln' ,'R':'Arg' ,'S':'Ser' ,'T':'Thr' ,'V':'Val' ,'W':'Trp' ,'X':'X',
               'Y':'Tyr' ,'Z':'Glx' }
        aamap = {v:k for k,v in onetothree.items()}
        #phosphoGlutamate?  This matches for
        aamap['Glp'] = 'Q'
        url = f'{self.url}/get/cpd:{compound_id}'
        raw_results = requests.get(url)
        results = raw_results.text.split('\n')
        mode = 'looking'
        for line in results:
            if mode == 'looking' and line.startswith('SEQUENCE'):
                x = ' '.join(line.strip().split()[1:])
                mode = 'reading'
            elif mode == 'reading':
                ls = line.strip()
                if ls.startswith('ORGANISM') or ls.startswith('TYPE'):
                    break
                x += " " + ls
        #At least one of these things contains a one-letter AA sequence (?!) C16008.  Try to recognize it
        toks = x.split()
        lens = [len(t) for t in toks]
        modelen = max(set(lens), key=lens.count)
        if modelen == 10:
            #single aa code, broken into blocks of 10
            return ''.join(toks)
        elif modelen != 3:
            #probably still a one-AA list, but let's check some cases
            if len(toks) == 1 or len(toks[0]) == 10:
                return ''.join(toks)
            else:
                print("not sure what this is",x)
                raise(x)
        #OK, anything left should be a 3-letter AA string
        #remove parenthetical comments
        regex = "\((.*?)\)"
        xprime = re.sub(regex, '', x)
        #do a cleanup for things like Arg-NH2
        xps = xprime.split()
        c = []
        for a in xprime.split():
            q = a.split('-')
            for qq in q:
                if qq in aamap:
                    c.append(qq)
                    break
        #Change to 1 letter codes
        s = [ aamap[a] for a in c ]
        return ''.join(s)

    """
    This one is a bit tough because we have to go through gene to orthology, blah blah. Don't need it for now
    
    def enzyme_get_chemicals(self,enzyme_node):
        ""To get chemicals from an enzyme, we first look up the reactions for the enzyme.
        Then we pull the reaction which gives us (1) the chemicals and (2) whether the chemical
        is a reactant or a product.""
        reactions = self.enzyme_get_reaction(enzyme_node)
        enzyme_ids = set([Text.un_curie(x) for x in enzyme_node.get_synonyms_by_prefix('EC')])
        results = []
        reactset=set()
        prodset=set()
        for reaction_id in reactions:
            rxn = self.get_reaction(reaction_id)
            input_identifier = rxn['enzyme']
            up_synth = LabeledID('CTD:increases^chemical synthesis', label='increases synthesis of')
            up_deg = LabeledID('CTD:increases^degradation', label='increases degradation of')
            self.add_chem_results(rxn['reactants'], up_deg, enzyme_node,input_identifier,results,reactset)
            self.add_chem_results(rxn['products'], up_synth, enzyme_node,input_identifier,results,prodset)
            #self.add_chem_results(rxn['reactants'], LabeledID('RO:0002449','negatively regulates, entity to entity'),enzyme_node,input_identifier,results,reactset)
            #self.add_chem_results(rxn['products'], LabeledID('RO:0002449','negatively regulates, entity to entity'),enzyme_node,input_identifier,results,prodset)
        return results
    """



