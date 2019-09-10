import os
from collections import defaultdict
from crawler.crawl_util import pull_and_decompress
from Bio import SwissProt
import requests


def pull_uniprot(repull=False):
    xmlname = os.path.join(os.path.dirname (__file__), 'uniprot_sprot_human.dat')
    if repull:
        xmldata = pull_and_decompress('ftp.uniprot.org','/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/' ,'uniprot_sprot_human.dat.gz')
        with open(xmlname,'w') as xmlfile:
            xmlfile.write(xmldata)
    seq_to_idlist = defaultdict(set)
    #I only want the PRO sequences.  One day, I could get the -1 -2 sequences as well if
    # there were a reason.
    with open(xmlname,'r') as unif:
        for record in SwissProt.parse(unif):
            uniprotid = f'UniProtKB:{record.accessions[0]}'
            #xrefs = [ f"{x[0]}:{x[1]}" for x in record.cross_references if x[0].lower() in ['mint','string','nextprot']]
            #xrefs.append( f'PR:{record.accessions[0]}' )
            #xrefs.append( uniprotid )
            feats = [ f for f in record.features if f[4].startswith('PRO_') and isinstance(f[1],int) and isinstance(f[2],int) ]
            fseq = [(record.sequence[f[1]-1:f[2]],f[4]) for f  in feats ]
            #seq_to_idlist[record.sequence].update(xrefs)
            for fs,fn in fseq:
                seq_to_idlist[fs].add(f'{uniprotid}#{fn}')
    return seq_to_idlist

def pull_iuphar():
    s2iuphar = pull_iuphar_by_structure()
    hand_iuphar = pull_iuphar_by_hand()
    return s2iuphar,hand_iuphar

def pull_iuphar_by_hand():
    fname = os.path.join(os.path.dirname (__file__), 'data','iuphar_concord.txt')
    conc = []
    with open(fname,'r') as iupf:
        for line in iupf:
            if line.startswith('#'):
                continue
            x = set(line.strip().split(','))
            conc.append(x)
    return conc

def pull_iuphar_by_structure():
    r=requests.get('https://www.guidetopharmacology.org/DATA/peptides.tsv')
    lines = r.text.split('\n')
    seq_to_iuphar = defaultdict(set)
    for line in lines[1:]:
        x = line.strip().split('\t')
        if len(x) < 2:
            continue
        if not 'Human' in x[2]:
            continue
        if len(x[14]) > 0:
            seq = x[14][1:-1]
            seq3 = x[15][1:-1]
            iuid = f'GTOPDB:{x[0][1:-1]}'
            if 'X' in seq:
                xind = seq.find('X')
                bad = seq3.split('-')[xind]
                if bad == 'pGlu':
                    seq = seq[:xind] + 'Q' + seq[xind+1:]
                elif bad == 'Hyp':
                    seq = seq[:xind] + 'P' + seq[xind+1:]
                else:
                    print(iuid,bad)
            seq_to_iuphar[seq].add(iuid)
    return seq_to_iuphar
            
 

if __name__ == '__main__':
    pull_uniprot()

