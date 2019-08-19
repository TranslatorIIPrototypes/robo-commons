from crawler.crawl_util import pull_and_decompress, pull_via_ftp

def pull_chebi():
    #Get stuff from the SDF.  This will be things with smiles and with or without inchi
    ck = { x:x for x in ['chebiname', 'chebiid', 'secondarychebiid','inchikey','smiles',
              'keggcompounddatabaselinks', 'pubchemdatabaselinks'] }
    chebi_parts = pull_chebi_sdf(ck)
    chebi_with_structure,chebi_pubchem,chebi_kegg = extract_from_chebi_sdf(chebi_parts)
    #We should have anything with a structure handled. But what about stuff that doesn't have one?
    # Check the db_xref
    kegg_chebi, pubchem_chebi = pull_database_xrefs(skips = chebi_with_structure)
    return chebi_pubchem + pubchem_chebi, chebi_kegg + kegg_chebi

def pull_database_xrefs(skips=[]):
    chebixrefs = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/Flat_file_tab_delimited/', 'database_accession.tsv').decode()
    lines = chebixrefs.split('\n')
    kegg_chebi = []
    pubchem_chebi = []
    for line in lines[1:]:
        x = line.strip().split('\t')
        if len(x) < 4:
            continue
        cid = f'CHEBI:{x[1]}'
        if cid in skips:
            continue
        if x[3] == 'KEGG COMPOUND accession':
            kegg_chebi.append( (cid, f'KEGG.COMPOUND:{x[4]}') )
        if x[3] == 'Pubchem accession':
            pubchem_chebi.append( (cid, f'PUBCHEM:{x[4]}') )
    return kegg_chebi,pubchem_chebi

def extract_from_chebi_sdf(chebi_parts):
    #Now, we have a choice.  In terms of going chebi to kegg/pubchem we can do it for everything
    # or just for things without inchi.
    #The problem with with doing it for things with inchi is that we're trusting chebi without
    # verifying the KEGG inchi i.e. UniChem is already doing this, and we're not trusting them
    # here.  What to do about conficts (even if we notice them?)
    #The problem with not doing things with inchi is the case where the Chebi has an Inchi but
    # KEGG doesn't.  KEGG doesn't make a download available, which makes this more complicated than
    # it needs to be.  IF we DID have a KEGG download, we could be more careful. As is, let's assume
    # that the CHEBI/KEGG and CHEBI/PUBCHEM are good and return mappings for everything.
    chebi_pubchem = []
    chebi_kegg = []
    chebi_with_structure = set()
    for cid,props in chebi_parts.items():
        chebi_with_structure.add(cid)
        kk = 'keggcompounddatabaselinks'
        if kk in props:
            chebi_kegg.append( (cid,f'KEGG:COMPOUND:{props[kk]}'))
        pk = 'pubchemdatabaselinks'
        if pk in props:
            v = props[pk]
            parts = v.split('SID: ')
            for p in parts:
                if 'CID' in p:
                    x = p.split('CID: ')[1]
                    r = (cid, f'PUBCHEM:{x}')
                    chebi_pubchem.append(r)
    return chebi_with_structure,chebi_pubchem,chebi_kegg

def pull_chebi_sdf(interesting_keys):
    chebisdf = pull_and_decompress('ftp.ebi.ac.uk', '/pub/databases/chebi/SDF/', 'ChEBI_complete.sdf.gz')
    chebi_props = {}
    lines = chebisdf.split('\n')
    chunk = []
    for line in lines:
        if '$$$$' in line:
            chebi_id,chebi_dict = chebi_sdf_entry_to_dict(chunk, interesting_keys= interesting_keys)
            chebi_props[chebi_id] = chebi_dict
            chunk = []
        else:
            if line != '\n':
                line = line.strip('\n')
                chunk += [line]
    return chebi_props


def chebi_sdf_entry_to_dict(sdf_chunk, interesting_keys = {}):
    """
    Converts each SDF entry to a dictionary
    """
    final_dict = {}
    current_key = 'mol_file'
    chebi_id = ''
    for line in sdf_chunk:
        if len(line):
            if '>' == line[0]:
                current_key = line.replace('>','').replace('<','').strip().replace(' ', '').lower()
                current_key = 'formula' if current_key == 'formulae' else current_key
                if current_key in interesting_keys:
                    final_dict[interesting_keys[current_key]] = ''
                continue
            if current_key == 'chebiid':
                chebi_id = line
            if current_key in interesting_keys:
                final_dict[interesting_keys[current_key]] += line
    return (chebi_id, final_dict)

if __name__ == '__main__':
    pull_database_xrefs()