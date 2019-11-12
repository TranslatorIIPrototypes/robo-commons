from neo4j.v1 import GraphDatabase
import os

# Maybe make this a notebook?

def get_driver(url):
    driver = GraphDatabase.driver(url, auth=("neo4j", os.environ['NEO4J_PASSWORD']))
    return driver

def run_query(url,cypherquery):
    driver = get_driver(url)
    with driver.session() as session:
        results = session.run(cypherquery)
    return list(results)

def get_chemicals(url):
    """This is all the variants.  We might want to filter on source"""
    cquery = f'''match (a:chemical_substance) where a.smiles is not NULL and a.inchikey is not null RETURN a.inchikey, a.smiles'''
    records = run_query(url,cquery)
    with open('smiles.txt','w') as outf:
        outf.write(f"Compound_name\tCASRN\tSMILES\tSolubility(µM)\tSolubility(µg / mL)\tlogSo(mol / L)\tSource\n")

        for r in records:
            #print(r)
            #outf.write(f'{r["a.inchikey"]}\t{r["a.smiles"]}\n')
            outf.write(f'noname\tnocasrn\t{r["a.smiles"]}\t0\t0\t0\tnosource\n')

if __name__ == '__main__':
    url = 'bolt://robokopdb2.renci.org:7687'
    get_chemicals(url)