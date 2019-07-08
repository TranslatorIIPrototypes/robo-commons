from  greent import node_types
from greent.edge_inheritance.chebi_resolver import Chebi_resolver



class Heirarchy_resolver:
    # probably a good call to make this a singleton and all of its class too
    __heirarchy_resolver_instance = None
    class __Hierarchy_resolver:
        def __init__ (self, rosetta):
            #register ontological heirarcy resolvers here. 
            self.rosetta = rosetta
            print(' INIT inner')
            self.lazy_load_map = {
                node_types.CHEMICAL_SUBSTANCE: {
                    'CHEBI': lambda node_type, rosetta: Chebi_resolver('ok', node_type, rosetta)
                }
            }
            self.intialized_service = {}
        def __getattr__(self, node_type):
            services = self.intialized_service.get(node_type, None)
            if services == None:
                services = self.lazy_load_map.get(node_type, {})
                #initilize the services
                servs = {x:  services[x](node_type, self.rosetta) for x in  services}
                if servs == {}:
                    raise AttributeError(f'No resolvers found for {node_type}')                
                self.intialized_service[node_type] = servs
            return self.intialized_service[node_type]
    
    @staticmethod
    def create_resolver(rosetta):
        if Heirarchy_resolver.__heirarchy_resolver_instance == None:
            Heirarchy_resolver.__heirarchy_resolver_instance = Heirarchy_resolver.__Hierarchy_resolver(rosetta) 
        return Heirarchy_resolver.__heirarchy_resolver_instance

    def __getattr__(self, name):
    # proxy calls to inner class
        return getattr(Heirarchy_resolver.__heirarchy_resolver_instance, name)
