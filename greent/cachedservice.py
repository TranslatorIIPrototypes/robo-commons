import re
import requests
from greent.service import Service
from greent.cache import Cache

class CachedService(Service):
    """ A service that caches requests. """
    def __init__(self, name, context):
        super(CachedService,self).__init__(name, context)
        self.punctuation = re.compile('[ ?=\./:{}]+')
    def get(self,url):
        key = self.punctuation.sub ('', url)
        #print (f"==================> {url}")
        obj = self.context.cache.get(key)
        if not obj:
            if url.endswith('/'):
                url = url[:-1]
            rv = requests.get(url)
            if rv.status_code == 200:
                obj = rv.json()
                self.context.cache.set(key, obj)
            else:
                obj = None
        return obj
