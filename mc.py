from twisted.internet import defer, threads
from twisted.python import threadable, log
threadable.init(1)
import traceback, sys, StringIO, random

class Mc:
    
    def __init__(self, servers, pool_size=1, old_lib=False):
        c_hash = False
        try:
            if old_lib: raise ImportError
            import pylibmc
            c = pylibmc.Client
            c_hash = True
        except ImportError: 
            log.msg('WARN: pylibmc not installed')    
            import memcache
            c = memcache.Client
            
        log.msg('INFO: Creating memcache connection pool to servers %s' % servers)
        self.pool = []
        for i in xrange(pool_size):
            cli = c(servers)
            if c_hash:
                # Use consistent hashing
                cli.set_behaviors({
                    'ketama': 1,
                    'ketama_hash': 1,
                })
            self.pool.append(cli)
        log.msg('INFO: Created %s connections to memcache servers %s' % (len(self.pool), servers))

    def cache_pool(self):
        return random.choice(self.pool)
                        
    def set(self, key, value, time=0):
        return threads.deferToThread(self.cache_pool().set, key, value, time)

    def add(self, key, value, time=0):
        return threads.deferToThread(self.cache_pool().add, key, value, time)
        
    def set_multi(self, mapping, time=0):
        return threads.deferToThread(self.cache_pool().set_multi, mapping, time)
        
    def delete(self, key):
        return threads.deferToThread(self.cache_pool().delete, key)

    def delete_multi(self, keys):
        return threads.deferToThread(self.cache_pool().delete_multi, keys)
        
    def get(self, key):
        return threads.deferToThread(self.cache_pool().get, key)
        
    def get_multi(self, keys):
        return threads.deferToThread(self.cache_pool().get_multi, keys)
        
    def increment(self, key):
        return threads.deferToThread(self.cache_pool().incr, key)

    def decrement(self, key):
        return threads.deferToThread(self.cache_pool().decr, key)
        
    def flush_all(self):
        return threads.deferToThread(self.cache_pool().flush_all)

