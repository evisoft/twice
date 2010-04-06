from twisted.internet import reactor, protocol, defer
from twisted.enterprise import adbapi
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.python import log
import traceback, urllib, time
import cache, http, mail, mc

class DataStore:
    
    prefetch_dependent_elements = ['favorite', 'subscription', 'unread']
    prefetch_types = ['session']
    
    # Status codes
    uncacheable_status = [500, 502, 503, 504, 304, 307]
    uncacheable_methods = ['POST', 'PUT', 'DELETE']
    short_status = [404]
    
    def __init__(self, config):
        self.config = config   

        # Memcache Backend
        servers = config.get('backend_memcache').split(',')
        log.msg('Creating connections to backend_memcache servers %s...' % ','.join(servers))
        try:
            self.proto = mc.Mc(servers, pool_size=5)
            log.msg('backend_memcache OK')
        except:
            log.msg('ERROR: Failed to connect to backend_memcache')
            log.msg(traceback.format_exc())

        # Database Backend
        try:
            self.db = adbapi.ConnectionPool("pyPgSQL.PgSQL", 
                database=config['backend_dbname'], 
                host=config['backend_dbhost'], 
                user=config['backend_dbuser'], 
                password=config['backend_dbpass'],
                cp_noisy=True,
                cp_reconnect=True,
                cp_min=5,
                cp_max=20,
            )
            log.msg("Connected to db.")
        except ImportError:
            mail.error("Could not import PyPgSQL!\n%s" % traceback.format_exc())
        except:
            mail.error("Unable to connect to backend database.\n%s" % traceback.format_exc())
            
         # HTTP Backend
        try:
            self.backend_host, self.backend_port = self.config['backend_webserver'].split(':')
            self.backend_port = int(self.backend_port)
        except:
            self.backend_host = self.config['backend_webserver']
            self.backend_port = 80
            
        # Cache Backend
        log.msg('Initializing cache...')
        cache_type = config['cache_type'].capitalize() + 'Cache'
        self.cache = getattr(cache, cache_type)(config)     
        
        # Memorize variants of a uri
        self.uri_lookup = {}

        # Request pileup queue
        self.pending_requests = {}
        
    # Init status

    def dbConnected(self, db):
        log.msg('Database connection success.')
        self.db = db
                    
    # Main methods

    def get(self, keys, request, force=False):
        if not isinstance(keys, list): keys = [keys]
        if force:
            d = self.handleMisses(dict(zip(keys, [None for key in keys])), request)
        else:
            d = defer.maybeDeferred(self.cache.get, keys)
            d.addCallback(self.handleMisses, request)
            d.addErrback(self.getError)
        return d
        
    def delete(self, keys):
        if not isinstance(keys, list): keys = [keys]
        self.cache.delete(keys)
        
    def flush(self):
        "Flush entire cache"
        self.cache.flush()

    def handleMisses(self, dictionary, request):
        "Process hits, check for validity, and fetch misses / invalids"
        missing_deferreds = []
        missing_elements = []
        present_elements = []
        for key, value in dictionary.items():
            fetch = False
            if value is None:
                #log.msg('MISS [%s]' % key)
                fetch = True
            elif not getattr(self, 'valid_' + self.elementType(key))(request, self.elementId(key), value):
                log.msg('INVALID [%s]' % key)
                fetch = True
            else:
                pass
                #log.msg('HIT [%s]' % key)
            if fetch:
                d = defer.maybeDeferred(getattr(self, 'fetch_' + self.elementType(key)), request, self.elementId(key))
                d.addErrback(self.fetchError, key)
                missing_deferreds.append(d)
                missing_elements.append(key)
            else:
                present_elements.append(key)
        if present_elements:
            log.msg('HIT %s' % ', '.join(present_elements))
        # Wait for all items to be fetched
        if missing_deferreds:
            log.msg('MISS %s' % ', '.join(missing_elements))
            deferredList = defer.DeferredList(missing_deferreds)
            deferredList.addCallback(self.returnElements, dictionary, missing_elements)
            return deferredList
        else:
            return defer.succeed(dictionary)
        
    def returnElements(self, results, dictionary, missing_elements):
        if not isinstance(results, list): results = [results]
        uncached_elements = dict([(key, results.pop(0)[1]) for key in missing_elements])
        dictionary.update(uncached_elements)
        return dictionary

    def fetchError(self, result, key):
        log.msg('Error calling fetch_%s for key %s' % (self.elementType(key), self.elementId(key)))
        log.msg(result.getErrorMessage().replace('\n', ' '))
        return {}

    def getError(self, dictionary):
        log.msg('uh oh! %s' % dictionary)
        traceback.print_exc()

    # Hashing
            
    def elementHash(self, request, element_type, element_id = None):
        "Hash function for elements"
        return getattr(self, 'hash_' + element_type.lower())(request, element_id)
    
    # elementType's are not allowed to have _ in them because we use those in composing ids
    def elementType(self, key):
        return key.split('_')[0]
        
    def elementId(self, key):
        return '_'.join(key.split('_')[1:])
        
    # Expirations

    def hash_expiration(self, request, id):
        return 'expiration_' + request.uri.rstrip("?")

    def fetch_expiration(self, request, id):
        return False

    def valid_expiration(self, request, id, value):
        return True
                
    # Page
    
    def hash_page(self, request, id=None, cookies = []):
        # Hash the request key
        key = 'page_' + (request.getHeader('x-real-host') or request.getHeader('host') or '') + request.uri.split('#')[0].rstrip("?")
        # Internationalization salt
        if self.config.get('hash_lang_header'):
            header = request.getHeader('accept-language') or self.config.get('hash_lang_default', 'en-us')
            if header:
                try:
                    lang = header.replace(' ', '').split(';')[0].split(',')[0].lower()
                    #log.msg('lang: %s' % lang)
                    key += '//' + lang
                except:
                    traceback.print_exc()
        if cookies:
            # Grab the cookies we care about from the request
            found_cookies = []
            for cookie in cookies:
                val = request.getCookie(cookie)
                if val:
                    found_cookies.append('%s=%s' % (cookie, val))
            # Update key based on cookies we care about
            if found_cookies:
                key += '//' + ','.join(found_cookies)
        return key
        
    def fetch_page(self, request, id, ignoreResult=False):
        # Prevent idental request pileup
        key = self.hash_page(request)
        if key in self.pending_requests and ignoreResult:
            log.msg('PENDING: Request is already pending for %s' % request.uri)
            return True
        # Tell backend that we are Twice and strip cache-control headers
        request.setHeader(self.config.get('twice_header'), 'true')
        request.removeHeader('cache-control')
        # Make the request
        sender = http.HTTPRequestSender(request)
        sender.noisy = False
        reactor.connectTCP(self.backend_host, self.backend_port, sender)
        # Defer the result 
        d = sender.deferred.addCallback(self.extract_page, request).addErrback(self.page_failed, request)
        self.pending_requests[key] = d
        return d
        
    def valid_page(self, request, id, value):
        "Determine whether the page can be served from the cache"
        now = time.time()
        # Do not serve cached versions of pages if the method is not cacheable
        if request.method.upper() in self.uncacheable_methods:
            log.msg('PASS-THROUGH [%s]' % request.method.upper())
            return False
        # Force refetch of very stale (3x cache_control value) pages
        elif now > value['rendered_on'] + value['cache_control'] * 3:
            log.msg('STALE-HARD [%s]' % id)
            return False
        # Sevre semi-stale pages but refresh in the background
        elif now > value['rendered_on'] + value['cache_control']:
            log.msg('STALE-SOFT [%s]' % id)

            # Extend the valid cache length by 30s so we can fetch it
            response = value['response']
            cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
            key = self.hash_page(request, cookies = cookies)
            cache_control = response.getCacheControlHeader(self.config.get('cache_header')) or 0
            if response.status in self.uncacheable_status:
                log.msg('NO-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = False
            elif response.status in self.short_status:
                log.msg('SHORT-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = True
                cache_control = 30
            elif cache_control and cache_control > 0:
                log.msg('CACHE [%s] (for %ss)' % (key, cache_control))
                cache = True
            else:
                log.msg('NO-CACHE (No cache data) [%s]' % key)
                cache = False
            value['rendered_on'] += 30
            if cache:
                self.cache.set({key : value}, 60) # Give 60s to refresh the page

            # Now fetch a fresh copy in the background
            self.fetch_page(request, id, ignoreResult=True)
            return True
        # Valid page
        else:
            return True
        
    def page_failed(self, response, request):
        # Release pending lock
        key = self.hash_page(request)
        if key in self.pending_requests:
            del self.pending_requests[key]
        
        log.msg('ERROR: Could not retrieve [%s]' % request.uri.rstrip("?h"))
        response.printBriefTraceback()
        # TODO: Return something meaningful!
        return ''
        
    def extract_page(self, response, request):
        # Release pending lock
        key = self.hash_page(request)
        if key in self.pending_requests:
            del self.pending_requests[key]
        
        # Extract uniqueness info
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
        key = self.hash_page(request, cookies = cookies)

        # Store uri variant
        if key not in self.uri_lookup.setdefault(request.uri.rstrip("?"), []):
            log.msg('Added new variant for %s: %s' % (request.uri.rstrip("?"), key))
            self.uri_lookup[request.uri.rstrip("?")].append(key)

        # Override for non GET's
        if request.method.upper() in self.uncacheable_methods:
            log.msg('NO-CACHE (Method is %s) [%s]' % (request.method, key))
            cache = False
            cache_control = 0
        else:    
            # Cache logic
            cache_control = response.getCacheControlHeader(self.config.get('cache_header')) or 0
            if response.status in self.uncacheable_status:
                log.msg('NO-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = False
            elif cache_control and cache_control > 0:
                log.msg('CACHE [%s] (for %ss)' % (key, cache_control))
                cache = True
            elif response.status in self.short_status:
                log.msg('SHORT-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = True
                cache_control = 30
            else:
                log.msg('NO-CACHE (No cache data) [%s]' % key)
                cache = False
                
        # Actual return value  
        value =  {
            'dependencies' : [],
            'response' : response,
            'rendered_on' : time.time(),
            'cache_control' : cache_control
        }
        if cache:
            response.cookies = []
            self.cache.set({key : value}, cache_control + 86400) # Cache for an extra day
        return value
            
    # Memcache
    
    def hash_memcache(self, request, id):
        return 'memcache_' + id
    
    def fetch_memcache(self, request, id):
        #log.msg('Looking up memcache %s' % id)
        return self.proto.get(id).addCallback(self.extract_memcache, request, id)  
        
    def extract_memcache(self, result, request, id):
        # Un-comment for the twisted memcached library
        #value = result and result[1]
        value = result
        key = self.hash_memcache(request, id)
        self.cache.set({key: value}, 30) # 30 seconds
        return value
        
    def valid_memcache(self, request, id, value):
        return True
                
    def incr_memcache(self, key):
        #log.msg('Incrementing memcache %s' % key)
        return self.proto.increment(key)

    def decr_memcache(self, key):
        #log.msg('Decrementing memcache %s' % key)
        return self.proto.decrement(key)
        
    def delete_memcache(self, key):
        self.cache.delete(key)
        return self.proto.delete(key)
        
    def set_memcache(self, key, val):
        #log.msg('Setting memcache %s' % key)
        return self.proto.set(key, val)

    # geo location
    def hash_geo(self, request, id):
        return 'ip'
        
    # ip address
    def hash_ip(self, request, id):
        return 'ip'

    # User session
    
    def hash_session(self, request, id):
        id = self._read_session(request)
        if id:
            return 'session_' + id
        else:
            return ''
      
    def fetch_session(self, request, id):
        id = self._read_session(request)
        return self.db.runInteraction(self._session, id).addCallback(self.extract_session, request, id)
    
    def extract_session(self, result, request, id):
        if len(result):
            session = dict(zip(result[0].keys(), result[0].values()))
            output = session
        else:
            output = {}
        key = self.hash_session(request, id)
        self.cache.set({key : output}, 86400) # 24 hours
        return output
    
    def valid_session(self, request, id, value):
        return True
    
    def _read_session(self, request):
        return urllib.unquote(request.getCookie(self.config['session_cookie']) or '')

    def _session(self, txn, id):
        log.msg('Looking up session %s' % id)
        users_query = "select users.* from users where persistent_cookie = '%s'" % id
        #log.msg('Running query: %s' % users_query)
        txn.execute(users_query)
        users_result = txn.fetchall()
        return users_result
        
