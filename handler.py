from __future__ import with_statement
from twisted.internet import reactor, defer, protocol, threads
from twisted.python import threadable, log
import sys, urllib, time, re, traceback, os, time
import cPickle as pickle
import parser, storage, http, cache, mc
threadable.init(1)

try:
    import GeoIP
    gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
except:
    log.msg('Unable to load GeoIP library:\n%s' % traceback.format_exc())

# for adding commas to strings
comma_re = re.compile(r"(?:\d*\.)?\d{1,3}-?")

# acts as a fake dictionary for <& get geo ip &>
class GeoLookup:
    def __init__(self, request, connection):
        self.request = request
        self.connection = connection
        self.geos = dict()
        
    def get(self, ip="ip"):
        if ip not in self.geos: 
            try:
                if ip == "ip":
                    lookup = self.request.getRemoteIp(self.connection)
                else:
                    lookup = ip
                self.geos[ip] = gi.country_code_by_addr(lookup)
            except:
                pass
        return self.geos[ip]

# acts as a fake dictionary for <& get ip current &>
class IpLookup:
    def __init__(self, request, connection):
        self.request = request
        self.connection = connection
        self.ip = None
        
    def get(self, ignored=None):
        if not self.ip:
            self.ip = self.request.getRemoteIp(self.connection)
        return self.ip
        
class MessageSender(protocol.Protocol):
    def __init__(self, message):
        self.message = message

    def connectionMade(self):
        # log.msg("Sending message to logging server: %s" % self.message)
        # send the message
        self.transport.write(self.message)
        # close the connection
        self.transport.loseConnection()

class RequestHandler(http.HTTPRequestDispatcher):
    def __init__(self, config):
        # Caches and config
        self.config = config
        
        # Template format
        self.specialization_re = re.compile(self.config['template_regex'])

        # Data Store
        log.msg('Initializing data store...')
        self.store = storage.DataStore(config)

    def objectReceived(self, connection, request):
        "Main request handler"
            
        # Handle mark dirty requests
        if request.getHeader(self.config.get('purge_header')) is not None:
            self.markDirty(connection, request)
            return
            
        # Overwrite host field
        real_host = self.config.get('rewrite_host', request.getHeader('x-real-host'))
        if real_host:
            request.setHeader('host', real_host)
        try:
            mygeo = GeoLookup(request, connection).get()
            request.setHeader('x-geo', mygeo)
        except:
            log.msg("Failed to set geo header")
        # Add in prefetch keys
        keys = []
        keys.append(self.store.elementHash(request, 'page'))
        keys.append(self.store.elementHash(request, 'expiration'))
        session_key = self.store.elementHash(request, 'session')
        if session_key:
            keys.append(session_key)

        # Retrieve keys
        log.msg('PREFETCH: %s' % keys)
        self.store.get(keys, request).addCallback(self.checkPage, connection, request)
            
# ---------- CACHE EXPIRATION -----------
            
    def markDirty(self, connection, request):
        "Mark a uri as dirty"
        uri = request.uri
        try:
            kind = request.getHeader(self.config.get('purge_header')).lower()
        except:
            log.msg('Could not read expiration type: %s' % repr(request.getHeader(self.config.get('purge_header'))))
            return
        log.msg("Expire type: %s, arg: %s" % (kind, uri))
        # Parse request
        if kind == '*':
            self.store.flush()
            log.msg('Cleared entire cache')
        elif kind == 'url':
            try:
                key = self.store.elementHash(request, 'expiration')
                self.store.cache.set({key : time.time()}, 86400)
                log.msg('Expired all variants of %s' % uri)
            except:
                log.msg('Could not delete variants of %s' % uri)
        elif kind == 'session':
            try:
                types = ['friendrequestedby', 'friendrequest', 'friend', 'favorite', 'subscription', 'unread', 'session', 'ratelimit']
                keys = ['%s_%s' % (t, uri[1:]) for t in types]
                self.store.delete(keys)
                log.msg('Deleted session-related keys: %s' % keys)
            except:
                pass
        else:
            try:
                key = kind + '_' + uri[1:]
                self.store.delete(key)
                log.msg('Deleted %s_%s' % (kind, uri[1:]))
            except:
                pass
        # Write response
        connection.sendCode(200, "Expired %s_%s" % (kind, uri))
        return True       
                
# ---------- CLIENT RESPONSE -----------  

    def checkPage(self, elements, connection, request, extra = {}):
        "See if we have the correct version of the page"       
        elements.update(extra)         
        rkey, rval = [(key, val) for key, val in elements.items() if key.startswith('page_')][0]
        if 'response' not in rval:
            connection.sendCode(408, 'Request timed out.')
            return True
        response = rval['response']
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
        key = self.store.hash_page(request, cookies = cookies)

        # If the page we fetched doesn't have the right cookies, try again!
        if key != self.store.hash_page(request):
            del elements[rkey]
            return self.store.get(key, request).addCallback(self.scanPage, connection, request, elements)
            #return self.store.get(key, request).addCallback(self.checkExpiry, connection, request, elements)
                            
        # If the page is expired, request a new copy
        expire_time = [v for k, v in elements.items() if k.startswith('expiration_')][0]
        if expire_time and rval['rendered_on'] < expire_time:
            log.msg('EXPIRED: rendered_on %s, expire_time %s' % (rval['rendered_on'], expire_time))
            del elements[rkey]
            return self.store.get(key, request, force=True).addCallback(self.checkPage, connection, request, elements)
        
        return self.scanPage(elements, connection, request)


#        # Page looks good, carry on!
#        return self.checkExpiry(elements, connection, request)
        
#    def checkExpiry(self, elements, connection, request, extra = {}):
#        "If the page is expired, request a new copy"
#        elements.update(extra)        
#        rkey, rval = [(key, val) for key, val in elements.items() if key.startswith('page_')][0]
#        if 'response' not in rval:
#            connection.sendCode(408, 'Request timed out.')
#            return True
#        response = rval['response']
#        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
#        key = self.store.hash_page(request, cookies = cookies)
#        expire_time = [v for k, v in elements.items() if k.startswith('expiration_')][0]
#        if expire_time and rval['rendered_on'] < expire_time:
#            log.msg('EXPIRED: rendered_on %s, expire_time %s' % (rval['rendered_on'], expire_time))
#            del elements[rkey]
#            return self.store.get(key, request, force=True).addCallback(self.checkPage, connection, request, elements)
#        return self.scanPage(elements, connection, request)

    def scanPage(self, elements, connection, request, extra = {}):
        "Scan for missing elements"
        elements.update(extra)        
        logged_in = [True for key, value in elements.items() if key.startswith('session_') and value is not None]
        data = [val for key, val in elements.items() if key.startswith('page_')][0]['response'].body
        matches = self.specialization_re.findall(data)
        missing_keys = []
        for match in matches:
            # Parse element
            try:
                parts = match.strip().split()
                command = parts[0].lower()
                element_type = parts[1].lower()
                element_id = parts[2]
            except:
                log.msg('Error in scanPage:\n%s' % traceback.format_exc())
                continue
            if element_type not in ['page', 'session', 'geo', 'ip']:
                if element_type in ['memcache', 'viewdb', 'ratelimit'] or logged_in:
                    try:
                        key = self.store.elementHash(request, element_type, element_id)
                    except:
                        log.msg('Error: could not generate hash for type %s: %s' % (element_type, traceback.format_exc()))
                        key = None
                    if key and key not in missing_keys:
                        missing_keys.append(key)
        if missing_keys:
            d = self.store.get(missing_keys, request)
            d.addCallback(self.renderPage, connection, request, elements)
        else:
            self.renderPage({}, connection, request, elements)

    def renderPage(self, new_elements, connection, request, elements):
        "Write the page out to the request's connection"
        elements.update(new_elements)
        # unread must come after session
        for etype in ['page', 'session', 'friendrequestedby', 'friendrequest', 'friend', 'favorite', 'subscription', 'unread', 'ratelimit']:
            eitems = [val for key, val in elements.items() if key.startswith(etype + "_")]
            if eitems:
                eitems = eitems[0]
            else:
                eitems = {}
            setattr(self, 'current_' + etype, eitems)
            #log.msg('Current %s: %s' % (etype, eitems))

        for etype in ['memcache', 'viewdb']:
            #log.msg('elements: %s' % elements.items())
            eitems = dict([(self.store.elementId(key), val) for key, val in elements.items() if key.startswith(etype + "_")])
            setattr(self, 'current_' + etype, eitems)
            #log.msg('Current %s: %s' % (etype, eitems))
        
        self.current_geo = GeoLookup(request, connection)
        self.current_ip  = IpLookup(request, connection)
        
        response = self.current_page['response']
        # Do Templating
        data = self.specialization_re.sub(self.specialize, response.body)
        # Remove current stuff
        for etype in ['session', 'friendrequestedby', 'friendrequest', 'friend', 'favorite', 'subscription', 'unread', 'ratelimit']:
            setattr(self, 'current_' + etype, {})
        # Log
        app_server = response.getHeader('x-app-server') or 'unknown'
        log.msg('RENDER %s [%s] (%.3fs from %s)' % (response.status, request.uri, (time.time() - request.received_on), app_server.strip()))
        # Overwrite headers
        response.setHeader('connection', 'close')
        response.setHeader('content-length', len(data))
        response.setHeader('via', 'Twice %s %s:%s' % (self.config['version'], self.config['hostname'], self.config['port']))
        # Send geo along to user
        try:
            response.setHeader('x-geo', self.current_geo.get())
        except:
            pass
        # Delete twice/cache headers
        response.removeHeader(self.config.get('cache_header'))
        response.removeHeader(self.config.get('twice_header'))
        response.removeHeader(self.config.get('cookies_header'))
        response.removeHeader('x-app-server')
        # Write response
        connection.transport.write(response.writeResponse(body = data))
        connection.shutdown()
        
        if 'embed/referee' in request.uri and hasattr(self, 'local_cache'):
            log.msg('LOCAL-CACHE ngx:%s (for 30s)' % request.uri)
            try:
                self.local_cache.set('ngx:%s' % request.uri, data, 30)
            except:
                log.msg('Error writing to local cache: %s' % traceback.format_exc())

# ---------- TEMPLATING -----------

    def specialize(self, expression):
        "Parse an expression and return the result"
        try:
            expression = expression.groups()[0].strip()
            parts = expression.split()
            # Syntax is: command target arg1 arg2 argn
            #   command - one of 'get', 'if', 'unless', 'incr', 'decr'
            #   target - one of 'memcache', 'session'
            #   arg[n] - usually the name of a key
            command, target, args = parts[0].lower(), parts[1], parts[2:]
            #log.msg('command: %s target: %s args: %s' % (command, target, repr(args)))
        except:
            log.msg('Could not parse expression: [%s]' % expression)
            return expression
        # Grab dictionary
        try:
            dictionary = getattr(self, 'current_' + target)
        except:
            dictionary = {}
        
        actual_args = []
        filters = []
        
        next_filter = False
        for arg in args:
            if arg == "|":
                next_filter = True
            elif next_filter:
                filters.append(arg)
            else:
                actual_args.append(arg)
        
        args = actual_args
        return_val = None
        
        #log.msg('dictionary: %s' % dictionary)
        # Handle commands
        if command == 'get' and len(args) >= 1:
            if len(args) >= 2:
                default = args[1]
            else:
                default = ''
            val = dictionary.get(args[0])
            if not val:
                val = default
            #log.msg('arg: %s val: %s (default %s)' % (args[0], val, default))
            return_val = str(val)
        elif command == 'pop' and len(args) >= 1:
            if len(args) >= 2:
                default = args[1]
            else:
                default = ''
            val = dictionary.get(args[0])
            if not val:
                val = default
            else:
                try:
                    getattr(self.store, command + '_delete')(args[0])
                except:
                    log.msg('Data store is missing %s_delete' % command)
            return_val = str(val)
        elif command == 'if' and len(args) >= 2:
            if dictionary.get(args[0]):
                return_val = str(args[1])
            elif len(args) >= 3:
                return_val = str(args[2])
            else:
                return_val = ''
        elif command == 'unless' and len(args) >= 2:
            if not dictionary.get(args[0]):
                return_val = str(args[1])
            elif len(args) >= 3:
                return_val = str(args[2])
            else:
                return_val = ''
        elif (command == 'incr' or command == 'decr') and len(args) >= 1:
            try:
                func = getattr(self.store, command + '_' + target)
                set_func = getattr(self.store, 'set_' + target)
            except:
                log.msg('Data store is missing %s_%s or set_%s' % (command, target, target))
                return_val = ''
            #log.msg('dict: %s' % dictionary)
            val = dictionary.get(args[0])
            if val:
                try:
                    func(args[0])
                    if command == 'incr':
                        dictionary[args[0]] = int(val) + 1
                    else:
                        dictionary[args[0]] = int(val) - 1
                except:
                    pass
            elif len(args) >= 2:
                set_func(args[0], args[1])
                dictionary[args[0]] = args[1]
            return_val = ''
        else:
            log.msg('Invalid command: %s' % command)
            return_val = expression
        
        return self.apply_filters(return_val, filters)
    
    def apply_filters(self, value, filters):
        for filter in filters:
            if filter == "js": value = value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
            elif filter == "html": value = value.replace("<", "&lt;").replace(">", "&gt;").replace('&', '&amp;')
            elif filter == "comma": value = ','.join(comma_re.findall(value[::-1]))[::-1]
            else: pass
        return value
                    
