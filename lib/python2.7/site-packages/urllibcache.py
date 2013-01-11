import os
from urllib import quote, unquote
import urllib2
import email
from email.Message import Message
from StringIO import StringIO
import md5


class CachedResponse(StringIO):
    def info(self):
        return self.headers

    def geturl(self):
        return self.url


class CachedHandler(urllib2.BaseHandler):
    def __init__(self, cache):
        self.cache = cache

    def default_open(self, request):
        if request.get_method() != 'GET':
            return None
        url = quote(request.get_full_url(), '')
        path = os.path.join(self.cache, url)
        if os.path.exists(path):
            f = open(path)
            data = email.message_from_file(f)
            if data.get('x-cache-md5') is None:
                return None
            payload = data.get_payload()
            if data.get('x-cache-md5') != md5.new(payload).hexdigest():
                return None
            response = CachedResponse(payload)
            response.url = request.get_full_url()
            response.headers = dict(data.items())
            try:
                response.code = int(data['x-cache-code'])
                response.msg = data['x-cache-msg']
            except (TypeError, KeyError):
                return None
            return response
        return None

    def http_response(self, request, response):
        if request.get_method() != 'GET':
            return response
        headers = response.info()
        if headers.get('x-cache-md5') == None:
            data = Message()
            for k,v in headers.items():
                data[k] = v
            payload = response.read()
            data['x-cache-md5'] = md5.new(payload).hexdigest()
            data['x-cache-code'] = str(response.code)
            data['x-cache-msg'] = response.msg
            data.set_payload(payload)
            url = quote(request.get_full_url(), '')
            path = os.path.join(self.cache, url)
            f = open(path, 'w')
            f.write(str(data))
            f.close()
            new_response = CachedResponse(payload)
            new_response.url = response.url
            new_response.headers = response.headers
            new_response.code = response.code
            new_response.msg = response.msg
            return new_response
        return response


def build_opener(cache):
    return urllib2.build_opener(CachedHandler(cache))
