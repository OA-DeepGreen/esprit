# The Raw ElasticSearch functions, no frills, just wrappers around the HTTP calls

import requests, json, urllib
from models import QueryBuilder

class ESWireException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

##################################################################
## Connection to the index

class Connection(object):
    def __init__(self, host, index, port=9200, auth=None, verify_ssl=True):
        self.host = host
        self.index = index
        self.port = port
        self.auth = auth
        self.verify_ssl = verify_ssl
        
        # make sure that host starts with "http://" or equivalent
        if not self.host.startswith("http"):
            self.host = "http://" + self.host
        
        # some people might tack the port onto the host
        if len(self.host.split(":")) > 2:
            self.port = self.host[self.host.rindex(":") + 1:]
            self.host = self.host[:self.host.rindex(":")]

def make_connection(connection, host, port, index, auth=None):
    if connection is not None:
        return connection
    return Connection(host, index, port, auth)

####################################################################
## URL management

def elasticsearch_url(connection, type=None, endpoint=None, params=None, omit_index=False):
    index = connection.index
    host = connection.host
    port = connection.port

    # normalise the indexes input
    if omit_index:
        index = ""
    elif index is None and not omit_index:
        index = "_all"
    if isinstance(index, list):
        index = ",".join(index)
    
    # normalise the types input
    if type is None:
        type = ""
    if isinstance(type, list):
        type = ",".join(type)
    
    # normalise the host
    if not host.startswith("http"):
        host = "http://" + host
    if host.endswith("/"):
        host = host[:-1]
    
    if port is not None:
        host += ":" + str(port)
    host += "/"
    
    url = host + index
    if type is not None and type != "":
        url += "/" + type
    
    if endpoint is not None:
        if not url.endswith("/"):
            url += "/"
        url += endpoint
    
    # FIXME: NOT URL SAFE - do this properly
    if params is not None:
        args = []
        for k, v in params.iteritems():
            args.append(k + "=" + v)
        q = "&".join(args)
        url += "?" + q
    
    return url

###############################################################
## HTTP Requests

def _do_head(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    return requests.head(url, **kwargs)

def _do_get(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    return requests.get(url, **kwargs)

def _do_post(url, conn, data=None, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    return requests.post(url, data, **kwargs)

def _do_put(url, conn, data=None, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    return requests.put(url, data, **kwargs)

def _do_delete(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    return requests.delete(url, **kwargs)


###############################################################
## Regular Search

def search(connection, type=None, query=None, method="POST", url_params=None):
    url = elasticsearch_url(connection, type, "_search", url_params)
    
    if query is None:
        query = QueryBuilder.match_all()
    if not isinstance(query, dict):
        query = QueryBuilder.query_string(query)
    
    resp = None
    if method == "POST":
        headers = {"content-type" : "application/json"}
        resp = _do_post(url, connection, data=json.dumps(query), headers=headers)
    elif method == "GET":
        resp = _do_get(url + "?source=" + urllib.quote_plus(json.dumps(query)), connection)
    return resp

def unpack_result(requests_response):
    j = requests_response.json()
    return unpack_json_result(j)

def unpack_json_result(j):
    objects = [i.get("_source") if "_source" in i else i.get("fields") for i in j.get('hits', {}).get('hits', [])]
    return objects

def get_facet_terms(json_result, facet_name):
    return json_result.get("facets", {}).get(facet_name, {}).get("terms", [])

#################################################################
## Scroll search

def initialise_scroll(connection, type=None, query=None, keepalive="1m"):
    return search(connection, type, query, url_params={"scroll" : keepalive})

def scroll_next(connection, scroll_id, keepalive="1m"):
    url = elasticsearch_url(connection, endpoint="_search/scroll", params={"scroll_id" : scroll_id, "scroll" : keepalive}, omit_index=True)
    resp = _do_get(url, connection)
    return resp

def scroll_timedout(requests_response):
    return requests_response.status_code == 500

def unpack_scroll(requests_response):
    j = requests_response.json()
    objects = unpack_json_result(j)
    sid = j.get("_scroll_id")
    return objects, sid

#################################################################
## Record retrieval

def get(connection, type, id):
    url = elasticsearch_url(connection, type, endpoint=id)
    resp = _do_get(url, connection)
    return resp

def unpack_get(requests_response):
    j = requests_response.json()
    return j.get("_source")

def mget(connection, type, ids, fields=None):
    if ids is None:
        raise ESWireException("mget requires one or more ids")
    docs = {"docs" : []}
    if fields is None:
        docs = {"ids" : ids}
    else:
        fields = [] if fields is None else fields if isinstance(fields, list) else [fields]
        for id in ids:
            docs["docs"].append({"_id" : id, "fields" : fields})
    url = elasticsearch_url(connection, type, endpoint="_mget")
    resp = _do_post(url, connection, data=json.dumps(docs))
    return resp

def unpack_mget(requests_response):
    j = requests_response.json()
    objects = [i.get("_source") if "_source" in i else i.get("fields") for i in j.get("docs")]
    return objects

####################################################################
## Mappings

def put_mapping(connection, type=None, mapping=None, make_index=True, es_version="0.90.13"):
    if mapping is None:
        raise ESWireException("cannot put empty mapping")
    
    if not index_exists(connection):
        if make_index:
            create_index(connection)
        else:
            raise ESWireException("index '" + str(connection.index) + "' does not exist")

    if es_version.startswith("0.9"):
        url = elasticsearch_url(connection, type, "_mapping")
        r = _do_put(url, connection, json.dumps(mapping))
        return r
    elif es_version.startswith("1."):
        url = elasticsearch_url(connection, "_mapping", type)
        r = _do_put(url, connection, json.dumps(mapping))
        return r

def has_mapping(connection, type, es_version="0.90.13"):
    if es_version.startswith("0.9"):
        url = elasticsearch_url(connection, type, endpoint="_mapping")
        resp = _do_get(url, connection)
        return resp.status_code == 200
    elif es_version.startswith("1."):
        url = elasticsearch_url(connection, "_mapping", type)
        resp = _do_get(url, connection)
        return resp.status_code == 200

def get_mapping(connection, type, es_version="0.90.13"):
    if es_version.startswith("0.9"):
        url = elasticsearch_url(connection, type, endpoint="_mapping")
        resp = _do_get(url, connection)
        return resp
    elif es_version.startswith("1."):
        url = elasticsearch_url(connection, "_mapping", type)
        resp = _do_get(url, connection)
        return resp

##########################################################
## Existence checks

def type_exists(connection, type, es_version="0.90.13"):
    url = elasticsearch_url(connection, type)
    if es_version.startswith("0"):
        resp = _do_get(url, connection)
    else:
        resp = _do_head(url, connection)
    return resp.status_code == 200

def index_exists(connection):
    iurl = elasticsearch_url(connection, endpoint="_mapping")
    resp = _do_get(iurl, connection)
    return resp.status_code == 200

###########################################################
## Index create

def create_index(connection, mapping=None):
    iurl = elasticsearch_url(connection)
    if mapping is None:
        resp = _do_post(iurl, connection)
    else:
        resp = _do_post(iurl, connection, data=json.dumps(mapping))
    return resp

############################################################
## Store records

def store(connection, type, record, id=None, params=None):
    url = elasticsearch_url(connection, type, endpoint=id, params=params)
    resp = None
    if id is not None:
        resp = _do_put(url, connection, data=json.dumps(record))
    else:
        resp = _do_post(url, connection, data=json.dumps(record))
    return resp

def to_bulk(records, idkey="id"):
    data = ''
    for r in records:
        data += json.dumps( {'index':{'_id':r[idkey]}} ) + '\n'
        data += json.dumps( r ) + '\n'
    return data

def bulk(connection, type, records, idkey='id'):
    data = to_bulk(records, idkey=idkey)
    url = elasticsearch_url(connection, type, endpoint="_bulk")
    resp = _do_post(url, connection, data=data)
    return resp

############################################################
## Delete records

def delete(connection, type=None, id=None):
    url = elasticsearch_url(connection, type, endpoint=id)
    resp = _do_delete(url, connection)
    return resp

def delete_by_query(connection, type, query, es_version="0.90.13"):
    url = elasticsearch_url(connection, type, endpoint="_query")
    if "query" in query and es_version.startswith("0.9"):
        # we have to unpack the query, as the endpoint covers that
        query = query["query"]
    resp = _do_delete(url, connection, data=json.dumps(query))
    return resp

##############################################################
## Refresh

def refresh(connection):
    url = elasticsearch_url(connection, endpoint="_refresh")
    resp = _do_post(url, connection)
    return resp
