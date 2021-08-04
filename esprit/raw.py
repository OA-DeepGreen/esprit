# The Raw ElasticSearch functions, no frills, just wrappers around the HTTP calls

import requests, json, urllib.request, urllib.parse, urllib.error, logging
from .models import QueryBuilder
from . import versions


class ESWireException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class BulkException(Exception):
    pass


class IndexPerTypeException(Exception):
    pass


DEFAULT_VERSION = "0.90.13"

# This is the type used when we are using the index-per type mapping pattern (ES < 7.0)
INDEX_PER_TYPE_SUBSTITUTE = '_doc'

logger = logging.getLogger(__name__)


def configure_logging(loglevel: int = logging.WARNING):
    logger.setLevel(loglevel)
    handler = logging.StreamHandler()
    handler.setLevel(loglevel)
    formatter = logging.Formatter('%(name)s:%(lineno)s - %(levelname)s - %(funcName)s(): %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


##################################################################
# Connection to the index

class Connection(object):
    def __init__(self, host, index, port=9200, auth=None, verify_ssl=True, index_per_type=False):
        """ Initialise a connection to an ES index. """
        self.host = host
        self.index = index
        self.port = port
        self.auth = auth
        self.verify_ssl = verify_ssl
        self.index_per_type = index_per_type

        # make sure that host starts with "http://" or equivalent
        if not self.host.startswith("http"):
            self.host = "http://" + self.host

        # some people might tack the port onto the host
        if len(self.host.split(":")) > 2:
            self.port = self.host[self.host.rindex(":") + 1:]
            self.host = self.host[:self.host.rindex(":")]


def make_connection(connection, host, port, index, auth=None, index_per_type=False):
    if connection is not None:
        return connection
    return Connection(host, index, port, auth, index_per_type=index_per_type)


####################################################################
# URL management

def elasticsearch_url(connection, type=None, endpoint=None, params=None, omit_index=False):
    index = connection.index
    host = connection.host
    port = connection.port

    # Re-create the connection if we are using index-per-type
    if type is not None and connection.index_per_type:
        index = type_to_index(connection, type)
        # Set the type to the default dummy type
        if not endpoint:
            type = ''
        elif endpoint == '_mapping':
            type = ''
            endpoint = endpoint
        else:
            type = INDEX_PER_TYPE_SUBSTITUTE

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
        if not url.endswith('/'):
            url += '/'
        url += type

    if endpoint is not None:
        if not url.endswith("/"):
            url += "/"
        url += endpoint

    # FIXME: NOT URL SAFE - do this properly
    if params is not None:
        args = []
        for k, v in params.items():
            args.append(k + "=" + v)
        q = "&".join(args)
        url += "?" + q

    return url


###############################################################
# HTTP Requests

def _do_head(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    kwargs["headers"] = {'Content-Type': 'application/json'}
    return requests.head(url, **kwargs)


def _do_get(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    kwargs["headers"] = {'Content-Type': 'application/json'}
    return requests.get(url, **kwargs)


def _do_post(url, conn, data=None, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    kwargs["headers"] = {'Content-Type': 'application/json'}
    return requests.post(url, data, **kwargs)


def _do_put(url, conn, data=None, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    kwargs["headers"] = {'Content-Type': 'application/json'}
    return requests.put(url, data, **kwargs)


def _do_delete(url, conn, **kwargs):
    if conn.auth is not None:
        if kwargs is None:
            kwargs = {}
        kwargs["auth"] = conn.auth
    kwargs["verify"] = conn.verify_ssl
    kwargs["headers"] = {'Content-Type': 'application/json'}
    return requests.delete(url, **kwargs)


# 2016-11-09 TD : A new search interface returning different output formats, e.g. csv
#               : Needs plugin org.codelibs/elasticsearch-dataformat/[version tag] ,
#               : (see https://github.com/codelibs/elasticsearch-dataformat for any details!)

###############################################################
## Dataformat Search

def data(connection, type=None, query=None, fmt="csv", method="POST", url_params=None):
    if url_params is None:
        url_params = { "format" : fmt }
    elif not isinstance(url_params, dict):
        url_params = { "format" : fmt }
    else:
        url_params["format"] = fmt

    url = elasticsearch_url(connection, type, "_data", url_params)

    if query is None:
        query = QueryBuilder.match_all()
    if not isinstance(query, dict):
        query = QueryBuilder.query_string(query)

    resp = None
    if method == "POST":
        headers = {"content-type" : "application/json"}
        resp = _do_post(url, connection, data=json.dumps(query), headers=headers)
    elif method == "GET":
        resp = _do_get(url + "&source=" + urllib.parse.quote(json.dumps(query)), connection)
    return resp


###############################################################
# Regular Search

def search(connection, type=None, query=None, method="POST", url_params=None):
    url = elasticsearch_url(connection, type, "_search", url_params)

    if query is None:
        query = QueryBuilder.match_all()
    if not isinstance(query, dict):
        query = QueryBuilder.query_string(query)

    resp = None
    if method == "POST":
        headers = {"content-type": "application/json"}
        resp = _do_post(url, connection, data=json.dumps(query), headers=headers)
    elif method == "GET":
        resp = _do_get(url + "?source=" + urllib.parse.quote_plus(json.dumps(query)), connection)
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
# Scroll search

def initialise_scroll(connection, type=None, query=None, keepalive="10m", scan=False):
    url_params = {"scroll": keepalive}
    if scan:
        url_params["search_type"] = "scan"
    return search(connection, type, query, url_params=url_params)


def scroll_next(connection, scroll_id, keepalive="10m"):
    url = elasticsearch_url(connection, endpoint="_search/scroll", params={"scroll_id": scroll_id, "scroll": keepalive},
                            omit_index=True)
    resp = _do_get(url, connection)
    return resp


def scroll_timedout(requests_response):
    # We are likely to receive a 404 (no search context found), perhaps 502 from a proxy. Count any error code.
    return requests_response.status_code >= 400


def unpack_scroll(requests_response):
    j = requests_response.json()
    objects = unpack_json_result(j)
    sid = j.get("_scroll_id")
    return objects, sid


#################################################################
# Record retrieval

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
    docs = {"docs": []}
    if fields is None:
        docs = {"ids": ids}
    else:
        fields = [] if fields is None else fields if isinstance(fields, list) else [fields]
        for id in ids:
            docs["docs"].append({"_id": id, "fields": fields})
    url = elasticsearch_url(connection, type, endpoint="_mget")
    resp = _do_post(url, connection, data=json.dumps(docs))
    return resp


def unpack_mget(requests_response):
    j = requests_response.json()
    objects = [i.get("_source") if "_source" in i else i.get("fields") for i in j.get("docs")]
    return objects


def total_results(requests_response):
    j = requests_response.json()
    return j.get("hits", {}).get("total", {}).get("value", 0)

####################################################################
# Mappings


def put_mapping(connection, type=None, mapping=None, make_index=True, es_version=DEFAULT_VERSION):
    if mapping is None:
        raise ESWireException("cannot put empty mapping")

    if not index_exists(connection, type):
        if make_index:
            create_index(connection, type, mapping={}, es_version=es_version)
        else:
            raise ESWireException("index '" + str(connection.index) + "' with type '" + type + "' does not exist")

    url = elasticsearch_url(connection, type=type, endpoint="_mapping")
    r = _do_put(url, connection, json.dumps(mapping))
    return r


def has_mapping(connection, type, es_version=DEFAULT_VERSION):
    resp = get_mapping(connection, type, es_version=DEFAULT_VERSION)
    return resp.status_code == 200


def get_mapping(connection, type, es_version=DEFAULT_VERSION):
    url = elasticsearch_url(connection, type, endpoint="_mapping")
    resp = _do_get(url, connection)
    return resp


##########################################################
# Existence checks

def type_exists(connection, type, es_version=DEFAULT_VERSION):
    if connection.index_per_type:
        return index_exists(connection, type)

    url = elasticsearch_url(connection, type)
    if versions.type_get(es_version):
        resp = _do_get(url, connection)
    else:
        resp = _do_head(url, connection)
    return resp.status_code == 200


def index_exists(connection, type=None):
    iurl = elasticsearch_url(connection, type, endpoint="")
    resp = _do_head(iurl, connection)
    return resp.status_code == 200


def alias_exists(connection, alias, type=None):
    aurl = elasticsearch_url(connection, type=type, endpoint="_aliases")
    resp = _do_get(aurl, connection)
    if index_exists(connection, type):
        return alias in list(resp.json()[connection.index]['aliases'].keys())
    else:
        return False


###########################################################
# Index create

def create_index(connection, type=None, mapping=None, es_version=DEFAULT_VERSION):
    iurl = elasticsearch_url(connection, type=type)
    return _do_create_index(connection, iurl, mapping, es_version)


def _do_create_index(connection, iurl, mapping, es_version):
    method = _do_put
    resp = method(iurl, connection)
    logger.debug(resp.text)
    if resp.status_code < 200 or resp.status_code >= 400:
        raise ESWireException(resp)
    return resp


# List and Delete indexes

def list_indexes(connection):
    url = elasticsearch_url(connection, endpoint='_status', omit_index=True)
    resp = _do_get(url, connection)
    return list(resp.json().get('indices').keys())


def delete_index_by_prefix(conn, index_prefix):
    """
    Delete all indexes starting with the given prefix. Remember that a complete match will also result in a delete, i.e.
    you may wish to include the separator so you don't delete too much (index_prefix='prefix-') so you don't delete
    an index just called 'prefix'.
    """
    indexes = [i for i in list_indexes(connection=conn) if i.startswith(index_prefix)]

    # Create a new Connection instance for the delete with all of the matching indexes
    del_conn = Connection(conn.host, index_prefix, conn.port, conn.auth, conn.verify_ssl)
    url = elasticsearch_url(del_conn)
    resp = _do_delete(url, del_conn)
    return resp


def delete_index(conn, type=None):
    url = elasticsearch_url(conn, type=type)
    resp = _do_delete(url, conn)
    return resp

############################################################
# Store records

def store(connection, type, record, id=None, params=None):
    url = elasticsearch_url(connection, type, endpoint=id, params=params)
    if id is not None:
        resp = _do_put(url, connection, data=json.dumps(record))
    else:
        resp = _do_post(url, connection, data=json.dumps(record))
    return resp


def to_bulk(records, idkey="id", index='', type_='', bulk_type="index", **kwargs):
    data = ''
    for r in records:
        data += to_bulk_single_rec(r, idkey=idkey, index=index, type_=type_, bulk_type=bulk_type, **kwargs)
    return data


def to_bulk_single_rec(record, idkey="id", index='', type_='', bulk_type="index", **kwargs):
    data = ''

    idpath = idkey.split(".")
    context = record
    for pathseg in idpath:
        if pathseg in context:
            context = context[pathseg]
        else:
            raise BulkException("'{0}' not available in record to generate bulk _id: {1}".format(idkey, json.dumps(record)))

    datadict = {bulk_type: {'_id': context}}
    if index:
        datadict[bulk_type]['_index'] = index
    if type_:
        datadict[bulk_type]['_type'] = type_

    datadict[bulk_type].update(kwargs)

    data += json.dumps(datadict) + '\n'
    data += json.dumps(record) + '\n'
    return data


def bulk(connection, records, idkey='id', type_='', bulk_type="index", **kwargs):
    data = to_bulk(records, idkey=idkey, bulk_type=bulk_type, **kwargs)
    url = elasticsearch_url(connection, type_, endpoint="_bulk")
    resp = _do_post(url, connection, data=data)
    return resp


def raw_bulk(connection, data, type=""):
    url = elasticsearch_url(connection, type, endpoint="_bulk")
    resp = _do_post(url, connection, data=data)
    return resp


############################################################
# Delete records

def delete(connection, type=None, id=None):
    url = elasticsearch_url(connection, type, endpoint=id)
    resp = _do_delete(url, connection)
    return resp


def delete_by_query(connection, type, query, es_version=DEFAULT_VERSION):
    url = elasticsearch_url(connection, type, endpoint="_query")
    if "query" in query and es_version.startswith("0.9"):
        # we have to unpack the query, as the endpoint covers that
        query = query["query"]
    resp = _do_delete(url, connection, data=json.dumps(query))
    return resp


def to_bulk_del(ids):
    data = ''
    for i in ids:
        data += json.dumps({'delete': {'_id': i}}) + '\n'
    return data


def bulk_delete(connection, type, ids):
    data = to_bulk_del(ids)
    url = elasticsearch_url(connection, type, endpoint="_bulk")
    resp = _do_post(url, connection, data=data)
    return resp


##############################################################
# Refresh

def refresh(connection, type):
    url = elasticsearch_url(connection, type=type, endpoint="_refresh")
    resp = _do_post(url, connection)
    return resp


##############################################################
# Aliases

def to_alias_actions(add=None, remove=None):
    """
    Create a list of actions to post to the alias endpoint
    :param add: a list of dicts: [{"alias": <alias name>, "index": <index name>}] to add to the alias list
    :param remove: a list of dicts: [{"alias": <alias name>, "index": <index name>}] to remove from the alias list
    :return: The actions packaged together, as {"actions": [...]}
    """
    acts = {"actions": []}
    add = add if add else []
    remove = remove if remove else []
    [acts["actions"].append({"add": a}) for a in add]
    [acts["actions"].append({"remove": r}) for r in remove]
    return acts


def post_alias(connection, alias_actions):
    url = elasticsearch_url(connection, endpoint="_aliases", omit_index=True)
    resp = _do_post(url, connection, json.dumps(alias_actions))
    return resp

##############################################################
# List types

def list_types(connection):
    if connection.index_per_type:
        raise IndexPerTypeException('list_types is meaningless for index-per-type connections. '
                                    'The only type is {0}'.format(INDEX_PER_TYPE_SUBSTITUTE))

    url = elasticsearch_url(connection, "_mapping")
    resp = _do_get(url, connection).json()
    index = list(resp.keys())[0]
    return list(resp[index]['mappings'].keys())


###############################################################
# Support for index-per-type

def type_to_index(connection, typ):
    if typ is not None:
        if type(typ) != list:
            typ = [typ]
        new_index = []
        old_index = connection.index
        if type(old_index) != list:
            old_index = [old_index]
        for i in old_index:
            new_index += ['{0}-{1}'.format(i, t) for t in list(typ)]
        return new_index
    return connection.index
