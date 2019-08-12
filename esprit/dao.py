import uuid, json
from esprit import raw, util, tasks, versions
from copy import deepcopy
import time


class StoreException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DAO(object):
    __es_version__ = "1.7.5"

    def __init__(self, raw=None):
        try:
            object.__getattribute__(self, "data")
        except AttributeError:
            self.data = {} if raw is None else raw
        try:
            object.__getattribute__(self, "_es_version")
        except AttributeError:
            self._es_version = self.__es_version__

        super(DAO, self).__init__()

    @property
    def id(self):
        return self.data.get('id', None)

    @id.setter
    def id(self, val):
        self.data["id"] = val

    @property
    def created_date(self):
        return self.data.get("created_date")

    @created_date.setter
    def created_date(self, val):
        self.data["created_date"] = val

    @property
    def last_updated(self):
        return self.data.get("last_updated")

    @last_updated.setter
    def last_updated(self, val):
        self.data["last_updated"] = val

    @property
    def json(self):
        return json.dumps(self.data)

    @property
    def raw(self):
        return self.data

    def save(self, conn=None, makeid=True, created=True, updated=True, blocking=False, type=None, max_wait=False):
        if conn is None:
            conn = self._get_connection()

        if type is None:
            type = self._get_write_type()

        if blocking and not updated:
            raise StoreException("Unable to do blocking save on record where last_updated is not set")

        now = util.now()
        if blocking:
            # we need the new last_updated time to be later than the new one
            if now == self.last_updated:
                time.sleep(1)   # timestamp granularity is seconds, so just sleep for 1
            now = util.now()    # update the new timestamp

        # the main body of the save
        if makeid:
            if "id" not in self.data:
                self.id = self.makeid()
        if created:
            if 'created_date' not in self.data:
                self.data['created_date'] = now
        if updated:
            self.data['last_updated'] = now

        resp = raw.store(conn, type, self.data, self.id)
        if resp.status_code < 200 or resp.status_code >= 400:
            raise raw.ESWireException(resp)

        if blocking:
            if versions.fields_query(self._es_version):
                self._es_field_block(conn, type, now, max_wait)
            else:
                self._es_source_block(conn, type, now, max_wait)

    def _es_field_block(self, conn, type, now, max_wait=False):
        q = {
            "query": {
                "term": {"id.exact": self.id}
            },
            "fields": ["last_updated"]
        }
        waited = 0.0
        while True:
            if max_wait is not False and waited >= max_wait:
                break
            res = raw.search(conn, type, q)
            j = raw.unpack_result(res)
            if len(j) == 0:
                time.sleep(0.5)
                waited += 0.5
                continue
            if len(j) > 1:
                raise StoreException("More than one record with id {x}".format(x=self.id))
            if j[0].get("last_updated")[0] == now:  # NOTE: only works on ES > 1.x
                break
            else:
                time.sleep(0.5)
                waited += 0.5
                continue

    def _es_source_block(self, conn, type, now, max_wait=False):
        q = {
            "query": {
                "term": {"id": self.id}
            },
            "_source": ["last_updated"]
        }
        waited = 0.0
        while True:
            if max_wait is not False and waited >= max_wait:
                break
            res = raw.search(conn, type, q)
            j = raw.unpack_result(res)
            if len(j) == 0:
                time.sleep(0.5)
                waited += 0.5
                continue
            if len(j) > 1:
                raise StoreException("More than one record with id {x}".format(x=self.id))
            if j[0].get("last_updated") == now:
                break
            else:
                time.sleep(0.5)
                waited += 0.5
                continue

    def delete(self, conn=None, type=None):
        if conn is None:
            conn = self._get_connection()

        # the record may be in any one of the read types, so we need to check them all
        if type is not None:
            if isinstance(type, list):
                types = type
            else:
                types = [type]
        else:
            types = self._get_read_types()

        # in the simple case of one type, just get on and issue the delete
        if len(types) == 1:
            raw.delete(conn, types[0], self.id)

        # otherwise, check all the types until we find the object, then issue the delete there
        for t in types:
            o = raw.get(conn, t, self.id)
            if o is not None:
                raw.delete(conn, t, self.id)

    @classmethod
    def makeid(cls):
        return uuid.uuid4().hex
    
    def actions(self, conn, action_queue):
        for action in action_queue:
            if list(action.keys())[0] == "remove":
                self._action_remove(conn, action)
            elif list(action.keys())[0] == "store":
                self._action_store(conn, action)

    def _action_remove(self, conn, remove_action):
        obj = remove_action.get("remove")
        if "index" not in obj:
            raise StoreException("no index provided for remove action")
        if "id" not in obj and "query" not in obj:
            raise StoreException("no id or query provided for remove action")
        if "id" in obj:
            raw.delete(conn, obj.get("index"), obj.get("id"))
        elif "query" in obj:
            raw.delete_by_query(conn, obj.get("index"), obj.get("query"))

    def _action_store(self, conn, store_action):
        obj = store_action.get("store")
        if "index" not in obj:
            raise StoreException("no index provided for store action")
        if "record" not in obj:
            raise StoreException("no record provided for store action")
        raw.store(conn, obj.get("index"), obj.get("record"), obj.get("id"))

    ##################################################
    # if you are subclassing, you need to implement these

    def _get_connection(self):
        raise NotImplementedError()

    def _get_write_type(self):
        raise NotImplementedError()

    def _get_read_types(self):
        raise NotImplementedError()


class DomainObject(DAO):
    __type__ = None
    __conn__ = None
    
    def __init__(self, raw=None):
        super(DomainObject, self).__init__(raw=raw)

    def _get_connection(self):
        return self.__conn__

    ################################################
    # somewhat messy type system

    # overrides of the instantiated DAO methods
    def _get_write_type(self):
        return self.get_write_type()

    def _get_read_types(self):
        return self.get_read_types()

    @classmethod
    def dynamic_read_types(cls):
        return None

    @classmethod
    def dynamic_write_type(cls):
        return None

    @classmethod
    def get_read_types(cls, types=None):
        if types is not None:
            if not isinstance(types, list):
                return [types]
            return types
        drt = cls.dynamic_read_types()
        if drt is not None:
            if not isinstance(drt, list):
                return [drt]
            return drt
        return [cls.__type__]

    @classmethod
    def get_write_type(cls, type=None):
        if type is not None:
            return type
        dwt = cls.dynamic_write_type()
        if dwt is not None:
            return dwt
        return cls.__type__

    # End of type system
    ################################################

    @classmethod
    def refresh(cls, conn=None):
        if conn is None:
            conn = cls.__conn__
        raw.refresh(conn)
    
    @classmethod
    def pull(cls, id_, conn=None, wrap=True, types=None):
        """Retrieve object by id."""
        if conn is None:
            conn = cls.__conn__

        types = cls.get_read_types(types)

        if id_ is None:
            return None
        try:
            for t in types:
                resp = raw.get(conn, t, id_)
                if resp.status_code == 404:
                    continue
                else:
                    j = raw.unpack_get(resp)
                    if wrap:
                        return cls(j)
                    else:
                        return j
            return None
        except Exception as e:
            print(e)
            return None
    
    @classmethod
    def query(cls, q='', terms=None, should_terms=None, facets=None, conn=None, types=None, **kwargs):
        """ Perform a query on backend.

        :param q: maps to query_string parameter if string, or query dict if dict.
        :param terms: dictionary of terms to filter on. values should be lists. 
        :param facets: dict of facets to return from the query.
        :param kwargs: any keyword args as per
            http://www.elasticsearch.org/guide/reference/api/search/uri-request.html
        """
        if conn is None:
            conn = cls.__conn__

        types = cls.get_read_types(types)
        
        if isinstance(q, dict):
            query = q
            if 'bool' not in query['query']:
                boolean = {'bool': {'must': []}}
                boolean['bool']['must'].append(query['query'])
                query['query'] = boolean
            if 'must' not in query['query']['bool']:
                query['query']['bool']['must'] = []
        elif q:
            query = {
                'query': {
                    'bool': {
                        'must': [
                            {'query_string': {'query': q}}
                        ]
                    }
                }
            }
        else:
            query = {
                'query': {
                    'bool': {
                        'must': [
                            {'match_all': {}}
                        ]
                    }
                }
            }

        if facets:
            if 'facets' not in query:
                query['facets'] = {}
            for k, v in list(facets.items()):
                query['facets'][k] = {"terms": v}

        if terms:
            boolean = {'must': []}
            for term in terms:
                if not isinstance(terms[term], list):
                    terms[term] = [terms[term]]
                for val in terms[term]:
                    obj = {'term': {}}
                    obj['term'][term] = val
                    boolean['must'].append(obj)
            if q and not isinstance(q, dict):
                boolean['must'].append({'query_string': {'query': q}})
            elif q and 'query' in q:
                boolean['must'].append(query['query'])
            query['query'] = {'bool': boolean}

        for k, v in list(kwargs.items()):
            if k == '_from':
                query['from'] = v
            else:
                query[k] = v

        if should_terms is not None and len(should_terms) > 0:
            for s in should_terms:
                if not isinstance(should_terms[s], list):
                    should_terms[s] = [should_terms[s]]
                query["query"]["bool"]["must"].append({"terms": {s: should_terms[s]}})

        r = raw.search(conn, types, query)
        return r.json()

    @classmethod
    def object_query(cls, q='', terms=None, should_terms=None, facets=None, conn=None, types=None, wrap=True, **kwargs):
        j = cls.query(q=q, terms=terms, should_terms=should_terms, facets=facets, conn=conn, types=types, **kwargs)
        res = raw.unpack_json_result(j)
        return [cls(r) if wrap else r for r in res]

    @classmethod
    def delete_by_query(cls, query, conn=None, es_version="0.90.13", type=None):
        if conn is None:
            conn = cls.__conn__
        type = cls.get_write_type(type)

        raw.delete_by_query(conn, type, query, es_version=es_version)

    @classmethod
    def iterate(cls, q, page_size=1000, limit=None, wrap=True, **kwargs):
        q = q.copy()
        q["size"] = page_size
        q["from"] = 0
        if "sort" not in q:
            q["sort"] = [{"_uid": {"order": "asc"}}]
        counter = 0
        while True:
            # apply the limit
            if limit is not None and counter >= limit:
                break

            res = cls.query(q=q, **kwargs)
            rs = [r.get("_source") if "_source" in r else r.get("fields") for r in res.get("hits", {}).get("hits", [])]
            # print counter, len(rs), res.get("hits", {}).get("total"), len(res.get("hits", {}).get("hits", [])), json.dumps(q)
            if len(rs) == 0:
                break
            for r in rs:
                # apply the limit (again)
                if limit is not None and counter >= limit:
                    break
                counter += 1
                if wrap:
                    yield cls(r)
                else:
                    yield r
            q["from"] += page_size

    @classmethod
    def iterall(cls, page_size=1000, limit=None, **kwargs):
        return cls.iterate(deepcopy(all_query), page_size, limit, **kwargs)

    @classmethod
    def count(cls, q, **kwargs):
        q = deepcopy(q)
        q["size"] = 0
        res = cls.query(q=q, **kwargs)
        return res.get("hits", {}).get("total")

    @classmethod
    def scroll(cls, q=None, page_size=1000, limit=None, keepalive="1m", conn=None, raise_on_scroll_error=True, types=None, wrap=True):
        if conn is None:
            conn = cls.__conn__
        types = cls.get_read_types(types)

        if q is None:
            q = {"query": {"match_all": {}}}

        gen = tasks.scroll(conn, types, q, page_size=page_size, limit=limit, keepalive=keepalive)

        try:
            for o in gen:
                if wrap:
                    yield cls(o)
                else:
                    yield o
        except tasks.ScrollException as e:
            if raise_on_scroll_error:
                raise e
            else:
                return
    
########################################################################
# Some useful ES queries
########################################################################


all_query = {
    "query": {
        "match_all": {}
    }
}
