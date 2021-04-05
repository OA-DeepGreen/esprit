from copy import deepcopy
import string
from esprit import versions

unicode_punctuation_map = dict((ord(char), None) for char in string.punctuation)


class Query(object):
    def __init__(self, raw=None):
        self.q = QueryBuilder.match_all() if raw is None else raw
        if "query" not in self.q:
            self.q["query"] = {"match_all": {}}

    def query_string(self, s, op=None, must=False, should=False):
        self.clear_match_all()

        qs = {"query": s}
        if op is not None:
            qs["default_operator"] = op

        if must:
            self.add_must()
            self.q["bool"]["must"].append(qs)
        elif should:
            self.add_should()
            self.q["bool"]["must"].append(qs)
        else:
            self.q["query"]["query_string"] = qs

    def add_should(self):
        if "bool" not in self.q["query"]:
            self.q["query"]["bool"] = {}
        if "should" not in self.q["query"]["bool"]:
            self.q["query"]["bool"]["should"] = []

    def add_must(self):
        if "bool" not in self.q["query"]:
            self.q["query"]["bool"] = {}
        if "must" not in self.q["query"]["bool"]:
            self.q["query"]["bool"]["must"] = []

    def clear_match_all(self):
        if "match_all" in self.q["query"]:
            del self.q["query"]["match_all"]

    def include_source(self, values, es_version="0.90.13"):
        if "_source" not in self.q:
            self.q["_source"] = {}
        if versions.source_include(es_version):
            self.q["_source"]["include"] = values
        else:
            self.q["_source"]["includes"] = values

    def set_source(self, values):
        self.q["_source"] = values

    def as_dict(self):
        return self.q


class QueryBuilder(object):
    _match_all = {"query": {"match_all": {}}}
    _query_string = {"query": {"query_string": {"query": "<query string>"}}}
    _term = {"query": {"term": {}}} # term : {"<key>" : "<value>"}
    
    _terms_filter = {"query": {"filtered": {"filter": {"terms": {}}}}} # terms : {"<key>" : ["<value>"]}
    _term_filter = {"query": {"filtered": {"filter": {"term": {}}}}} # terms : {"<key>" : "<value>"}
    
    _fields_constraint = {"fields": []}
    
    _special_chars = ["+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", '"', "~", "*", "?", ":", "/"]
    _escape_char = "\\" # which is a special special character too!
    
    @classmethod
    def match_all(cls):
        return deepcopy(cls._match_all)
    
    @classmethod
    def query_string(cls, query):
        q = deepcopy(cls._query_string)
        q["query"]["query_string"]["query"] = query
        return q
        
    @classmethod
    def term(cls, key, value):
        q = deepcopy(cls._term) 
        q["query"]["term"][key] = value
        return q
    
    @classmethod
    def term_filter(cls, key, value):
        q = deepcopy(cls._term_filter)
        q["query"]["filtered"]["filter"]["term"][key] = value
        return q
    
    @classmethod
    def terms_filter(cls, key, values):
        if not isinstance(values, list):
            values = [values]
        q = deepcopy(cls._terms_filter)
        q["query"]["filtered"]["filter"]["terms"][key] = values
        return q
    
    @classmethod
    def fields(cls, query, fields=None):
        fields = [] if fields is None else fields if isinstance(fields, list) else [fields]
        fc = deepcopy(cls._fields_constraint)
        fc["fields"] = fields
        query.update(fc)
        return query

    @classmethod
    def tokenise(cls, text_string):
        # FIXME: note that we don't do anything about stopwords right now.
        out = text_string
        if type(text_string) == "str":
            out = text_string.translate(str.maketrans("", ""), string.punctuation)
        elif type(text_string) == "unicode":
            out = text_string.translate(unicode_punctuation_map)
        return list(set([o.lower() for o in out.split(" ") if o != ""]))
    
    @classmethod
    def escape(cls, query_string):
        qs = query_string.replace(cls._escape_char, cls._escape_char + cls._escape_char) # escape the escape char
        for sc in cls._special_chars:
            qs = qs.replace(sc, cls._escape_char + sc)
        return qs
