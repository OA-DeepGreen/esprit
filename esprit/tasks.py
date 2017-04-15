from esprit import raw, models
import json, sys, time


class ScrollException(Exception):
    pass


def copy(source_conn, source_type, target_conn, target_type, limit=None, batch_size=1000, method="POST", q=None, keyword_subfield="exact"):
    if q is None:
        q = models.QueryBuilder.match_all()
    batch = []
    for r in iterate(source_conn, source_type, q, page_size=batch_size, limit=limit, method=method, keyword_subfield=keyword_subfield):
        batch.append(r)
        if len(batch) >= batch_size:
            print "writing batch of", len(batch)
            raw.bulk(target_conn, target_type, batch)
            batch = []
    if len(batch) > 0:
        print "writing batch of", len(batch)
        raw.bulk(target_conn, target_type, batch)


def scroll(conn, type, q=None, page_size=1000, limit=None, keepalive="1m", keyword_subfield="exact"):
    if q is not None:
        q = q.copy()
    if q is None:
        q = {"query": {"match_all": {}}}
    if "size" not in q:
        q["size"] = page_size

    # To ensure complete coverage on a changing index, sort by id is our best bet. If there's .exact, even better.
    if "sort" not in q:
        q["sort"] = [{"id" + ('.' + keyword_subfield if keyword_subfield else ''): {"order": "asc"}}]

    resp = raw.initialise_scroll(conn, type, q, keepalive)
    if resp.status_code != 200:
        # something went wrong initialising the scroll
        raise ScrollException("Unable to initialise scroll - could be your mappings are broken")

    # otherwise, carry on
    results, scroll_id = raw.unpack_scroll(resp)

    counter = 0
    for r in results:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break
        counter += 1
        yield r

    while True:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break

        sresp = raw.scroll_next(conn, scroll_id, keepalive=keepalive)
        if raw.scroll_timedout(sresp):
            raise ScrollException("Scroll timed out - you probably need to raise the keepalive value")
        results = raw.unpack_result(sresp)

        if len(results) == 0:
            break
        for r in results:
            # apply the limit (again)
            if limit is not None and counter >= int(limit):
                break
            counter += 1
            yield r


def iterate(conn, type, q, page_size=1000, limit=None, method="POST", keyword_subfield="exact"):
    q = q.copy()
    q["size"] = page_size
    q["from"] = 0
    # To ensure complete coverage on a changing index, sort by id is our best bet. If there's .exact, even better.
    if "sort" not in q:
        q["sort"] = [{"id" + ('.' + keyword_subfield if keyword_subfield else ''): {"order": "asc"}}]
    counter = 0
    while True:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break
        
        res = raw.search(conn, type=type, query=q, method=method)
        rs = raw.unpack_result(res)
        
        if len(rs) == 0:
            break
        for r in rs:
            # apply the limit (again)
            if limit is not None and counter >= int(limit):
                break
            counter += 1
            yield r
        q["from"] += page_size


def dump(conn, type, q=None, page_size=1000, limit=None, method="POST", out=None, transform=None):
    q = q if q is not None else {"query": {"match_all": {}}}
    out = out if out is not None else sys.stdout
    for record in iterate(conn, type, q, page_size=page_size, limit=limit, method=method):
        if transform is not None:
            record = transform(record)
        out.write(json.dumps(record))


def create_alias(conn, alias):
    actions = raw.to_alias_actions(add=[{"alias": alias, "index": conn.index}])
    print "Alias create reply: ", raw.post_alias(conn, actions).json()


def repoint_alias(old_conn, new_conn, alias):
    actions = raw.to_alias_actions(add=[{"alias": alias, "index": new_conn.index}],
                                   remove=[{"alias": alias, "index": old_conn.index}])
    print "Alias re-point reply: ", raw.post_alias(new_conn, actions).json()


def reindex(old_conn, new_conn, alias, types, new_mappings=None, new_version="0.90.13"):
    """
    Re-index without search downtime by aliasing and duplicating the specified types from the existing index
    :param old_conn: Connection to the existing index
    :param new_conn: Connection to the new index (will create if it doesn't exist)
    :param alias: Existing alias which is used to access the index. Will be changed to point to the new index.
    :param types: List of types to copy across to the new index
    :param new_mappings: New mappings to use, as a dictionary of {<type>: mapping}
    :param new_version: The version of the new index (fixme: used for the mapping function)
    """

    # Ensure the old index is available via alias, and the new one is not
    if raw.alias_exists(new_conn, alias):
        raise Exception("Alias incorrectly set - check you have the connections the right way around.")
    elif not raw.alias_exists(old_conn, alias):
        print "The specified alias {0} does not exist for index {1}. Creating it.".format(alias, old_conn.index)
        create_alias(old_conn, alias)
    else:
        print "Alias OK"

    # Create a new index with the new mapping
    for t in types:
        r = raw.put_mapping(new_conn, type=t, mapping=new_mappings[t], make_index=True, es_version=new_version)
        print "Creating ES Type+Mapping for", t, "; status:", r.status_code
    print "Mapping OK"
    time.sleep(1)

    # Copy the data from old index to new index. The index should be unchanging (and may not have .exact) so don't use
    # keyword_subfield.
    for t in types:
        print "Copying type {0}".format(t)
        copy(old_conn, t, new_conn, t, keyword_subfield='')
    print "Copy OK"

    time.sleep(1)

    # Switch alias to point to second index
    repoint_alias(old_conn, new_conn, alias)
    print "Reindex complete."


def compare_index_counts(conns, types, q=None):
    """ Compare two or more indexes by doc counts of given types. Returns True if all counts equal, False otherwise """
    if q is not None:
        q = q.copy()
        if "size" not in q or q['size'] != 0:
            q["size"] = 0
    if q is None:
        q = {"query": {"match_all": {}}, "size": 0}

    equal_counts = []

    for t in types:
        print "\ntype:", t
        counts = []
        for c in conns:
            resp = raw.search(connection=c, type=t, query=q)
            try:
                count = resp.json()["hits"]["total"]
                counts.append(count)
                print "index {0}: {1}".format(c.index, count)
            except KeyError:
                print resp.json()

        equal_counts.append(reduce(lambda x, y: x == y, counts))

    return reduce(lambda x, y: x and y, equal_counts)


class JSONListWriter(object):
    def __init__(self, path):
        self.f = open(path, "wb")
        self.f.write("[")
        self.first = True

    def write(self, serialised_json_object):
        if self.first:
            self.first = False
        else:
            self.f.write(",")
        self.f.write(serialised_json_object)

    def close(self):
        self.f.write("]")
        self.f.close()
