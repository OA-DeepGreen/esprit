from esprit import raw, models
import json, sys, time, codecs, os


class ScrollException(Exception):
    pass


def bulk_load(conn, type, source_file, limit=None, max_content_length=100000000):
    source_size = os.path.getsize(source_file)
    with codecs.open(source_file, "rb", "utf-8") as f:
        if limit is None and source_size < max_content_length:
            # if we aren't selecting a portion of the file, and the file is below the max content length, then
            # we can just serve it directly
            raw.raw_bulk(conn, f, type)
        else:
            count = 0
            while True:
                chunk = _make_next_chunk(f, max_content_length)
                if chunk == "":
                    break

                finished = False
                if limit is not None:
                    newlines = chunk.count("\n")
                    records = newlines / 2
                    if count + records > limit:
                        max = (limit - count) * 2
                        lines = chunk.split("\n")
                        allowed = lines[:max]
                        chunk = "\n".join(allowed) + "\n"
                        count += max
                        finished = True
                    else:
                        count += records

                resp = raw.raw_bulk(conn, chunk, type)
                if resp.status_code != 200:
                    raise Exception("did not get expected response")
                if finished:
                    break


def _make_next_chunk(f, max_content_length):
    offset = f.tell()
    chunk = f.read(max_content_length)
    if chunk.endswith("\n"):
        last_line_idx = chunk.rfind("\n", 0, len(chunk) - 1) + 1
        if not chunk[last_line_idx:].startswith('{"index": {"_id": '):
            return chunk
        else:
            chunk = chunk[:last_line_idx]
            f.seek(offset + last_line_idx)
            return chunk
    else:
        last_line_idx = chunk.rfind("\n")
        new_end = last_line_idx + 1
        if chunk[new_end:].startswith('{"index": {"_id": '):
            chunk = chunk[:new_end]
        else:
            second_last_line_idx = chunk.rfind("\n", 0, last_line_idx)
            new_end = second_last_line_idx + 1
            chunk = chunk[:new_end]
        f.seek(offset + new_end)
        return chunk


def copy(source_conn, source_type, target_conn, target_type, limit=None, batch_size=1000, method="POST", q=None):
    if q is None:
        q = models.QueryBuilder.match_all()
    batch = []
    for r in iterate(source_conn, source_type, q, page_size=batch_size, limit=limit, method=method):
        batch.append(r)
        if len(batch) >= batch_size:
            print "writing batch of", len(batch)
            raw.bulk(target_conn, batch, type_=target_type)
            batch = []
    if len(batch) > 0:
        print "writing batch of", len(batch)
        raw.bulk(target_conn, batch, type_=target_type)


def scroll(conn, type, q=None, page_size=1000, limit=None, keepalive="1m"):
    if q is not None:
        q = q.copy()
    if q is None:
        q = {"query": {"match_all": {}}}
    if "size" not in q:
        q["size"] = page_size
    if "sort" not in q:
        q["sort"] = [{"_uid": {"order": "asc"}}]

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


def iterate(conn, type, q, page_size=1000, limit=None, method="POST"):
    q = q.copy()
    q["size"] = page_size
    q["from"] = 0
    if "sort" not in q:
        q["sort"] = [{"_uid": {"order": "asc"}}]
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


def dump(conn, type, q=None, page_size=1000, limit=None, method="POST", out=None, out_template=None, transform=None, es_bulk_format=True, idkey='id', es_bulk_fields=None, out_batch_sizes=100000):
    q = q if q is not None else {"query": {"match_all": {}}}

    filenames = []
    n = 1
    current_file = None
    if out_template is not None:
        current_file = out_template + "." + str(n)
        filenames.append(current_file)
    if out is None and current_file is not None:
        out = codecs.open(current_file, "wb", "utf-8")
    else:
        out = sys.stdout

    count = 0
    for record in iterate(conn, type, q, page_size=page_size, limit=limit, method=method):
        if transform is not None:
            record = transform(record)

        data = ""
        if es_bulk_format:
            kwargs = {}
            if es_bulk_fields is None:
                es_bulk_fields = ["_id", "_index", "_type"]
            for key in es_bulk_fields:
                if key == "_id":
                    kwargs["idkey"] = idkey
                if key == "_index":
                    kwargs["index"] = conn.index
                if key == "_type":
                    kwargs["type_"] = type
            data = raw.to_bulk_single_rec(record, **kwargs)
        else:
            data = json.dumps(record) + "\n"

        out.write(data)
        if out_template is not None:
            count += 1
            if count > out_batch_sizes:
                count = 0
                n += 1
                current_file = out_template + "." + str(n)
                filenames.append(current_file)
                out.close()
                out = codecs.open(current_file, "wb", "utf-8")

        return filenames

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
        copy(old_conn, t, new_conn, t)
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
