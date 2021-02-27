from esprit import tasks, raw


def copy(source, source_type, target, target_type, limit=None, batch=1000):
    source_index = source.split("/")[-1]
    source_url = "/".join(source.split("/")[:-1])
    sconn = raw.Connection(source_url, source_index)

    target_index = target.split("/")[-1]
    target_url = "/".join(target.split("/")[:-1])
    tconn = raw.Connection(target_url, target_index)

    tasks.copy(sconn, source_type, tconn, target_type, limit, batch)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--copy", action="store_true", help="carry out a copy action")

    # arguments used with copy
    parser.add_argument("-s", "--source", help="url of source index")
    parser.add_argument("-o", "--target", help="url of target index")
    parser.add_argument("-f", "--sourcetype", help="data type to copy from")
    parser.add_argument("-t", "--targettype", help="data type to copy to")
    parser.add_argument("-l", "--limit", help="maximum number of records to copy")
    parser.add_argument("-b", "--batch", help="batch size in copy operation")

    args = parser.parse_args()

    if args.copy:
        source = args.source
        target = args.target
        source_type = args.sourcetype
        target_type = args.targettype
        limit = args.limit if args.limit else None
        batch = args.batch if args.batch else 1000
        print("copying with", source, source_type, target, target_type, "limit", limit, "batch size", batch)
        copy(source, source_type, target, target_type, limit, batch)
