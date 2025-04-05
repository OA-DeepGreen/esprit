EXACT = {
    "strings": {
        "match_mapping_type": "string",
        "mapping": {
            "type": "text",
            "fields": {
                "exact": {
                    "type": "keyword",
                    "normalizer": "lowercase"
                }
            }
        }
    }
}

NESTED = {
    "objects": {
        "match": "*",
        "match_mapping_type" : "object",
        "mapping": {
            "type": "nested",
        }
    }
}



def properties(field_mappings):
    return {"properties": field_mappings}


def type_mapping(field, type):
    return {field: {"type": type}}


def make_mapping(type):
    # FIXME: obviously this is not all there is to it
    return {"type": type}


def dynamic_type_template(name, match, mapping):
    return {
        name: {
            "match": match,
            "mapping": mapping
        }
    }


def dynamic_templates(templates):
    return {"dynamic_templates": templates}


def for_type(typename, *mapping):
    full_mapping = {}
    for m in mapping:
        full_mapping.update(m)
    return {typename: full_mapping}


def parent(childtype, parenttype):
    return {
        childtype: {
            "_parent": {
                "type": parenttype
            }
        }
    }

def apply_mapping_opts(field_name, path, spec, mapping_opts):
    dot_path = '.'.join(path + (field_name,))
    if dot_path in mapping_opts.get('exceptions', {}):
        return mapping_opts['exceptions'][dot_path]
    elif spec['coerce'] in mapping_opts['coerces']:
        return mapping_opts['coerces'][spec['coerce']]
    else:
        # We have found a data type in the struct we don't have a map for to ES type.
        raise Exception("Mapping error - no mapping found for {}".format(spec['coerce']))


def create_mapping(struct, mapping_opts, path=()):
    result = {"properties": {}}

    for field, spec in struct.get("fields", {}).items():
        result["properties"][field] = apply_mapping_opts(field, path, spec, mapping_opts)

    for field, spec in struct.get("lists", {}).items():
        if "coerce" in spec:
            result["properties"][field] = apply_mapping_opts(field, path, spec, mapping_opts)

    for struct_name, struct_body in struct.get("structs", {}).items():
        result["properties"][struct_name] = create_mapping(struct_body, mapping_opts, path + (struct_name,))

    return result


def mappings(typ):
    # DEFINES LEGACY DEFAULT MAPPINGS
    return {
        typ : for_type(
            typ,
            properties(type_mapping("location", "geo_point")),
            dynamic_templates([EXACT,])
        )
    }


def default_mapping():
    mapping = properties(type_mapping("location", "geo_point"))
    mapping.update(dynamic_templates([EXACT,]))
    return mapping


def default_nested_mapping():
    nested_mapping = properties(type_mapping("location", "geo_point"))
    nested_mapping.update(dynamic_templates([EXACT, NESTED]))
    return nested_mapping
