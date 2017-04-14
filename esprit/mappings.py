EXACT = {
    "default": {
        "match": "*",
        "match_mapping_type": "string",
        "mapping": {
            "type": "multi_field",
            "fields": {
                "{name}": {"type": "{dynamic_type}", "index": "analyzed", "store": "no"},
                "exact": {"type": "{dynamic_type}", "index": "not_analyzed", "store": "yes"}
            }
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
