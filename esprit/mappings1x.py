def make_dynamic_templates_entry(name, match=None, match_type=None, mapping=None):
    if match is None:
        match = "*"
    tmp = {
        name: {
            "match": match,
            "mapping": mapping
        }
    }
    if match_type is not None:
        tmp[name]["match_mapping_type"] = match_type
    return tmp


def make_dynamic_templates(entries):
    return {"dynamic_templates": entries}


def make_field(type, fields=None, **kwargs):
    fm = {"type": type}
    for k, v in kwargs.iteritems():
        fm[k] = v
    if fields is not None:
        fm["fields"] = fields
    return fm


def make_properties(paths_to_fields):
    props = {}
    for path, field in paths_to_fields.iteritems():
        parts = path.split(".")
        context = props
        for i in range(len(parts)):
            p = parts[i]
            if p not in context:
                context[p] = {"properties": {}} if i < len(parts) - 1 else field
            if "properties" in context[p]:
                context = context[p]["properties"]

    return props


EXACT = make_dynamic_templates_entry("exact", "*", "string",
        make_field("string", store="no", index="analyzed",
        fields={"exact": make_field("string", index="not_analyzed", store="yes")}
            )
        )


def make_mapping(type, dynamic_templates=None, properties=None):
    # create the type entry
    m = {type: {}}

    # if there are dynamic templates, add them
    if dynamic_templates is not None:
        if not isinstance(dynamic_templates, list):
            dynamic_templates = [dynamic_templates]
        m[type]["dynamic_templates"] = dynamic_templates

    # if there are properties, add them
    if properties is not None:
        m[type]["properties"] = properties

    return m
