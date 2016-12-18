def fields_query(v):
    # fields queries were deprecated in 5.0
    if v.startswith("5"):
        return False
    return True

def mapping_url_0x(v):
    return v.startswith("0")

def type_get(v):
    return v.startswith("0")

def create_with_mapping_post(v):
    return not v.startswith("5")