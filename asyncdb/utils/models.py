import json


def model_to_json(obj):
    dic = {
        field.name: field.value_from_object(obj)
        for field in obj._meta.fields
    }
    if len(dic):
        return json.dumps(dic)
    else:
        return None
