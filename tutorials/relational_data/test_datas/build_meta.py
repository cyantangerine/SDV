from typing import Dict
from pandas import DataFrame


def remove_copy_tag(key: str):
    index = key.find("_COPY")
    if index != -1:
        return key[:index]
    else:
        return key


def build_sdv_metadata_from_origin_tables(added_origin: Dict[str, DataFrame], meta, otables):
    # meta, otables = fe(x_arg, path)
    # meta = meta
    def del_ref(obj: dict, key: str):
        o = obj.copy()
        if 'ref' in obj:
            del o['ref']
        if o['type'] == 'id':
            o["type"] = "numerical"
            o["subtype"] = "integer"
        return o


    for table_name, table in added_origin.items():
        cols = otables[table_name].columns
        maps = {
            key: [
                k for k in table.columns if k.find(f"{key}_COPY") != -1
            ] for key in cols
        }

        new_fields = {}
        for k, v in maps.items():
            new_fields.update({item: del_ref(meta["tables"][table_name]["fields"][k], item) for item in v})
        meta["tables"][table_name]["fields"].update(new_fields)

    return meta
