import os
import json


def _this_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _schema_dir() -> str:
    return os.path.join(os.path.dirname(_this_dir()), "aw_core", "schemas")


def get_json_schema(name: str) -> dict:
    with open(os.path.join(_schema_dir(), name + ".json")) as f:
        data = json.load(f)
    return data


if __name__ == "__main__":
    print(get_json_schema("event"))
