#  MIT License
#
#  Copyright (c) 2022 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
from collections import namedtuple
from typing import Dict

from ..core import Configuration, Session

FieldInfo = namedtuple("FieldInfo", ["name", "type", "source"])


def fetch_and_validate_table_schema(
    base_name: str, table_name: str, schema: dict[str, FieldInfo]
) -> dict:
    config = Configuration.get_global_config()
    base_id = fetch_airtable_base_id(base_name)
    url = config["airtable_api_base_url"] + f"/meta/bases/{base_id}/tables"
    session = Session.get_global_session("airtable")
    base_schema = session.get(url).json()
    column_ids = {}
    column_fields = {info.name: name for name, info in schema.items()}
    for table in base_schema["tables"]:  # type: dict
        if table.get("name") == table_name:
            table_id = table.get("id")
            for field in table.get("fields"):  # type: Dict[str, str]
                if (name := field.get("name")) in column_fields:
                    column_ids[column_fields[name]] = field.get("id")
                    if schema[column_fields[name]].type != field.get("type"):
                        raise TypeError(
                            f"Airtable field {name} "
                            f"has expected type {schema[column_fields[name]].type} "
                            f"but actual type {field.get('type')}"
                        )
            break
    else:
        raise KeyError(f"Base schema has no table named '{table_name}'")
    if missing := set(schema.keys()) - set(column_ids.keys()):
        raise KeyError(f"Table schema is missing fields for: {missing}")
    return dict(base_id=base_id, table_id=table_id, column_ids=column_ids)


def fetch_airtable_base_id(base_name: str) -> str:
    config = Configuration.get_global_config()
    session = Session.get_global_session("airtable")
    url = config["airtable_api_base_url"] + "/meta/bases"
    schema = session.get(url).json()
    for base in schema["bases"]:  # type: Dict[str, str]
        if base.get("name") == base_name:
            if base_id := base.get("id"):
                return base_id
    else:
        raise KeyError(f"No base named '{base_name}' in schema: {schema}")
