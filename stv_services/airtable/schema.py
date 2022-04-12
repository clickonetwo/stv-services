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
from typing import Dict

from ..core import Configuration, Session


def fetch_and_validate_airtable_schema(
    base_name: str, table_name: str, expected_schema: [(str, str, str)]
) -> (dict, dict):
    config = Configuration.get_global_config()
    session = Session.get_global_session("airtable")
    base_id = fetch_airtable_base_id(base_name)
    url = config["airtable_api_base_url"] + f"/meta/bases/{base_id}/tables"
    base_schema = session.get(url).json()
    column_ids, column_fields, column_types = {}, {}, {}
    for field_name, column_name, column_type in expected_schema:
        column_fields[column_name] = field_name
        column_types[column_name] = column_type
    for table in base_schema["tables"]:  # type: dict
        if table.get("name") == table_name:
            for field in table.get("fields"):  # type: Dict[str, str]
                if (column_name := field.get("name")) in column_fields:
                    column_ids[column_name] = field.get("id")
                    etype, atype = column_types[column_name], field.get("type")
                    if etype != atype:
                        raise TypeError(
                            f"Airtable field {column_name} "
                            f"has expected type {etype} "
                            f"but actual type {atype}"
                        )
    return column_ids


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
