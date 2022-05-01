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
import base64
import hmac
import json

from dateutil.parser import parse

from stv_services.airtable.schema import fetch_airtable_base_id
from stv_services.core import Configuration, Session


def register_hook(name: str, base_id: str, table_id: str, field_ids: list[str]):
    config = Configuration.get_global_config()
    hook_info: dict = config.setdefault("airtable_webhooks", {})
    if hook_info.get(name):
        # already registered
        return
    spec = {
        "options": {
            "filters": {
                "fromSources": ["client", "automation"],
                "dataTypes": ["tableData"],
                "recordChangeScope": table_id,
                "watchDataInFieldIds": field_ids,
            },
            "includes": {
                "includePreviousCellValues": True,
            },
        },
    }
    url = None
    if server_url := config.get("stv_api_base_url"):
        url = server_url + "/airtable/notifications"
    body = {
        "specification": spec,
        "notificationUrl": url,
    }
    session = Session.get_global_session("airtable")
    url = config["airtable_api_base_url"] + f"/bases/{base_id}/webhooks"
    response = session.post(url, json=body)
    if response.status_code == 422:
        print("Unprocessable entity exception")
        return
    else:
        response.raise_for_status()
    results = response.json()
    reg_data = {
        "spec": spec,
        "hook_name": name,
        "hook_id": results["id"],
        "base_id": base_id,
        "table_id": table_id,
        "secret": results["macSecretBase64"],
        "cursor": 0,
    }
    hook_info[name] = reg_data
    config.save_to_data_store()


def sync_hooks(verbose: bool = True, force_remove: bool = False):
    """
    Remove any Airtable webhooks we aren't aware of. Also remove any webhooks
    on our side that Airtable is not aware of, and return the names of those
    hooks that need to be re-registered.

    This code assumes that all of our hooks are against the single Airtable
    base that we are configured to use.  Even though we keep the Base ID with
    each hook, that's not an indication that some hooks have different base
    IDs than others.
    """
    config = Configuration.get_global_config()
    api_url = config["airtable_api_base_url"]
    # organize our hooks by ID for quick access
    hook_info: dict = config.get("airtable_webhooks", {})
    hooks_by_id = {info["hook_id"]: name for name, info in hook_info.items()}
    # fetch the existing hooks from Airtable
    base_name = config["airtable_stv_base_name"]
    if verbose:
        print(f"Fetching Airtable webhooks for base '{base_name}'...")
    base_id = fetch_airtable_base_id(base_name)
    session = Session.get_global_session("airtable")
    url = api_url + f"/bases/{base_id}/webhooks"
    response = session.get(url)
    response.raise_for_status()
    base_hooks: list[dict] = response.json()["webhooks"]
    # walk the existing hooks and remove non-matches on either side
    for hook in base_hooks:
        hook_id = hook["id"]
        name = hooks_by_id.get(hook_id)
        if name and not force_remove:
            if verbose:
                print(f"Hook '{name}' is registered.")
            del hooks_by_id[hook_id]
        if force_remove or not name:
            if name:
                if verbose:
                    print(f"Deleting hook '{name}'...")
                del hook_info[name]
            else:
                if verbose:
                    print(
                        f"Deleting unknown hook '{hook_id}' "
                        f"with spec: {hook['specification']}..."
                    )
            url = api_url + f"/bases/{base_id}/webhooks/{hook_id}"
            session.delete(url).raise_for_status()
    if not force_remove:
        if missing := list(hooks_by_id.values()):
            if verbose:
                print(f"Deleting unregistered hooks: {missing}")
            for name in missing:
                del hook_info[name]
        else:
            if verbose:
                if hook_info:
                    print(f"All hooks are registered.")
                else:
                    print(f"There are no registered hooks.")
    else:
        if verbose:
            print(f"There are no registered hooks.")
    config.save_to_data_store()


def fetch_hook_payloads(name: str) -> list[dict]:
    config = Configuration.get_global_config()
    info = config["airtable_webhooks"][name]
    base_id = info["base_id"]
    hook_id = info["hook_id"]
    cursor = info["cursor"]
    api_url = config["airtable_api_base_url"]
    url = api_url + f"/bases/{base_id}/webhooks/{hook_id}/payloads"
    might_have_more = True
    payloads: list[dict] = []
    while might_have_more:
        query = url + f"?cursor={cursor}"
        session = Session.get_global_session("airtable")
        response = session.get(query)
        response.raise_for_status()
        data: dict = response.json()
        payloads += data.get("payloads", [])
        cursor = data.get("cursor", cursor)
        might_have_more = data.get("mightHaveMore", False)
    info["cursor"] = cursor
    config.save_to_data_store()
    return payloads


def validate_notification(payload: dict, body: bytes, digest: str) -> str:
    base_id = payload.get("base") and payload.get("base").get("id")
    hook_id = payload.get("webhook") and payload.get("webhook").get("id")
    timestamp = payload.get("timestamp") and parse(payload.get("timestamp"))
    if not base_id or not hook_id or not timestamp:
        ValueError(f"Notification is missing required elements: {payload}")
    hook_info: dict = Configuration.get_global_config()["airtable_webhooks"]
    for name, info in hook_info.items():
        if hook_id == info["hook_id"] and base_id == info["base_id"]:
            validate_notification_signature(info, body, digest)
            return name
    else:
        ValueError(f"Unknown base or hook ID: {payload}")


def validate_notification_signature(info: dict, body: bytes, digest: str):
    if digest.startswith("hmac-sha256="):
        digest = digest[len("hmac-sha256=") :]
        digest_bytes = bytes.fromhex(digest)
    else:
        raise ValueError(f"Digest header in incorrect format: '{digest}'")
    secret64 = info["secret"]
    secret = base64.b64decode(secret64)
    try:
        body_str = body.decode("ascii")
        trimmed_body = body_str.strip().encode("ascii")
    except (UnicodeEncodeError, UnicodeDecodeError):
        raise ValueError(f"Notification body not ascii: {body}")
    correct_bytes = hmac.digest(secret, body, "sha256")
    valid = hmac.compare_digest(digest_bytes, correct_bytes)
    if not valid:
        # should fail with ValueError, but we don't care
        correct = correct_bytes.hex()
        raise ValueError(
            f"HMAC validation fails:\n"
            f"\thook name: {info['hook_name']}"
            f"\tbody: {body_str}\n"
            f"\tour digest: {correct}\n"
            f"\ttheir digest: {digest}"
        )
