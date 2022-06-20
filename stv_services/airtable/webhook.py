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

from dateutil.parser import parse
from sqlalchemy.future import Connection

from stv_services.airtable.schema import fetch_airtable_base_id
from stv_services.core import Configuration, Session
from stv_services.core.logging import get_logger

logger = get_logger(__name__)


def register_hook(
    name: str, base: str, table: str, targets: list[str], watches: list[str] = None
):
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
                "recordChangeScope": table,
                "watchDataInFieldIds": targets,
            },
            "includes": {
                "includePreviousCellValues": True,
            },
        },
    }
    if watches:
        spec["options"]["includes"]["includeCellValuesInFieldIds"] = watches
    url = None
    if server_url := config["stv_api_base_url"]:
        url = server_url + "/airtable/notifications"
    body = {
        "specification": spec,
        "notificationUrl": url,
    }
    session = Session.get_global_session("airtable")
    url = config["airtable_api_base_url"] + f"/bases/{base}/webhooks"
    response = session.post(url, json=body)
    if response.status_code == 422:
        logger.info("Unprocessable entity exception")
        return
    else:
        response.raise_for_status()
    results = response.json()
    reg_data = {
        "spec": spec,
        "hook_name": name,
        "hook_id": results["id"],
        "base_id": base,
        "table_id": table,
        "secret": results["macSecretBase64"],
        "cursor": 0,
    }
    hook_info[name] = reg_data
    config.save_to_data_store()


def sync_hooks(verbose: bool = True, force_remove: bool = False):
    """
    Remove any Airtable webhooks we aren't aware of. Also remove any webhooks
    on our side that Airtable is not aware of.

    When we do the sync, we insist that the spec found match the one we
    have registered on our side.  Otherwise, we remove the old one.  So
    the canonical action to take after changing a webhook is to sync and
    then re-register all hooks.

    This code assumes that all of our hooks are against the single Airtable
    base that we are configured to use.  Even though we keep the Base ID with
    each hook, that's not an indication that some hooks have different base
    IDs than others.
    """
    config = Configuration.get_global_config()
    api_url = config["airtable_api_base_url"]
    hook_info: dict = config.get("airtable_webhooks", {})
    # fetch the existing hooks from Airtable
    base_name = config["airtable_stv_base_name"]
    if verbose:
        logger.info(f"Fetching Airtable webhooks for base '{base_name}'...")
    base_id = fetch_airtable_base_id(base_name)
    session = Session.get_global_session("airtable")
    url = api_url + f"/bases/{base_id}/webhooks"
    response = session.get(url)
    response.raise_for_status()
    base_hooks: list[dict] = response.json()["webhooks"]
    # walk the existing hooks and remove non-matches on either side
    matched = set()
    for hook in base_hooks:
        name = find_spec(hook, hook_info)
        if name and not force_remove:
            if verbose:
                logger.info(f"Hook '{name}' is registered.")
            matched.add(name)
        if force_remove or not name:
            hook_id = hook["id"]
            if name:
                if verbose:
                    logger.info(f"Deleting hook '{name}'...")
                del hook_info[name]
            else:
                if verbose:
                    spec = hook["specification"]
                    logger.info(f"Deleting unknown hook with spec: {spec}...")
            url = api_url + f"/bases/{base_id}/webhooks/{hook_id}"
            session.delete(url).raise_for_status()
    if not force_remove:
        if missing := [name for name in hook_info if name not in matched]:
            if verbose:
                logger.info(f"Deleting unregistered hooks: {missing}")
            for name in missing:
                del hook_info[name]
        else:
            if verbose:
                if hook_info:
                    logger.info(f"All hooks are registered.")
                else:
                    logger.info(f"There are no registered hooks.")
    else:
        if verbose:
            logger.info(f"There are no registered hooks.")
    config.save_to_data_store()


def fetch_hook_payloads(conn: Connection, name: str) -> list[dict]:
    logger.info(f"Fetching airtable '{name}' payloads...")
    config = Configuration.get_session_config(conn)
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
    config.save_to_connection(conn)
    logger.info(f"Got {len(payloads)} '{name}' payload(s).")
    return payloads


def validate_notification(payload: dict, body: bytes, digest: str) -> str:
    base_id = payload.get("base") and payload.get("base").get("id")
    hook_id = payload.get("webhook") and payload.get("webhook").get("id")
    timestamp = payload.get("timestamp") and parse(payload.get("timestamp"))
    if not base_id or not hook_id or not timestamp:
        ValueError(f"Notification is missing required elements: {payload}")
    config = Configuration.get_global_config()
    hook_info = config.get("airtable_webhooks", {})
    for name, info in hook_info.items():
        if hook_id == info.get("hook_id") and base_id == info.get("base_id"):
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


def find_spec(hook: dict, hook_info: dict) -> str:
    """
    Look for a hook in hooks whose spec matches that of hook.
    Args:
        hook: a retrieved webhook body from Airtable
        hook_info: the dictionary of STV registered hooks

    Returns:
        The matching STV hook name, or the empty string
    """
    hook_id = hook["id"]
    for name, info in hook_info.items():
        if info["hook_id"] != hook_id:
            continue
        s1 = hook["specification"]
        s2 = info["spec"]
        o1 = s1["options"]
        o2 = s2["options"]
        targets1: list = o1["filters"]["watchDataInFieldIds"]
        targets2: list = o2["filters"]["watchDataInFieldIds"]
        if targets1.sort() != targets2.sort():
            continue
        watches1: list = o1["includes"].get("includeCellValuesInFieldIds", [])
        watches2: list = o2["includes"].get("includeCellValuesInFieldIds", [])
        if watches1.sort() != watches2.sort():
            continue
        return name
    else:
        return ""
