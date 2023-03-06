import requests


class BaserowApi:
    def __init__(self, token=None, token_path=None):
        if token_path:
            self._token = load_token(token_path)
        if token:
            self._token = token

    def get_data(self, table_id):
        return get_data(table_id, self._token)

    def _create_row(self, table_id, data):
        resp = requests.post(
            f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true",
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json=data,
        )
        resp.raise_for_status()
        resp_data = resp.json()
        if "id" in resp_data:
            return resp_data["id"]
        else:
            raise RuntimeError(f"Malformed response {resp_data}")

    def _update_row(self, table_id, row_id, data):
        resp = requests.patch(
            f"https://phenotips.charite.de/api/database/rows/table/{table_id}/{row_id}/?user_field_names=true",
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json=data,
        )
        resp.raise_for_status()

    def add_data(self, table_id, data, row_id=None):
        if row_id:
            self._update_row(table_id, row_id, data)
        else:
            self._create_row(table_id, data)


def load_token(token_path):
    with open(token_path) as tokenfile:
        token = tokenfile.readline().strip()
    return token

def get_fields(table_id, token):
    resp = requests.get(
        f"https://phenotips.charite.de/api/database/fields/table/{table_id}/",
        headers={
            "Authorization": f"Token {token}"
        }
    )

    resp.raise_for_status()
    data = resp.json()
    return data


def get_writable_fields(table_id, token):
    writable_fields = [
        f for f in get_fields(table_id, token) if not f["read_only"]
    ]
    return writable_fields


def _get_data(url, token):
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Token {token}"
        }
    )
    data = resp.json()

    if "results" not in data:
        raise RuntimeError

    if data["next"]:
        return data["results"] + _get_data(data["next"], token)
    return data["results"]


def format_value(raw_value, field_info):
    if field_info["type"] == "single_select":
        if isinstance(raw_value, dict):
            return raw_value["id"]
        elif raw_value is None:
            return raw_value
        raise RuntimeError(f"malformed single_select {raw_value}")
    elif field_info["type"] == "multiple_select":
        if isinstance(raw_value, list):
            return [
                v["id"] for v in raw_value
            ]
        raise RuntimeError(f"malformed multiple_select {raw_value}")
    elif field_info["type"] == "link_row":
        if isinstance(raw_value, list):
            return [
                v["id"] for v in raw_value
            ]
        raise RuntimeError(f"malformed link_row {raw_value}")
    else:
        return raw_value


def get_data(table_id, token):
    """Check a given table for empty keys.
    """
    writable_fields = get_writable_fields(table_id, token)
    writable_names = {f['name']: f for f in writable_fields}
    data = _get_data(f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true", token)

    writable_data = {
        d['id']: {k: format_value(v, writable_names[k]) for k, v in d.items() if k in writable_names} for d in data
    }

    return writable_data
