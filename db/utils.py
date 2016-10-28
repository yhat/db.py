import os
import json
import base64

def profile_path(profile_id, profile):
    """Create full path to given provide for the current user."""
    user = os.path.expanduser("~")
    return os.path.join(user, profile_id + profile)

def load_profile(f):
    return json.loads(base64.decodestring(open(f, 'rb').read()).encode('utf-8'))

def load_from_json(file_path):
    """Load the stored data from json, and return as a dict."""
    if os.path.exists(file_path):
        raw_data = open(file_path, 'rb').read()
        return json.loads(base64.decodestring(raw_data).decode('utf-8'))

def dump_to_json(file_path, data):
    with open(file_path, 'wb') as f:
        json_data = json.dumps(data)
        try:
            f.write(base64.encodestring(json_data))
        except:
            f.write(base64.encodestring(bytes(json_data, 'utf-8')))
