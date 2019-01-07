import os

_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

API_KEY = None
KEY_SECRET = None
PASS_PHRASE = None

with open(os.path.join(_ROOT_DIR, 'api_key_v3')) as key_file:
    API_KEY = key_file.read().strip()
with open(os.path.join(_ROOT_DIR, 'secret_key_v3')) as secret_file:
    KEY_SECRET = secret_file.read().strip()
with open(os.path.join(_ROOT_DIR, 'pass_phrase_v3')) as secret_file:
    PASS_PHRASE = secret_file.read().strip()
