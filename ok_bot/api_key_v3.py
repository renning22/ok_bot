import os

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

API_KEY = open(f'{SOURCE_DIR}/api_key_v3').read().strip()
KEY_SECRET = open(f'{SOURCE_DIR}/secret_key_v3').read().strip()
PASS_PHRASE = open(f'{SOURCE_DIR}/pass_phrase_v3').read().strip()