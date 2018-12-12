import os

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(SOURCE_DIR, '..', 'key.txt')) as key_file:
    api = key_file.read().strip()
with open(os.path.join(SOURCE_DIR, '..', 'secret.txt')) as secret_file:
    secret = secret_file.read().strip()
