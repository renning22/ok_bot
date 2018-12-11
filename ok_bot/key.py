import os

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(SOURCE_DIR, '..', 'key.txt')) as file:
    api = file.read().strip()
with open(os.path.join(SOURCE_DIR, '..', 'secret.txt')) as file:
    secret = file.read().strip()
