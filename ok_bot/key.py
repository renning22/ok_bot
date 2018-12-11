import os

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

api = open(f'{SOURCE_DIR}/key.txt').read().strip()
secret = open(f'{SOURCE_DIR}/secret.txt').read().strip()
