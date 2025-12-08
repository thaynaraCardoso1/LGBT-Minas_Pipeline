import json
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)
