import json
import os


def define_env(env):
    """mkdocs-macros hook: load docs/dumps.json into template context as 'dumps'."""
    dumps_path = os.path.join(os.path.dirname(__file__), "dumps.json")
    try:
        with open(dumps_path, "r", encoding="utf-8") as f:
            env.variables["dumps"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        env.variables["dumps"] = {"latest_version": None, "releases": []}
