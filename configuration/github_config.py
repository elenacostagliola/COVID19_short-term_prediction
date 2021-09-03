import os
from . import load_config

TOKEN = os.environ.get("GITHUB_TOKEN")

if TOKEN is None:
    print(f"[Error] Missing github token environment variable")
    exit(-1)
