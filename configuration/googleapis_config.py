import os
from . import load_config

GOOGLE_APIKEY = os.environ.get("GOOGLE_API_KEY")

if GOOGLE_APIKEY is None:
    print(f"[Error] Missing Google API Key environment variable.")
    exit(-1)
