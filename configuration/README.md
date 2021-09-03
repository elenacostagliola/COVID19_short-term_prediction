# Configuration package

This package contains modules that load the required credentials and API keys from
environment variables.

The following modules read environment variables:
- `dbconfig.py` --> MySQL and MongoDB credentials
- `github_config.py` --> GitHub API token
- `googleapis_config.py` --> Google API key (for Calendar API)

`load_config.py` reads .env file that creates environment variables when running the code locally.

If the .env file is not available, each environment variable must be set manually.