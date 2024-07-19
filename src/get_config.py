import os
import subprocess
import json

def get_auth(agave_ep):
    token_ep = f"{agave_ep}/token"
    cmd = f"curl -sku '{os.environ['IW_API_KEY']}:{os.environ['IW_API_SECRET']}' -d grant_type=password -d username={os.environ['IW_USERNAME']} -d password={os.environ['IW_PASSWORD']} -d scope=PRODUCTION -d client_name={os.environ['IW_CLIENT_NAME']} {api_endpoint}"
    res = subprocess.run(["/bin/bash", "-c", cmd], capture_output = True)
    # Get token from response.
    full_token = json.loads(res.stdout.decode())
    access_token = full_token["access_token"]

def get_config(config_file):
    config = None
    with open(config_file) as f:
        config = json.load(f)
    if config["tapis_config"].get("agave_token") is None:
        if "AGAVE_TOKEN" in os.environ:
            config["tapis_config"]["agave_token"] = os.environ["AGAVE_TOKEN"]
        else:
            config["tapis_config"]["agave_token"] = get_auth(config["tenant_url"])
    if config["tapis_config"].get("hcdp_api_token") is None:
        config["tapis_config"]["hcdp_api_token"] = os.environ["HCDP_API_TOKEN"]
    return config