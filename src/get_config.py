import os
import subprocess
import json

def get_auth(tenant_url):
    token_ep = f"{tenant_url}/token"
    cmd = f"curl -sku '{os.environ['IW_API_KEY']}:{os.environ['IW_API_SECRET']}' -d grant_type=password -d username={os.environ['IW_USERNAME']} -d password={os.environ['IW_PASSWORD']} -d scope=PRODUCTION -d client_name={os.environ['IW_CLIENT_NAME']} {token_ep}"
    res = subprocess.run(["/bin/bash", "-c", cmd], capture_output = True)
    # Get token from response.
    full_token = json.loads(res.stdout.decode())
    access_token = full_token["access_token"]
    return access_token

def get_config(config_file):
    config = None
    with open(config_file) as f:
        config = json.load(f)
    if "agave_token" not in config["tapis_config"]:
        if "AGAVE_TOKEN" in os.environ:
            config["tapis_config"]["agave_token"] = os.environ["AGAVE_TOKEN"]
        else:
            config["tapis_config"]["agave_token"] = get_auth(config["tapis_config"]["tenant_url"])
    if "hcdp_api_token" not in config["tapis_config"]:
        config["tapis_config"]["hcdp_api_token"] = os.environ["HCDP_API_TOKEN"]
    return config