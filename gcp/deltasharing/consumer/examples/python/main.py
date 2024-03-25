import requests
import uuid
from jwcrypto import jwk, jwt
from datetime import datetime, timezone
import os
import json

with open ('config.json', 'r') as f:
    config = json.load(f)

# Variables from integration
kid = os.environ["kid"] 
scope = config["scope"] 
integration_id = os.environ["integration_id"] 

# Environment specific variables
maskinporten_audience = "https://test.sky.maskinporten.no"
maskinporten_token = "https://test.sky.maskinporten.no/token"

timestamp = int(datetime.now(timezone.utc).timestamp())

with open('maskinporten.pem', mode='rb') as f:
  key = jwk.JWK.from_pem(
  data=f.read(),
  #password=str('PASSWORD').encode() <-- if password needed
)
f.closed


jwt_header = {
  'alg': 'RS256',
  'kid': kid
}

jwt_claims = {
  'aud': maskinporten_audience,
  'iss': integration_id,
  'scope': scope,
  'resource': config["resource"],
  'iat': timestamp,
  'exp': timestamp+100,
  'jti': str(uuid.uuid4())
}

jwt_token = jwt.JWT(
  header = jwt_header,
  claims = jwt_claims,
)
jwt_token.make_signed_token(key)
signed_jwt = jwt_token.serialize()

body = {
  'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
  'assertion': signed_jwt
}

res = requests.post(maskinporten_token, data=body)
with open("token.json", "w") as file:
    file.write(res.text)

json_config_info = {
  "type": "external_account",
  "audience": f"//iam.googleapis.com/projects/{config['PROJECT_NUMBER']}/locations/global/workloadIdentityPools/{config['POOL_PROVIDER']}/providers/{config['PROVIDER_NAME']}",
  "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
  "token_url": "https://sts.googleapis.com/v1/token",
  "credential_source": {
    "file": "token.json",
    "format": {
      "type": "json",
      "subject_token_field_name": "access_token"
    }
  },
   "service_account_impersonation_url": f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{config['SERVICE_ACCOUNT_EMAIL']}:generateAccessToken",
}

from google.cloud import storage
from google.auth import identity_pool

credentials = identity_pool.Credentials.from_info(json_config_info)

storage_client = storage.Client(project=config['GOOGLE_PROJECT_ID'], credentials=credentials)
print(list(storage_client.list_blobs(config['bucket_name'])))