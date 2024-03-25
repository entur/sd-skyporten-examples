import requests
import uuid
from jwcrypto import jwk, jwt
from datetime import datetime, timezone
import os
import json
from google.cloud import storage
from google.auth import identity_pool


def get_bucket_file(*, config, bucketname, filename):
    """Load content of remote delta sharing file,
    residing on a gcp file storage bucket, using
    maskinporten for access.
    """
    # Variables from integration
    kid = os.environ["MASKINPORTEN_KID"]
    scope = config["scope"]
    integration_id = os.environ["MASKINPORTEN_INTEGRATION_ID"]

    # Environment specific variables
    maskinporten_audience = "https://test.sky.maskinporten.no"
    maskinporten_token = "https://test.sky.maskinporten.no/token"

    timestamp = int(datetime.now(timezone.utc).timestamp())

    with open('../../creds/maskinporten.pem', mode='rb') as f:
        key = jwk.JWK.from_pem(
            data=f.read(),
            #password=str('PASSWORD').encode() <-- if password needed
        )
        # f.closed

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

    credentials = identity_pool.Credentials.from_info(json_config_info)
    storage_client = storage.Client(project=config['GOOGLE_PROJECT_ID'], credentials=credentials)
    print(list(storage_client.list_blobs(bucketname)))
    print("__init__.py:" + repr(84) + ":bucketname:" + repr(bucketname))
    bucket = storage_client.get_bucket(bucketname)
    print("__init__.py:" + repr(84) + ":bucket:" + repr(bucket))
    # return bucket
    # bucket = storage_client.get_bucket(f"{bucketname}/{filename}")
    # blob = storage_client.get_blob(f"{bucketname}/{filename}")
    blob = bucket.get_blob(filename)
    ret = blob.download_as_string()
    print("__init__.py:" + repr(86) + ":ret:" + repr(ret))
    return ret
