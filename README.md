# cis_hris_publisher
An integration connector for hris data to be stored in Mozilla's CIS Identity Vault.

# Local Development
1. Setup python3 virtualenv and install dev reqs
1. Set environment variables

       export STAGE=testing
       export CIS_DYNAMODB_TABLE=CISStaging-VaultandStreams-IdentityVaultUsers-O35P6M8U9LNW
       export CIS_ARN_MASTER_KEY=arn:aws:kms:us-west-2:656532927350:key/9e231aa0-04e4-4517-a45d-633c3bb055f0
       export CIS_STREAM_ARN=arn:aws:kinesis:us-west-2:656532927350:stream/CISStaging-VaultandStreams-CISInputStream-P7DYU9FBQ2OW
       export CIS_KINESIS_STREAM_NAME=CISStaging-VaultandStreams-CISInputStream-P7DYU9FBQ2OW
       export CIS_IAM_ROLE_ARN= arn:aws:iam::656532927350:role/CISPublisherRole
       export CIS_PUBLISHER_NAME=hris
       export CIS_IAM_ROLE_SESSION_NAME=test_hris_client
       export CIS_IAM_ROLE_ARN=arn:aws:iam::656532927350:role/CISPublisherRole
       export CIS_LAMBDA_VALIDATOR_ARN=arn:aws:lambda:us-west-2:656532927350:function:cis_functions_stage_validator
       export CIS_PERSON_API_URL=person-api.sso.allizom.org
       export CIS_PERSON_API_AUDIENCE=https://person-api.sso.allizom.org
       export CIS_PERSON_API_VERSION=v1
       export CIS_OAUTH2_DOMAIN=auth-dev.mozilla.auth0.com
       export LDAP_NAMESPACE=Mozilla-LDAP-Dev
       export SIGNING_KEY_ARN=arn:aws:kms:us-west-2:656532927350:key/33a642fd-dd44-4405-9c85-2ad32eeeb87b

## Prod variables

    export STAGE=prod
    export CIS_DYNAMODB_TABLE=cis-stream-prod-and-idv-IdentityVaultUsers-LMGGZ2XE8K6F
    export CIS_ARN_MASTER_KEY=arn:aws:kms:us-west-2:371522382791:key/adef50ad-2846-46df-b783-1c8f35e858cb
    export CIS_STREAM_ARN=arn:aws:kinesis:us-west-2:371522382791:stream/cis-stream-prod-and-idv-CISInputStream-1R29T8G6ZQTQ5
    export CIS_KINESIS_STREAM_NAME=cis-stream-prod-and-idv-CISInputStream-1R29T8G6ZQTQ5
    export CIS_IAM_ROLE_ARN=arn:aws:iam::371522382791:role/CISPublisherRole
    export CIS_PUBLISHER_NAME=hris
    export CIS_IAM_ROLE_SESSION_NAME=prod_hris_client
    export CIS_LAMBDA_VALIDATOR_ARN=arn:aws:lambda:us-west-2:371522382791:function:cis_functions_prod_latest_validator
    export CIS_PERSON_API_URL=person-api.sso.mozilla.com
    export CIS_PERSON_API_AUDIENCE=https://person-api.sso.mozilla.com
    export CIS_PERSON_API_VERSION=v1
    export CIS_OAUTH2_DOMAIN=auth.mozilla.auth0.com
    export LDAP_NAMESPACE=Mozilla-LDAP
    export SIGNING_KEY_ARN=arn:aws:kms:us-west-2:371522382791:key/e7373ecf-e32b-4cfc-8f4c-8eab112357c4

# Deployment Instructions
TBD

# To Do
Enrich Parsys_Test with some HRIS data as part of every run.
Request additional LDAP Test Account (Viorela)

# Testing

Due to the nature of the complex interactions these tests can only be run by a developer who has assumed the
CIS developer role.

# How to Run Locally

```
docker run --rm -ti \
-v ~/.aws:/root/.aws \
-v ~/workspace/cis_hris_publisher/:/workspace \
mozillaiam/docker-sls:latest \
/bin/bash
```
