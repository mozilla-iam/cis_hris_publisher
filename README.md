# cis_hris_publisher
An integration connector for hris data to be stored in Mozilla's CIS Identity Vault.

# Local Development
1. Setup python3 virtualenv and install dev reqs
1. `export STAGE=testing`

export CIS_DYNAMODB_TABLE=CISStaging-VaultandStreams-IdentityVaultUsers-O35P6M8U9LNW
export CIS_ARN_MASTER_KEY=arn:aws:kms:us-west-2:656532927350:key/9e231aa0-04e4-4517-a45d-633c3bb055f0
export CIS_STREAM_ARN=arn:aws:kinesis:us-west-2:656532927350:stream/CISStaging-VaultandStreams-CISInputStream-P7DYU9FBQ2OW
export CIS_KINESIS_STREAM_NAME=CISStaging-VaultandStreams-CISInputStream-P7DYU9FBQ2OW
export CIS_IAM_ROLE_ARN= arn:aws:iam::656532927350:role/CISPublisherRole
export CIS_PUBLISHER_NAME=hris
export CIS_IAM_ROLE_SESSION_NAME=test_hris_client
export CIS_IAM_ROLE_ARN=arn:aws:iam::656532927350:role/CISPublisherRole
export CIS_LAMBDA_VALIDATOR_ARN=arn:aws:lambda:us-west-2:656532927350:function:cis_functions_stage_validator

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
