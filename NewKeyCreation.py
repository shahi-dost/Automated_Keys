import boto3
from datetime import datetime
import uuid

def lambda_handler(event, context):
    kms_client = boto3.client('kms')
    dynamodb = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    
    table = dynamodb.Table('KMSKeysTable') 
    bucket_name = 'rotatekeysgenstorage'  
  
    today = datetime.now().strftime('%Y%m%d')
    unique_id = str(uuid.uuid4())[:8] 
    key_alias = f'alias/myproject-key-{today}-{unique_id}'

    # Create a new CMK
    response = kms_client.create_key(
        Description=f'CMK for {today}',
        KeyUsage='ENCRYPT_DECRYPT',
        Origin='AWS_KMS'
    )
    
    key_id = response['KeyMetadata']['KeyId']

    kms_client.create_alias(
        AliasName=key_alias,
        TargetKeyId=key_id
    )

    table.put_item(
        Item={
            'KeyID': key_id,  
            'Alias': key_alias,
            'CreationDate': today,
            'UUID': unique_id
        }
    )

    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'aws:kms',
                        'KMSMasterKeyID': key_id
                    }
                }
            ]
        }
    )
    
    return {
        'statusCode': 200,
        'body': f'Created CMK with alias {key_alias} and updated S3 bucket encryption'
    }
