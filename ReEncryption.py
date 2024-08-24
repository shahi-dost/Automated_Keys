import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime

def get_recent_keys(table):
    response = table.scan(
        FilterExpression=Attr('CreationDate').lte(datetime.now().strftime("%Y%m%d"))
    )
    keys = sorted(response['Items'], key=lambda x: (x['CreationDate'], x['UUID']), reverse=True)
    if len(keys) >= 2:
        return keys[0]['KeyID'], keys[1]['KeyID'] 
    else:
        raise Exception("Not enough keys found in DynamoDB to perform re-encryption.")

def re_encrypt_files(bucket_name, old_key_id, new_key_id):
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    if 'Contents' in objects:
        for obj in objects['Contents']:
            object_key = obj['Key']
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response['Body'].read()

            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=file_content,
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=new_key_id
            )

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('KMSKeysTable') 
    bucket_name = 'rotatekeysgenstorage' 
    new_key_id, old_key_id = get_recent_keys(table)
    
    if not old_key_id:
        raise Exception("No previous key found, cannot re-encrypt using an old key.")
    re_encrypt_files(bucket_name, old_key_id, new_key_id)

    return {
        'statusCode': 200,
        'message': 'Files re-encrypted successfully'
    }
