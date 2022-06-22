import json
import boto3
s3 = boto3.client('s3')
def create_bucket(event, context): # ?name=<BucketName>
    bucketName = event["queryStringParameters"]["name"]
    s3.create_bucket(Bucket=bucketName)
    return {
        'statusCode': 200,
        'body': json.dumps("hello from lambda")
    }

def list_buckets(event, context):
    buckets = s3.list_buckets()
    bucket_list=[]
    for bucket in buckets["Buckets"]:
        bucket_list.append(bucket["Name"])
    return {
        'statusCode': 200,
        'body': json.dumps(bucket_list)
    }