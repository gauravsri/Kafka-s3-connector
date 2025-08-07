#!/usr/bin/env python3

import boto3
from botocore.exceptions import ClientError

def create_s3_bucket():
    # Configure S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )
    
    bucket_name = 'test-data-lake'
    
    try:
        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Successfully created bucket: {bucket_name}")
        
        # List buckets to confirm
        buckets = s3_client.list_buckets()
        print("📋 Available buckets:")
        for bucket in buckets['Buckets']:
            print(f"  - {bucket['Name']}")
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"✅ Bucket {bucket_name} already exists")
        else:
            print(f"❌ Error creating bucket: {e}")

if __name__ == "__main__":
    create_s3_bucket()