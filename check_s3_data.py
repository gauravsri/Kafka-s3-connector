#!/usr/bin/env python3

import boto3
import json
from botocore.exceptions import ClientError

def check_s3_data():
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
        # List objects in bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        
        print("📋 Files in S3 bucket:")
        for obj in objects.get('Contents', []):
            print(f"  📄 {obj['Key']} (size: {obj['Size']} bytes)")
        
        # Read and display content of each file
        for obj in objects.get('Contents', []):
            key = obj['Key']
            print(f"\n🔍 Content of {key}:")
            print("=" * 80)
            
            # Get object content
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Try to pretty print as JSON
            try:
                data = json.loads(content)
                print("📊 Structure: JSON Array")
                print(f"📊 Number of records: {len(data) if isinstance(data, list) else 1}")
                
                # Show first record
                if isinstance(data, list) and len(data) > 0:
                    print(f"📄 Sample record:")
                    print(json.dumps(data[0], indent=2))
                else:
                    print(json.dumps(data, indent=2))
                    
            except json.JSONDecodeError:
                print("⚠️  Not valid JSON, showing raw content:")
                print(content)
                
        print("\n" + "=" * 80)
            
    except ClientError as e:
        print(f"❌ Error accessing S3: {e}")

if __name__ == "__main__":
    check_s3_data()