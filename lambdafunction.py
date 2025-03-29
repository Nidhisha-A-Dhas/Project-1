import boto3
import json
import os
from datetime import datetime
import urllib.parse

rekognition = boto3.client('rekognition')
dynamodb = boto3.client('dynamodb')
sns = boto3.client('sns')

SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:148831181875:RekognitionResults'
DYNAMODB_TABLE = 'VideoMetaData'

def start_rekognition_job(bucket_name, video_key):
    print(f"Bucket Name: {bucket_name}, Video Key: {video_key}")

    response = rekognition.start_label_detection(
        Video={'S3Object': {'Bucket': bucket_name, 'Name': video_key}},
        NotificationChannel={
            'SNSTopicArn': SNS_TOPIC_ARN,
            'RoleArn': 'arn:aws:iam::148831181875:role/service-role/final-func-role-8trybpto'
        }
    )

    job_id = response['JobId']
    print(f"Correct JobId stored in DynamoDB: {job_id}")

    dynamodb.put_item(
        TableName=DYNAMODB_TABLE,
        Item={
            'job_id': {'S': job_id},
            'video_key': {'S': video_key},
            'Timestamp': {'S': datetime.utcnow().isoformat()},
            'status': {'S': 'IN_PROGRESS'}
        }
    )

    print(f"Rekognition job started with JobId: {job_id}")

def process_rekognition_results(message):
    rekognition_results = json.loads(message['Records'][0]['Sns']['Message'])
    labels = rekognition_results.get('Labels', [])

    # Check for critical event (e.g., fire detection)
    critical_event_detected = any(label['Name'].lower() == 'fire' for label in labels)

    if critical_event_detected:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({
                'default': 'Critical Alert: Fire detected in the video!',
                'video_key': rekognition_results.get('JobTag', 'Unknown video')
            }),
            Subject='Critical Alert: Fire Detected',
            MessageStructure='json'
        )

def lambda_handler(event, context):
    for record in event['Records']:
        if 's3' in record:
            s3_info = record['s3']
            bucket_name = s3_info['bucket']['name']
            video_key = urllib.parse.unquote(s3_info['object']['key'])
            start_rekognition_job(bucket_name, video_key)

        elif 'Sns' in record:
            process_rekognition_results(record)

    return {'statusCode': 200, 'body': 'Processed successfully'}
