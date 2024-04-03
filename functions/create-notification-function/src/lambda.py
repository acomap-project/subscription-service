import datetime
from typing import Dict, List
import boto3
import json
from jinja2 import Environment, FileSystemLoader
import os
from decimal import Decimal

root_dir = os.path.join(
    os.path.dirname(os.path.join(os.path.abspath(__file__))), 
    '..'
)
print(f'root_dir: {root_dir}')

class CreateNotificationEventRecord:
    region: str
    created_date: str
    city_code: str
    area_code: str

class CreateNotificationEvent:
    Records: List[Dict]

class Subscription:
    subscription_region: str
    email: str
    subscription_date: str
    city_code: str
    area_code: str

class Notification:
    region: str
    created_date: str
    status: str
    sent_time: int
    city_code: str
    area_code: str

def init():
    global s3
    global sqs
    global template
    global isInit
    global notificationTable
    global subscriptionTable
    global accomTable
    global sendEmailS3Bucket
    global sqsQueueUrl

    isInit = False
    template = None

    if isInit == True:
        return

    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')

    notificationTable = dynamodb.Table(os.environ.get('NOTIFICATION_TABLE_NAME'))
    subscriptionTable = dynamodb.Table(os.environ.get('SUBSCRIPTION_TABLE_NAME'))
    accomTable = dynamodb.Table(os.environ.get('ACCOMMODATION_TABLE_NAME'))
    sendEmailS3Bucket = os.environ.get('SEND_EMAIL_QUEUE_S3_BUCKET')
    sqsQueueUrl = os.environ.get('SEND_EMAIL_QUEUE_URL')
    
    env = Environment(loader=FileSystemLoader(os.path.join(root_dir, 'static')))
    template = env.get_template('email-template.html')

    isInit = True


def handler(event: CreateNotificationEvent, context):
    init()

    record: CreateNotificationEventRecord = json.loads(event['Records'][0]['body'])  # only handle one record at a time
    accom_list = accomTable.query(
        IndexName='region-index',
        KeyConditionExpression='#rg = :region and sent_date = :sent_date',
        ExpressionAttributeValues={
            ':region': record['region'],
            ':sent_date': record['sent_date']
        },
        ExpressionAttributeNames={
            '#rg': 'region',
        }
    )['Items']

    # Get notification by region and created_date
    notification = notificationTable.get_item(
        Key={
            'region': record['region'],
            'sent_date': record['created_date']
        }
    ).get('Item')

    if notification is not None:
        print(f"Notification already exists for region {record['region']} and created_date {record['created_date']}")
        return {
            'result': 'SUCCESS',
        }

    # Get subscription list by region
    subscription_list: list[Subscription] = subscriptionTable.query(
        KeyConditionExpression='subscription_region = :subscription_region',
        ExpressionAttributeValues={
            ':subscription_region': record['region']
        }
    )['Items']

    # if subscription_list is empty, return
    if len(subscription_list) == 0:
        print(f'No subscription found for region {record["city_code"]} - {record["area_code"]}')
        return {
            'result': 'SUCCESS',
        }
    
    # Put an item in dynamoDB
    notification: Notification = {
        'region': record['region'],
        'sent_date': record['sent_date'],
        'city_code': record['city_code'],
        'area_code': record['area_code'],
        'created_at': Decimal(str(datetime.datetime.now().timestamp()))
    }

    # put item to dynamoDB
    notificationTable.put_item(Item=notification)

    send_email_for_notification(notification, subscription_list, accom_list)

    # Your code here
    return {
        'result': 'SUCCESS',
    }

def get_subscription_list(region: str):
    # query subscription list from subscriptionTable by city_code and area_code
    response = subscriptionTable.query(
        KeyConditionExpression='subscription_region = :subscription_region',
        ExpressionAttributeValues={
            ':subscription_region': region
        }
    )
    return response['Items']


def send_email_for_notification(notification: dict, subscription_list: list[Subscription], item_list: list[dict]):
    queue_key_id = f"{notification['created_date']}/{notification['region']}"
    rendered_template = template.render({
        'date': notification['sent_date'],
        'city_code': notification['city_code'],
        'area_code': notification['area_code'],
        'items': item_list
    })

    email_list = [subscription['email'] for subscription in subscription_list]

    s3.put_object(
        Body=rendered_template,
        Bucket=sendEmailS3Bucket,
        Key=f"{queue_key_id}/index.html"
    )
    s3.put_object(
        Body=json.dumps(email_list),
        Bucket=sendEmailS3Bucket,
        Key=f"{queue_key_id}/receiver_list.json"
    )

    sqs.send_message(
        QueueUrl=sqsQueueUrl,
        MessageBody=json.dumps({
            'subject': f"Notification for {notification['region']} on {notification['sent_date']}",
        }),
        MessageAttributes={
            'queue_key_id': {
                'DataType': 'String',
                'StringValue': queue_key_id
            }
        }
    )
