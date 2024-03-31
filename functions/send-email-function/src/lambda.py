import datetime
from typing import Dict, List
import boto3
import json
import os
from decimal import Decimal

root_dir = os.path.join(
    os.path.dirname(os.path.join(os.path.abspath(__file__))), 
    '..'
)
print(f'root_dir: {root_dir}')

class SendEmailEvent:
    Records: List[Dict]

def init():
    global s3
    global ses
    global sqs
    global template
    global isInit
    global sendEmailS3Bucket
    global senderEmail
    global messageTrackingTable
    global sqsSendEmailReplyQueueUrl

    isInit = False
    template = None

    if isInit == True:
        return

    s3 = boto3.client('s3')
    ses = boto3.client('ses')
    dynamodb = boto3.resource('dynamodb')
    

    sendEmailS3Bucket = os.environ.get('SEND_EMAIL_QUEUE_S3_BUCKET')
    senderEmail = os.environ.get('SENDER_EMAIL')
    messageTrackingTable = dynamodb.Table(os.environ.get('MESSAGE_TRACKING_TABLE'))

    isInit = True


def handler(event: SendEmailEvent, context):
    init()

    record = event['Records'][0]

    # check if message payload is valid
    s3KeyId = record['messageAttributes']['queue_key_id']['stringValue']
    if s3KeyId is None:
        raise Exception('Missing queue_key_id in the event')
    
    # check if message ID is already processed
    msgId = record['messageId']
    item = messageTrackingTable.get_item(
        Key={
            'message_id': msgId
        }
    ).get('Item')
    if item is not None:
        print(f"Message ID {msgId} already processed")
        return {
            'result': 'SUCCESS'
        }

    # process logic
    body = json.loads(record['body'])
    try:
        indexHtmlKey = f"{s3KeyId}/index.html"
        receiverListJsonKey = f"{s3KeyId}/receiver_list.json"

        # Get the content of index.html
        indexHtmlResponse = s3.get_object(
            Bucket=sendEmailS3Bucket,
            Key=indexHtmlKey
        )
        indexHtmlContent = indexHtmlResponse['Body'].read().decode('utf-8')
    
        # Get the content of receiver_list.json
        receiverListJsonResponse = s3.get_object(
            Bucket=sendEmailS3Bucket,
            Key=receiverListJsonKey
        )
        email_list = json.loads(receiverListJsonResponse['Body'].read().decode('utf-8'))
    except Exception as e:
        raise Exception(f"Error while getting the content of index.html and receiver_list.json: {e}")

    try:
        body = indexHtmlContent

        response = ses.send_email(
            Source=senderEmail,
            Destination={
                'ToAddresses': email_list
            },
            Message={
                'Subject': {
                    'Data': body['subject']
                },
                'Body': {
                    'Html': {
                        'Data': body
                    }
                }
            }
        )
        messageTrackingTable.put_item(
            Item={
                'message_id': msgId,
                'created_at': Decimal(str(datetime.datetime.now().timestamp())),
                'data': json.dumps(record),
                'expired_at': Decimal(str((datetime.datetime.now() + datetime.timedelta(days=7)).timestamp())) # expired in 7 days
            }
        )

        for email in email_list:
            print(f"Email sent to {email}. Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Error while sending email: {e}")

    return {
        'result': 'SUCCESS'
    }