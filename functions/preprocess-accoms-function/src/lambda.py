import datetime
import decimal
import json
from typing import Dict, List
import boto3
from botocore.exceptions import ClientError
import os

class AccommodationRecord:
    id: str
    source: str
    address: str
    propUrl: str
    cityCode: str
    areaCode: str
    price: float
    publishedDate: str

class PreprocessAccomsEvent:
    Records: List[Dict]

class Accommodation:
    region: str
    created_at: str
    id: str
    source: str
    address: str
    prop_url: str
    city_code: str
    area_code: str
    price: decimal.Decimal
    published_date: str

def init():
    global sqs
    global template
    global isInit
    global accomTableName
    global sqsQueueUrl
    global dynamodb

    isInit = False
    template = None

    if isInit == True:
        return

    dynamodb = boto3.resource('dynamodb')
    sqs = boto3.client('sqs')

    accomTableName = os.environ.get('ACCOMMODATION_TABLE_NAME')
    sqsQueueUrl = os.environ.get('SEND_EMAIL_QUEUE_URL')

    isInit = True


def handler(event: PreprocessAccomsEvent, context):
    init()

    accom_list: List[AccommodationRecord] = [json.loads(record['body']) for record in event['Records']]


    # Remove duplication in accom_list
    unique_accom_list = []
    duplicates = []
    seen = set()
    for accom in accom_list:
        if (accom['source'], accom['id']) in seen:
            duplicates.append(accom)
            continue
        else:
            seen.add((accom['source'], accom['id']))
            unique_accom_list.append(accom)

    if duplicates:
        print("Accommodations with duplicated source and id:")
        for accom in duplicates:
            print(f"Source: {accom['source']}, ID: {accom['id']}")
    else:
        print("No accommodations with duplicated source and id.")

    # Get unique region list
    region_list = set()
    for accom in accom_list:
        region = f"{accom['cityCode']}_{accom['areaCode']}"
        region_list.add(region)

    notification_sent_date = datetime.datetime.now().strftime('%d/%m/%Y')

    # Write accom list to DynamoDB
    try:
        db = boto3.client('dynamodb')
        response = db.transact_write_items(
            TransactItems=[
                {
                    'Put': {
                        'TableName': accomTableName,
                        'Item': {
                            'source': {'S': accom['source']},
                            'id': {'S': accom['id']},
                            'region': {'S': f"{accom['cityCode']}_{accom['areaCode']}"},
                            'sent_date': {'S': notification_sent_date},
                            'published_date': {'S': accom['publishedDate']},
                            'address': {'S': accom['address']},
                            'prop_url': {'S': accom['propUrl']},
                            'city_code': {'S': accom['cityCode']},
                            'area_code': {'S': accom['areaCode']},
                            'price': {'N': str(accom['price'])},
                            'expired_at': {'N': str(int(datetime.datetime.now().timestamp()) + 24 * 60 * 60)}, # expired in 24 hours
                        }
                    }
                }
                for accom in unique_accom_list
            ]
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e
    else:
        print("TransactWriteItems succeeded")

    # Send message to SQS
    for region in region_list:
        city_code, area_code = region.split('_')
        message = {
            'region': region,
            'sent_date': notification_sent_date,
            'city_code': city_code,
            'area_code': area_code,
        }
        sqs.send_message(
            QueueUrl=sqsQueueUrl,
            MessageBody=json.dumps(message)
        )

    return {
        'result': 'SUCCESS',
    }