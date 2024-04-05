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

    accomList: List[AccommodationRecord] = [json.loads(record['body']) for record in event['Records']]
    region_list = set()
    for accom in accomList:
        region = f"{accom['cityCode']}_{accom['areaCode']}"
        region_list.add(region)

    notification_sent_date = datetime.datetime.now().strftime('%d/%m/%Y')

    duplicates = []
    seen = set()
    for accom in accomList:
        if (accom['source'], accom['id']) in seen:
            duplicates.append(accom)
        else:
            seen.add((accom['source'], accom['id']))

    if duplicates:
        print("Accommodations with duplicated source and id:")
        for accom in duplicates:
            print(f"Source: {accom['source']}, ID: {accom['id']}")
    else:
        print("No accommodations with duplicated source and id.")

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