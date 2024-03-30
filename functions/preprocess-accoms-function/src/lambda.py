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

    notification_created_at = datetime.datetime.now().strftime('%d/%m/%Y')

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
                            'created_date': {'S': notification_created_at},
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
                for accom in accomList
            ]
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e
    else:
        print("TransactWriteItems succeeded")

    for region in region_list:
        city_code, area_code = region.split('_')
        message = {
            'region': region,
            'created_date': notification_created_at,
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