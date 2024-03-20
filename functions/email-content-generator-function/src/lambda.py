from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import boto3
import os

class Accommodation:
    id: str
    source: str
    address: str
    price: float
    propUrl: str

class EmailContentGeneratorEvent:
    date: str
    cityCode: str
    areaCode: str
    items: list[Accommodation]

# Load environment variables from .env file
load_dotenv('../.env')

def init():
    global template
    global isInit

    isInit = False
    template = None

    if isInit == True:
        return
    
    env = Environment(loader=FileSystemLoader('static'))
    template = env.get_template('email-template.html')

    isInit = True


def handler(event: EmailContentGeneratorEvent, context):
    init()

    rendered_template = template.render({
        'date': event['date'],
        'cityCode': event['cityCode'],
        'areaCode': event['areaCode'],
        'items': event['items']
    })

    # Call the function to upload the rendered template to S3
    upload_to_s3(event, rendered_template)

    # Your code here
    return {
        'result': 'SUCCESS',
    }


def upload_to_s3(event: EmailContentGeneratorEvent, rendered_template: str):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('BUCKET_NAME')

    date_parts = event['date'].split('/')
    reversed_date = f"{date_parts[2]}/{date_parts[1]}/{date_parts[0]}"

    object_key = f"{reversed_date}/{event['cityCode']}/{event['areaCode']}.html"

    s3.put_object(
        Body=rendered_template,
        Bucket=bucket_name,
        Key=object_key
    )
