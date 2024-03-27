from flask import Blueprint, jsonify, request
from boto3 import resource
from marshmallow import Schema, fields, ValidationError
import os
import datetime

class SubscribeSchema(Schema):
    email = fields.Email(required=True)
    city_code = fields.Str(required=True)
    area_code = fields.Str(required=True)

class GetSubscriptionSchema(Schema):
    email = fields.Email(required=True)

dynamodb = resource('dynamodb')

# Create a new Blueprint
api = Blueprint('subscription', __name__)

@api.route('/', methods=['POST'])
def subscribe():
    # init dynamoDB
    table_name = os.environ.get('SUBSCRIPTION_TABLE_NAME')
    print(f'Table name: {table_name}')
    subscriptionTable = dynamodb.Table(table_name)

    # Handle the request
    schema = SubscribeSchema()
    try:
        data = schema.load(request.get_json())
    except ValidationError as e:
        return jsonify({
            'result': 'Error',
            'message': 'Bad request',
            'data': e.messages
        }), 400

    # Handle the request
    data = request.get_json()

    subscription_region = f'{data["city_code"]}_{data["area_code"]}'

    # get subscription data by email
    response = subscriptionTable.get_item(
        Key={
            'email': data['email'],
            'subscription_region': subscription_region
        }
    )

    subscription = response.get('Item')
    # Check if the email already exists
    if subscription is not None:
        return jsonify({
            'result': 'OK',
            'message': 'You have already subscribed'
        }), 200

    subscription = {
        'email': data['email'],
        'subscription_region': subscription_region,
        'city_code': data['city_code'],
        'area_code': data['area_code'],
        'subscription_date': datetime.datetime.now().strftime("%Y/%m/%d"),
        'created_at': int(datetime.datetime.now().timestamp())
    }

    # Put the item in the table
    subscriptionTable.put_item(Item=subscription)

    return jsonify({
        'result': 'OK',
        'message': 'Subscription created successfully',
        'data': subscription,
    }), 201


@api.route('/', methods=['GET'])
def get_subscriptions():
    # init dynamoDB
    table_name = os.environ.get('SUBSCRIPTION_TABLE_NAME')
    print(f'Table name: {table_name}')
    subscriptionTable = dynamodb.Table(table_name)
    
    # Handle the request
    schema = GetSubscriptionSchema()
    try:
        data = schema.load(request.args.to_dict())
    except ValidationError as e:
        return jsonify({
            'result': 'Error',
            'message': 'Bad request',
            'data': e.messages
        }), 400

    response = subscriptionTable.query(
        IndexName='email-index',
        KeyConditionExpression='email = :email',
        ExpressionAttributeValues={
            ':email': data['email']
        }
    )

    subscriptions = response.get('Items')

    return jsonify({
        'result': 'OK',
        'message': 'Subscriptions retrieved successfully',
        'data': {
            'items': subscriptions
        }
    }), 200