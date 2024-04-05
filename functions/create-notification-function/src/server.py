# load environment variables from .env file
import json
import os
from dotenv import load_dotenv
parent_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(parent_dir, '../.env'))

from flask import Flask, jsonify, request
import importlib

lambda_module = importlib.import_module('lambda')
handler = lambda_module.handler

app = Flask(__name__)

@app.route('/create-notification', methods=['POST'])
def generateEmailTemplate():
    data = request.get_json()

    data = {
        'Records': [{**record, 'body': json.dumps(record['body'])} for record in data['Records']]
    }

    try:
        result = handler(data, None)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({
            'error': e
        }), 500

if __name__ == '__main__':
    app.run(port=3000, debug=True)