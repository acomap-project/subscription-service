from flask import Flask, jsonify, request
from lambda_handler import handler

app = Flask(__name__)

@app.route('/generate-email-template', methods=['POST'])
def generateEmailTemplate():
    data = request.get_json()

    try:
        result = handler(data, None)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(port=3000, debug=True)