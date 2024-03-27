def handler(event, context):
    if 'headers' in event and 'Host' in event['headers']:
        from .app.views import app
        from serverless_wsgi import handle_request
        return handle_request(app, event, context)
    print('Request does not come from API Gateway')
    return {
        'result': 'Error',
        'message': 'Bad request'
    }
