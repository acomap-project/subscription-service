from flask import Flask
from .controllers.subscription import api as subscription_api

app = Flask(__name__)
app.register_blueprint(subscription_api, url_prefix='/subscriptions')
