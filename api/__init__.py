from flask import Flask
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

from api import routes
