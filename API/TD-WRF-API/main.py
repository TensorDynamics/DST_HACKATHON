"""
Main API server start
# In this set up. The following is a bare backbone of the API gateway. It
# shall have a minimal implementation. It will use password validation token
# generation. Followed by token validation in every call. It will have
# the following:
# 1. Authentication
# 2. Validation
# 3. Data passing
"""
import os

from flask import Flask, request
from flask_cors import CORS
from loguru import logger

from src.views import login, dataloader


app = Flask(__name__)
CORS(app, resources={r"*": {"origins": "*"}})

sink_file = os.path.join(os.getcwd(), '..', "logs", "server.log")
logger.add(sink=sink_file,
           level='DEBUG',
           backtrace=True,
           diagnose=True)


app.add_url_rule('/login/',
                 view_func=login,
                 methods=['POST'],
                 strict_slashes=False)

app.add_url_rule('/getdata/',
                 view_func=dataloader,
                 methods=['GET'],
                 strict_slashes=False)


@app.after_request
def after_request(response):
    """ Logging after every request. """
    # This avoids the duplication of registry in the log,
    # since that 500 is already logged via @app.errorhandler.
    if (response.status_code != 500) and (request.method != 'OPTIONS'):
        try:
            user = request.json['user']
        except:
            user = 'Could not catch request'
        log_message = f"User - {user} " \
                      f"{request.remote_addr} {request.method} " \
                      f"{request.scheme} {request.full_path} " \
                      f"{response.status}"
        logger.info(log_message)
    return response


if __name__ == '__main__':
    app.run(debug=True, port=9001, host='0.0.0.0')
