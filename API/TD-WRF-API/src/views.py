"""
API Method views
"""
from functools import wraps
import json

import pandas as pd
from flask import request

from configs import db_config
from src.password_manager import PasswordManager
from src.sessionmgmt import ValidateSession
from src.site_data_extract import SiteDataExtractor, ev_data_meta


def login():
    """ [POST] Login METHOD """
    ps = PasswordManager.fromrequest(request)
    status, resp = ps.validate()
    if status:
        json_obj = ps.messageformat("Logged in sucessfully", 200)
        json_obj['auth_code'] = resp
        return json_obj, 200
    else:
        json_obj = ps.messageformat(resp, 403)
        return json_obj, 403


def login_required(func):
    """ WRAPPER for API calls where auth is required"""

    @wraps(func)
    def new_func(*args, **kwargs):
        user = request.args.get('user')
        vs = ValidateSession(request.headers, user)
        status, resp = vs.validate()
        if not status:
            return resp, 403
        return func(*args, **kwargs)

    return new_func


@login_required
def dataloader():
    """ GET getdata method"""
    site_name = request.args.get('site_name')
    start_date = pd.Timestamp('today')
    end_date = pd.Timestamp(start_date + pd.Timedelta(days=3))
    extract_init = SiteDataExtractor(db_connection=None,
                                     table_name=db_config.wrf_view,
                                     schema_name=db_config.wrf_schema,
                                     site_name=site_name,
                                     date_start=start_date.date(),
                                     date_end=end_date.date(),
                                     site_column_label='site_name',
                                     datetime_column_label='timestamp',
                                     eng='connectorx')
    site_data = extract_init.read_data()
    site_data['timestamp'] = site_data['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    meta_data = ev_data_meta()  # ADD META INFO
    meta_data['start_date'] = start_date.strftime('%Y-%m-%d')
    meta_data['end_date'] = end_date.strftime('%Y-%m-%d')
    meta_data['data'] = site_data.to_dict()
    return json.dumps(meta_data)

