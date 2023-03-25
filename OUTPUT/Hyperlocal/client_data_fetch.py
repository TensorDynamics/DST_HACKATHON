"""
Sample script to download site data at client end
"""
#!/usr/bin/env python
# coding: utf-8

import requests
import json


def log_in_and_get_session_auth_code(td_url, user, password):
    headers = {'Content-type': 'application/json'}
    login_reponse = requests.post(url=f'{td_url}/login/',
                                  data={'user': user, 'password': password}, headers=headers)
    login_content = login_reponse.json()
    if login_content.get('status_code') == 200:
        return login_content['auth_code']
    print(login_content.get('error'))
    return None


def get_data(user, td_url, site_name='all', password=None, access_key=None):
    params = {'user': user, 'site_name': site_name}
    if access_key is None:
        if password is None:
            raise ValueError("Either password or access key should be provided.")
        access_key = log_in_and_get_session_auth_code(user=user,
                                                      td_url=td_url,
                                                      password=password)
    headers = {'access-key': access_key}
    data_response = requests.get(url=f"{td_url}/getdata",
                                 headers=headers,
                                 params=params)
    if data_response.status_code != 200:
        print(data_response.json()['error'])
    else:
        return data_response.json()


if __name__ == '__main__':
    td_url = 'http://localhost:9001'
    user = 'test_client'
    password = 'test123'
    site_name = 'Barod'

    access_key = log_in_and_get_session_auth_code(td_url=td_url,
                                                  user=user,
                                                  password=password)
    downloaded_data = get_data(user=user,
                               td_url=td_url,
                               site_name=site_name,
                               access_key=access_key)
    file_name = downloaded_data.get('start_date')
    with open(f'{file_name}.json', 'w') as f:
        json.dump(downloaded_data, f)
