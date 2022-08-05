import json
import requests

with open('secrets_fritz_token.json','r') as f:
    dat = json.load(f)

fritz_token = dat['token']


def api_params(method, endpoint, data=None):
    headers = {'Authorization': f'token {fritz_token}'}
    response = requests.request(method, endpoint, params=data, headers=headers)

    return response

