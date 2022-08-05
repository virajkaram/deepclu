from penquins import Kowalski
from astropy.io import ascii
from astropy.time import Time

def connect_kowalski():
    secrets = ascii.read('secrets.csv', format = 'csv')
    username_kowalski = secrets['kowalski_user'][0]
    password_kowalski = secrets['kowalski_pwd'][0]
    # load username & passw credentials
    protocol, host, port = "https", "kowalski.caltech.edu", 443
    kowalski = Kowalski(username=username_kowalski, password=password_kowalski,protocol=protocol,host=host,port=port)
    connection_ok = kowalski.ping()
    print(f'Connection OK: {connection_ok}')
    return kowalski


def run_pipeline_offline(k, pipeline):
    q = {
        "query_type": "aggregate",
        "query": {
            "catalog": "ZTF_alerts",
            "pipeline": pipeline,
            "kwargs": {

            }
        }
    }

    response = k.query(query=q)
    data = response.get("data")
    print(f"Response {response['status']}:{response['message']}")
    return data


def find_alert_from_id(k, ztfid):
    q = {
        "query_type": "find",
        "query": {
            "catalog": "ZTF_alerts",
            "filter": {
                "objectId": f"{ztfid}",
                "candidate.programid": 3
            },
            "projection": {
                "_id": 0,
                "candidate.jd": 1,
                "candidate.programid": 1,
                "candidate.field": 1
            },
            "kwargs": {

            }
        }
    }

    response = k.query(query=q)
    data = response.get("data")
    print(f"Response {response['status']}:{response['message']}")
    return data


def find_deepclu_alerts(k, deepclu_start_jd=Time('2022-08-01').isot):
    q = {
        "query_type": "find",
        "query": {
            "catalog": "ZTF_alerts",
            "filter": {
                "candidate.programid": 3,
                "candidate.jd": {"$gt": deepclu_start_jd},
                "candidate.exptime": {"$gt": 250}
            },
            "projection": {
                "_id": 0,
                "objectId": 1,
                "candidate.jd": 1,
                "candidate.programid": 1,
                "candidate.field": 1
            },
            "kwargs": {
            }
        }
    }

    response = k.query(query=q)
    data = response.get("data")
    print(f"Response {response['status']}:{response['message']}")
    return data


def find_alert_aux_from_id(k, ztfid):
    q = {
        "query_type": "find",
        "query": {
            "catalog": "ZTF_alerts_aux",
            "filter": {
                "_id": f"{ztfid}"

            },
            "projection": {
                "_id": 0,
            },
            "kwargs": {
                "limit": 1
            }
        }
    }

    response = k.query(query=q)
    data = response.get("data")
    print(f"Response {response['status']}:{response['message']}")
    return data