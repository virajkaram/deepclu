import argparse
from kowalski_utils import connect_kowalski, find_deepclu_alerts
import numpy as np
from astropy.time import Time


def get_fields_that_generated_alerts(ut_start_jd):
    k = connect_kowalski()
    deepclu_alerts = find_deepclu_alerts(k,deepclu_start_jd=ut_start_jd)
    jds = np.array([x['candidate']['jd'] for x in deepclu_alerts])
    fields = np.array([x['candidate']['field'] for x in deepclu_alerts])
    uniq_fields = []
    nobservations = []
    with open('data/fields_alert_generated.csv','w') as writefile:
        for f in np.unique(fields):
            jds_obs = np.unique(jds[(fields==f)])
            nobs = len(jds_obs)
            writefile.write(f'{f},{nobs},{jds_obs}\n')
            nobservations.append(nobs)

    return np.unique(fields), nobservations

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ut_start', type=str, default='2022-08-01')

    args = parser.parse_args()
    ut_start_jd = Time(args.ut_start).jd
    fields, nobs = get_fields_that_generated_alerts(ut_start_jd=ut_start_jd)
    print(fields, nobs)