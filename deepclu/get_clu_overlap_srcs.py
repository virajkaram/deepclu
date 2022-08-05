import argparse
from astropy.time import Time
from fritz_utils import api_params
from kowalski_utils import connect_kowalski, find_alert_from_id
import numpy as np


def get_dc_srcs_in_clu(ut_start, ut_end, deepclu_start_jd = Time('2022-08-01').jd):
    pagenum = 1
    query_finished = False
    deepclu_sources = []
    k = connect_kowalski()
    while not query_finished:
        data = {
            'savedStatus': 'all',
            'startDate': ut_start,
            'endDate': ut_end,
            'groupIDs': '43',
            'pageNumber': pagenum,
        }

        response = api_params('GET', 'https://fritz.science/api/candidates', data=data)
        print(f'Queried page {pagenum}')
        pagenum += 1
        if response.status_code == 400:
            query_finished = True

        data = response.json()['data']

        if len(data) == 0:
            continue
        candidates = data['candidates']
        print(f'Found {len(candidates)} candidates')
        for cand in candidates:
            ztfid = cand['id']
            alerts = find_alert_from_id(k, ztfid)
            jds = np.array([x['candidate']['jd'] for x in alerts])
            pids = np.array([x['candidate']['programid'] for x in alerts])
            if np.any(jds > deepclu_start_jd):
                deepclu_sources.append(ztfid)
    return np.sort(deepclu_sources)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('ut_start', type=str, default='2022-08-01')
    parser.add_argument('ut_end', type=str, default='2022-08-02')

    args = parser.parse_args()
    ut_start_jd = Time(args.ut_start)
    ut_end_jd = Time(args.ut_end)

    deepclu_sources = get_dc_srcs_in_clu(args.ut_start, args.ut_end)
    print(f'The following {len(deepclu_sources)} sources also passed CLU.')
    print(deepclu_sources)

