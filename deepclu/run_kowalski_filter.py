from astropy.time import Time
import argparse
from deepclu_filter import get_deep_clu_agg_pipeline, get_fritz_deepclu_agg_pipeline
from kowalski_utils import connect_kowalski, run_pipeline_offline
import json
import numpy as np


def run_clu_filter_on_kowalski(pipe, ut_start_jd, ut_end_jd):
    k = connect_kowalski()
    passed_srcs = run_pipeline_offline(k, pipeline=pipe)
    print(f'Retrieved {len(passed_srcs)} sources from kowalski that passed the filter.')
    with open(f'data/passed_{int(ut_start_jd)}_{int(ut_end_jd)}', 'w') as f:
        json.dump(passed_srcs, f)
    print(f'Data dumped at data/passed_{int(ut_start_jd)}_{int(ut_end_jd)}')
    names = np.array([x['objectId'] for x in passed_srcs])
    return names


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('ut_start', type=str, default='2022-08-01')
    parser.add_argument('ut_end', type=str, default='2022-08-02')
    parser.add_argument('--fritz', action="store_true")

    args = parser.parse_args()

    ut_start_jd = Time(args.ut_start).jd
    ut_end_jd = Time(args.ut_end).jd

    if args.fritz:
        pipe = get_fritz_deepclu_agg_pipeline(ut_start_jd, ut_end_jd)
    else:
        pipe = get_deep_clu_agg_pipeline(ut_start_jd, ut_end_jd)
    names = run_clu_filter_on_kowalski(pipe, ut_start_jd, ut_end_jd)
    print(f"{len(names)} passed filter")
    print(f"Here they are : {np.sort(names)}")