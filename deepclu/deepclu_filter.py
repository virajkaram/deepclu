def get_upstream_agg_pipe(start_jd, end_jd):
    pipe = upstream_pipeline = [{"$match": {
        "candidate.programid": 3,
        "candidate.exptime":300,
        "candidate.jd": {"$gt": start_jd}
    }
    },
        {"$match": {
            "candidate.jd": {"$lt": end_jd}
        }
        },
        {"$project": {"cutoutScience": 0, "cutoutTemplate": 0, "cutoutDifference": 0}},
        {
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        {
            "$project": {
                "cross_matches": {
                    "$arrayElemAt": [
                        "$aux.cross_matches",
                        0
                    ]
                },
                "schemavsn": 1,
                "publisher": 1,
                "objectId": 1,
                "candid": 1,
                "candidate": 1,
                "classifications": 1,
                "coordinates": 1,

            }
        },
        {"$match": {
            "cross_matches.CLU_20190625.0": {
                "$exists": "true"
            }
        }
        },
        {
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        {
            "$project": {
                "prv_candidates": {
                    "$filter": {
                        "input": {
                            "$arrayElemAt": [
                                "$aux.prv_candidates",
                                0
                            ]
                        },
                        "as": "item",
                        "cond": {
                            "$and": [
                                {
                                    "$in": [
                                        "$$item.programid",
                                        [
                                            1,
                                            2,
                                            3
                                        ]
                                    ]
                                },
                                {
                                    "$lt": [
                                        {
                                            "$subtract": [
                                                "$candidate.jd",
                                                "$$item.jd"
                                            ]
                                        },
                                        365
                                    ]
                                }
                            ]
                        }
                    }
                },
                "schemavsn": 1,
                "publisher": 1,
                "objectId": 1,
                "candid": 1,
                "candidate": 1,
                "classifications": 1,
                "coordinates": 1,
                "cross_matches": 1,

            }
        }
    ]
    return pipe


def get_deep_clu_agg_pipeline(start_jd, end_jd):

    clu_filt = [{
        "$project": {
            "_id": 0,
            "candid": 1,
            "objectId": 1,
            "cross_matches_CLU": "$cross_matches.CLU_20190625",
            "isdiffpos": "$candidate.isdiffpos",
            "m_now": "$candidate.magpsf",
            "m_app": "$candidate.magap",
            "t_now": "$candidate.jd",
            "fid_now": "$candidate.fid",
            "sgscore": "$candidate.sgscore1",
            "sgscore2": "$candidate.sgscore2",
            "sgscore3": "$candidate.sgscore3",
            "srmag": "$candidate.srmag1",
            "srmag2": "$candidate.srmag2",
            "srmag3": "$candidate.srmag3",
            "sgmag": "$candidate.sgmag1",
            "simag": "$candidate.simag1",
            "rbscore": "$candidate.rb",
            "drb": "$candidate.drb",
            "magnr": "$candidate.magnr",
            "distnr": "$candidate.distnr",
            "distpsnr1": "$candidate.distpsnr1",
            "distpsnr2": "$candidate.distpsnr2",
            "distpsnr3": "$candidate.distpsnr3",
            "scorr": "$candidate.scorr",
            "fwhm": "$candidate.fwhm",
            "elong": "$candidate.elong",
            "nbad": "$candidate.nbad",
            "chipsf": "$candidate.chipsf",
            "gal_lat": "$coordinates.b",
            "ssdistnr": "$candidate.ssdistnr",
            "ssmagnr": "$candidate.ssmagnr",
            "ssnamenr": "$candidate.ssnamenr",
            "jdstarthist": "$candidate.jdstarthist",
            "jdendhist": "$candidate.jdendhist",
            "deltajd": {
                "$subtract": [
                    "$candidate.jdendhist",
                    "$candidate.jdstarthist"
                ]
            },
            "psfminap": {
                "$subtract": [
                    "$candidate.magpsf",
                    "$candidate.magap"
                ]
            },
        }
    },
        {
            "$project": {
                "objectId": 1,
                "cross_matches_CLU": 1,
                "t_now": 1,
                "m_now": 1,
                "fid_now": 1,
                "sgscore": 1,
                "drbscore": 1,
                "magnr": 1,
                "distnr": 1,
                "scorr": 1,
                "ssdistnr": 1,
                "ssnamenr": 1,
                "rbscore": 1,
                "drb": 1,
                "sgmag": 1,
                "srmag": 1,
                "simag": 1,
                "distpsnr1": 1,
                "distpsnr2": 1,
                "distpsnr3": 1,
                "fwhm": 1,
                "elong": 1,
                "gal_lat": 1,
                "jdstarthist": 1,
                "jdendhist": 1,
                "psfminap": 1,

                "bright": {
                    "$lt": [
                        "$m_now",
                        99
                    ]
                },
                "positivesubtraction": {
                    "$in": [
                        "$isdiffpos",
                        [
                            1,
                            "1",
                            "t",
                            "true"
                        ]
                    ]
                },
                "real": {
                    "$and": [
                        {
                            "$gt": [
                                "$rbscore",
                                0.3
                            ]
                        },
                        {
                            "$gt": [
                                "$drb",
                                0.5
                            ]
                        },
                        {
                            "$gt": [
                                "$fwhm",
                                0.5
                            ]
                        },
                        {
                            "$lt": [
                                "$fwhm",
                                8
                            ]
                        },
                        {
                            "$lt": [
                                "$nbad",
                                5
                            ]
                        },
                        {
                            "$lt": [
                                {
                                    "$abs": "$psfminap"
                                },
                                0.75
                            ]
                        }
                    ]
                },
                "nopointunderneath": {
                    "$not": [
                        {
                            "$and": [
                                {
                                    "$gt": [
                                        "$sgscore",
                                        0.76
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        2
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "brightstar": {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr2",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag2",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag2",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore2",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr3",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag3",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag3",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore3",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$eq": [
                                        "$sgscore",
                                        0.5
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        0.5
                                    ]
                                },
                                {
                                    "$or": [
                                        {
                                            "$lt": [
                                                "$sgmag",
                                                17
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                "$srmag",
                                                17
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                "$simag",
                                                17
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "variablesource": {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        0.4
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        19
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        0.8
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        17
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        1.2
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "rock": {
                    "$and": [
                        {
                            "$gte": [
                                "$ssdistnr",
                                0
                            ]
                        },
                        {
                            "$lt": [
                                "$ssdistnr",
                                12
                            ]
                        },
                        {
                            "$lt": [
                                {
                                    "$abs": "$ssmagnr"
                                },
                                20
                            ]
                        }
                    ]
                },
            }
        },
        {
            "$project": {
                "objectId": 1,
                "cross_matches_CLU": 1,
                "t_now": 1,
                "m_now": 1,
                "fid_now": 1,
                "sgscore": 1,
                "drbscore": 1,
                "magnr": 1,
                "distnr": 1,
                "scorr": 1,
                "gal_lat": 1,
                "ssdistnr": 1,
                "ssnamenr": 1,
                "rbscore": 1,
                "drb": 1,
                "sgmag": 1,
                "srmag": 1,
                "simag": 1,
                "distpsnr1": 1,
                "distpsnr2": 1,
                "distpsnr3": 1,
                "fwhm": 1,
                "elong": 1,
                "jdstarthist": 1,
                "jdendhist": 1,
                "psfminap": 1,
                "bright": 1,
                "positivesubtraction": 1,
                "real": 1,
                "nopointunderneath": 1,
                "brightstar": 1,
                "variablesource": 1,
                "rock": 1,

            }
        },
        {
            "$match": {
                "bright": True,
                "nopointunderneath": True,
                "positivesubtraction": True,
                "real": True,
                "brightstar": False,
                "rock": False
            }
        }
    ]
    upstream_pipeline = get_upstream_agg_pipe(start_jd, end_jd)
    full_pipeline = upstream_pipeline + clu_filt
    return full_pipeline


def get_fritz_deepclu_agg_pipeline(start_jd, end_jd):
    clu_filt = [{
        "$project": {
            "_id": 0,
            "candid": 1,
            "objectId": 1,
            "cross_matches_CLU": "$cross_matches.CLU_20190625",
            "prv_candidates.jd": 1,
            "prv_candidates.magpsf": 1,
            "prv_candidates.fid": 1,
            "prv_candidates.isdiffpos": 1,
            "isdiffpos": "$candidate.isdiffpos",
            "m_now": "$candidate.magpsf",
            "m_app": "$candidate.magap",
            "t_now": "$candidate.jd",
            "fid_now": "$candidate.fid",
            "sgscore": "$candidate.sgscore1",
            "sgscore2": "$candidate.sgscore2",
            "sgscore3": "$candidate.sgscore3",
            "srmag": "$candidate.srmag1",
            "srmag2": "$candidate.srmag2",
            "srmag3": "$candidate.srmag3",
            "sgmag": "$candidate.sgmag1",
            "simag": "$candidate.simag1",
            "rbscore": "$candidate.rb",
            "drb": "$candidate.drb",
            "magnr": "$candidate.magnr",
            "distnr": "$candidate.distnr",
            "distpsnr1": "$candidate.distpsnr1",
            "distpsnr2": "$candidate.distpsnr2",
            "distpsnr3": "$candidate.distpsnr3",
            "scorr": "$candidate.scorr",
            "fwhm": "$candidate.fwhm",
            "elong": "$candidate.elong",
            "nbad": "$candidate.nbad",
            "chipsf": "$candidate.chipsf",
            "gal_lat": "$coordinates.b",
            "ssdistnr": "$candidate.ssdistnr",
            "ssmagnr": "$candidate.ssmagnr",
            "ssnamenr": "$candidate.ssnamenr",
            "jdstarthist": "$candidate.jdstarthist",
            "jdendhist": "$candidate.jdendhist",
            "deltajd": {
                "$subtract": [
                    "$candidate.jdendhist",
                    "$candidate.jdstarthist"
                ]
            },
            "psfminap": {
                "$subtract": [
                    "$candidate.magpsf",
                    "$candidate.magap"
                ]
            },
            "candidates_fid": {
                "$concatArrays": [
                    {
                        "$filter": {
                            "input": "$prv_candidates",
                            "as": "cand",
                            "cond": {
                                "$and": [
                                    {
                                        "$eq": [
                                            "$$cand.fid",
                                            "$candidate.fid"
                                        ]
                                    },
                                    {
                                        "$gt": [
                                            "$$cand.magpsf",
                                            0
                                        ]
                                    },
                                    {
                                        "$lt": [
                                            "$$cand.magpsf",
                                            99
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    [
                        {
                            "jd": "$candidate.jd",
                            "magpsf": "$candidate.magpsf"
                        }
                    ]
                ]
            }
        }
    },
        {
            "$project": {
                "objectId": 1,
                "cross_matches_CLU": 1,
                "t_now": 1,
                "m_now": 1,
                "fid_now": 1,
                "sgscore": 1,
                "drbscore": 1,
                "magnr": 1,
                "distnr": 1,
                "scorr": 1,
                "ssdistnr": 1,
                "ssnamenr": 1,
                "rbscore": 1,
                "drb": 1,
                "sgmag": 1,
                "srmag": 1,
                "simag": 1,
                "distpsnr1": 1,
                "distpsnr2": 1,
                "distpsnr3": 1,
                "fwhm": 1,
                "elong": 1,
                "gal_lat": 1,
                "jdstarthist": 1,
                "jdendhist": 1,
                "psfminap": 1,
                "candidates_fid": 1,
                "m_max_index": {
                    "$indexOfArray": [
                        "$candidates_fid.magpsf",
                        {
                            "$max": [
                                "$candidates_fid.magpsf"
                            ]
                        }
                    ]
                },
                "m_min_index": {
                    "$indexOfArray": [
                        "$candidates_fid.magpsf",
                        {
                            "$min": [
                                "$candidates_fid.magpsf"
                            ]
                        }
                    ]
                },
                "bright": {
                    "$lt": [
                        "$m_now",
                        99
                    ]
                },
                "positivesubtraction": {
                    "$in": [
                        "$isdiffpos",
                        [
                            1,
                            "1",
                            "t",
                            True
                        ]
                    ]
                },
                "real": {
                    "$and": [
                        {
                            "$gt": [
                                "$rbscore",
                                0.3
                            ]
                        },
                        {
                            "$gt": [
                                "$drb",
                                0.5
                            ]
                        },
                        {
                            "$gt": [
                                "$fwhm",
                                0.5
                            ]
                        },
                        {
                            "$lt": [
                                "$fwhm",
                                8
                            ]
                        },
                        {
                            "$lt": [
                                "$nbad",
                                5
                            ]
                        },
                        {
                            "$lt": [
                                {
                                    "$abs": "$psfminap"
                                },
                                0.75
                            ]
                        }
                    ]
                },
                "nopointunderneath": {
                    "$not": [
                        {
                            "$and": [
                                {
                                    "$gt": [
                                        "$sgscore",
                                        0.76
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        2
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "brightstar": {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr2",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag2",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag2",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore2",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distpsnr3",
                                        20
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$srmag3",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$srmag3",
                                        0
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$sgscore3",
                                        0.49
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$eq": [
                                        "$sgscore",
                                        0.5
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$distpsnr1",
                                        0.5
                                    ]
                                },
                                {
                                    "$or": [
                                        {
                                            "$lt": [
                                                "$sgmag",
                                                17
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                "$srmag",
                                                17
                                            ]
                                        },
                                        {
                                            "$lt": [
                                                "$simag",
                                                17
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "variablesource": {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        0.4
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        19
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        0.8
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        17
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "$lt": [
                                        "$distnr",
                                        1.2
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$magnr",
                                        15
                                    ]
                                },
                                {
                                    "$gt": [
                                        "$age",
                                        90
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "rock": {
                    "$and": [
                        {
                            "$gte": [
                                "$ssdistnr",
                                0
                            ]
                        },
                        {
                            "$lt": [
                                "$ssdistnr",
                                12
                            ]
                        },
                        {
                            "$lt": [
                                {
                                    "$abs": "$ssmagnr"
                                },
                                20
                            ]
                        }
                    ]
                },
                "stationary": {
                    "$anyElementTrue": {
                        "$map": {
                            "input": "$prv_candidates",
                            "as": "cand",
                            "in": {
                                "$and": [
                                    {
                                        "$gt": [
                                            {
                                                "$abs": {
                                                    "$subtract": [
                                                        "$t_now",
                                                        "$$cand.jd"
                                                    ]
                                                }
                                            },
                                            0.02
                                        ]
                                    },
                                    {
                                        "$lt": [
                                            "$$cand.magpsf",
                                            99
                                        ]
                                    },
                                    {
                                        "$in": [
                                            "$$cand.isdiffpos",
                                            [
                                                1,
                                                "1",
                                                True,
                                                "t"
                                            ]
                                        ]
                                    },
                                    {
                                        "$or": [
                                            {
                                                "$lt": [
                                                    "$ssdistnr",
                                                    -0.5
                                                ]
                                            },
                                            {
                                                "$gt": [
                                                    "$ssdistnr",
                                                    2
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "m_max": {
                    "$arrayElemAt": [
                        "$candidates_fid.magpsf",
                        "$m_max_index"
                    ]
                },
                "m_min": {
                    "$arrayElemAt": [
                        "$candidates_fid.magpsf",
                        "$m_min_index"
                    ]
                },
                "t_max": {
                    "$arrayElemAt": [
                        "$candidates_fid.jd",
                        "$m_max_index"
                    ]
                },
                "t_min": {
                    "$arrayElemAt": [
                        "$candidates_fid.jd",
                        "$m_min_index"
                    ]
                },
                "objectId": 1,
                "cross_matches_CLU": 1,
                "t_now": 1,
                "m_now": 1,
                "fid_now": 1,
                "sgscore": 1,
                "drbscore": 1,
                "magnr": 1,
                "distnr": 1,
                "scorr": 1,
                "gal_lat": 1,
                "ssdistnr": 1,
                "ssnamenr": 1,
                "rbscore": 1,
                "drb": 1,
                "sgmag": 1,
                "srmag": 1,
                "simag": 1,
                "distpsnr1": 1,
                "distpsnr2": 1,
                "distpsnr3": 1,
                "fwhm": 1,
                "elong": 1,
                "jdstarthist": 1,
                "jdendhist": 1,
                "psfminap": 1,
                "bright": 1,
                "positivesubtraction": 1,
                "real": 1,
                "nopointunderneath": 1,
                "brightstar": 1,
                "variablesource": 1,
                "rock": 1,
                "stationary": 1
            }
        },
        {
            "$match": {
                "bright": True,
                "nopointunderneath": True,
                "positivesubtraction": True,
                "real": True,
                "brightstar": False,
                "rock": False
            }
        }
    ]

    upstream_pipeline = get_upstream_agg_pipe(start_jd, end_jd)
    full_pipeline = upstream_pipeline + clu_filt

    return full_pipeline