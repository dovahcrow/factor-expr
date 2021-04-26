import numpy as np
import pandas as pd
import asyncio

from ... import Factor, replay


FILENAME = "../assets/test.pq"


def test_index():
    f = Factor("(TSMean 10 :price_ask_l1_open)")
    asyncio.run(
        replay(
            [FILENAME],
            [f],
            trim=False,
            index_col="time",
            pbar=False,
        )
    )


def test_trim():
    f = Factor("(TSMean 10 :price_ask_l1_open)")
    asyncio.run(replay([FILENAME], [f], trim=True, pbar=False))


def test_predicate():
    f = Factor("(TSMean 10 :price_ask_l1_open)")
    predicate = Factor(
        "(> (TSStd 60 (TSLogReturn 120 (+ :price_bid_l1_close :price_ask_l1_close))) 0.005)"
    )
    asyncio.run(replay([FILENAME], [f], predicate=predicate, pbar=False))


def test_predicate_with_trim():
    f = Factor("(TSMean 10 :price_ask_l1_open)")
    predicate = Factor(
        "(> (TSStd 60 (TSLogReturn 120 (+ :price_bid_l1_close :price_ask_l1_close))) 0.005)"
    )
    asyncio.run(replay([FILENAME], [f], predicate=predicate, trim=True, pbar=False))
