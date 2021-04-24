import numpy as np
import pandas as pd
import asyncio

from ... import Factor, replay


FILENAME = "../assets/test.pq"


def test_gt():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(> :price_ask_l1_open 0)")],
            pbar=False,
            index_col="time",
        )
    )

    assert ((df.price_ask_l1_open > 0).values == result.iloc[:, 0].values).all()


def test_gte():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(>= :price_ask_l1_low 0)")],
            pbar=False,
            index_col="time",
        )
    )

    assert ((df.price_ask_l1_low >= 0).values == result.iloc[:, 0].values).all()


def test_lt():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(< :price_ask_l1_high 0)")],
            pbar=False,
            index_col="time",
        )
    )

    assert ((df.price_ask_l1_high < 0).values == result.iloc[:, 0].values).all()


def test_lte():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(<= :price_ask_l1_close 0)")],
            pbar=False,
            index_col="time",
        )
    )

    assert ((df.price_ask_l1_close <= 0).values == result.iloc[:, 0].values).all()


def test_or():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(Or (< :price_ask_l1_close 0) (> :price_bid_l1_high 0))")],
        )
    )

    assert (
        ((df.price_ask_l1_close < 0).values | (df.price_bid_l1_high > 0).values)
        == result.iloc[:, 0].values
    ).all()


def test_and():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(And (< :price_ask_l1_open 0) (> :price_bid_l1_low 0))")],
        )
    )

    assert (
        ((df.price_ask_l1_open < 0).values & (df.price_bid_l1_low > 0).values)
        == result.iloc[:, 0].values
    ).all()


def test_not():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(! (And (< :price_ask_l1_close 0) (> :price_bid_l1_low 0)))")],
        )
    )

    assert (
        ~((df.price_ask_l1_close < 0).values & (df.price_bid_l1_low > 0).values)
        == result.iloc[:, 0].values
    ).all()


def test_if():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [
                Factor(
                    "(If (< :price_ask_l1_high 0) :price_ask_l1_close :price_bid_l1_open)"
                )
            ],
            pbar=False,
            index_col="time",
        )
    )

    assert (
        np.where(
            (df.price_ask_l1_high < 0).values,
            df.price_ask_l1_close.values,
            df.price_bid_l1_open.values,
        )
        == result.iloc[:, 0].values
    ).all()
