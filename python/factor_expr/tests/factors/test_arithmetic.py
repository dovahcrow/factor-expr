import numpy as np
import pandas as pd
import asyncio

from ... import Factor, replay


FILENAME = "../assets/test.pq"


def test_add():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(+ :price_ask_l1_high :price_bid_l1_open)")],
            pbar=False,
            index_col="time",
        )
    )

    assert np.isclose(
        df.price_ask_l1_high + df.price_bid_l1_open,
        result.iloc[:, 0],
    ).all()


def test_sub():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(- :price_ask_l1_open :price_bid_l1_open)")],
            pbar=False,
            index_col="time",
        )
    )

    assert np.isclose(
        df.price_ask_l1_open - df.price_bid_l1_open,
        result.iloc[:, 0],
    ).all()


def test_mul():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME], [Factor("(* :price_ask_l1_open :price_bid_l1_low)")], pbar=False
        )
    )

    assert np.isclose(
        df.price_ask_l1_open * df.price_bid_l1_low,
        result.iloc[:, 0],
    ).all()


def test_div():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay(
            [FILENAME],
            [Factor("(/ :price_ask_l1_close :price_bid_l1_high)")],
            pbar=False,
            index_col="time",
        )
    )

    assert np.isclose(
        df.price_ask_l1_close / df.price_bid_l1_high,
        result.iloc[:, 0],
    ).all()


def test_power():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay([FILENAME], [Factor("(^ 3 :price_ask_l1_open)")], pbar=False)
    )

    assert np.isclose(
        df.price_ask_l1_open ** 3,
        result.iloc[:, 0],
    ).all()


def test_signed_power():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay([FILENAME], [Factor("(SPow 2 :price_ask_l1_low)")], pbar=False)
    )

    assert np.isclose(
        np.sign(df.price_ask_l1_low) * df.price_ask_l1_low.abs() ** 2,
        result.iloc[:, 0],
    ).all()


def test_log():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay([FILENAME], [Factor("(LogAbs :volume_ask_l1_high)")], pbar=False)
    )

    assert np.isclose(
        np.log(np.abs(df.volume_ask_l1_high)),
        result.iloc[:, 0],
    ).all()


def test_sign():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay([FILENAME], [Factor("(Sign :price_ask_l1_close)")], pbar=False)
    )

    assert np.isclose(
        np.sign(df.price_ask_l1_close),
        result.iloc[:, 0],
    ).all()


def test_abs():
    df = pd.read_parquet(FILENAME)

    result = asyncio.run(
        replay([FILENAME], [Factor("(Abs :price_ask_l1_open)")], pbar=False)
    )

    assert np.isclose(
        np.abs(df.price_ask_l1_open),
        result.iloc[:, 0],
    ).all()
