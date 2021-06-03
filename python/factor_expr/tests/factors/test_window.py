import numpy as np
import pandas as pd
import asyncio

from ... import Factor, replay


FILENAME = "../assets/test.pq"


def test_sum():
    df = pd.read_parquet(FILENAME)
    f = Factor("(TSSum 10 :price_ask_l1_close)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_close.rolling(10).sum().values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_mean():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSMean 10 :price_ask_l1_open)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_open.rolling(10).mean().values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_correlation():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSCorr 10 :price_ask_l1_high :price_bid_l1_low)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    def func(sub):
        subdf = df.loc[sub.index]
        return np.corrcoef(subdf.price_ask_l1_high, subdf.price_bid_l1_low)[0, 1]

    assert np.isclose(
        np.nan_to_num(
            df.price_ask_l1_high.rolling(10)
            .apply(func, raw=False)
            .values[f.ready_offset() :]
        ),
        result.values.ravel(),
    ).all()


def test_min():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSMin 10 :price_ask_l1_close)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_close.rolling(10).min().values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_max():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSMax 10 :price_ask_l1_open)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_open.rolling(10).max().values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_argmax():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSArgMax 10 :price_ask_l1_close)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    def func(sub):
        subdf = df.loc[sub.index]
        return np.argmax(subdf.price_ask_l1_close)

    assert np.isclose(
        df.price_ask_l1_close.rolling(10).apply(func).values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_argmin():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSArgMin 10 :price_ask_l1_low)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    def func(sub):
        subdf = df.loc[sub.index]
        return np.argmin(subdf.price_ask_l1_low)

    assert np.isclose(
        df.price_ask_l1_low.rolling(10).apply(func).values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_std():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSStd 10 :price_ask_l1_high)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_high.rolling(10).std().values[f.ready_offset() :],
        result.values.ravel(),
        atol=1e-5,
    ).all()


def test_skew():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSSkew 10 :price_bid_l1_high)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        np.nan_to_num(
            df.price_bid_l1_high.rolling(10).skew().values[f.ready_offset() :]
        ),
        result.values.ravel(),
    ).all()


def test_delay():
    df = pd.read_parquet(FILENAME)

    f = Factor("(Delay 10 :price_ask_l1_close)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_ask_l1_close.shift(10).values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_rank():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSRank 10 :price_ask_l1_open)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    def func(sub):
        subdf = df.loc[sub.index]
        return (
            subdf.price_ask_l1_open.values[-1] > subdf.price_ask_l1_open.values
        ).sum()

    assert np.isclose(
        df.price_ask_l1_open.rolling(10).apply(func).values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_logreturn():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSLogReturn 100 (Abs :price_ask_l1_high))")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        np.log(
            np.abs(
                df.price_ask_l1_high.values[f.ready_offset() :]
                / df.price_ask_l1_high.values[: -f.ready_offset()]
            )
        ),
        result.values.ravel(),
    ).all()


def test_quantile():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSQuantile 30 0.3 :price_bid_l1_open)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_bid_l1_open.rolling(30)
        .quantile(0.3, "lower")
        .values[f.ready_offset() :],
        result.values.ravel(),
    ).all()


def test_median():
    df = pd.read_parquet(FILENAME)

    f = Factor("(TSQuantile 37 0.5 :price_bid_l1_open)")
    result = asyncio.run(
        replay([FILENAME], [f], trim=True, pbar=False, index_col="time")
    )

    assert np.isclose(
        df.price_bid_l1_open.rolling(37).median().values[f.ready_offset() :],
        result.values.ravel(),
    ).all()
