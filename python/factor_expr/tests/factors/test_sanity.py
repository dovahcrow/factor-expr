import numpy as np
import pandas as pd
import asyncio

from ... import Factor, replay


FILENAME = "../assets/test.pq"


def test_index():
    f = Factor("(Mean 10 :price_ask_l1_open)")
    asyncio.run(
        replay(
            [FILENAME],
            [f],
            pbar=False,
        )
    )
