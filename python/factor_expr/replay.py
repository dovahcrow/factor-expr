from asyncio import get_event_loop, as_completed
from concurrent.futures import ThreadPoolExecutor
from sys import stderr
from typing import Iterable, List, Literal, Optional, Set, Tuple, Union, AsyncGenerator
from functools import partial

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from ._lib import Factor
from ._lib import replay as _native_replay

try:
    from IPython import get_ipython

    if get_ipython() is not None:
        from tqdm.notebook import tqdm
    else:
        from tqdm import tqdm
except Exception:
    from tqdm import tqdm


async def replay(
    files: Iterable[str],
    factors: List[Factor],
    *,
    predicate: Optional[Factor] = None,
    batch_size: int = 40960,
    n_data_jobs: int = 1,
    n_factor_jobs: int = 1,
    pbar: bool = True,
    trim: bool = False,
    index_col: Optional[str] = None,
    verbose: bool = False,
    output: Literal["pandas", "pyarrow", "raw"] = "pandas",
) -> Union[pd.DataFrame, pa.Table]:
    """
    Replay a list of factors on a bunch of data.

    Parameters
    ----------
    files: Iterable[str]
        Paths to the datasets. Currently only parquet format is supported.
    factors: List[Factor]
        A list of Factors to replay on the given set of files.
    predicate: Optional[Factor] = None
        Use a predicate to pre-filter the replay result. Any value larger than 0 is treated as True.
    batch_size: int = 40960
        How many rows to replay at one time. Default is 40960 rows.
    n_data_jobs: int = 1
        How many datasets to run in parallel. Note that the factor level parallelism is controlled by n_factor_jobs.
    n_factor_jobs: int = 1
        How many factors to run in parallel for **each** dataset.
        e.g. if `n_data_jobs=3` and `n_factor_jobs=5`, you will have 3 * 5 threads running concurrently.
    pbar: bool = True
        Whether to show the progress bar using tqdm.
    trim: bool = False
        Whether to trim the warm up period off from the result.
    index_col: Optional[str] = None
        Set the index column.
    verbose: bool = False
        If True, failed factors will be printed out in stderr.
    output: Literal["pandas" | "pyarrow" | "raw"] = "pandas"
        The return format, can be pandas DataFrame ("pandas") or pyarrow Table ("pyarrow") or un-concatenated pyarrow Tables ("raw").

    Examples
    --------
    ```python
        replay(
            files = [
                "2020-11-02T12:00:07.860000~2020-11-03T17:09:01.pq",
                "2020-11-03T17:09:39.072000~2020-11-04T15:23:36.pq"
            ],
            factors = [
                Factor("(> (TSStd 60 (TSLogReturn 120 (+ :price_bid_l1_close :price_bid_l1_close))) 0.0005)"),
                Factor("(Abs (TSLogReturn 120 (+ :price_bid_l1_close :price_ask_l1_close)))"),
            ]
        )
    ```
    """
    factor_tables: List[pa.Table] = []
    files = list(files)

    with tqdm(total=len(files), leave=False, disable=not pbar) as progress:
        async for _, fvals in replay_iter(
            files,
            factors,
            predicate=predicate,
            batch_size=batch_size,
            n_data_jobs=n_data_jobs,
            n_factor_jobs=n_factor_jobs,
            trim=trim,
            index_col=index_col,
            verbose=verbose,
        ):
            factor_tables.append(fvals)
            progress.update(1)

    if output == "pyarrow":
        factor_table = pa.concat_tables(factor_tables)
    elif output == "pandas":
        factor_table = pa.concat_tables(factor_tables)
        factor_table = factor_table.to_pandas(self_destruct=True)
    elif output == "raw":
        factor_table = factor_tables
    else:
        raise ValueError(f"Unsupported output type {output}")

    return factor_table


async def replay_iter(
    files: Iterable[str],
    factors: List[Factor],
    *,
    predicate: Optional[Factor] = None,
    batch_size: int = 40960,
    n_data_jobs: int = 1,
    n_factor_jobs: int = 1,
    trim: bool = False,
    index_col: Optional[str] = None,
    unordered: bool = False,
    verbose: bool = False,
) -> AsyncGenerator[Tuple[str, pa.Table], None]:
    LOOP = get_event_loop()

    with ThreadPoolExecutor(max_workers=n_data_jobs) as pool:
        tasks = []

        for dname in files:
            fut = LOOP.run_in_executor(
                pool,
                partial(
                    named,
                    dname,
                    _replay_single,
                    dname,
                    [f.clone() for f in factors],
                    predicate=predicate.clone() if predicate is not None else None,
                    batch_size=batch_size,
                    trim=trim,
                    index_col=index_col,
                    verbose=verbose,
                    n_factor_jobs=n_factor_jobs,
                ),
            )

            tasks.append(fut)

        if unordered:
            tasks = as_completed(tasks)

        for task in tasks:
            dname, (fvals, failures) = await task

            if verbose:
                print(len(failures), "failed in total", file=stderr)

            yield dname, fvals


def _replay_single(
    dname: str,
    factors: List[Factor],
    *,
    predicate: Optional[Factor] = None,
    batch_size: int = 40960,
    trim: bool = False,
    index_col: Optional[str] = None,
    n_factor_jobs: int = 1,
    verbose: bool = False,
) -> Tuple[pa.Table, Set[str]]:
    if predicate is not None:
        # put the predicate as the last
        replay_result = _native_replay(
            dname, [*factors, predicate], batch_size=batch_size, njobs=n_factor_jobs
        )
    else:
        replay_result = _native_replay(
            dname, factors, batch_size=batch_size, njobs=n_factor_jobs
        )

    table_datas, table_names = [], []

    if index_col is not None:
        index = pq.read_table(dname, columns=[index_col]).column(index_col)
        table_datas.append(index)
        table_names.append(index_col)

    predicate_values = None
    for i, (data_ptr, schema_ptr) in replay_result["succeeded"].items():
        arr = pa.Array._import_from_c(data_ptr, schema_ptr)

        if predicate is not None and i == len(factors):  # is the predicate col
            predicate_values = arr
        else:
            table_datas.append(arr)
            table_names.append(str(factors[i]))

    # Fill in the failed columns
    N = replay_result["nrows"]
    nanarr = pa.array(np.empty(N, "f8"), mask=np.ones(N, "b1"))

    for i, reason in replay_result["failed"].items():
        if predicate is not None and i == len(factors):
            raise ValueError("predicate failed to compute: {}", reason)
        else:
            table_datas.append(nanarr)
            table_names.append(str(factors[i]))

        if verbose:
            print(f"{factors[i]} failed: {reason}", file=stderr)

    tb = pa.Table.from_arrays(
        table_datas,
        names=table_names,
    )

    if trim:
        if index_col is not None:
            # the first column is the index
            data_starts = 1
        else:
            data_starts = 0

        ready_offset = np.max(
            [Factor(col).ready_offset() for col in tb.column_names[data_starts:]]
        )

        tb = tb.slice(ready_offset)

        # trim predicate as well
        if predicate_values is not None:
            predicate_values = predicate_values.slice(ready_offset)

    if predicate is not None:
        assert (
            predicate_values is not None
        ), "predicate_values is none, this is not possible"

        # filter the table using the predicate
        tb = pc.filter(tb, pc.greater(predicate_values, 0.0))

    if index_col is not None:
        # sort the columns based on the order passed in
        tb = tb.select([index_col] + [str(f) for f in factors])

        # set the metadata for the index col, so that when `.to_pandas` is called,
        # the index col automatically becomes the index.
        header = tb.slice(0).to_pandas()
        header = header.set_index(index_col)
        _, _, metadata = pa.pandas_compat.dataframe_to_types(header, True)
        tb = tb.replace_schema_metadata(metadata)
    else:
        # sort the columns based on the order passed in
        tb = tb.select([str(f) for f in factors])

    return (
        tb,
        {str(factors[k]) for k in replay_result["failed"].keys()},
    )


def named(name, func, *args, **kwargs):
    return name, func(*args, **kwargs)