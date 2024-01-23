from pyarrow.cffi import ffi
from asyncio import get_event_loop, as_completed
from concurrent.futures import ThreadPoolExecutor
from sys import stderr
from typing import Iterable, List, Literal, Optional, Set, Tuple, Union, AsyncGenerator, cast
from functools import partial
from tqdm.auto import tqdm

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from ._lib import Factor
from ._lib import replay as _native_replay, replay_file as _native_replay_file


async def replay(
    files: Iterable[str | pa.Table],
    factors: List[Factor],
    *,
    reset: bool = True,
    batch_size: int = 40960,
    n_data_jobs: int = 1,
    n_factor_jobs: int = 1,
    pbar: bool = True,
    verbose: bool = False,
    output: Literal["pyarrow", "raw"] = "pyarrow",
) -> pa.Table:
    """
    Replay a list of factors on a bunch of data.

    Parameters
    ----------
    files: Iterable[str | pa.Table]
        Paths to the datasets. Or already read pyarrow Tables.
    factors: List[Factor]
        A list of Factors to replay.
    reset: bool = True
        Whether to reset the factors. Factors carries memory about the data they already replayed. If you are calling
        replay multiple times and the factors should not starting from fresh, set this to False.
    batch_size: int = 40960
        How many rows to replay at one time. Default is 40960 rows.
    n_data_jobs: int = 1
        How many datasets to run in parallel. Note that the factor level parallelism is controlled by n_factor_jobs.
    n_factor_jobs: int = 1
        How many factors to run in parallel for **each** dataset.
        e.g. if `n_data_jobs=3` and `n_factor_jobs=5`, you will have 3 * 5 threads running concurrently.
    pbar: bool = True
        Whether to show the progress bar using tqdm.
    verbose: bool = False
        If True, failed factors will be printed out in stderr.
    output: Literal["pyarrow" | "raw"] = "pyarrow"
        The return format, can be pyarrow Table ("pyarrow") or un-concatenated pyarrow Tables ("raw").

    Examples
    --------
    ```python
        replay(
            files = [
                "2020-11-02T12:00:07.860000~2020-11-03T17:09:01.pq",
                "2020-11-03T17:09:39.072000~2020-11-04T15:23:36.pq"
            ],
            factors = [
                Factor("(> (Std 60 (LogReturn 120 (+ :price_bid_l1_close :price_bid_l1_close))) 0.0005)"),
                Factor("(Abs (LogReturn 120 (+ :price_bid_l1_close :price_ask_l1_close)))"),
            ]
        )
    ```
    ```python
        tbs = [
            pq.read_parquet("2020-11-02T12:00:07.860000~2020-11-03T17:09:01.pq"),
            pq.read_parquet("2020-11-03T17:09:39.072000~2020-11-04T15:23:36.pq"),
        ]
        replay(
            files = tbs,
            factors = [
                Factor("(> (Std 60 (LogReturn 120 (+ :price_bid_l1_close :price_bid_l1_close))) 0.0005)"),
                Factor("(Abs (LogReturn 120 (+ :price_bid_l1_close :price_ask_l1_close)))"),
            ]
        )
    ```
    """
    factor_tables: List[pa.Table] = []
    files = list(files)

    if reset:
        for factor in factors:
            factor.reset()

    with tqdm(total=len(files), leave=False, disable=not pbar) as progress:
        async for _, fvals in replay_iter(
            files,
            factors,
            batch_size=batch_size,
            n_data_jobs=n_data_jobs,
            n_factor_jobs=n_factor_jobs,
            verbose=verbose,
        ):
            factor_tables.append(fvals)
            progress.update(1)

    if output == "pyarrow":
        factor_table = pa.concat_tables(factor_tables)
    elif output == "raw":
        factor_table = factor_tables
    else:
        raise ValueError(f"Unsupported output type {output}")

    return factor_table


async def replay_iter(
    files: Iterable[str | pa.Table],
    factors: List[Factor],
    *,
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
                    batch_size=batch_size,
                    verbose=verbose,
                    n_jobs=n_factor_jobs,
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


def table_to_pointers(tb: pa.Table):
    batches = tb.to_batches()

    schema = []
    arrays = []
    keepalive = []
    for i, batch in enumerate(batches):
        for array, name in zip(batch.columns, batch.column_names):
            c_array = ffi.new("struct ArrowArray*")
            ptr_array = int(ffi.cast("uintptr_t", c_array))
            if i == 0:
                c_schema = ffi.new("struct ArrowSchema*")
                ptr_schema = int(ffi.cast("uintptr_t", c_schema))

                array._export_to_c(ptr_array, ptr_schema)

                name = ffi.new("char[]", name.encode("utf8"))
                c_schema.name = name

                schema.append(ptr_schema)
                keepalive.append(name)
                keepalive.append(c_schema)
            else:
                array._export_to_c(ptr_array)

            arrays.append(ptr_array)
            keepalive.append(c_array)

    return schema, arrays, keepalive


def _replay_single(
    file: str | pa.Table,
    factors: List[Factor],
    *,
    batch_size: int = 40960,
    n_jobs: int = 1,
    verbose: bool = False,
) -> Tuple[pa.Table, Set[str]]:
    if isinstance(file, str):
        replay_result = _native_replay_file(file, factors, njobs=n_jobs)
    else:
        schema = file.schema
        ffi_schema, ffi_arrays, keepalive = table_to_pointers(file)

        replay_result = _native_replay(ffi_schema, ffi_arrays, factors, njobs=n_jobs)

    table_datas, table_names = [], []

    for i, (data_ptr, schema_ptr) in replay_result["succeeded"].items():
        arr = pa.Array._import_from_c(data_ptr, schema_ptr)

        table_datas.append(arr)
        table_names.append(str(factors[i]))

    # Fill in the failed columns
    if isinstance(file, pa.Table):
        N = len(file)
    elif table_datas:
        N = len(table_datas[0])
    else:
        tb = pq.read_metadata(file)
        N = tb.num_rows

    nanarr = pa.array(np.empty(N, "f8"), mask=np.ones(N, "b1"))

    for i, reason in replay_result["failed"].items():
        table_datas.append(nanarr)
        table_names.append(str(factors[i]))

        if verbose:
            print(f"{factors[i]} failed: {reason}", file=stderr)

    tb = pa.Table.from_arrays(table_datas, names=table_names)

    # sort the columns based on the order passed in
    tb = tb.select([str(f) for f in factors])

    return (
        tb,
        {str(factors[k]) for k in replay_result["failed"].keys()},
    )


def named(name, func, *args, **kwargs):
    return name, func(*args, **kwargs)
