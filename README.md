# Factor Expr [![status][ci_badge]][ci_page] [![pypi][pypi_badge]][pypi_page]
[ci_badge]: https://github.com/dovahcrow/factor-expr/workflows/ci/badge.svg
[ci_page]: https://github.com/dovahcrow/factor-expr/actions
[pypi_badge]: https://img.shields.io/pypi/v/factor-expr?color=green&style=flat-square
[pypi_page]: https://pypi.org/project/factor-expr/

<center>
<table>
<tr>
<th>Factor Expression</th>
<th>+</th>
<th>Historical Data</th>
<th>=</th>
<th>Factor Values</th>
</tr>
<tr>
<td>(TSLogReturn 30 :close)</td>
<td>+</td>
<td>2019-12-27~2020-01-14.pq</td>
<td>=</td>
<td>[0.01, 0.035, ...]</td>
</tr>
</table>
</center>

----------

Extreme fast factor expression & computation library for quantitative trading in Python.

On a server with an E7-4830 CPU (16 cores, 2000MHz),
computing 48 factors over a dataset with 24,513,435 rows x 683 columns (12GB) takes 150s.

Join [\[Discussions\]](https://github.com/dovahcrow/factor-expr/discussions) for Q&A and feature proposal!

## Features

* Express factors in [S-Expression](https://en.wikipedia.org/wiki/S-expression).
* Compute factors in parallel over multiple factors and multiple datasets.
  
## Usage

There are three steps to use this library.

1. Prepare the datasets into files. Currently, only the [Parquet](https://parquet.apache.org/) format is supported.
2. Define factors using [S-Expression](https://en.wikipedia.org/wiki/S-expression).
3. Run `replay` to compute the factors on the dataset.

### 1. Prepare the dataset

A dataset is a tabular format with float64 columns and arbitrary column names.
For example, here is an OHLC candle dataset with 2 rows:

```python
df = pd.DataFrame({
    "open": [3.1, 5.8], 
    "high": [8.8, 7.7], 
    "low": [1.1, 2.1], 
    "close": [4.4, 3.4]
})
```

You can use the following code to store the DataFrame into a Parquet file:
```python
df.to_parquet("data.pq")
```

### 2. Define your factors

`Factor Expr` uses the S-Expression to describe a factor. 
For example, on a daily OHLC dataset, the 30 days log return on the column `close` is expressed as:

```python
from factor_expr import Factor

Factor("(TSLogReturn 30 :close)")
```

Note, in `Factor Expr`, column names are referred to by using `:column-name`.

### 3. Compute the factors on the prepared dataset

Following step 1 and 2, you can now compute the factors using the `replay` function:

```python
from factor_expr import Factor, replay

result = await replay(
    ["data.pq"],
    [Factor("(TSLogReturn 30 :close)")]
)
```

The first parameter of `replay` is a list of dataset files and the second parameter is a list of Factors. This gives you the ability to compute multiple factors on multiple datasets.
Don't worry about the performance! `Factor Expr` allows you parallelize the computation over the factors as well as the datasets by setting `n_factor_jobs` and `n_data_jobs` in the `replay` function.

The returned result is a pandas DataFrame with factors as the column names and `time` as the index. 
In case of multiple datasets are passed in, the results will be concatenated with the exact order of the datasets. This is useful if you have a scattered dataset. E.g. one file for each year.

For example, the code above will give you a DataFrame looks similar to this:

| index | (TSLogReturn 30 :close) |
| ----- | ----------------------- |
| 0     | 0.23                    |
| ...   | ...                     |

Check out the [docstring](#replay) of `replay` for more information!

## Installation

```bash
pip install factor-expr
```

## Supported Functions
Notations: 
* `<const>` means a constant, e.g. `3`.
* `<expr>` means either a constant or an S-Expression or a column name, e.g. `3` or `(+ :close 3)` or `:open`.

Here's the full list of supported functions. If you didn't find one you need, 
consider asking on [Discussions](https://github.com/dovahcrow/factor-expr/discussions) or creating a PR!

### Arithmetics
* Addition: `(+ <expr> <expr>)`
* Subtraction: `(- <expr> <expr>)`
* Multiplication: `(* <expr> <expr>)`
* Division: `(/ <expr> <expr>)`
* Power: `(^ <const> <expr>)` - compute `<expr> ^ <const>`
* Negation: `(Neg <expr>)`
* Signed Power: `(SPow <const> <expr>)` - compute `sign(<expr>) * abs(<expr>) ^ <const>`
* Natural Logarithm after Absolute: `(LogAbs <expr>)`
* Sign: `(Sign <expr>)`
* Abs: `(Abs <expr>)`
### Logics
* If: `(If <expr> <expr> <expr>)` - if the first `<expr>` is larger than 0, return the second `<expr>` otherwise return the third `<expr>`
* And: `(And <expr> <expr>)`
* Or: `(Or <expr> <expr>)`
* Less Than: `(< <expr> <expr>)`
* Less Than or Equal: `(<= <expr> <expr>)`
* Great Than: `(> <expr> <expr>)`
* Greate Than or Equal: `(>= <expr> <expr>)`
* Equal: `(== <expr> <expr>)`
* Not: `(! <expr>)`

### Window Functions

All the window functions take a window size as the first argument. The computation will be done on the look-back window with the size given in `<const>`.

* Sum of the window elements: `(TSSum <const> <expr>)`
* Mean of the window elements: `(TSMean <const> <expr>)`
* Min of the window elements: `(TSMin <const> <expr>)`
* Max of the window elements: `(TSMax <const> <expr>)`
* The index of the min of the window elements: `(TSArgMin <const> <expr>)`
* The index of the max of the window elements: `(TSArgMax <const> <expr>)`
* Stdev of the window elements: `(TSStd <const> <expr>)`
* Skew of the window elements: `(TSSkew <const> <expr>)`
* The rank (ascending) of the current element in the window: `(TSRank <const> <expr>)`
* The value `<const>` ticks back: `(Delay <const> <expr>)`
* The log return of the value `<const>` ticks back to current value: `(TSLogReturn <const> <expr>)`
* Rolling correlation between two series: `(TSCorrelation <const> <expr> <expr>)`

#### Warm-up Period for Window Functions

Factors containing window functions require a warm-up period. For example, for
`(TSSum 10 :close)`, it will not generate data until the 10th tick is replayed.
In this case, `replay` will write `NaN` into the result during the warm-up period, until the factor starts to produce data.
This ensures the length of the factor output will be as same as the length of the input dataset. You can use the `trim`
parameter to let replay trim off the warm-up period before it returns.

## Factors Failed to Compute

`Factor Expr` guarantees that there will not be any `inf`, `-inf` or `NaN` appear in the result, except for the warm-up period. However, sometimes a factor can fail due to numerical issues. For example, `(Pow 3 (Pow 3 (Pow 3 :volume)))` might overflow and become `inf`, and `1 / inf` will become `NaN`. `Factor Expr` will detect these situations and mark these factors as failed. The failed factors will still be returned in the replay result, but the values in that column will be all `NaN`. You can easily remove these failed factors from the result by using `pd.DataFrame.dropna(axis=1, how="all")`.

## I Want to Have a Time Index for the Result

The `replay` function optionally accepts a `index_col` parameter. 
If you want to set a column from the dataset as the index of the returned result, you can do the following:

```python
from factor_expr import Factor, replay

pd.DataFrame({
    "time": [datetime(2021,4,23), datetime(2021,4,24)], 
    "open": [3.1, 5.8], 
    "high": [8.8, 7.7], 
    "low": [1.1, 2.1], 
    "close": [4.4, 3.4],
}).to_parquet("data.pq")

result = await replay(
    ["data.pq"],
    [Factor("(TSLogReturn 30 :close)")],
    index_col="time",
)
```

Note, accessing the `time` column from factor expressions will cause an error. 
Factor expressions can only read `float64` columns.

## API

There are two components in `Factor Expr`, a `Factor` class and a `replay` function.

### Factor

The factor class takes an S-Expression to construct. It has the following signature:

```python
class Factor:
    def __init__(sexpr: str) -> None:
        """Construct a Factor using an S-Expression"""

    def ready_offset(self) -> int:
        """Returns the first index after the warm-up period. 
        For non-window functions, this will always return 0."""

    def __len__(self) -> int:
        """Returns how many subtrees contained in this factor tree.

        Example
        -------
        `(+ (/ :close :open) :high)` has 5 subtrees, namely:
        1. (+ (/ :close :open) :high)
        2. (/ :close :open)
        3. :close
        4. :open
        5. :high
        """

    def __getitem__(self, i:int) -> Factor:
        """Get the i-th subtree of the sequence from the pre-order traversal of the factor tree.

        Example
        -------
        `(+ (/ :close :open) :high)` is traversed as:
        0. (+ (/ :close :open) :high)
        1. (/ :close :open)
        2. :close
        3. :open
        4. :high

        Consequently, f[2] will give you `Factor(":close")`.
        """

    def depth(self) -> int:
        """How deep is this factor tree.

        Example
        -------
        `(+ (/ :close :open) :high)` has a depth of 2, namely:
        1. (+ (/ :close :open) :high)
        2. (/ :close :open)
        """

    def child_indices(self) -> List[int]:
        """The indices for the children of this factor tree.

        Example
        -------
        The child_indices result of `(+ (/ :close :open) :high)` is [1, 4]
        """
        
    def replace(self, i: int, other: Factor) -> Factor:
        """Replace the i-th node with another subtree.

        Example
        -------
        `Factor("+ (/ :close :open) :high").replace(4, Factor("(- :high :low)")) == Factor("+ (/ :close :open) (- :high :low)")`
        """

    def columns(self) -> List[str]:
        """Return all the columns that are used by this factor.

        Example
        -------
        `(+ (/ :close :open) :high)` uses [:close, :open, :high].
        """
    
    def clone(self) -> Factor:
        """Create a copy of itself."""
```

### replay

Replay has the following signature:

```python
async def replay(
    files: Iterable[str],
    factors: List[Factor],
    *,
    batch_size: int = 40960,
    n_data_jobs: int = 1,
    n_factor_jobs: int = 1,
    pbar: bool = True,
    trim: bool = False,
    index_col: Optional[str] = None,
    verbose: bool = False,
    output: Literal["pandas", "pyarrow"] = "pandas",
) -> Union[pd.DataFrame, pa.Table]:
    """
    Replay a list of factors on a bunch of data.

    Parameters
    ----------
    files: Iterable[str]
        Paths to the datasets. Currently, only parquet format is supported.
    factors: List[Factor]
        A list of Factors to replay on the given set of files.
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
    output: Literal["pandas" | "pyarrow"] = "pandas"
        The return format, can be pandas DataFrame ("pandas") or pyarrow Table ("pyarrow").
    """
```

