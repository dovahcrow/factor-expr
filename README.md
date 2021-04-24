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

A tabular format with at least a `time` column is required for the dataset. 
This means except for the `time` column, you can have other columns with any name you want in the dataset.
For example, here is an OHLC candle dataset with 2 rows:

```python
    df = pd.DataFrame({
        "time": [DateTime(2021,4,23), DateTime(2021,4,24)], 
        "open": [3.1, 5.8], 
        "high": [8.8, 7.7], 
        "low": [1.1, 2.1], 
        "close": [4.4, 3.4]
    })
```

You can use the following code to store the DataFrame into a Parquet file:
```python
import pyarrow as pa
import pyarrow.parquet as pq

tb = pa.Table.from_pandas(df)
tb = tb.cast(
    pa.schema(
        [
            ("time", pa.timestamp("ms")),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
        ]
    )
)
pq.write_table(tb, f"data.pq", version="2.0")
```

Several things need to be noticed:
1. The time column is required and the data type must be `pa.timestamp("ms")`.
2. Other columns must have the `pa.float64()` data type.
3. The version for the Parquet file must be "2.0".
   
In the future 1 and 3 might be relaxed.

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

result = replay(
    ["data.pq"],
    [Factor("(TSLogReturn 30 :close)")]
)
```

The first parameter of `replay` is a list of dataset files and the second parameter is a list of Factors. This gives you the ability to compute multiple factors on multiple datasets. Don't worry about the performance! `Factor Expr` will automatically parallelize over the Factors as well as the datasets.

The returned result is a pandas DataFrame with factors as the column names and `time` as the index. 
In case of multiple datasets are passed in, the results will be concatenated with the exact order of the datasets. This is useful if you have a scattered dataset. E.g. one file for each year.

For example, the code above will give you a DataFrame looks similar to this:

| __index__  | (TSLogReturn 30 :close) |
| ---------- | ----------------------- |
| 2021-04-24 | 0.23                    |
| ...        | ...                     |

Checkout the docstring of `replay` for more information!

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

### Factors Failed to Compute

`Factor Expr` guarantees that there will not be any `inf`, `-inf` or `NaN` appear in the result, except for the warm-up period. However, sometimes a factor can fail due to numerical issues. For example, `(Pow 3 (Pow 3 (Pow 3 :volume)))` might overflow and become `inf` and `1 / inf` will become `NaN`. `Factor Expr` will detect these situations and mark these factors as failed. The failed factors will still be returned in the replay result, but the values in that column will be all `NaN`. You can easily remove these failed factor results by using `pd.DataFrame.dropna(axis=0, how="all")`.
