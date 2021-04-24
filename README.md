# Factor Expr [![status][ci_badge]][ci_page]
[ci_badge]: https://github.com/dovahcrow/factor-expr/workflows/ci/badge.svg
[ci_page]: https://github.com/dovahcrow/factor-expr/actions

Extreme fast factor expression & computation library for Python.

On a server with an E7-4830 CPU (16 cores, 2000MHz),
replaying 48 factors over a dataset with 24,513,435 rows x 683 columns (12GB) takes 150s.

## Usage

There are three steps to use this library.

1. Prepare your dataset into a file. Currently, only the [Parquet](https://parquet.apache.org/) format is supported.
2. Define your factors using [S-Expression](https://en.wikipedia.org/wiki/S-expression).
3. Run `replay` to compute the factors on the dataset.

### 1. Prepare the dataset

A tabular format with at least a `time` column is required for the dataset. 
This means except for the `time` column, you can have other columns with any name you want in the dataset.
For example, here is an OHLC candle dataset with 1 row:

```python
    df = pd.DataFrame({
        "time": [datetime.now()], 
        "open": [3.1], 
        "high": [8.8], 
        "low": [1.1], 
        "close": [4.4]
    })
```

You can use the following code to store the DataFrame into a Parquet file:
```python
df.to_parquet("data.pq")
```

### 2. Define your factors

Using `Factor Expr`, you can easily define your factors in S-Expression. 
For example, the 30 days log return on the column `close` can be expressed as:

```python
from factor_expr import Factor

Factor("(TSLogReturn 30 :close)")
```

if each row in your dataset represents a 1-day OHLC candle. 

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

| __index__                  | (TSLogReturn 30 :close) |
| -------------------------- | ----------------------- |
| 2021-04-24 03:28:19.763974 | 0.23                    |
| ...                        | ...                     |

Checkout the docstring of `replay` for more information!

## Installation

```bash
pip install factor-expr
```

## Supported Functions
Notations: 
* `<const>` means a constant, e.g. `3`.
* `<expr>` means either a constant or an S-Expression or a column name, e.g. `3` or `(+ :close 3)` or `:open`.

Here's the full list of supported functions. If you didn't find one you need, consider creating an issue or PR!

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
In this case, `replay` will write `NaN` into the result by default,
so that the length of the output will be the same as the input dataset. You can use the `trim`
parameter to control this behaviour.
