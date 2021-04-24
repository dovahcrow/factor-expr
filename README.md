# Factor Expr [![status][ci_badge]][ci_page]
[ci_badge]: https://github.com/wooya/factor-expr/workflows/ci/badge.svg
[ci_page]: https://github.com/wooya/factor-expr/actions

Extreme fast factor expression & computation library for Python.

## Usage

There are three steps to use this library.

1. Store your data in a file. Currently only parquet format is supported.
2. Define your factors using [S-Expression](https://en.wikipedia.org/wiki/S-expression).
3. Run `replay` to get the values of the factors on the dataset.

## Installation

```bash
pip install factor-expr
```
