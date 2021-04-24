bootstrap-python:
    cp README.md python/README.md
    cd python && poetry install
    rm python/README.md

build-extension:
    cd native && cargo build --release
    ls native/target/release
    cd python && poetry run python ../scripts/python-helper.py copy-extension

build-wheel: build-extension
  cp README.md python/README.md
  cd python && poetry build
  cd python && poetry run python ../scripts/python-helper.py rename-wheel
  rm python/README.md

test +ARGS="": build-extension
  cd python && poetry run pytest factor_expr/tests {{ARGS}}
