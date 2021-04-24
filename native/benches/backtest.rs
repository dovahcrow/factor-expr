#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]

use criterion::{black_box, Criterion};
use criterion_macro::criterion;
use ndarray::Array1;
use tower0::backtest::{vectorized_backtest, SignalsView, TickersView};

mod profiler;
mod reader;

fn config() -> Criterion {
    Criterion::default().with_profiler(profiler::FlamegraphProfiler::new(100))
}

#[criterion(config())]
pub fn criterion_benchmark(c: &mut Criterion) {
    let tickers = reader::read_tickers();
    let signals = reader::read_signals();

    let tickers = TickersView {
        time: tickers.time.view(),
        asks: tickers.asks.view(),
        bids: tickers.bids.view(),
    };

    let signals = SignalsView {
        time: signals.time.view(),
        direction: signals.direction.view(),
        takeprofit: signals.takeprofit.view(),
        stoploss: signals.stoploss.view(),
        expiry: signals.expiry.view(),
    };

    let mut positions = Array1::default((tickers.time.len(),));
    c.bench_function("backtest", |b| {
        b.iter(|| {
            vectorized_backtest(
                black_box(positions.view_mut()),
                tickers,
                signals,
                0.0005,
                30,
            )
        })
    });
}
