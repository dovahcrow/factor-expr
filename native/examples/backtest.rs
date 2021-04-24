mod reader;

pub(crate) use firestorm::profile_fn;
use ndarray::Array1;
use tower0::backtest::{vectorized_backtest, SignalsView, TickersView};

pub fn bench() {
    profile_fn!(bench);
    let tickers = reader::read_tickers("ticker.2020-01-01-2021-01-21.pq");
    let signals = reader::read_signals("tvec22.pq");

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

    vectorized_backtest(positions.view_mut(), tickers, signals, 0.0005, 30);
}

fn main() {
    bench();
    // // Clear samples taken during warmup.
    // firestorm::clear();
    // // Run the bench for real.
    // bench();
    // // Save the data. Make sure this is an empty
    // // directory so that no important files are overwritten.
    // firestorm::save("./firestorm/").unwrap();
}
