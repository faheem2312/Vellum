//! Benchmarks for the matching engine's hot path: `OrderBook::add_order`.
//!
//! Run with: cargo bench
//! HTML report generated at target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use orderbook::OrderBook;
use protocol::Side;

/// Benchmark: adding an order that doesn't match anything (pure insert cost).
fn bench_resting_order(c: &mut Criterion) {
    c.bench_function("add_order_no_match", |b| {
        b.iter_batched(
            OrderBook::new,
            |mut book| {
                black_box(book.add_order(1, Side::Buy, 100, 10));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: adding an order that fully matches one resting order.
fn bench_full_match(c: &mut Criterion) {
    c.bench_function("add_order_full_match", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new();
                book.add_order(1, Side::Sell, 100, 10);
                book
            },
            |mut book| {
                black_box(book.add_order(2, Side::Buy, 100, 10));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: adding an order into a book that already has many resting
/// orders at different price levels — tests how matching cost scales as
/// the book grows.
fn bench_deep_book(c: &mut Criterion) {
    c.bench_function("add_order_deep_book_1000_levels", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new();
                for i in 0..1000u64 {
                    book.add_order(i, Side::Sell, 1000 + i, 10);
                }
                book
            },
            |mut book| {
                // This buy price is below every resting ask, so it just
                // rests — measures insert cost into an already-deep book.
                black_box(book.add_order(9999, Side::Buy, 500, 10));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Isolate just the cost of a BTreeMap lookup in a book with 1000 levels,
/// with NO matching or mutation involved — pure read cost.
fn bench_deep_book_lookup_only(c: &mut Criterion) {
    let mut book = OrderBook::new();
    for i in 0..1000u64 {
        book.add_order(i, Side::Sell, 1000 + i, 10);
    }
    c.bench_function("best_ask_lookup_1000_levels", |b| {
        b.iter(|| {
            black_box(book.best_ask());
        });
    });
}

/// Same deep book, but insert at price 50000 (past every existing level)
/// instead of 500 (before every level) — tests whether position within
/// the BTreeMap matters.
fn bench_deep_book_insert_at_end(c: &mut Criterion) {
    c.bench_function("add_order_deep_book_insert_at_end", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new();
                for i in 0..1000u64 {
                    book.add_order(i, Side::Sell, 1000 + i, 10);
                }
                book
            },
            |mut book| {
                black_box(book.add_order(9999, Side::Buy, 50_000, 10));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_deep_book_ref(c: &mut Criterion) {
    c.bench_function("add_order_deep_book_ref_no_rebuild", |b| {
        b.iter_batched_ref(
            || {
                let mut book = OrderBook::new();
                for i in 0..1000u64 {
                    book.add_order(i, Side::Sell, 1000 + i, 10);
                }
                book
            },
            |book| {
                black_box(book.add_order(9999, Side::Buy, 500, 10));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_resting_order,
    bench_full_match,
    bench_deep_book,
    bench_deep_book_lookup_only,
    bench_deep_book_insert_at_end,
    bench_deep_book_ref
);
criterion_main!(benches);