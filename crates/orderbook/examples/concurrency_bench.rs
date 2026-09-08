//! Standalone concurrency comparison: Arc<Mutex<OrderBook>> (Phase 1 design)
//! vs. single-writer thread + mpsc channel (Phase 2 design).
//!
//! Run with: cargo run --release --example concurrency_bench -p orderbook

use orderbook::OrderBook;
use protocol::Side;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_queue::ArrayQueue;
use std::hint;
use std::sync::atomic::{AtomicBool, Ordering};

const NUM_THREADS: usize = 3;
const ORDERS_PER_THREAD: usize = 20_000;

/// Phase 1 design: every thread locks the same Mutex to touch the book.
fn bench_mutex_design() -> Duration {
    let book = Arc::new(Mutex::new(OrderBook::new()));
    let start = Instant::now();

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let book = Arc::clone(&book);
            thread::spawn(move || {
                for i in 0..ORDERS_PER_THREAD {
                    let order_id = (t * ORDERS_PER_THREAD + i) as u64;
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    let price = 1000 + (i % 50) as u64;
                    let mut book = book.lock().unwrap();
                    book.add_order(order_id, side, price, 10);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    start.elapsed()
}

enum Command {
    NewOrder {
        order_id: u64,
        side: Side,
        price: u64,
        qty: u32,
        reply: mpsc::Sender<()>,
    },
}

/// Phase 2 design: one engine thread owns the book; other threads send
/// commands and wait for a reply — same shape as the real server code.
fn bench_channel_design() -> Duration {
    let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();

    let engine = thread::spawn(move || {
        let mut book = OrderBook::new();
        for cmd in cmd_rx {
            let Command::NewOrder { order_id, side, price, qty, reply } = cmd;
            book.add_order(order_id, side, price, qty);
            let _ = reply.send(());
        }
    });

    let start = Instant::now();

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let cmd_tx = cmd_tx.clone();
            thread::spawn(move || {
                let (reply_tx, reply_rx) = mpsc::channel(); // created ONCE per thread
                for i in 0..ORDERS_PER_THREAD {
                    let order_id = (t * ORDERS_PER_THREAD + i) as u64;
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    let price = 1000 + (i % 50) as u64;
                    cmd_tx
                        .send(Command::NewOrder { order_id, side, price, qty: 10, reply: reply_tx.clone() })
                        .unwrap();
                    reply_rx.recv().unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    drop(cmd_tx); // closes the channel so the engine thread's `for cmd in rx` loop ends
    engine.join().unwrap();
    start.elapsed()
}

const QUEUE_CAPACITY: usize = 4096;

struct RingCommand {
    order_id: u64,
    side: Side,
    price: u64,
    qty: u32,
    reply_slot: Arc<ArrayQueue<()>>,
}

/// Phase 2b design: lock-free ring buffer + busy-polling engine thread.
/// No OS-level blocking anywhere — the engine spins checking for work,
/// and producer threads spin waiting for their reply.
fn bench_ring_buffer_design() -> Duration {
    let queue: Arc<ArrayQueue<RingCommand>> = Arc::new(ArrayQueue::new(QUEUE_CAPACITY));
    let running = Arc::new(AtomicBool::new(true));

    let engine_queue = Arc::clone(&queue);
    let engine_running = Arc::clone(&running);
    let engine = thread::spawn(move || {
        let mut book = OrderBook::new();
        loop {
            match engine_queue.pop() {
                Some(cmd) => {
                    book.add_order(cmd.order_id, cmd.side, cmd.price, cmd.qty);
                    let _ = cmd.reply_slot.push(());
                }
                None => {
                    if !engine_running.load(Ordering::Relaxed) && engine_queue.is_empty() {
                        break;
                    }
                    hint::spin_loop();
                }
            }
        }
    });

    let start = Instant::now();

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let reply_slot: Arc<ArrayQueue<()>> = Arc::new(ArrayQueue::new(1));
                for i in 0..ORDERS_PER_THREAD {
                    let order_id = (t * ORDERS_PER_THREAD + i) as u64;
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    let price = 1000 + (i % 50) as u64;

                    let mut cmd = RingCommand {
                        order_id,
                        side,
                        price,
                        qty: 10,
                        reply_slot: Arc::clone(&reply_slot),
                    };
                    // Ring buffer is full: spin until there's room.
                    while let Err(returned) = queue.push(cmd) {
                        cmd = returned;
                        hint::spin_loop();
                    }
                    // Spin until the engine thread signals completion.
                    while reply_slot.pop().is_none() {
                        hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    running.store(false, Ordering::Relaxed);
    engine.join().unwrap();
    start.elapsed()
}

fn main() {
    let total_orders = NUM_THREADS * ORDERS_PER_THREAD;
    println!("Vellum concurrency benchmark: {NUM_THREADS} threads x {ORDERS_PER_THREAD} orders each ({total_orders} total)\n");

    let mutex_time = bench_mutex_design();
    println!(
        "Mutex design:   {:>10?} total, {:>8?} per order",
        mutex_time,
        mutex_time / total_orders as u32
    );

    let channel_time = bench_channel_design();
    println!(
        "Channel design: {:>10?} total, {:>8?} per order",
        channel_time,
        channel_time / total_orders as u32
    );

    let ring_time = bench_ring_buffer_design();
    println!(
        "Ring buffer:    {:>10?} total, {:>8?} per order",
        ring_time,
        ring_time / total_orders as u32
    );
}