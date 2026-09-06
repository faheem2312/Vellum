//! Simple interactive client for testing Vellum's matching engine.
//!
//! Usage after connecting:
//!   buy <price> <qty>      e.g. "buy 100 10"
//!   sell <price> <qty>     e.g. "sell 105 5"
//!   cancel <order_id>
//!   quit
//!
//! Run with: cargo run --release --bin client -- 127.0.0.1:7878

use protocol::{Message, Side};
use std::io::{self, BufRead, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

static NEXT_ORDER_ID: AtomicU64 = AtomicU64::new(1);

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let stream = TcpStream::connect(&addr).expect("failed to connect");
    println!("[client] connected to {addr}");

    let read_stream = stream.try_clone().expect("clone stream");
    let write_stream = Arc::new(std::sync::Mutex::new(stream));

    // Background thread: print every message the server sends us.
    thread::spawn(move || {
        let mut reader = read_stream;
        loop {
            match protocol::read_message(&mut reader) {
                Ok(msg) => println!("[server -> me] {msg:?}"),
                Err(e) => {
                    println!("[client] connection closed ({e})");
                    break;
                }
            }
        }
    });

    println!("Commands: buy <price> <qty> | sell <price> <qty> | cancel <order_id> | quit");
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap_or_default();
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let msg = match parts[0] {
            "buy" | "sell" if parts.len() == 3 => {
                let price: u64 = match parts[1].parse() {
                    Ok(p) => p,
                    Err(_) => { println!("bad price"); continue; }
                };
                let qty: u32 = match parts[2].parse() {
                    Ok(q) => q,
                    Err(_) => { println!("bad qty"); continue; }
                };
                let side = if parts[0] == "buy" { Side::Buy } else { Side::Sell };
                let order_id = NEXT_ORDER_ID.fetch_add(1, Ordering::Relaxed);
                println!("[client] submitting order_id={order_id}");
                Message::NewOrder { order_id, side, price, qty }
            }
            "cancel" if parts.len() == 2 => {
                let order_id: u64 = match parts[1].parse() {
                    Ok(id) => id,
                    Err(_) => { println!("bad order_id"); continue; }
                };
                Message::Cancel { order_id }
            }
            "quit" => break,
            _ => { println!("unrecognized command"); continue; }
        };

        let mut w = write_stream.lock().unwrap();
        if msg.write_to(&mut *w).is_err() {
            println!("[client] failed to send, connection may be closed");
            break;
        }
        w.flush().ok();
    }
}