//! Simple interactive client for testing Vellum's matching engine.
//!
//! Usage after connecting:
//!   buy <price> <qty>              e.g. "buy 100 10"
//!   sell <price> <qty>             e.g. "sell 105 5"
//!   cancel <server_order_id>       use the server_order_id from an Ack, not your own count
//!   quit
//!
//! Run with: cargo run --release --bin client -- 127.0.0.1:7878

use protocol::{Message, Side};
use std::io::{self, BufRead, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

static NEXT_CLIENT_ORDER_ID: AtomicU64 = AtomicU64::new(1);

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let stream = TcpStream::connect(&addr).expect("failed to connect");
    println!("[client] connected to {addr}");

    let read_stream = stream.try_clone().expect("clone stream");
    let write_stream = Arc::new(std::sync::Mutex::new(stream));

    thread::spawn(move || {
        let mut reader = read_stream;
        loop {
            match protocol::read_message(&mut reader) {
                Ok(Message::Ack { client_order_id, server_order_id }) => {
                    println!(
                        "[server -> me] Ack: my client_order_id={client_order_id} -> server_order_id={server_order_id} (use this for cancels)"
                    );
                }
                Ok(msg) => println!("[server -> me] {msg:?}"),
                Err(e) => {
                    println!("[client] connection closed ({e})");
                    break;
                }
            }
        }
    });

    println!("Commands: buy <price> <qty> | sell <price> <qty> | cancel <server_order_id> | quit");
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
                let client_order_id = NEXT_CLIENT_ORDER_ID.fetch_add(1, Ordering::Relaxed);
                println!("[client] submitting client_order_id={client_order_id}");
                Message::NewOrder { client_order_id, side, price, qty }
            }
            "cancel" if parts.len() == 2 => {
                let server_order_id: u64 = match parts[1].parse() {
                    Ok(id) => id,
                    Err(_) => { println!("bad server_order_id"); continue; }
                };
                Message::Cancel { server_order_id }
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