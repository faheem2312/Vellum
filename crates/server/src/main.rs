//! Vellum matching engine server.
//!
//! Phase 1 architecture (deliberately simple, correctness-first):
//! - One shared `OrderBook` behind a `Mutex`.
//! - One OS thread per client connection.
//! - Each incoming message is applied to the book while holding the lock,
//!   and any resulting Ack/Trade messages are written back to the
//!   connection that sent the order.
//!
//! This is NOT how a real low-latency matching engine is built — a mutex
//! held across matching logic is a latency bottleneck. That's exactly what
//! Phase 2 will fix: replacing this with a single-writer thread + channel.
//!
//! Run with: cargo run --release --bin server -- 127.0.0.1:7878

use orderbook::OrderBook;
use protocol::Message;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn handle_client(stream: TcpStream, book: Arc<Mutex<OrderBook>>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!("[server] client connected: {peer}");

    let mut reader = stream.try_clone().expect("clone stream for reading");
    let mut writer = stream;

    loop {
        let msg = match protocol::read_message(&mut reader) {
            Ok(m) => m,
            Err(e) => {
                println!("[server] client {peer} disconnected ({e})");
                break;
            }
        };

        match msg {
            Message::NewOrder { order_id, side, price, qty } => {
                let trades = {
                    let mut book = book.lock().expect("book lock poisoned");
                    book.add_order(order_id, side, price, qty)
                };

                if (Message::Ack { order_id }).write_to(&mut writer).is_err() {
                    break;
                }
                for t in &trades {
                    let trade_msg = Message::Trade {
                        resting_order_id: t.resting_order_id,
                        incoming_order_id: t.incoming_order_id,
                        price: t.price,
                        qty: t.qty,
                    };
                    if trade_msg.write_to(&mut writer).is_err() {
                        break;
                    }
                }
                println!(
                    "[server] order {order_id} ({side:?} {qty}@{price}) -> {} trade(s)",
                    trades.len()
                );
            }
            Message::Cancel { order_id } => {
                let found = {
                    let mut book = book.lock().expect("book lock poisoned");
                    book.cancel_order(order_id)
                };
                let reply = if found {
                    Message::Ack { order_id }
                } else {
                    Message::Reject { order_id, reason: 1 }
                };
                if reply.write_to(&mut writer).is_err() {
                    break;
                }
            }
            other => {
                println!("[server] unexpected message from client: {other:?}");
            }
        }
    }
}

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let listener = TcpListener::bind(&addr).expect("failed to bind");
    println!("[server] listening on {addr}");

    let book = Arc::new(Mutex::new(OrderBook::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let book = Arc::clone(&book);
                thread::spawn(move || handle_client(stream, book));
            }
            Err(e) => eprintln!("[server] connection failed: {e}"),
        }
    }
}