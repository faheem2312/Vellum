//! Vellum matching engine server — Phase 2b, with server-assigned order IDs.
//!
//! Architecture: a single engine thread owns the OrderBook exclusively and
//! busy-polls a lock-free ring buffer (crossbeam's ArrayQueue) for work.
//!
//! Order IDs: clients propose a `client_order_id` for their own tracking,
//! but this server assigns the authoritative, globally-unique
//! `server_order_id` used inside the OrderBook — this is required for
//! correctness once more than one client can be connected at a time.
//!
//! Run with: cargo run --release --bin server -- 127.0.0.1:7878

use crossbeam_queue::ArrayQueue;
use orderbook::{OrderBook, Trade};
use protocol::{Message, Side};
use std::hint;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

const QUEUE_CAPACITY: usize = 4096;

enum Command {
    NewOrder { client_order_id: u64, side: Side, price: u64, qty: u32 },
    Cancel { server_order_id: u64 },
}

enum EngineReply {
    OrderResult { client_order_id: u64, server_order_id: u64, trades: Vec<Trade> },
    CancelResult { server_order_id: u64, found: bool },
}

struct Request {
    cmd: Command,
    reply_slot: Arc<ArrayQueue<EngineReply>>,
}

/// The engine thread's main loop. Busy-polls the queue; the ONLY place
/// OrderBook is ever touched. Owns the global order-ID counter too, since
/// ID assignment must happen in the same serialized place as book access.
fn run_engine(queue: Arc<ArrayQueue<Request>>, running: Arc<AtomicBool>) {
    let mut book = OrderBook::new();
    let mut next_server_id: u64 = 1;

    loop {
        match queue.pop() {
            Some(req) => {
                let reply = match req.cmd {
                    Command::NewOrder { client_order_id, side, price, qty } => {
                        let server_order_id = next_server_id;
                        next_server_id += 1;
                        let trades = book.add_order(server_order_id, side, price, qty);
                        EngineReply::OrderResult { client_order_id, server_order_id, trades }
                    }
                    Command::Cancel { server_order_id } => {
                        let found = book.cancel_order(server_order_id);
                        EngineReply::CancelResult { server_order_id, found }
                    }
                };
                let _ = req.reply_slot.push(reply);
            }
            None => {
                if !running.load(Ordering::Relaxed) && queue.is_empty() {
                    break;
                }
                hint::spin_loop();
            }
        }
    }
}

fn submit(queue: &ArrayQueue<Request>, cmd: Command) -> EngineReply {
    let reply_slot: Arc<ArrayQueue<EngineReply>> = Arc::new(ArrayQueue::new(1));
    let mut req = Request { cmd, reply_slot: Arc::clone(&reply_slot) };
    while let Err(returned) = queue.push(req) {
        req = returned;
        hint::spin_loop();
    }
    loop {
        if let Some(reply) = reply_slot.pop() {
            return reply;
        }
        hint::spin_loop();
    }
}

fn handle_client(stream: TcpStream, queue: Arc<ArrayQueue<Request>>) {
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
            Message::NewOrder { client_order_id, side, price, qty } => {
                let reply = submit(&queue, Command::NewOrder { client_order_id, side, price, qty });
                let EngineReply::OrderResult { client_order_id, server_order_id, trades } = reply
                else {
                    unreachable!("engine always replies with OrderResult to NewOrder")
                };

                if (Message::Ack { client_order_id, server_order_id }).write_to(&mut writer).is_err() {
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
                    "[server] order client_id={client_order_id} -> server_id={server_order_id} ({side:?} {qty}@{price}) -> {} trade(s)",
                    trades.len()
                );
            }
            Message::Cancel { server_order_id } => {
                let reply = submit(&queue, Command::Cancel { server_order_id });
                let EngineReply::CancelResult { server_order_id, found } = reply else {
                    unreachable!("engine always replies with CancelResult to Cancel")
                };

                let reply_msg = if found {
                    Message::Ack { client_order_id: server_order_id, server_order_id }
                } else {
                    Message::Reject { server_order_id, reason: 1 }
                };
                if reply_msg.write_to(&mut writer).is_err() {
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

    let queue: Arc<ArrayQueue<Request>> = Arc::new(ArrayQueue::new(QUEUE_CAPACITY));
    let running = Arc::new(AtomicBool::new(true));

    let engine_queue = Arc::clone(&queue);
    let engine_running = Arc::clone(&running);
    thread::spawn(move || run_engine(engine_queue, engine_running));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let queue = Arc::clone(&queue);
                thread::spawn(move || handle_client(stream, queue));
            }
            Err(e) => eprintln!("[server] connection failed: {e}"),
        }
    }
}