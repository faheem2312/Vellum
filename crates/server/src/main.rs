//! Vellum matching engine server — Phase 2.
//!
//! Architecture change from Phase 1: instead of an `Arc<Mutex<OrderBook>>`
//! shared across client threads, a single dedicated "engine thread" owns
//! the OrderBook exclusively. Client threads never touch it directly —
//! they send a `Command` over an mpsc channel and wait for an `EngineReply`
//! back over a per-request reply channel.
//!
//! Why: a Mutex held across matching logic is a latency bottleneck under
//! contention. With a single-writer design, there is nothing to contend
//! over — the engine thread processes commands one at a time, in order,
//! with zero locking.
//!
//! Run with: cargo run --release --bin server -- 127.0.0.1:7878

use orderbook::{OrderBook, Trade};
use protocol::{Message, Side};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::thread;

/// A request sent from a client-handling thread to the engine thread.
enum Command {
    NewOrder {
        order_id: u64,
        side: Side,
        price: u64,
        qty: u32,
        reply: Sender<EngineReply>,
    },
    Cancel {
        order_id: u64,
        reply: Sender<EngineReply>,
    },
}

/// The engine thread's response to a Command, sent back over a
/// one-shot-style reply channel unique to that request.
enum EngineReply {
    OrderResult { order_id: u64, trades: Vec<Trade> },
    CancelResult { order_id: u64, found: bool },
}

/// The engine thread's main loop: owns the OrderBook, processes commands
/// one at a time, forever. This is the ONLY place OrderBook is touched.
fn run_engine(rx: mpsc::Receiver<Command>) {
    let mut book = OrderBook::new();
    for cmd in rx {
        match cmd {
            Command::NewOrder { order_id, side, price, qty, reply } => {
                let trades = book.add_order(order_id, side, price, qty);
                let _ = reply.send(EngineReply::OrderResult { order_id, trades });
            }
            Command::Cancel { order_id, reply } => {
                let found = book.cancel_order(order_id);
                let _ = reply.send(EngineReply::CancelResult { order_id, found });
            }
        }
    }
}

fn handle_client(stream: TcpStream, cmd_tx: Sender<Command>) {
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
                let (reply_tx, reply_rx) = mpsc::channel();
                if cmd_tx
                    .send(Command::NewOrder { order_id, side, price, qty, reply: reply_tx })
                    .is_err()
                {
                    break; // engine thread gone, nothing more we can do
                }

                let Ok(EngineReply::OrderResult { order_id, trades }) = reply_rx.recv() else {
                    break;
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
                let (reply_tx, reply_rx) = mpsc::channel();
                if cmd_tx.send(Command::Cancel { order_id, reply: reply_tx }).is_err() {
                    break;
                }

                let Ok(EngineReply::CancelResult { order_id, found }) = reply_rx.recv() else {
                    break;
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

    // The engine thread owns the OrderBook. cmd_tx is cloned into every
    // client thread; mpsc::Sender is cheap to clone and thread-safe.
    let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();
    thread::spawn(move || run_engine(cmd_rx));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cmd_tx = cmd_tx.clone();
                thread::spawn(move || handle_client(stream, cmd_tx));
            }
            Err(e) => eprintln!("[server] connection failed: {e}"),
        }
    }
}