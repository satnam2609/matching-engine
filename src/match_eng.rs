extern crate limit_order_book;
extern crate port_server;

use std::sync::Arc;
use std::thread;
use std::{
    sync::atomic::{
        AtomicPtr,
        Ordering::{Acquire, Release},
    },
    time::Duration,
};

use crossbeam_channel::{unbounded, Sender};
use limit_order_book::LimitOrderBook;
use limit_order_book::{order::OrderStatus, LOBError, LOBResponse};
use port_server::message::Message;
use threadpool::ThreadPool;

type Response = Result<LOBResponse, LOBError>;

type Book = Arc<AtomicPtr<LimitOrderBook>>;


/// This struct holds the Atomic reference to the Book and 
/// handles all the messages that the ports send via the UDP connection 
/// and those messages are processed by the [`LimitOrderBook`] and [`Engine`] returns 
/// those responses back to all the entities via the MultiCast connection.
pub struct Engine {
    pub book: Book, // Atomic reference of the limit order book.
    sender: Sender<Message>, // Channel for sending the messages to the engine.
}

impl Engine {
    /// This method creates new instance of the [`Engine`].
    /// To handle the messages and execute the [`LimitOrderBook::best_bid`] and [`LimitOrderBook::best_ask`],
    /// it needs [`ThreadPool`] and some threads to process the messages and execute the orders.
    pub fn new(id: i32, num_threads: usize, response_channel: Arc<Sender<Response>>) -> Engine {
        let lob = Box::into_raw(Box::new(LimitOrderBook::new(id)));

        let book = Arc::new(AtomicPtr::new(lob));
        let (sender, receiver) = unbounded();
        let pool = ThreadPool::new(num_threads);

        let thread_book = book.clone();

        let channel = response_channel.clone();

        thread::spawn(move || {
            if let Some(lob) = unsafe { thread_book.load(Acquire).as_ref() } {
                for msg in receiver {
                    let response_channel_clone = response_channel.clone();
                    pool.execute(move || match msg {
                        Message::Insert {
                            price,
                            shares,
                            order_type,
                        } => {
                            let res = lob.insert_order(price, shares, order_type);
                            let _ = response_channel_clone
                                .send(res)
                                .map_err(|e| println!("error:{e}"));
                        }
                        Message::Cancel { seq } => {
                            let res = lob.remove_order(seq, 0, &mut OrderStatus::CANCEL);
                            response_channel_clone.send(res).unwrap();
                        }
                    });
                }
            }
        });

        let thread_book = book.clone();
        thread::spawn(move || loop {
            Executor::run(thread_book.clone(), channel.clone());
        });

        Engine { book, sender }
    }

    pub fn send(&self, msg: Message) {
        self.sender.send(msg).unwrap();
    }
}

/// This struct has the soul job of executing orders.
pub struct Executor {}

impl Executor {
    pub fn run(book: Book, response_channel: Arc<Sender<Response>>) {
        if let Some(lob) = unsafe { book.load(Acquire).as_ref() } {
            let best_ask = &lob.best_ask;
            let best_bid = &lob.best_bid;

            if best_ask.load(Acquire).is_null() || best_bid.load(Acquire).is_null() {
                thread::park_timeout(Duration::from_nanos(100));
            } else {
                let bid_order = unsafe { &*best_bid.load(Acquire) };
                let ask_order = unsafe { &*best_ask.load(Acquire) };

                if ask_order.price <= bid_order.price {
                    // found match
                    let ask_shares = ask_order.shares.load(Acquire);
                    let bid_shares = bid_order.shares.load(Acquire);

                    let shares_to_execute = ask_shares.min(bid_shares);

                    ask_order.shares.fetch_sub(shares_to_execute, Release);
                    ask_order
                        .order_status
                        .store(&mut OrderStatus::PARTIAL, Release);
                    bid_order.shares.fetch_sub(shares_to_execute, Release);
                    bid_order
                        .order_status
                        .store(&mut OrderStatus::PARTIAL, Release);

                    if ask_order.shares.load(Acquire) == 0 {
                        let res = lob.remove_order(
                            ask_order.seqeunce,
                            ask_shares,
                            &mut OrderStatus::FULL,
                        );
                        response_channel.send(res).unwrap();
                    } else {
                        // Notify that Order Executed Partially.
                        let res = Ok(LOBResponse::Executed(
                            ask_order.seqeunce,
                            shares_to_execute,
                            OrderStatus::PARTIAL,
                        ));
                        response_channel.send(res).unwrap();
                    }

                    if bid_order.shares.load(Acquire) == 0 {
                        let res = lob.remove_order(
                            bid_order.seqeunce,
                            bid_shares,
                            &mut OrderStatus::FULL,
                        );
                        response_channel.send(res).unwrap();
                    } else {
                        // Notify that Order Executed Partially.
                        let res = Ok(LOBResponse::Executed(
                            bid_order.seqeunce,
                            shares_to_execute,
                            OrderStatus::PARTIAL,
                        ));
                        response_channel.send(res).unwrap();
                    }
                } else {
                    thread::park_timeout(Duration::from_nanos(1000));
                }
            }
        }
    }
}
