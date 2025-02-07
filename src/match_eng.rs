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
use dashmap::DashMap;
use limit_order_book::LimitOrderBook;
use limit_order_book::{order::OrderStatus, LOBError, LOBResponse};
use port_server::message::Message;
use threadpool::ThreadPool;
use uuid::Uuid;

type Response = Result<LOBResponse, LOBError>;

type Book = Arc<AtomicPtr<LimitOrderBook>>;

/// This struct holds the Atomic reference to the Book and
/// handles all the messages that the ports send via the UDP connection
/// and those messages are processed by the [`LimitOrderBook`] and [`Engine`] returns
/// those responses back to all the entities via the MultiCast connection.
pub struct Engine {
    pub book: Book,                    // Atomic reference of the limit order book.
    sender: Sender<(String, Message)>, // Channel for sending the messages to the engine.
    pub request_map: Arc<DashMap<String, String>>, // Hashmap for mapping the order id with the user
}

impl Engine {
    /// This method creates new instance of the [`Engine`].
    /// To handle the messages and execute the [`LimitOrderBook::best_bid`] and [`LimitOrderBook::best_ask`],
    /// it needs [`ThreadPool`] and some threads to process the messages and execute the orders.
    pub fn new(
        id: i32,
        num_threads: usize,
        response_channel: Arc<Sender<(String, Response)>>,
    ) -> Engine {
        let lob = Box::into_raw(Box::new(LimitOrderBook::new(id)));

        let book = Arc::new(AtomicPtr::new(lob));
        let (sender, receiver) = unbounded::<(String, Message)>();
        let pool: ThreadPool = ThreadPool::new(num_threads);
        let request_map = Arc::new(DashMap::new());

        let thread_book = book.clone();

        let channel = response_channel.clone();
        let request_map_clone = request_map.clone();

        thread::spawn(move || {
            if let Some(lob) = unsafe { thread_book.load(Acquire).as_ref() } {
                for msg in receiver {
                    let response_channel_clone = response_channel.clone();
                    let thread_request_map_clone = request_map_clone.clone();
                    pool.execute(move || match msg.1 {
                        Message::Insert {
                            price,
                            shares,
                            order_type,
                        } => {
                            let order_id = Uuid::new_v4(); // Unique request identifier
                            let res =
                                lob.insert_order(order_id.to_string(), price, shares, order_type);
                            thread_request_map_clone.insert(order_id.to_string(), msg.0.clone());
                            let _ = response_channel_clone
                                .send((msg.0.clone(), res))
                                .map_err(|e| println!("error:{e}"));
                        }
                        Message::Cancel { id } => {
                            let res = lob.remove_order(id.clone(), 0, &mut OrderStatus::CANCEL);
                            if res.is_ok() {
                                thread_request_map_clone.remove(&id.clone());
                            }
                            response_channel_clone.send((msg.0, res)).unwrap();
                        }
                    });
                }
            }
        });

        let thread_book = book.clone();
        let executor_request_map = request_map.clone();
        thread::spawn(move || loop {
            Executor::run(
                thread_book.clone(),
                executor_request_map.clone(),
                channel.clone(),
            );
        });

        Engine {
            book,
            sender,
            request_map,
        }
    }

    pub fn send(&self, tuple: (String, Message)) {
        self.sender.send(tuple).unwrap();
    }
}

/// This struct has the soul job of executing orders.
pub struct Executor {}

impl Executor {
    pub fn run(
        book: Book,
        request_map: Arc<DashMap<String, String>>,
        response_channel: Arc<Sender<(String, Response)>>,
    ) {
        if let Some(lob) = unsafe { book.load(Acquire).as_ref() } {
            let best_ask = &lob.best_ask;
            let best_bid = &lob.best_bid;

            if best_ask.load(Acquire).is_null() || best_bid.load(Acquire).is_null() {
                thread::park_timeout(Duration::from_nanos(100));
            } else {
                let bid_order = unsafe { &*best_bid.load(Acquire) };
                let ask_order = unsafe { &*best_ask.load(Acquire) };

                let ask_user = request_map.get(&ask_order.id).unwrap().value().to_owned();
                let bid_user = request_map.get(&bid_order.id).unwrap().value().to_owned();

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
                            ask_order.id.clone(),
                            ask_shares,
                            &mut OrderStatus::FULL,
                        );

                        if res.is_ok() {
                            request_map.remove(&ask_user);
                        }
                        response_channel.send((ask_user, res)).unwrap();
                    } else {
                        // Notify that Order Executed Partially.
                        let res = Ok(LOBResponse::Executed(
                            ask_order.id.clone(),
                            ask_order.price,
                            shares_to_execute,
                            OrderStatus::PARTIAL,
                        ));
                        response_channel.send((ask_user, res)).unwrap();
                    }

                    if bid_order.shares.load(Acquire) == 0 {
                        let res = lob.remove_order(
                            bid_order.id.clone(),
                            bid_shares,
                            &mut OrderStatus::FULL,
                        );

                        if res.is_ok() {
                            request_map.remove(&bid_user);
                        }
                        response_channel.send((bid_user, res)).unwrap();
                    } else {
                        // Notify that Order Executed Partially.
                        let res = Ok(LOBResponse::Executed(
                            bid_order.id.clone(),
                            bid_order.price,
                            shares_to_execute,
                            OrderStatus::PARTIAL,
                        ));
                        response_channel.send((bid_user, res)).unwrap();
                    }
                } else {
                    thread::park_timeout(Duration::from_nanos(1000));
                }
            }
        }
    }
}
