pub mod match_eng;

use std::sync::Arc;

use crossbeam_channel::unbounded;

use limit_order_book::{order::OrderType, LOBResponse};
use match_eng::*;

use port_server::message::{Response, UserRequest};
use tokio::net::UdpSocket;

use dotenv::dotenv;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct MarketDepth{
    pub price:f64,
    pub depth:u32,
    pub side:OrderType
}

/// Matching Engine gets the messages via the UDP Connection and
/// this messages are sent to the Engine via channel and this method
/// broadcasts all the responses back to the entites participating in
/// the system.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let engine_address = std::env::var("ENGINE_ADDRESS").expect("ENGINE_ADDRESS env error");
    let multicast_address =
        std::env::var("MULTICAST_ADDRESS").expect("MULTICAST_ADDRESS env error");

    println!("Matching Engine server running at {:?}...", engine_address);

    let (sender, receiver) = unbounded();
    let sock = Arc::new(UdpSocket::bind(engine_address).await?);
    let engine = Engine::new(1, 9, Arc::new(sender));
    let book = unsafe {
        engine
            .book
            .load(std::sync::atomic::Ordering::Relaxed)
            .as_ref()
            .unwrap()
    };
    let engine_sock_clone = sock.clone();
    let level_sock = sock.clone();

    tokio::spawn(async move {
        loop {
            let mut buf = [0; 1024];
            let (len, _) = engine_sock_clone
                .recv_from(&mut buf)
                .await
                .expect("Failed to read from buffer...");
            let req: UserRequest = bincode::deserialize(&buf[..len]).unwrap();
            // println!("Got message:{:?}", req.message);
            let tuple = (req.session, req.message);
            engine.send(tuple);
        }
    });

    for res in receiver {
        let level_sock_clone = level_sock.clone();
        let order_res=res.1.clone();
        tokio::spawn(async move{
            if let Ok(order_res) = order_res {
                let market_depth = match order_res {
                    LOBResponse::Inserted(_, price, _, side) => MarketDepth {
                        price,
                        depth: book.view_level(price, side).unwrap_or_else(|| 0),
                        side,
                    },
                    LOBResponse::Executed(_, _, price, _, _, side) => MarketDepth {
                        price,
                        depth: book.view_level(price, side).unwrap_or_else(|| 0),
                        side,
                    },
                    LOBResponse::Cancelled(ref order) => MarketDepth {
                        price: order.price,
                        depth: book
                            .view_level(order.price, order.order_type)
                            .unwrap_or_else(|| 0),
                        side: order.order_type,
                    },
                };
    
                
    
                let mut buf = bincode::serialize(&market_depth).unwrap();
                level_sock_clone
                    .send_to(&mut buf, "127.0.0.1:4545")
                    .await
                    .expect("Failed to send to the market data");
            }
        });

        let response = Response {
            session: res.0,
            res: res.1,
        };

        println!("Response:{:?}", response);

        let mut serialized = bincode::serialize(&response).unwrap();
        sock.send_to(&mut serialized, multicast_address.clone())
            .await
            .expect("Failed to send response...");
    }

    Ok(())
}
