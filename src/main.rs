pub mod match_eng;

use std::sync::Arc;

use crossbeam_channel::unbounded;

use match_eng::*;

use port_server::message::Message;
use tokio::net::UdpSocket;

pub const ENGINE_ADDRESS: &'static str = "127.0.0.1:8080";
pub const MULTICAST_ADDRESS: &'static str = "224.0.0.1:5000";


/// Matching Engine gets the messages via the UDP Connection and 
/// this messages are sent to the Engine via channel and this method 
/// broadcasts all the responses back to the entites participating in
/// the system.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (sender, receiver) = unbounded();
    let sock = Arc::new(UdpSocket::bind(ENGINE_ADDRESS).await?);
    let engine = Engine::new(1, 9, Arc::new(sender));

    println!("Matching Engine server running at {:?}...", ENGINE_ADDRESS);

    let sock_clone = sock.clone();
    tokio::spawn(async move {
        loop {
            let mut buf = [0; 1024];
            let (len, _) = sock_clone
                .recv_from(&mut buf)
                .await
                .expect("Failed to read from buffer...");
            let message: Message = bincode::deserialize(&buf[..len]).unwrap();
            println!("Got message:{:?}", message);
            engine.send(message);
        }
    });

    for res in receiver {
        let mut serialized = bincode::serialize(&res).unwrap();

        sock.send_to(&mut serialized, MULTICAST_ADDRESS)
            .await
            .expect("Failed to send response...");
    }

    Ok(())
}
