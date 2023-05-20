use std::sync::Arc;
use tokio::sync::Mutex;

use std::collections::HashMap;
use futures::{StreamExt, SinkExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream, connect_async, MaybeTlsStream};
use url::Url;

mod utils;
use utils::*;

pub type DataStore = Arc<Mutex<HashMap<u64, HashMap<String, Arc<Mutex<Candle>>>>>>;

async fn process_binance_stream(
    coin: String,
    interval: String,
    data_store: DataStore,
) -> Result<(), MyError> {
    let mut web_socket = connect_to_binance(&coin, &interval).await?;

    loop {
        match web_socket.recv().await {
            Ok(Message::Text(msg)) => {
                let candle: BinanceResponse = serde_json::from_str(&msg)?;
                let candle = Candle::new(
                    candle.k.t,
                    candle.k.o.parse()?,
                    candle.k.c.parse()?,
                    candle.k.h.parse()?,
                    candle.k.l.parse()?,
                );

                let mut data_store = data_store.lock().await;

                data_store
                    .entry(candle.t)
                    .or_insert_with(HashMap::new)
                    .insert(coin.clone(), candle);

                println!("Candle for {}@{}: {:?}", coin, interval, candle);
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Binary(_)) => {}
            Ok(Message::Close(_)) => {
                println!("Server requested to close the connection, exiting...");
                break;
            }
            Err(e) => {
                println!("Error receiving message: {}", e);
                break;
            }
            _ => break,
        }
    }

    Ok(())
}


async fn handle_socket(socket: TcpStream) -> Result<Request, MyError> {
    let websocket = accept_async(socket).await?;

    let (_, mut read) = websocket.split();

    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let request: Request = serde_json::from_str(&text)?;
                return Ok(request);
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => (),
            Err(e) => println!("Error reading message: {}", e),
        }
    }

    Err(MyError::InvalidMessage)
}

async fn subscribe_to_binance(req: &Request) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, MyError> {
    let url = Url::parse("wss://fstream.binance.com/stream")?;
    let (mut websocket, _) = connect_async(url).await?;

    let message = Message::Text(serde_json::to_string(&BinanceSubscription {
        method: "SUBSCRIBE".to_string(),
        params: parse_streams(&req.stream),
        id: req.id,
    })?);

    websocket.send(message).await?;

    Ok(websocket)
}

async fn server() -> Result<(), MyError> {
    let try_socket = TcpListener::bind("127.0.0.1").await?;
    
    loop {
        let (socket, _) = try_socket.accept().await?;
        
        tokio::spawn(async move {
            match handle_socket(socket).await {
                Ok(request) => {
                    match subscribe_to_binance(&request).await {
                        Ok(mut binance_ws) => {
                            if let Err(e) = process_request(&request, &mut binance_ws).await {
                                println!("Error processing request: {}", e);
                            }
                        },
                        Err(e) => println!("Error connecting to Binance: {}", e),
                    }
                },
                Err(e) => println!("Error handling connection: {}", e),
            }
        });
    }
}


fn main() {}
