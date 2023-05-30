use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use log::{error, info};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::{Message, WebSocket};
use tokio_tungstenite::{accept_async, connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

mod utils;
use utils::*;

struct ServerState {
    connections: RwLock<HashMap<String, SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
}

struct Server {
    state: Arc<ServerState>,
}

impl Server {
    pub fn new() -> Server {
        Server {
            state: Arc::new(ServerState {
                connections: RwLock::default(),
            }),
        }
    }

    pub async fn serve(&self, addr: &SocketAddr) -> Result<(), MyError> {
        let try_socket = TcpListener::bind(addr).await?;
        loop {
            let (socket, _) = try_socket.accept().await?;
            let state = self.state.clone();
            tokio::spawn(async move {
                let state2 = state.clone();
                match Self::handle_socket(socket).await {
                    Ok(request) => match Self::subscribe_to_binance(state, &request).await {
                        Ok(_) => {
                            tokio::spawn(async move {
                                match Self::process_binance_stream(state2, &request).await {
                                    Err(e) => println!("Error processing Binance stream: {}", e),
                                    _ => {}
                                }
                            });
                        }
                        Err(e) => println!("Error connecting to Binance: {}", e),
                    },
                    Err(e) => println!("Error handling connection: {}", e),
                }
            });
        }
    }

    async fn process_binance_stream(state: Arc<ServerState>, req: &Request) -> Result<(), MyError> {
        Ok(())
    }

    async fn connect_websocket() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, MyError> {
        timeout(
            Duration::from_secs(5),
            connect_async("wss://fstream.binance.com/stream"),
        )
        .await
        .map_err(|_| MyError::WebSocketTimeout)?
        .map_err(|_| MyError::WebSocketConnect)
        .map(|(ws, _)| ws)
    }

    async fn send_subscription(
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        subscription: &BinanceSubscription,
    ) -> Result<(), MyError> {
        let subscribe_message = Message::text(serde_json::to_string(&subscription)?);
        write
            .send(subscribe_message)
            .await
            .map_err(|_| MyError::WebSocketWrite)
    }

    async fn subscribe_to_binance(state: Arc<ServerState>, req: &Request) -> Result<(), MyError> {
        // if req.method != "SUBSCRIBE" ...

        info!("Subscribing to stream: {}", &req.stream);

        let mut state_lock = state.connections.write().await;
        if state_lock.contains_key(&req.stream) {
            info!("Stream {} is already subscribed", &req.stream);
            return Ok(());
        }

        let ws_socket = Self::connect_websocket().await?;
        let (mut write, read) = ws_socket.split();

        let subscription = BinanceSubscription {
            id: req.id,
            method: req.method.clone(),
            params: parse_streams(&req.stream),
        };

        Self::send_subscription(&mut write, &subscription).await?;
        state_lock.insert(req.stream.clone(), read);
        info!("Stream {} subscribed successfully", &req.stream);

        Ok(())
    }

    async fn handle_socket(socket: TcpStream) -> Result<Request, MyError> {
        info!("Handling new WebSocket connection...");
    
        let websocket = match accept_async(socket).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("Error accepting WebSocket connection: {:?}", e);
                return Err(MyError::WebSocketAccept);
            }
        };
    
        let (_, mut read) = websocket.split();
    
        while let Some(message_result) = read.next().await {
            match message_result {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str(&text) {
                        Ok(request) => {
                            info!("Received valid request: {:?}", request);
                            return Ok(request);
                        },
                        Err(e) => {
                            error!("Error parsing request: {:?}", e);
                            return Err(MyError::Serde(e));
                        }
                    };
                }
                Ok(Message::Close(_)) => {
                    info!("Received close message, ending connection");
                    break;
                }
                Ok(other) => {
                    info!("Received unsupported message type: {:?}", other);
                }
                Err(e) => {
                    error!("Error reading message: {:?}", e);
                    return Err(MyError::InvalidMessage(e.to_string()));
                },
            }
        }
    
        error!("Connection closed without valid request");
        Err(MyError::InvalidMessage("Connection closed without valid request".into()))
    }

    async fn close_connection(&self, key: String) -> Result<(), MyError> {
        let mut state_lock = self.state.connections.write().await;
    
        if let Some(_stream) = state_lock.remove(&key) {
            info!("Connection with key '{}' successfully closed.", key);
            Ok(())
        } else {
            error!("Attempted to close connection with non-existing key '{}'.", key);
            Err(MyError::KeyNotFound)
        }
    }
}

fn main() {}


// pub type DataStore = Arc<Mutex<HashMap<u64, HashMap<String, Arc<Mutex<Candle>>>>>>;

// struct RequestState {
//     remaining_operations: VecDeque<Operation>,
//     current_result: Option<Candle>,
// }

// async fn process_binance_stream(
//     coin: String,
//     interval: String,
//     data_store: DataStore,
// ) -> Result<(), MyError> {
//     let mut web_socket = connect_to_binance(&coin, &interval).await?;

//     loop {
//         match web_socket.recv().await {
//             Ok(Message::Text(msg)) => {
//                 let candle: BinanceResponse = serde_json::from_str(&msg)?;
//                 let candle = Candle::new(
//                     candle.k.t,
//                     candle.k.o.parse()?,
//                     candle.k.c.parse()?,
//                     candle.k.h.parse()?,
//                     candle.k.l.parse()?,
//                 );

//                 let mut data_store = data_store.lock().await;

//                 data_store
//                     .entry(candle.t)
//                     .or_insert_with(HashMap::new)
//                     .insert(coin.clone(), candle);

//                 println!("Candle for {}@{}: {:?}", coin, interval, candle);
//             }
//             Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Binary(_)) => {}
//             Ok(Message::Close(_)) => {
//                 println!("Server requested to close the connection, exiting...");
//                 break;
//             }
//             Err(e) => {
//                 println!("Error receiving message: {}", e);
//                 break;
//             }
//             _ => break,
//         }
//     }

//     Ok(())
// }
