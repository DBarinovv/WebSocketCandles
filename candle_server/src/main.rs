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

    async fn connect_websocket() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ServerError>
    {
        timeout(
            Duration::from_secs(5),
            connect_async("wss://fstream.binance.com/stream"),
        )
        .await
        .map_err(|_| ServerError::WebSocketTimeout)?
        .map_err(|_| ServerError::WebSocketConnect)
        .map(|(ws, _)| ws)
    }

    async fn send_subscription(
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        subscription: &BinanceSubscription,
    ) -> Result<(), ServerError> {
        let subscribe_message = Message::text(serde_json::to_string(&subscription)?);
        write
            .send(subscribe_message)
            .await
            .map_err(|_| ServerError::WebSocketWrite)
    }

    async fn subscribe_to_binance(
        state: Arc<ServerState>,
        req: &Request,
    ) -> Result<(), ServerError> {
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

    async fn handle_socket(socket: TcpStream) -> Result<Request, ServerError> {
        info!("Handling new WebSocket connection...");

        let websocket = match accept_async(socket).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("Error accepting WebSocket connection: {:?}", e);
                return Err(ServerError::WebSocketAccept);
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
                        }
                        Err(e) => {
                            error!("Error parsing request: {:?}", e);
                            return Err(ServerError::Serde(e));
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
                    return Err(ServerError::InvalidMessage(e.to_string()));
                }
            }
        }

        error!("Connection closed without valid request");
        Err(ServerError::InvalidMessage(
            "Connection closed without valid request".into(),
        ))
    }

    async fn close_connection(&self, key: String) -> Result<(), ServerError> {
        let mut state_lock = self.state.connections.write().await;

        if let Some(_stream) = state_lock.remove(&key) {
            info!("Connection with key '{}' successfully closed.", key);
            Ok(())
        } else {
            error!(
                "Attempted to close connection with non-existing key '{}'.",
                key
            );
            Err(ServerError::KeyNotFound)
        }
    }

    pub async fn serve(&self, addr: &SocketAddr) -> Result<(), ServerError> {
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

    async fn process_binance_stream(
        state: Arc<ServerState>,
        req: &Request,
    ) -> Result<(), ServerError> {
        let rpn_tokens = to_rpn(&parse(&req.stream)?[..])?;
        let mut candle_stack: Vec<Arc<Mutex<Candle>>> = Vec::new();

        loop {
            for token in rpn_tokens {
                match token {
                    Token::Operand(symbol) => {
                        if let Some(ref mut connection) =
                            state.connections.read().await.get(&symbol)
                        {
                            while let Some(message) = connection.next().await {
                                match message {
                                    Ok(data) => {
                                        let parsed_data: BinanceMessage =
                                            serde_json::from_str(&data.to_string())?;
                                        let kline = parsed_data.data.k;

                                        let candle = Candle::new(
                                            kline.t,
                                            kline.o.parse()?,
                                            kline.c.parse()?,
                                            kline.h.parse()?,
                                            kline.l.parse()?,
                                        );
                                        candle_stack.push(candle);
                                    }
                                    Err(e) => return Err(ServerError::WebsocketError(e)),
                                }
                            }
                        }
                    }
                    Token::Operator(op) => {
                        let rhs = candle_stack.pop().unwrap();
                        let lhs = candle_stack.pop().unwrap();
                        let result = match op {
                            Operator::Plus => lhs.lock().await.add(*rhs.lock().await),
                            Operator::Minus => lhs.lock().await.sub(*rhs.lock().await),
                            Operator::Multiply => lhs.lock().await.mul(*rhs.lock().await),
                            Operator::Divide => lhs.lock().await.div(*rhs.lock().await),
                            Operator::NotOperator => return Err(ServerError::ParsingStream),
                        }?;
                        candle_stack.push(Candle::new(
                            result.t, result.o, result.c, result.h, result.l,
                        ));
                    }
                    _ => return Err(ServerError::ParsingStream),
                }
            }

            let result_candle = candle_stack.pop().unwrap();
            let result_candle = result_candle.lock().await;

            let result_message = ResultMessage {
                stream: req.stream.clone(),
                data: ResultData {
                    t: result_candle.t,
                    o: result_candle.o,
                    c: result_candle.c,
                    h: result_candle.h,
                    l: result_candle.l,
                },
            };

            let result_message = serde_json::to_string(&result_message)?;
            println!("{}", result_message);
            break;
        }

        Ok(())
    }
}

fn main() {}
