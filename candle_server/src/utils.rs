use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;
use serde::{Serialize, Deserialize};
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum MyError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
    
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("Division by zero")]
    DivisionByZero,
    
    #[error("Operation on mismatched timestamps")]
    MismatchedTimestamps,

    #[error("Invalid message")]
    InvalidMessage,
}

#[derive(Debug)]
pub enum Operation {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Serialize)]
pub struct Response {
    stream: String,
    data: Candle,
}


#[derive(Debug, Clone, Copy, Serialize)]
pub struct Candle {
    t: u64, // start time
    o: f64, // open price
    c: f64, // close price
    h: f64, // high price
    l: f64, // low price
}

impl Candle {
    pub fn new(t: u64, o: f64, c: f64, h: f64, l: f64) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { t, o, c, h, l }))
    }

    fn assert_timestamps(&self, other: Self) -> Result<(), MyError> {
        if self.t != other.t {
            return Err(MyError::MismatchedTimestamps);
        }

        Ok(())
    }

    pub fn add(&self, other: Self) -> Result<Self, MyError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o + other.o,
            c: self.c + other.c,
            h: self.h + other.h,
            l: self.l + other.l,
        })
    }

    pub fn sub(&self, other: Self) -> Result<Self, MyError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o - other.o,
            c: self.c - other.c,
            h: self.h - other.h,
            l: self.l - other.l,
        })
    }

    pub fn mul(&self, other: Self) -> Result<Self, MyError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o * other.o,
            c: self.c * other.c,
            h: self.h * other.h,
            l: self.l * other.l,
        })
    }

    pub fn div(&self, other: Self) -> Result<Self, MyError> {
        if other.o == 0.0 || other.c == 0.0 || other.h == 0.0 || other.l == 0.0 {
            return Err(MyError::DivisionByZero);
        }

        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o / other.o,
            c: self.c / other.c,
            h: self.h / other.h,
            l: self.l / other.l,
        })
    }
}


pub fn perform_operation(a: &Candle, b: &Candle, op: &Operation) -> Result<Candle, MyError> {
    if a.t != b.t {
        return Err(MyError::MismatchedTimestamps);
    }

    match op {
        Operation::Add => a.add(*b),
        Operation::Subtract => a.sub(*b),
        Operation::Multiply => a.mul(*b),
        Operation::Divide => a.div(*b),
    }
}


pub fn parse_operation(symbol: char) -> Option<Operation> {
    match symbol {
        '+' => Some(Operation::Add),
        '-' => Some(Operation::Subtract),
        '*' => Some(Operation::Multiply),
        '/' => Some(Operation::Divide),
        _ => None,
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub id: u32,
    pub method: String,
    pub stream: String,
}

#[derive(Serialize)]
pub struct BinanceSubscription {
    pub id: u32,
    pub method: String,
    pub params: Vec<String>,
}

pub fn parse_price(s: &str) -> f64 {
    s.parse().unwrap_or(0.0)
}

pub fn parse_streams(streams: &str) -> Vec<String> {
    streams.split(|c| "+-*/".contains(c))
        .map(|s| format!("{}@kline_1m", s))
        .collect()
}
