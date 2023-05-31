use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum ServerError {
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

    #[error("Key not found")]
    KeyNotFound,

    #[error("Can not connect to WebSocket")]
    WebSocketConnect,

    #[error("Can not accept WebSocket connection")]
    WebSocketAccept,

    #[error("Сonnection time expired")]
    WebSocketTimeout,

    #[error("Can not write to WebSocket")]
    WebSocketWrite,

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Websocket error")] //
    WebsocketError(tokio_tungstenite::tungstenite::Error),

    #[error("Can not serialize")]
    Serialization,

    #[error("Operation on mismatched timestamps")]
    MismatchedTimestamps,

    #[error("Wrong stream")]
    ParsingStream,

    #[error("Invalid message")]
    InvalidMessage(String),
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

#[derive(Debug, Deserialize)]
pub struct BinanceMessage {
    pub data: BinanceData,
}

#[derive(Debug, Deserialize)]
pub struct BinanceData {
    pub e: String,
    pub E: u64,
    pub s: String,
    pub k: BinanceKlineData,
}

#[derive(Debug, Deserialize)]
pub struct BinanceKlineData {
    pub t: u64,
    pub T: u64,
    pub s: String,
    pub i: String,
    pub f: u64,
    pub L: u64,
    pub o: String, // Open price
    pub c: String, // Close price
    pub h: String, // High price
    pub l: String, // Low price
    pub v: String,
    pub n: u64,
    pub x: bool,
    pub q: String,
    pub V: String,
    pub Q: String,
    pub B: String,
}

#[derive(Debug, Serialize)]
pub struct ResultMessage {
    pub stream: String,
    pub data: ResultData,
}

#[derive(Debug, Serialize)]
pub struct ResultData {
    pub t: u64, // kline start time
    pub o: f64, // open price: 26884.70 + 1806.09
    pub c: f64, // close price: 26886.20 + 1806.14
    pub h: f64, // high price: 26892.50 + 1806.33
    pub l: f64, // low price: 26877.80 + 1805.67
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Candle {
    pub t: u64, // start time
    pub o: f64, // open price
    pub c: f64, // close price
    pub h: f64, // high price
    pub l: f64, // low price
}

impl Candle {
    pub fn new(t: u64, o: f64, c: f64, h: f64, l: f64) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { t, o, c, h, l }))
    }

    fn assert_timestamps(&self, other: Self) -> Result<(), ServerError> {
        if self.t != other.t {
            return Err(ServerError::MismatchedTimestamps);
        }

        Ok(())
    }

    pub fn add(&self, other: Self) -> Result<Self, ServerError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o + other.o,
            c: self.c + other.c,
            h: self.h + other.h,
            l: self.l + other.l,
        })
    }

    pub fn sub(&self, other: Self) -> Result<Self, ServerError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o - other.o,
            c: self.c - other.c,
            h: self.h - other.h,
            l: self.l - other.l,
        })
    }

    pub fn mul(&self, other: Self) -> Result<Self, ServerError> {
        self.assert_timestamps(other)?;

        Ok(Self {
            t: self.t,
            o: self.o * other.o,
            c: self.c * other.c,
            h: self.h * other.h,
            l: self.l * other.l,
        })
    }

    pub fn div(&self, other: Self) -> Result<Self, ServerError> {
        if other.o == 0.0 || other.c == 0.0 || other.h == 0.0 || other.l == 0.0 {
            return Err(ServerError::DivisionByZero);
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

pub fn perform_operation(a: &Candle, b: &Candle, op: &Operation) -> Result<Candle, ServerError> {
    if a.t != b.t {
        return Err(ServerError::MismatchedTimestamps);
    }

    match op {
        Operation::Add => a.add(*b),
        Operation::Subtract => a.sub(*b),
        Operation::Multiply => a.mul(*b),
        Operation::Divide => a.div(*b),
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

pub fn parse_streams(input: &str) -> Vec<String> {
    // Looking for @
    let divider_index = input.rfind('@').unwrap();

    // Getting postfix
    let postfix = format!("@kline_{}", &input[(divider_index + 1)..]);

    // Getting all tokens
    let re = Regex::new(r"([()+*/-])").unwrap();
    let tokens = re.split(&input[..divider_index]).collect::<Vec<&str>>();

    tokens
        .iter()
        .filter(|&&token| !token.trim().is_empty())
        .map(|&token| format!("{}{}", token, postfix))
        .collect()
}

const OPERATOR_PRECEDENCES: [(char, usize); 5] = [
    ('+', 1),
    ('-', 1),
    ('*', 2),
    ('/', 2),
    ('(', 0), // lower than any other operator
];

#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    Operator(Operator),
    Operand(String),
    LeftParenthesis,
    RightParenthesis,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operator {
    Plus,
    Minus,
    Multiply,
    Divide,
    NotOperator,
}

impl From<char> for Operator {
    fn from(value: char) -> Self {
        match value {
            '+' => Operator::Plus,
            '-' => Operator::Minus,
            '*' => Operator::Multiply,
            '/' => Operator::Divide,
            _ => Operator::NotOperator,
        }
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let symbol = match self {
            Operator::Plus => '+',
            Operator::Minus => '-',
            Operator::Multiply => '*',
            Operator::Divide => '/',
            _ => ' ',
        };
        write!(f, "{}", symbol)
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Token::Operator(op) => write!(f, "{}", op),
            Token::Operand(op) => write!(f, "{}", op),
            Token::LeftParenthesis => write!(f, "("),
            Token::RightParenthesis => write!(f, ")"),
        }
    }
}

pub fn parse(input: &str) -> Result<Vec<Token>, ServerError> {
    let mut tokens = Vec::new();
    let mut current_operand = String::new();
    let divider_index = input.rfind('@').unwrap();
    let postfix = format!("@kline_{}", &input[(divider_index + 1)..]);

    for c in input[..divider_index].chars() {
        match c {
            '+' | '-' | '*' | '/' => {
                if !current_operand.is_empty() {
                    tokens.push(Token::Operand(current_operand.clone() + &postfix));
                    current_operand.clear();
                }
                tokens.push(Token::Operator(c.into()));
            }
            '(' => tokens.push(Token::LeftParenthesis),
            ')' => {
                if !current_operand.is_empty() {
                    tokens.push(Token::Operand(current_operand.clone() + &postfix));
                    current_operand.clear();
                }
                tokens.push(Token::RightParenthesis);
            }
            _ => {
                if c.is_alphanumeric() {
                    current_operand.push(c);
                } else {
                    return Err(ServerError::ParsingStream);
                }
            }
        }
    }

    if !current_operand.is_empty() {
        tokens.push(Token::Operand(current_operand + &postfix));
    }

    Ok(tokens)
}

pub fn to_rpn(tokens: &[Token]) -> Result<Vec<Token>, ServerError> {
    let mut rpn = Vec::<Token>::new();
    let mut stack: Vec<&Token> = Vec::new();
    let mut open_brackets = 0;

    let precedence = |t: &Token| match t {
        Token::Operator(op) => match op {
            Operator::Plus | Operator::Minus => 1,
            Operator::Multiply | Operator::Divide => 2,
            _ => 0,
        },
        Token::LeftParenthesis => 0,
        _ => usize::MAX,
    };

    for token in tokens {
        match token {
            Token::Operator(_) => {
                while let Some(&last) = stack.last() {
                    if precedence(token) <= precedence(last) {
                        rpn.push((*stack.pop().unwrap()).clone());
                    } else {
                        break;
                    }
                }
                stack.push(token);
            }
            Token::LeftParenthesis => {
                stack.push(token);
                open_brackets += 1;
            }
            Token::RightParenthesis => {
                if open_brackets == 0 {
                    return Err(ServerError::ParsingStream);
                }
                while let Some(top) = stack.pop() {
                    if matches!(top, Token::LeftParenthesis) {
                        break;
                    }
                    rpn.push((*top).clone());
                }
                open_brackets -= 1;
            }
            _ => rpn.push((*token).clone()),
        }
    }

    if open_brackets != 0 {
        return Err(ServerError::ParsingStream);
    }

    while let Some(op) = stack.pop() {
        rpn.push((*op).clone());
    }

    Ok(rpn)
}

#[cfg(test)]
mod tests_parse {
    use super::parse_streams;

    #[test]
    fn test_parse_streams_single_token() {
        let input = "btcusdt@1m";
        let expected = vec!["btcusdt@kline_1m"];
        assert_eq!(parse_streams(input), expected);
    }

    #[test]
    fn test_parse_streams_multiple_tokens() {
        let input = "btcusdt+ethusdt@1h";
        let expected = vec!["btcusdt@kline_1h", "ethusdt@kline_1h"];
        assert_eq!(parse_streams(input), expected);
    }

    #[test]
    fn test_parse_streams_with_operations() {
        let input = "(btcusdt-ethusdt)*bnbusdt@1d";
        let expected = vec!["btcusdt@kline_1d", "ethusdt@kline_1d", "bnbusdt@kline_1d"];
        assert_eq!(parse_streams(input), expected);
    }

    // should panic?
    #[test]
    fn test_parse_streams_with_empty_tokens() {
        let input = "btcusdt++ethusdt@1m";
        let expected = vec!["btcusdt@kline_1m", "ethusdt@kline_1m"];
        assert_eq!(parse_streams(input), expected);
    }
}

#[cfg(test)]
mod tests_rpn {
    use super::{parse, to_rpn, Operator, Token};

    #[test]
    fn test_to_rpn_simple_expression() {
        let tokens = parse("btcusdt+ethusdt@1m").unwrap();
        let result = to_rpn(&tokens).unwrap();
        assert_eq!(
            result,
            vec![
                Token::Operand("btcusdt@kline_1m".into()),
                Token::Operand("ethusdt@kline_1m".into()),
                Token::Operator(Operator::Plus)
            ]
        );
    }

    #[test]
    fn test_to_rpn_expression_with_parentheses() {
        let tokens = parse("(btcusdt+ethusdt)*adausdt@1m").unwrap();
        let result = to_rpn(&tokens).unwrap();
        assert_eq!(
            result,
            vec![
                Token::Operand("btcusdt@kline_1m".into()),
                Token::Operand("ethusdt@kline_1m".into()),
                Token::Operator(Operator::Plus),
                Token::Operand("adausdt@kline_1m".into()),
                Token::Operator(Operator::Multiply)
            ]
        );
    }

    #[test]
    fn test_to_rpn_mismatched_parentheses() {
        let tokens = parse("(btcusdt+ethusdt*adausdt@1m").unwrap();
        let result = to_rpn(&tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_rpn_operator_precedence() {
        let tokens = parse("btcusdt+ethusdt*adausdt@1h").unwrap();
        let result = to_rpn(&tokens).unwrap();
        assert_eq!(
            result,
            vec![
                Token::Operand("btcusdt@kline_1h".into()),
                Token::Operand("ethusdt@kline_1h".into()),
                Token::Operand("adausdt@kline_1h".into()),
                Token::Operator(Operator::Multiply),
                Token::Operator(Operator::Plus)
            ]
        );
    }

    #[test]
    fn test_to_rpn_with_complex_expression() {
        let tokens = parse("btcusdt+ethusdt*(bnbusdt-trxusdt)@1h").unwrap();
        let expected = vec![
            Token::Operand("btcusdt@kline_1h".into()),
            Token::Operand("ethusdt@kline_1h".into()),
            Token::Operand("bnbusdt@kline_1h".into()),
            Token::Operand("trxusdt@kline_1h".into()),
            Token::Operator(Operator::Minus),
            Token::Operator(Operator::Multiply),
            Token::Operator(Operator::Plus),
        ];
        assert_eq!(to_rpn(&tokens).unwrap(), expected);
    }

    #[test]
    fn test_to_rpn_with_no_parentheses() {
        let tokens = parse("btcusdt+ethusdt*bnbusdt/trxusdt@1M").unwrap();
        let expected = vec![
            Token::Operand("btcusdt@kline_1M".into()),
            Token::Operand("ethusdt@kline_1M".into()),
            Token::Operand("bnbusdt@kline_1M".into()),
            Token::Operator(Operator::Multiply),
            Token::Operand("trxusdt@kline_1M".into()),
            Token::Operator(Operator::Divide),
            Token::Operator(Operator::Plus),
        ];
        assert_eq!(to_rpn(&tokens).unwrap(), expected);
    }

    #[test]
    fn test_to_rpn_with_all_operators() {
        let tokens = parse("btcusdt+ethusdt-bnbusdt*trxusdt/bchusdt@1M").unwrap();
        let expected = vec![
            Token::Operand("btcusdt@kline_1M".into()),
            Token::Operand("ethusdt@kline_1M".into()),
            Token::Operator(Operator::Plus),
            Token::Operand("bnbusdt@kline_1M".into()),
            Token::Operand("trxusdt@kline_1M".into()),
            Token::Operator(Operator::Multiply),
            Token::Operand("bchusdt@kline_1M".into()),
            Token::Operator(Operator::Divide),
            Token::Operator(Operator::Minus),
        ];
        assert_eq!(to_rpn(&tokens).unwrap(), expected);
    }

    #[test]
    fn test_to_rpn_with_multiple_parentheses() {
        let tokens = parse("(btcusdt+(ethusdt-(bnbusdt*(trxusdt/bchusdt))))@1M").unwrap();
        let expected = vec![
            Token::Operand("btcusdt@kline_1M".into()),
            Token::Operand("ethusdt@kline_1M".into()),
            Token::Operand("bnbusdt@kline_1M".into()),
            Token::Operand("trxusdt@kline_1M".into()),
            Token::Operand("bchusdt@kline_1M".into()),
            Token::Operator(Operator::Divide),
            Token::Operator(Operator::Multiply),
            Token::Operator(Operator::Minus),
            Token::Operator(Operator::Plus),
        ];
        assert_eq!(to_rpn(&tokens).unwrap(), expected);
    }
}
