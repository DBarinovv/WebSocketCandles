use serde::{Deserialize, Serialize};
use std::{sync::Arc, collections::HashMap};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;
use regex::Regex;

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

    #[error("Can not serialize")]
    Serialization,

    #[error("Operation on mismatched timestamps")]
    MismatchedTimestamps,

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

    tokens.iter().filter(|&&token| !token.trim().is_empty()).map(|&token| format!("{}{}", token, postfix)).collect()
}

// fn precedence(op: char) -> i32 {
//     match op {
//         '+' | '-' => 1,
//         '*' | '/' => 2,
//         '(' => 3,
//         _ => 0,
//     }
// }


const OPERATOR_PRECEDENCES: [(char, usize); 5] = [
    ('+', 1),
    ('-', 1),
    ('*', 2),
    ('/', 2),
    ('(', 0), // lower than any other operator
];

pub enum Token {
    Operator(char),
    Operand(char),
    LeftParenthesis,
    RightParenthesis,
}

// impl From<Token> for char {
//     fn from(value: Token) -> Self {
//         match value {
//             Token::Operator(ch) | Token::Operand(ch) => ch,
//             Token::LeftParenthesis => '(',
//             Token::RightParenthesis => ')',
//         }
//     }
// }

// impl From<&Token> for char {
//     fn from(value: &Token) -> Self {
//         match value {
//             Token::Operator(ch) | Token::Operand(ch) => *ch,
//             Token::LeftParenthesis => '(',
//             Token::RightParenthesis => ')',
//         }
//     }
// }

pub fn parse(input: &str) -> Result<Vec<Token>, &'static str> {
    let mut tokens = Vec::new();
    for c in input.chars() {
        match c {
            '+' | '-' | '*' | '/' => tokens.push(Token::Operator(c)),
            '(' => tokens.push(Token::LeftParenthesis),
            ')' => tokens.push(Token::RightParenthesis),
            _ => {
                if c.is_alphabetic() {
                    tokens.push(Token::Operand(c));
                } else {
                    return Err("Invalid character in the input");
                }
            }
        }
    }
    Ok(tokens)
}

pub fn to_rpn(tokens: &[Token]) -> Result<String, &'static str> {
    let mut rpn = String::with_capacity(tokens.len());
    let mut stack: Vec<&Token> = Vec::new();
    let mut open_brackets = 0;

    let precedence = |c| {
        OPERATOR_PRECEDENCES.iter().find(|&&(op, _)| if let Token::Operator(op_ch) = op { op_ch == c } else { false }).map(|&(_, p)| p).unwrap_or_else(|| usize::MAX)
    };
    
    for token in tokens {
        match token {
            Token::Operator(c) => {
                while let Some(&last) = stack.last() {
                    if precedence(*c) <= precedence(last) {
                        rpn.push(stack.pop().unwrap());
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
                    return Err("Mismatched parentheses");
                }
                while let Some(top) = stack.pop() {
                    if top == '(' {
                        break;
                    }
                    rpn.push(top);
                }
                open_brackets -= 1;
            }
            Token::Operand(c) => rpn.push(*c),
        }
    }

    if open_brackets != 0 {
        return Err("Mismatched parentheses");
    }

    while let Some(op) = stack.pop() {
        rpn.push(op);
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
    use super::{to_rpn, parse};

    #[test]
    fn test_to_rpn_basic() {
        let tokens = parse("a+b*c").unwrap();
        let expected = "abc*+".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_parentheses() {
        let tokens = parse("(a+b)*c").unwrap();
        let expected = "ab+c*".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_complex() {
        let tokens = parse("a+b*c-d/e").unwrap();
        let expected = "abc*+de/-".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_parentheses_complex() {
        let tokens = parse("(a+b)*(c-d)/(e+f)").unwrap();
        let expected = "ab+cd-*ef+/".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_multiple_parentheses() {
        let tokens = parse("a/(b+c*(d-e/f))").unwrap();
        let expected = "abcdef/-*+/".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_nested_parentheses() {
        let tokens = parse("a(b-c*d/(f/g)+h)").unwrap();
        let expected = "abcd*fg//-h+".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_no_parentheses() {
        let tokens = parse("a+b*c/d-e*f").unwrap();
        let expected = "abc*d/+ef*-".to_string();
        assert_eq!(to_rpn(&tokens), Ok(expected));
    }

    #[test]
    fn test_to_rpn_with_invalid_characters() {
        let tokens = parse("a+b*c$").unwrap();
        assert_eq!(to_rpn(&tokens), Err("Invalid character in the input"));
    }

    #[test]
    fn test_to_rpn_with_unmatched_parentheses() {
        let tokens = parse("a+(b+c").unwrap();
        assert_eq!(to_rpn(&tokens), Err("Mismatched parentheses"));

        let tokens = parse("a+b+c)").unwrap();
        assert_eq!(to_rpn(&tokens), Err("Mismatched parentheses"));

        let tokens = parse("(a+(b*c)))*d").unwrap();
        assert_eq!(to_rpn(&tokens), Err("Mismatched parentheses"));
    }

}
