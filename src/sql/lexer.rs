//! SQL Lexer (Tokenizer)
//!
//! This module converts SQL strings into a stream of tokens.

use super::token::Token;
use crate::error::{Error, Result};

/// SQL Lexer
pub struct Lexer {
    /// Input characters
    input: Vec<char>,
    /// Current position in input
    position: usize,
}

impl Lexer {
    /// Create a new lexer for the given input
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }

        Ok(tokens)
    }

    /// Get the next token from the input
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();
        self.skip_comments();
        self.skip_whitespace();

        if self.is_at_end() {
            return Ok(Token::Eof);
        }

        let ch = self.current_char();

        // Single character tokens
        let token = match ch {
            '(' => {
                self.advance();
                return Ok(Token::LParen);
            }
            ')' => {
                self.advance();
                return Ok(Token::RParen);
            }
            ',' => {
                self.advance();
                return Ok(Token::Comma);
            }
            ';' => {
                self.advance();
                return Ok(Token::Semicolon);
            }
            '.' => {
                self.advance();
                return Ok(Token::Dot);
            }
            ':' => {
                self.advance();
                return Ok(Token::Colon);
            }
            '+' => {
                self.advance();
                return Ok(Token::Plus);
            }
            '-' => {
                self.advance();
                // Check for negative number
                if !self.is_at_end() && self.current_char().is_ascii_digit() {
                    let num = self.read_number()?;
                    return match num {
                        Token::IntegerLiteral(n) => Ok(Token::IntegerLiteral(-n)),
                        Token::FloatLiteral(n) => Ok(Token::FloatLiteral(-n)),
                        _ => Ok(num),
                    };
                }
                return Ok(Token::Minus);
            }
            '*' => {
                self.advance();
                return Ok(Token::Asterisk);
            }
            '/' => {
                self.advance();
                return Ok(Token::Slash);
            }
            '%' => {
                self.advance();
                return Ok(Token::Percent);
            }
            '=' => {
                self.advance();
                return Ok(Token::Eq);
            }
            '<' => {
                self.advance();
                if !self.is_at_end() {
                    match self.current_char() {
                        '=' => {
                            self.advance();
                            return Ok(Token::Lte);
                        }
                        '>' => {
                            self.advance();
                            return Ok(Token::Neq);
                        }
                        _ => {}
                    }
                }
                return Ok(Token::Lt);
            }
            '>' => {
                self.advance();
                if !self.is_at_end() && self.current_char() == '=' {
                    self.advance();
                    return Ok(Token::Gte);
                }
                return Ok(Token::Gt);
            }
            '!' => {
                self.advance();
                if !self.is_at_end() && self.current_char() == '=' {
                    self.advance();
                    return Ok(Token::Neq);
                }
                return Err(Error::UnexpectedCharacter('!', self.position));
            }
            '|' => {
                self.advance();
                if !self.is_at_end() && self.current_char() == '|' {
                    self.advance();
                    return Ok(Token::Concat);
                }
                return Err(Error::UnexpectedCharacter('|', self.position));
            }
            '\'' => {
                return self.read_string();
            }
            '"' => {
                return self.read_quoted_identifier();
            }
            _ => None,
        };

        if token.is_some() {
            return Ok(token.unwrap());
        }

        // Numbers
        if ch.is_ascii_digit() {
            return self.read_number();
        }

        // Identifiers and keywords
        if ch.is_alphabetic() || ch == '_' {
            return self.read_identifier();
        }

        Err(Error::UnexpectedCharacter(ch, self.position))
    }

    /// Check if we've reached the end of input
    fn is_at_end(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Get the current character
    fn current_char(&self) -> char {
        self.input[self.position]
    }

    /// Peek at the next character
    fn peek_char(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            Some(self.input[self.position + 1])
        } else {
            None
        }
    }

    /// Advance to the next character
    fn advance(&mut self) {
        self.position += 1;
    }

    /// Skip whitespace characters
    fn skip_whitespace(&mut self) {
        while !self.is_at_end() && self.current_char().is_whitespace() {
            self.advance();
        }
    }

    /// Skip SQL comments (-- and /* */)
    fn skip_comments(&mut self) {
        if self.is_at_end() {
            return;
        }

        // Single line comment: --
        if self.current_char() == '-' && self.peek_char() == Some('-') {
            while !self.is_at_end() && self.current_char() != '\n' {
                self.advance();
            }
            self.skip_whitespace();
            self.skip_comments();
        }

        // Multi-line comment: /* */
        if self.current_char() == '/' && self.peek_char() == Some('*') {
            self.advance(); // skip /
            self.advance(); // skip *

            while !self.is_at_end() {
                if self.current_char() == '*' && self.peek_char() == Some('/') {
                    self.advance(); // skip *
                    self.advance(); // skip /
                    break;
                }
                self.advance();
            }
            self.skip_whitespace();
            self.skip_comments();
        }
    }

    /// Read a string literal (single-quoted)
    fn read_string(&mut self) -> Result<Token> {
        let start_pos = self.position;
        self.advance(); // skip opening quote

        let mut value = String::new();

        while !self.is_at_end() {
            let ch = self.current_char();

            if ch == '\'' {
                // Check for escaped quote ''
                if self.peek_char() == Some('\'') {
                    value.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // skip closing quote
                    return Ok(Token::StringLiteral(value));
                }
            } else {
                value.push(ch);
                self.advance();
            }
        }

        Err(Error::UnterminatedString(start_pos))
    }

    /// Read a quoted identifier (double-quoted)
    fn read_quoted_identifier(&mut self) -> Result<Token> {
        let start_pos = self.position;
        self.advance(); // skip opening quote

        let mut value = String::new();

        while !self.is_at_end() {
            let ch = self.current_char();

            if ch == '"' {
                // Check for escaped quote ""
                if self.peek_char() == Some('"') {
                    value.push('"');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // skip closing quote
                    return Ok(Token::Identifier(value));
                }
            } else {
                value.push(ch);
                self.advance();
            }
        }

        Err(Error::UnterminatedString(start_pos))
    }

    /// Read a number (integer or float)
    fn read_number(&mut self) -> Result<Token> {
        let start_pos = self.position;
        let mut value = String::new();
        let mut is_float = false;

        while !self.is_at_end() {
            let ch = self.current_char();

            if ch.is_ascii_digit() {
                value.push(ch);
                self.advance();
            } else if ch == '.' && !is_float {
                // Check if it's a float or a dot operator
                if let Some(next) = self.peek_char() {
                    if next.is_ascii_digit() {
                        is_float = true;
                        value.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else if (ch == 'e' || ch == 'E') && !value.is_empty() {
                // Scientific notation
                is_float = true;
                value.push(ch);
                self.advance();

                if !self.is_at_end() && (self.current_char() == '+' || self.current_char() == '-') {
                    value.push(self.current_char());
                    self.advance();
                }
            } else {
                break;
            }
        }

        if is_float {
            value
                .parse::<f64>()
                .map(Token::FloatLiteral)
                .map_err(|_| Error::InvalidNumber(start_pos))
        } else {
            value
                .parse::<i64>()
                .map(Token::IntegerLiteral)
                .map_err(|_| Error::InvalidNumber(start_pos))
        }
    }

    /// Read an identifier or keyword
    fn read_identifier(&mut self) -> Result<Token> {
        let mut value = String::new();

        while !self.is_at_end() {
            let ch = self.current_char();

            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Check if it's a keyword
        if let Some(keyword) = Token::from_keyword(&value) {
            Ok(keyword)
        } else {
            Ok(Token::Identifier(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Asterisk,
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_select_with_where() {
        let mut lexer = Lexer::new("SELECT id, name FROM users WHERE id = 1");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("id".to_string()),
                Token::Comma,
                Token::Identifier("name".to_string()),
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Where,
                Token::Identifier("id".to_string()),
                Token::Eq,
                Token::IntegerLiteral(1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_create_table() {
        let mut lexer =
            Lexer::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
        assert_eq!(tokens[3], Token::LParen);
    }

    #[test]
    fn test_string_literal() {
        let mut lexer = Lexer::new("SELECT 'hello world'");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::StringLiteral("hello world".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_escaped_string() {
        let mut lexer = Lexer::new("SELECT 'it''s a test'");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[1], Token::StringLiteral("it's a test".to_string()));
    }

    #[test]
    fn test_comparison_operators() {
        let mut lexer = Lexer::new("a < b <= c > d >= e <> f != g");
        let tokens = lexer.tokenize().unwrap();

        assert!(tokens.contains(&Token::Lt));
        assert!(tokens.contains(&Token::Lte));
        assert!(tokens.contains(&Token::Gt));
        assert!(tokens.contains(&Token::Gte));
        assert_eq!(tokens.iter().filter(|t| **t == Token::Neq).count(), 2);
    }

    #[test]
    fn test_float_literal() {
        let mut lexer = Lexer::new("SELECT 3.14, 2.5e10");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[1], Token::FloatLiteral(3.14));
    }

    #[test]
    fn test_comments() {
        let mut lexer = Lexer::new("SELECT -- this is a comment\n* FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Asterisk,
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_multiline_comment() {
        let mut lexer = Lexer::new("SELECT /* comment */ * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Asterisk);
    }
}
