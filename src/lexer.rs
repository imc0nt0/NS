use crate::error::{NsError, NsResult, SourceMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    // single
    LParen, RParen, LBrace, RBrace, LBracket, RBracket,
    Comma, Dot, Semicolon, At, Colon,

    // ops
    Plus, Minus, Star, Slash,
    Bang,
    Equal,
    Less, Greater,

    // two-char
    BangEqual,
    EqualEqual,
    LessEqual,
    GreaterEqual,

    PlusEqual,
    MinusEqual,
    StarEqual,
    SlashEqual,

    AmpAmp,
    PipePipe,

    // literals
    Ident,
    Number,
    String,

    // keywords
    Let,
    Fn,
    If,
    Else,
    While,
    For,
    Break,
    Continue,
    Return,
    True,
    False,
    Null,
    Export,
    Import,
    As,

    Eof,
}

#[derive(Clone, Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
    pub line: u32,
}

pub struct Lexer<'a> {
    map: &'a SourceMap<'a>,
    i: usize,
    line: u32,
}

impl<'a> Lexer<'a> {
    pub fn new(map: &'a SourceMap<'a>) -> Self {
        Self { map, i: 0, line: 1 }
    }

    fn err<T>(&self, pos: usize, msg: impl Into<String>, error_len: usize) -> NsResult<T> {
        Err(NsError::new(self.map, pos, msg, error_len))
    }

    fn src(&self) -> &'a str { self.map.src }

    fn is_at_end(&self) -> bool { self.i >= self.src().len() }

    fn peek(&self) -> char {
        self.src()[self.i..].chars().next().unwrap_or('\0')
    }

    fn peek2(&self) -> char {
        let mut it = self.src()[self.i..].chars();
        let _ = it.next();
        it.next().unwrap_or('\0')
    }

    fn advance(&mut self) -> char {
        let ch = self.peek();
        if ch == '\0' { return '\0'; }
        self.i += ch.len_utf8();
        if ch == '\n' { self.line += 1; }
        ch
    }

    fn skip_ws(&mut self) -> NsResult<()> {
        loop {
            let ch = self.peek();
            match ch {
                ' ' | '\r' | '\t' | '\n' => { self.advance(); }
                '/' if self.peek2() == '/' => {
                    while self.peek() != '\n' && !self.is_at_end() { self.advance(); }
                }
                '/' if self.peek2() == '*' => {
                    let start_pos = self.i;
                    self.advance();
                    self.advance();
                    loop {
                        if self.is_at_end() {
                            return self.err(start_pos, "Unterminated block comment.", 1);
                        }
                        if self.peek() == '*' && self.peek2() == '/' {
                            self.advance();
                            self.advance();
                            break;
                        }
                        self.advance();
                    }
                }
                _ => break
            }
        }
        Ok(())
    }

    pub fn next(&mut self) -> NsResult<Token> {
        self.skip_ws()?;
        let start = self.i;
        let line = self.line;

        if self.is_at_end() {
            return Ok(Token { kind: TokenKind::Eof, start, end: start, line });
        }

        let ch = self.advance();
        let kind = match ch {
            '(' => TokenKind::LParen,
            ')' => TokenKind::RParen,
            '{' => TokenKind::LBrace,
            '}' => TokenKind::RBrace,
            '[' => TokenKind::LBracket,
            ']' => TokenKind::RBracket,
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            ';' => TokenKind::Semicolon,
            '@' => TokenKind::At,
            ':' => TokenKind::Colon,

            '+' => {
                if self.peek() == '=' { self.advance(); TokenKind::PlusEqual } else { TokenKind::Plus }
            }
            '-' => {
                if self.peek() == '=' { self.advance(); TokenKind::MinusEqual } else { TokenKind::Minus }
            }
            '*' => {
                if self.peek() == '=' { self.advance(); TokenKind::StarEqual } else { TokenKind::Star }
            }
            '/' => {
                if self.peek() == '=' { self.advance(); TokenKind::SlashEqual } else { TokenKind::Slash }
            }
            '!' => {
                if self.peek() == '=' { self.advance(); TokenKind::BangEqual } else { TokenKind::Bang }
            }
            '=' => {
                if self.peek() == '=' { self.advance(); TokenKind::EqualEqual } else { TokenKind::Equal }
            }
            '<' => {
                if self.peek() == '=' { self.advance(); TokenKind::LessEqual } else { TokenKind::Less }
            }
            '>' => {
                if self.peek() == '=' { self.advance(); TokenKind::GreaterEqual } else { TokenKind::Greater }
            }
            '&' => {
                if self.peek() == '&' { self.advance(); TokenKind::AmpAmp } else { return self.err(start, "Unexpected character '&'.", 1); }
            }
            '|' => {
                if self.peek() == '|' { self.advance(); TokenKind::PipePipe } else { return self.err(start, "Unexpected character '|'.", 1); }
            }

            '"' => {
                while self.peek() != '"' && !self.is_at_end() {
                    self.advance();
                }
                if self.is_at_end() {
                    return self.err(start, "Unterminated string.", 1);
                }
                self.advance();
                TokenKind::String
            }

            c if c.is_ascii_digit() => {
                while self.peek().is_ascii_digit() { self.advance(); }
                if self.peek() == '.' && self.peek2().is_ascii_digit() {
                    self.advance();
                    while self.peek().is_ascii_digit() { self.advance(); }
                }
                TokenKind::Number
            }

            c if c.is_ascii_alphabetic() || c == '_' => {
                while self.peek().is_ascii_alphanumeric() || self.peek() == '_' { self.advance(); }
                let text = &self.src()[start..self.i];
                match text {
                    "let" => TokenKind::Let,
                    "fn" => TokenKind::Fn,
                    "if" => TokenKind::If,
                    "else" => TokenKind::Else,
                    "while" => TokenKind::While,
                    "for" => TokenKind::For,
                    "break" => TokenKind::Break,
                    "continue" => TokenKind::Continue,
                    "return" => TokenKind::Return,
                    "true" => TokenKind::True,
                    "false" => TokenKind::False,
                    "null" => TokenKind::Null,
                    "export" => TokenKind::Export,
                    "import" => TokenKind::Import,
                    "as" => TokenKind::As,
                    _ => TokenKind::Ident,
                }
            }

            _ => {
                return self.err(start, "Unexpected character.", 1);
            }
        };

        Ok(Token { kind, start, end: self.i, line })
    }
}
