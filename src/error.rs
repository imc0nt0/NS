use std::fmt;

#[derive(Clone)]
pub struct SourceMap<'a> {
    pub name: &'a str,
    pub src: &'a str,
}

impl<'a> SourceMap<'a> {
    pub fn new(name: &'a str, src: &'a str) -> Self {
        Self { name, src }
    }

    pub fn line_col(&self, pos: usize) -> (usize, usize) {
        let mut line = 1usize;
        let mut col = 1usize;
        for (i, ch) in self.src.char_indices() {
            if i >= pos { break; }
            if ch == '\n' { line += 1; col = 1; } else { col += 1; }
        }
        (line, col)
    }
}

#[derive(Clone)]
pub struct NsError {
    pub msg: String,
    pub pos: usize,
    pub name: String,
    pub line: usize,
    pub col: usize,
    pub len: usize, // Added field
    pub snippet: String,
}

impl NsError {
    pub fn new(map: &SourceMap, pos: usize, msg: impl Into<String>, error_len: usize) -> Self { // Added error_len
        let (line, col) = map.line_col(pos);
        let snippet = map.src.lines().nth(line.saturating_sub(1)).unwrap_or("").to_string();
        Self {
            msg: msg.into(),
            pos,
            name: map.name.to_string(),
            line,
            col,
            len: error_len, // Initialize len
            snippet,
        }
    }
}

impl fmt::Display for NsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const RED: &str = "\x1b[31m";
        const RESET: &str = "\x1b[0m";

        // Print the main error message line
        writeln!(
            f,
            "[NS:ERROR] [{}:{}:{}]: {}",
            self.name, self.line, self.col, self.msg
        )?;

        // If a snippet is available, print it with the error part colored red
        if !self.snippet.is_empty() {
            let mut prefix = String::new();
            let mut error_span_text = String::new();
            let mut suffix = String::new();

            let char_start_idx = self.col.saturating_sub(1); // 0-based character index
            let char_end_idx = char_start_idx + self.len;

            // Iterate through characters to build prefix, error_span_text, and suffix
            for (i, c) in self.snippet.chars().enumerate() {
                if i < char_start_idx {
                    prefix.push(c);
                } else if i >= char_start_idx && i < char_end_idx {
                    error_span_text.push(c);
                } else {
                    suffix.push(c);
                }
            }

            // Print the snippet with the error span colored
            writeln!(f, "  {}{}{}{}{}", prefix, RED, error_span_text, RESET, suffix)?;
        }

        if self.msg == "Expect '(' after 'if'." {
            write!(f, "{}", RED)?;
            write!(f, "\nAdd parentheses around the condition, e.g., `if ((condition)) {{ ... }}`")?; // Print message (escaped braces)
            writeln!(f, "{}", RESET)?;
        }
        Ok(())
    }
}

impl From<String> for NsError {
    fn from(msg: String) -> Self {
        NsError {
            msg,
            pos: 0,
            name: "<unknown>".to_string(),
            line: 0,
            col: 0,
            len: 1,
            snippet: "".to_string(),
        }
    }
}

pub type NsResult<T> = Result<T, NsError>;