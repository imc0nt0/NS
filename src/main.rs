mod error;
mod lexer;
mod opcode;
mod chunk;
mod value;
mod compiler;
pub mod jit;
mod vm;
mod dis;

use std::env;
use std::fs;

use crate::error::SourceMap;
use crate::vm::VM;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut vm = VM::new();

    if args.len() <= 1 {
        vm.repl();
        return;
    }

    let path = &args[1];
    let src = fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("[NS:ERROR] read failed: {e}");
        std::process::exit(1);
    });

    let map = SourceMap::new(path, &src);
    let run_result = vm.run_map(&map);
    match run_result {
        Ok(_) => {
                        if let Ok(final_value) = vm.pop(&map, 0) {
                            if let Err(s) = vm.value_to_string(final_value) {
                                eprintln!("[NS:ERROR] {}", s); // Error during string conversion, not NsError
                            }
                        } else {
                            eprintln!("[NS:ERROR] Failed to retrieve final result from VM stack.");
                            std::process::exit(1);
                        }
        }
        Err(e) => {
            eprintln!("{e}"); // NsError's Display impl already prefixes "[NS:ERROR]"
            std::process::exit(1);
        }
    }
}