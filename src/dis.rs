#![allow(dead_code)]
use crate::chunk::Chunk;
use crate::opcode::OpCode;
use crate::value::Value;

fn print_value(v: &Value) {
    match v {
        Value::Number(n) => print!("{}", n),
        Value::Bool(b) => print!("{}", b),
        Value::Null => print!("null"),
        Value::Obj(o) => print!("<obj {}>", o),
    }
}

pub fn disassemble_chunk(chunk: &Chunk, name: &str) {
    println!("== {} ==", name);
    let mut offset = 0;
    while offset < chunk.code.len() {
        offset = disassemble_instruction(chunk, offset);
    }
}

pub fn disassemble_instruction(chunk: &Chunk, offset: usize) -> usize {
    print!("{:04} ", offset);
    if offset > 0 && chunk.lines[offset] == chunk.lines[offset - 1] {
        print!("   | ");
    } else {
        print!("{:4} ", chunk.lines[offset]);
    }

    let instruction = OpCode::from_u8(chunk.code[offset]);
    match instruction {
        OpCode::Constant => constant_instruction("Constant", chunk, offset),
        OpCode::Null => simple_instruction("Null", offset),
        OpCode::True => simple_instruction("True", offset),
        OpCode::False => simple_instruction("False", offset),
        OpCode::Pop => simple_instruction("Pop", offset),
        OpCode::GetLocal => u16_instruction("GetLocal", chunk, offset),
        OpCode::SetLocal => u16_instruction("SetLocal", chunk, offset),
        OpCode::GetGlobal => u16_instruction("GetGlobal", chunk, offset),
        OpCode::DefineGlobal => u16_instruction("DefineGlobal", chunk, offset),
        OpCode::SetGlobal => u16_instruction("SetGlobal", chunk, offset),
        OpCode::Equal => simple_instruction("Equal", offset),
        OpCode::Greater => simple_instruction("Greater", offset),
        OpCode::Less => simple_instruction("Less", offset),
        OpCode::Add => simple_instruction("Add", offset),
        OpCode::Sub => simple_instruction("Sub", offset),
        OpCode::Mul => simple_instruction("Mul", offset),
        OpCode::Div => simple_instruction("Div", offset),
        OpCode::Not => simple_instruction("Not", offset),
        OpCode::Negate => simple_instruction("Negate", offset),
        OpCode::Jump => jump_instruction("Jump", 1, chunk, offset),
        OpCode::JumpIfFalse => jump_instruction("JumpIfFalse", 1, chunk, offset),
        OpCode::Loop => jump_instruction("Loop", -1, chunk, offset),
        OpCode::Call => byte_instruction("Call", chunk, offset),
        OpCode::Return => simple_instruction("Return", offset),
        OpCode::BuildList => u16_instruction("BuildList", chunk, offset),
        OpCode::BuildMap => u16_instruction("BuildMap", chunk, offset),
        OpCode::MapKeys => simple_instruction("MapKeys", offset),
        OpCode::MapValues => simple_instruction("MapValues", offset),
        OpCode::ArrayGet => simple_instruction("ArrayGet", offset),
        OpCode::ArraySet => simple_instruction("ArraySet", offset),
        _ => {
            println!("Unknown opcode {}", instruction as u8);
            offset + 1
        }
    }
}

fn simple_instruction(name: &str, offset: usize) -> usize {
    println!("{}", name);
    offset + 1
}

fn byte_instruction(name: &str, chunk: &Chunk, offset: usize) -> usize {
    let slot = chunk.code[offset + 1];
    println!("{:-16} {:4}", name, slot);
    offset + 2
}

fn u16_instruction(name: &str, chunk: &Chunk, offset: usize) -> usize {
    let slot = (chunk.code[offset + 1] as u16) | ((chunk.code[offset + 2] as u16) << 8);
    println!("{:-16} {:4}", name, slot);
    offset + 3
}

fn constant_instruction(name: &str, chunk: &Chunk, offset: usize) -> usize {
    let constant_idx = (chunk.code[offset + 1] as u16) | ((chunk.code[offset + 2] as u16) << 8);
    print!("{:-16} {:4} '", name, constant_idx);
    if let Some(value) = chunk.constants.get(constant_idx as usize) {
        print_value(value);
    } else {
        print!("INVALID");
    }
    println!("'");
    offset + 3
}

fn jump_instruction(name: &str, sign: i16, chunk: &Chunk, offset: usize) -> usize {
    let jump = (chunk.code[offset + 1] as u16) | ((chunk.code[offset + 2] as u16) << 8);
    println!("{:-16} {:4} -> {}", name, offset, (offset as i32) + 3 + (sign as i32) * (jump as i32));
    offset + 3
}
