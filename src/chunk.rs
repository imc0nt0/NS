use crate::opcode::OpCode;
use crate::value::{Obj, ObjRef, Value};

#[derive(Clone)]
pub struct Chunk {
    pub code: Vec<u8>,
    pub lines: Vec<u32>,
    pub source_offsets: Vec<usize>,
    pub constants: Vec<Value>,
    pub objs: Vec<Obj>,
    pub names: Vec<String>,
}

impl Chunk {
    pub fn new() -> Self {
        Self { code: vec![], lines: vec![], source_offsets: vec![], constants: vec![], objs: vec![], names: vec![] }
    }

    pub fn write_op(&mut self, op: OpCode, line: u32) {
        self.code.push(op as u8);
        self.lines.push(line);
    }

    pub fn write_u8(&mut self, b: u8, line: u32) {
        self.code.push(b);
        self.lines.push(line);
    }

    pub fn write_u16(&mut self, v: u16, line: u32) {
        self.code.push((v & 0xff) as u8);
        self.lines.push(line);
        self.code.push((v >> 8) as u8);
        self.lines.push(line);
    }

    pub fn add_const(&mut self, v: Value) -> u16 {
        self.constants.push(v);
        (self.constants.len() - 1) as u16
    }

    pub fn add_obj(&mut self, o: Obj) -> ObjRef {
        self.objs.push(o);
        self.objs.len() - 1
    }

    pub fn name_id(&mut self, s: &str) -> u16 {
        if let Some((i, _)) = self.names.iter().enumerate().find(|(_, n)| n.as_str() == s) {
            return i as u16;
        }
        self.names.push(s.to_string());
        (self.names.len() - 1) as u16
    }

    pub fn read_u16(code: &[u8], ip: &mut usize) -> u16 {
        let lo = code[*ip] as u16;
        let hi = code[*ip + 1] as u16;
        *ip += 2;
        lo | (hi << 8)
    }

    pub fn write_u16_at(code: &mut [u8], at: usize, v: u16) {
        code[at] = (v & 0xff) as u8;
        code[at + 1] = (v >> 8) as u8;
    }
}
