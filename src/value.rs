use std::collections::HashMap;

pub type ObjRef = usize;

#[derive(Clone, Copy, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    Obj(ObjRef),
}

impl Value {
    pub fn is_falsey(self) -> bool {
        matches!(self, Value::Null) || matches!(self, Value::Bool(false))
    }
}

#[derive(Clone)]
pub struct FunctionObj {
    pub name: Option<String>,
    pub arity: usize,
    pub locals: usize,
    pub chunk: crate::chunk::Chunk,
    pub jit: bool,
    pub exports: Vec<String>,
    pub is_module: bool,
}

#[derive(Clone)]
pub struct NativeObj {
    pub name: String,
    pub arity: isize,
    pub fun: fn(&mut crate::vm::VM, Vec<Value>) -> Result<Value, String>,
}

#[derive(Clone)]
pub enum Obj {
    String(String),
    StrBuf(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Function(FunctionObj),
    Native(NativeObj),
}

impl Obj {
    pub fn type_name(&self) -> &'static str {
        match self {
            Obj::String(_) => "string",
            Obj::StrBuf(_) => "strbuf",
            Obj::List(_) => "list",
            Obj::Map(_) => "map",
            Obj::Function(_) => "function",
            Obj::Native(_) => "native",
        }
    }
}

// 글로벌 슬롯: name -> u16
#[derive(Default)]
pub struct Intern {
    pub globals: HashMap<String, u16>,
    pub global_names: Vec<String>,
}
