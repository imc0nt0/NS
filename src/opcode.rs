#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpCode {
    Constant = 0,
    Null,
    True,
    False,
    Pop,

    GetLocal,
    SetLocal,

    GetGlobal,
    DefineGlobal,
    SetGlobal,

    ListNew,
    ArrayGet,
    ArraySet,
    ArrayLen,
    ArrayPush,
    ArrayPop,
    BuildList,

    BuildMap,
    CreateModule,
    MapKeys,
    MapValues,

    StrBufNew,
    StrBufAppend,
    StrBufToString,
    ToString,

    Equal,
    Greater,
    Less,

    Add,
    Sub,
    Mul,
    Div,

    Not,
    Negate,

    AppendLocal,
    AppendGlobal,

    Jump,
    JumpIfFalse,
    Loop,

    Call,
    Return,
}

impl OpCode {
    pub fn from_u8(b: u8) -> Self {
        unsafe { std::mem::transmute::<u8, OpCode>(b) }
    }
}
