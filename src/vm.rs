use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::chunk::Chunk;
use crate::compiler::Compiler;
use crate::error::{NsError, SourceMap};
use crate::jit::JitEngine;
use crate::opcode::OpCode;
use crate::value::{FunctionObj, Intern, NativeObj, Obj, ObjRef, Value};

const FRAMES_MAX: usize = 64;
const STACK_MAX: usize = 1024;

#[derive(Clone)]
struct CallFrame {
    fun: ObjRef,
    ip: usize,
    base: usize,
}

pub struct VM {
    heap: Vec<Obj>,
    stack: Vec<Value>,
    frames: Vec<CallFrame>,
    intern: Intern,
    globals: Vec<Value>,
    globals_defined: Vec<bool>,
    jit_engine: JitEngine,
}

impl VM {
    pub fn new() -> Self {
        let mut vm = Self {
            heap: vec![],
            stack: Vec::with_capacity(STACK_MAX),
            frames: Vec::with_capacity(FRAMES_MAX),
            intern: Intern::default(),
            globals: vec![],
            globals_defined: vec![],
            jit_engine: JitEngine::new(),
        };
        vm.define_natives();
        vm
    }

    fn deep_copy_value_from(&mut self, value: Value, from_vm: &VM, copied: &mut HashMap<ObjRef, ObjRef>) -> Result<Value, String> {
        Ok(match value {
            Value::Obj(obj_ref) => {
                if let Some(new_ref) = copied.get(&obj_ref) {
                    return Ok(Value::Obj(*new_ref));
                }
                
                let new_obj_val = match &from_vm.heap[obj_ref] {
                    Obj::String(s) => Obj::String(s.clone()),
                    Obj::List(list) => {
                        let new_list_ref = self.alloc(Obj::List(Vec::new()));
                        copied.insert(obj_ref, new_list_ref); // Insert early to handle cycles
                        let mut new_list = Vec::with_capacity(list.len());
                        for val in list {
                            new_list.push(self.deep_copy_value_from(*val, from_vm, copied)?);
                        }
                        self.heap[new_list_ref] = Obj::List(new_list); // Update the placeholder
                        return Ok(Value::Obj(new_list_ref));
                    }
                    Obj::Map(map) => {
                        let new_map_ref = self.alloc(Obj::Map(HashMap::new()));
                        copied.insert(obj_ref, new_map_ref); // Insert early to handle cycles
                        let mut new_map = HashMap::with_capacity(map.len());
                        for (k, v) in map {
                            new_map.insert(k.clone(), self.deep_copy_value_from(*v, from_vm, copied)?);
                        }
                        self.heap[new_map_ref] = Obj::Map(new_map); // Update the placeholder
                        return Ok(Value::Obj(new_map_ref));
                    }
                    Obj::Function(fun) => {
                        let new_fun_ref = self.alloc(Obj::String("temp".to_string())); // Placeholder: dummy value, will be replaced
                        copied.insert(obj_ref, new_fun_ref); // Insert early to handle cycles

                        let mut new_chunk = fun.chunk.clone();
                        for constant in &mut new_chunk.constants {
                            *constant = self.deep_copy_value_from(*constant, from_vm, copied)?;
                        }

                        let new_fun_obj = Obj::Function(FunctionObj {
                            chunk: new_chunk,
                            ..fun.clone()
                        });
                        self.heap[new_fun_ref] = new_fun_obj; // Replace the placeholder
                        return Ok(Value::Obj(new_fun_ref));
                    }
                    Obj::Native(_) => return Err("Cannot copy native functions between VMs.".to_string()),
                    Obj::StrBuf(_) => return Err("StrBuf is deprecated and cannot be copied.".to_string()),
                };
                Value::Obj(self.alloc(new_obj_val)) // Allocate the new object and return its ref
            }
            _ => value, // Primitives can be copied directly
        })
    }

    pub fn repl(&mut self) {
        let mut line = String::new();
        loop {
            line.clear();
            print!("ns> ");
            let _ = io::stdout().flush();
            if io::stdin().read_line(&mut line).is_err() { break; }
            let t = line.trim();
            if t.is_empty() { continue; }
            if t == ":q" || t == ":quit" { break; }

            let map = SourceMap::new("<repl>", t);
            let result_run = self.run_map(&map);
            match result_run {
                Ok(_) => {
                    if let Ok(result_val) = self.pop(&map, 0) {
                        if let Err(s) = self.value_to_string(result_val) {
                            eprintln!("[NS:ERROR] {}", s);
                        }
                    } else {
                        eprintln!("[NS:ERROR] Failed to retrieve result after successful execution in REPL.");
                    }
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }
    }

    pub fn run_map(&mut self, map: &SourceMap) -> Result<(), NsError> {
        let mut fun = Compiler::compile(map)?;
        self.heapify_chunk_recursive(&mut fun.chunk)?;
        self.patch_globals_recursive(&mut fun.chunk)?;
        let script_ref = self.alloc(Obj::Function(fun));
        self.push(Value::Obj(script_ref));
        self.call(script_ref, 0).map_err(|e| NsError::new(map, 0, e, 1))?;
        self.run(map)
    }

    fn alloc(&mut self, o: Obj) -> ObjRef {
        self.heap.push(o);
        self.heap.len() - 1
    }

    fn intern_global(&mut self, name: &str) -> u16 {
        if let Some(id) = self.intern.globals.get(name) { return *id; }
        let id = self.intern.global_names.len() as u16;
        self.intern.globals.insert(name.to_string(), id);
        self.intern.global_names.push(name.to_string());
        self.globals.push(Value::Null);
        self.globals_defined.push(false);
        id
    }

    fn global_get(&self, id: u16, map: &SourceMap, current_chunk: &Chunk, bytecode_pos: usize) -> Result<Value, NsError> {
        let i = id as usize;
        if i >= self.globals.len() || !self.globals_defined[i] {
            let name = self.intern.global_names.get(i).cloned().unwrap_or_else(|| "<unknown>".to_string());
            return Err(NsError::new(map, current_chunk.source_offsets[bytecode_pos], format!("Undefined variable '{}'.", name), 1));
        }
        Ok(self.globals[i])
    }

    fn global_set(&mut self, id: u16, v: Value, map: &SourceMap, current_chunk: &Chunk, bytecode_pos: usize) -> Result<(), NsError> {
        let i = id as usize;
        if i >= self.globals.len() || !self.globals_defined[i] {
            let name = self.intern.global_names.get(i).cloned().unwrap_or_else(|| "<unknown>".to_string());
            return Err(NsError::new(map, current_chunk.source_offsets[bytecode_pos], format!("Undefined variable '{}'.", name), 1));
        }
        self.globals[i] = v;
        Ok(())
    }

    fn global_define(&mut self, id: u16, v: Value) {
        let i = id as usize;
        if i >= self.globals.len() {
            self.globals.resize(i + 1, Value::Null);
            self.globals_defined.resize(i + 1, false);
        }
        self.globals[i] = v;
        self.globals_defined[i] = true;
    }

    #[inline] fn push(&mut self, v: Value) { self.stack.push(v); }

    pub fn pop(&mut self, map: &SourceMap, source_pos: usize) -> Result<Value, NsError> {
        if self.stack.is_empty() {
            return Err(NsError::new(map, source_pos, "Stack underflow.", 1));
        }
        Ok(self.stack.pop().unwrap())
    }

    #[inline] fn peek(&self, dist: usize) -> Value {
        self.stack[self.stack.len() - 1 - dist]
    }

    fn call(&mut self, fun_ref: ObjRef, arg_count: usize) -> Result<(), String> {
        let arity = match &self.heap[fun_ref] {
            Obj::Function(f) => f.arity,
            Obj::Native(n) => n.arity as usize, // Correctly handle native function arity
            _ => return Err("Not a function".into()),
        };
        if arg_count != arity && arity != -1isize as usize { // -1 arity for varargs natives
            return Err(format!("Expected {} arguments but got {}.
", arity, arg_count));
        }
        if self.frames.len() >= FRAMES_MAX {
            return Err("Stack overflow.".into());
        }
        let base = self.stack.len() - arg_count - 1;
        self.frames.push(CallFrame { fun: fun_ref, ip: 0, base });
        Ok(())
    }

    fn call_value(&mut self, callee: Value, arg_count: usize, map: &SourceMap, source_pos: usize) -> Result<(), NsError> {
        match callee {
            Value::Obj(r) => {
                let obj_clone = self.heap[r].clone();
                match obj_clone {
                    Obj::Native(n) => {
                        if n.arity >= 0 && arg_count as isize != n.arity {
                            return Err(NsError::new(map, source_pos, format!("Expected {} arguments but got {}.\n", n.arity, arg_count), 1));
                        }
                        let args_start = self.stack.len() - arg_count;
                        let args = self.stack[args_start..].to_vec();
                        self.stack.truncate(args_start - 1);
                        let fun = n.fun;
                        let result = fun(self, args).map_err(|e| NsError::new(map, source_pos, e, 1))?;
                        self.push(result);
                        Ok(())
                    }
                    Obj::Function(f) => {
                        if f.jit {
                            if let Some(jit_fn) = self.jit_engine.get_or_compile(r, &f) {
                                return self.run_jit_fn(jit_fn, arg_count, map, source_pos);
                            }
                        }
                        self.call(r, arg_count).map_err(|e| NsError::new(map, source_pos, e, 1))
                    }
                    _ => Err(NsError::new(map, source_pos, "Can only call functions.", 1)),
                }
            },
            _ => Err(NsError::new(map, source_pos, "Can only call functions.", 1)),
        }
    }
    
    fn run_jit_fn(&mut self, jit_fn: crate::jit::CompiledJit, arg_count: usize, map: &SourceMap, source_pos: usize) -> Result<(), NsError> {
        let args_start = self.stack.len() - arg_count;
        let mut f64_stack = Vec::with_capacity(jit_fn.max_stack);
        f64_stack.push(0.0);
        for i in 0..arg_count {
            match self.stack[args_start + i] {
                Value::Number(n) => f64_stack.push(n),
                _ => {
                    let fun_ref = match self.peek(arg_count) {
                        Value::Obj(r) => r,
                        _ => return Err(NsError::new(map, source_pos, "JIT call error.", 1)),
                    };
                    eprintln!("[NS:WARN] JIT function called with non-numeric arguments. Falling back to interpreter.");
                    return self.call(fun_ref, arg_count).map_err(|e| NsError::new(map, source_pos, e, 1));
                }
            }
        }
        self.stack.truncate(args_start - 1);
        let result_val = unsafe { (jit_fn.f)(f64_stack.as_mut_ptr()) };
        self.push(Value::Number(result_val));
        Ok(())
    }

    fn as_number(v: Value, map: &SourceMap, source_pos: usize) -> Result<f64, NsError> {
        match v {
            Value::Number(n) => Ok(n),
            _ => Err(NsError::new(map, source_pos, "Operand must be a number.", 1)),
        }
    }

    pub fn value_to_string(&self, v: Value) -> Result<String, String> {
        Ok(match v {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => {
                if (n.fract() - 0.0).abs() < f64::EPSILON { format!("{}", n as i64) } else { format!("{n}") }
            }
            Value::Obj(r) => match &self.heap[r] {
                Obj::String(s) => s.clone(),
                Obj::StrBuf(s) => s.clone(),
                Obj::List(a) => {
                    let mut s = "[".to_string();
                    for (i, item) in a.iter().enumerate() {
                        if i > 0 { s.push_str(", "); }
                        s.push_str(&self.value_to_string(*item)?);
                    }
                    s.push(']');
                    s
                }
                Obj::Map(m) => {
                    let mut s = "{".to_string();
                    let mut first = true;
                    for (k, v) in m.iter() {
                        if !first { s.push_str(", "); }
                        first = false;
                        s.push_str(&format!("\"{}\": {}", k, self.value_to_string(*v)?));
                    }
                    s.push('}');
                    s
                }
                Obj::Function(f) => match &f.name { Some(n) => format!("<fn {n}>"), None => "<fn>".to_string() },
                Obj::Native(n) => format!("<native fn {}>", n.name),
            }
        })
    }

    fn values_equal(&self, a: Value, b: Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(x), Value::Bool(y)) => x == y,
            (Value::Number(x), Value::Number(y)) => x == y,
            (Value::Obj(x), Value::Obj(y)) => x == y,
            _ => false,
        }
    }

    fn heapify_chunk_recursive(&mut self, chunk: &mut Chunk) -> Result<(), NsError> {
        let mut map: Vec<usize> = Vec::with_capacity(chunk.objs.len());
        for i in 0..chunk.objs.len() {
            let obj = std::mem::replace(&mut chunk.objs[i], Obj::String("".into()));
            if let Obj::Function(mut f) = obj {
                self.heapify_chunk_recursive(&mut f.chunk)?;
                let new_idx = self.alloc(Obj::Function(f));
                map.push(new_idx);
            } else {
                let new_idx = self.alloc(obj);
                map.push(new_idx);
            }
        }
        chunk.objs.clear();
        for c in chunk.constants.iter_mut() {
            if let Value::Obj(i) = c {
                if *i < map.len() {
                    *i = map[*i];
                }
            }
        }
        Ok(())
    }

    fn patch_globals_recursive(&mut self, chunk: &mut Chunk) -> Result<(), NsError> {
        self.patch_globals_in_chunk(chunk);
        let fun_refs: Vec<ObjRef> = chunk.constants.iter().filter_map(|c| {
            if let Value::Obj(r) = c {
                if let Some(Obj::Function(f)) = self.heap.get(*r) {
                    if !f.is_module {
                        return Some(*r);
                    }
                }
            }
            None
        }).collect();
        for r in fun_refs {
            let mut fun_clone = if let Some(Obj::Function(f)) = self.heap.get(r) { f.clone() } else { continue; };
            if fun_clone.is_module {
                continue;
            }
            self.patch_globals_recursive(&mut fun_clone.chunk)?;
            if let Some(Obj::Function(f)) = self.heap.get_mut(r) {
                *f = fun_clone;
            }
        }
        Ok(())
    }

    fn patch_globals_in_chunk(&mut self, chunk: &mut Chunk) {
        let mut ip = 0usize;
        while ip < chunk.code.len() {
            let op = OpCode::from_u8(chunk.code[ip]);
            ip += 1;
            match op {
                OpCode::Constant | OpCode::GetLocal | OpCode::SetLocal |
                OpCode::GetGlobal | OpCode::DefineGlobal | OpCode::SetGlobal |
                OpCode::Jump | OpCode::JumpIfFalse | OpCode::Loop |
                OpCode::BuildList | OpCode::BuildMap | OpCode::CreateModule |
                OpCode::AppendLocal | OpCode::AppendGlobal => { // Added AppendLocal, AppendGlobal
                    let pos = ip;
                    let old = Chunk::read_u16(&chunk.code, &mut ip);
                    if matches!(op, OpCode::GetGlobal | OpCode::DefineGlobal | OpCode::SetGlobal) {
                        if let Some(name) = chunk.names.get(old as usize) {
                            let slot = self.intern_global(name);
                            Chunk::write_u16_at(&mut chunk.code, pos, slot);
                        }
                    }
                }
                OpCode::Call => { ip += 1; }
                _ => {} 
            }
        }
        chunk.names.clear();
    }

    fn define_native(&mut self, name: &str, arity: isize, fun: fn(&mut VM, Vec<Value>) -> Result<Value, String>) {
        let r = self.alloc(Obj::Native(NativeObj { name: name.to_string(), arity, fun }));
        let gid = self.intern_global(name);
        self.global_define(gid, Value::Obj(r));
    }

    fn define_natives(&mut self) {
        self.define_native("clock", 0, |_, _| {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            Ok(Value::Number(now.as_secs_f64()))
        });

        self.define_native("print", -1, |vm: &mut VM, args: Vec<Value>| {
            for (i, arg) in args.iter().enumerate() {
                if i > 0 { print!(" "); }
                print!("{}", vm.value_to_string(*arg)?);
            }
            println!();
            Ok(Value::Null)
        });

        self.define_native("len", 1, |vm: &mut VM, args: Vec<Value>| {
            let len = match args[0] {
                Value::Obj(r) => match &vm.heap[r] {
                    Obj::String(s) => s.len(),
                    Obj::List(l) => l.len(),
                    Obj::Map(m) => m.len(),
                    _ => return Err("len() argument must be a string, list, or map.".into())
                }
                _ => return Err("len() argument must be a string, list, or map.".into()),
            };
            Ok(Value::Number(len as f64))
        });

        self.define_native("type", 1, |vm: &mut VM, args: Vec<Value>| {
            let type_name = match args[0] {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::Obj(r) => vm.heap[r].type_name(),
            };
            let s_obj = Obj::String(type_name.to_string());
            let s_ref = vm.alloc(s_obj);
            Ok(Value::Obj(s_ref))
        });
        
        self.define_native("str", 1, |vm: &mut VM, args: Vec<Value>| {
            let s = vm.value_to_string(args[0])?;
            let s_obj = Obj::String(s);
            let s_ref = vm.alloc(s_obj);
            Ok(Value::Obj(s_ref))
        });
    }

    fn run(&mut self, map: &SourceMap) -> Result<(), NsError> {
        loop {
            if self.frames.is_empty() { return Ok(()); }

            let fi = self.frames.len() - 1;
            let current_fun_ref = self.frames[fi].fun;
            let mut ip = self.frames[fi].ip;
            let base = self.frames[fi].base;

            let fun_clone = if let Obj::Function(f) = &self.heap[current_fun_ref] {
                f.clone()
            } else {
                return Err(NsError::new(map, 0, "Not a function", 1));
            };
            let (code, constants, current_chunk) = (&fun_clone.chunk.code, &fun_clone.chunk.constants, &fun_clone.chunk);
            
            macro_rules! read_u16_ip {
                () => {{ 
                    let val = (code[ip] as u16) | ((code[ip+1] as u16) << 8);
                    ip += 2;
                    val
                }};
            }

            let op = OpCode::from_u8(code[ip]);
            let bytecode_pos = ip;
            if bytecode_pos >= current_chunk.source_offsets.len() {
                panic!("SOURCE_OFFSETS MISMATCH (ip={}): code={}, lines={}, offsets={}", 
                    ip, code.len(), current_chunk.lines.len(), current_chunk.source_offsets.len());
            }
            let source_pos = current_chunk.source_offsets[bytecode_pos];
            ip += 1;

            match op {
                OpCode::Constant => { let idx = read_u16_ip!() as usize; self.push(constants[idx]); }
                OpCode::Null => self.push(Value::Null),
                OpCode::True => self.push(Value::Bool(true)),
                OpCode::False => self.push(Value::Bool(false)),
                OpCode::Pop => { self.pop(map, source_pos)?; }
                OpCode::GetLocal => {
                    let slot = read_u16_ip!() as usize;
                    let idx = base + slot;
                    if idx >= self.stack.len() {
                        return Err(NsError::new(map, source_pos, "Invalid local slot.", 1));
                    }
                    self.push(self.stack[idx]);
                }
                OpCode::SetLocal => {
                    let slot = read_u16_ip!() as usize;
                    let idx = base + slot;
                    if idx >= self.stack.len() {
                        return Err(NsError::new(map, source_pos, "Invalid local slot.", 1));
                    }
                    self.stack[idx] = self.peek(0);
                }
                OpCode::GetGlobal => { let id = read_u16_ip!(); let v = self.global_get(id, map, current_chunk, bytecode_pos)?; self.push(v); }
                OpCode::DefineGlobal => { let id = read_u16_ip!(); self.global_define(id, self.peek(0)); self.pop(map, source_pos)?; }
                OpCode::SetGlobal => { let id = read_u16_ip!(); let v = self.peek(0); self.global_set(id, v, map, current_chunk, bytecode_pos)?; }
                OpCode::AppendLocal => {
                    let slot_idx = read_u16_ip!() as usize;
                    let rhs_value = self.pop(map, source_pos)?;
                    let idx = base + slot_idx;
                    if idx >= self.stack.len() {
                        return Err(NsError::new(map, source_pos, "Invalid local slot.", 1));
                    }
                    let target_value_ref = &mut self.stack[idx];
                    match (target_value_ref, rhs_value) {
                        (Value::Number(num1), Value::Number(num2)) => { *num1 += num2; }
                        (Value::Obj(target_obj_ref), Value::Obj(rhs_obj_ref)) => {
                            let rhs_string_content: String;
                            if let Obj::String(rhs_s) = &self.heap[rhs_obj_ref] { rhs_string_content = rhs_s.clone(); }
                            else { return Err(NsError::new(map, source_pos, "AppendLocal: RHS must be a string or number.", 1)); }
                            let target_obj_ptr = &mut self.heap[*target_obj_ref];
                            if let Obj::String(target_s) = target_obj_ptr { target_s.push_str(&rhs_string_content); } 
                            else { return Err(NsError::new(map, source_pos, "AppendLocal: Target must be a string or number.", 1)); }
                        }
                        _ => { return Err(NsError::new(map, source_pos, "AppendLocal: Operands must be two numbers or two strings.", 1)); }
                    }
                }
                OpCode::AppendGlobal => {
                    let global_id = read_u16_ip!();
                    let rhs_value = self.pop(map, source_pos)?;
                    let target_value_ref = &mut self.globals[global_id as usize];
                    match (target_value_ref, rhs_value) {
                        (Value::Number(num1), Value::Number(num2)) => { *num1 += num2; }
                        (Value::Obj(target_obj_ref), Value::Obj(rhs_obj_ref)) => {
                            let rhs_string_content: String;
                            if let Obj::String(rhs_s) = &self.heap[rhs_obj_ref] { rhs_string_content = rhs_s.clone(); }
                            else { return Err(NsError::new(map, source_pos, "AppendGlobal: RHS must be a string or number.", 1)); }
                            let target_obj_ptr = &mut self.heap[*target_obj_ref];
                            if let Obj::String(target_s) = target_obj_ptr { target_s.push_str(&rhs_string_content); } 
                            else { return Err(NsError::new(map, source_pos, "AppendGlobal: Target must be a string or number.", 1)); }
                        }
                        _ => { return Err(NsError::new(map, source_pos, "AppendGlobal: Operands must be two numbers or two strings.", 1)); }
                    }
                }
                OpCode::ListNew => { let r = self.alloc(Obj::List(vec![])); self.push(Value::Obj(r)); }
                OpCode::ArrayGet => {
                    let idxv = self.pop(map, source_pos)?;
                    let recv = self.pop(map, source_pos)?;
                    match recv {
                        Value::Obj(r) => match &self.heap[r] {
                            Obj::List(a) => {
                                let i = VM::as_number(idxv, map, source_pos)? as isize;
                                let ui = if i < 0 { return Err(NsError::new(map, source_pos, "Index must be >= 0", 1)); } else { i as usize };
                                self.push(*a.get(ui).unwrap_or(&Value::Null));
                            }
                            Obj::Map(m) => {
                                if let Value::Obj(key_ref) = idxv {
                                    if let Obj::String(key) = &self.heap[key_ref] { self.push(*m.get(key).unwrap_or(&Value::Null)); } 
                                    else { return Err(NsError::new(map, source_pos, "Map key must be a string.", 1)); }
                                } else { return Err(NsError::new(map, source_pos, "Map key must be a string.", 1)); }
                            }
                            _ => return Err(NsError::new(map, source_pos, "Indexing only supported on lists or maps.", 1)),
                        }
                        _ => return Err(NsError::new(map, source_pos, "Indexing only supported on lists or maps.", 1)),
                    }
                }
                OpCode::ArraySet => {
                    let val = self.pop(map, source_pos)?;
                    let idx = self.pop(map, source_pos)?;
                    let recv = self.pop(map, source_pos)?;
                    let key_string = if let Value::Obj(key_ref) = idx {
                        if let Obj::String(s) = &self.heap[key_ref] { Some(s.clone()) } else { None }
                    } else { None };
                    if let Value::Obj(r) = recv {
                        match self.heap.get_mut(r) {
                            Some(Obj::List(a)) => {
                                let i = VM::as_number(idx, map, source_pos)? as isize;
                                let ui = if i < 0 { return Err(NsError::new(map, source_pos, "Index must be >= 0", 1)); } else { i as usize };
                                if ui >= a.len() { a.resize(ui + 1, Value::Null); }
                                a[ui] = val;
                            }
                            Some(Obj::Map(m)) => {
                                if let Some(key) = key_string { m.insert(key, val); } 
                                else { return Err(NsError::new(map, source_pos, "Map key must be a string.", 1)); }
                            }
                            _ => return Err(NsError::new(map, source_pos, "Index assignment only supported on lists or maps.", 1)),
                        }
                    } else { return Err(NsError::new(map, source_pos, "Index assignment only supported on lists or maps.", 1)); }
                    self.push(val);
                }
                OpCode::ArrayLen => {
                    let recv = self.pop(map, source_pos)?;
                    let len = if let Value::Obj(r) = recv {
                        match &self.heap[r] {
                            Obj::String(s) => s.len(),
                            Obj::List(l) => l.len(),
                            Obj::Map(m) => m.len(),
                            _ => return Err(NsError::new(map, source_pos, "length property only on lists/strings.", 1)),
                        }
                    } else { return Err(NsError::new(map, source_pos, "length property on non-object.", 1)); };
                    self.push(Value::Number(len as f64));
                }
                OpCode::ArrayPush => {
                    let val = self.peek(0);
                    let recv = self.peek(1);
                    if let Value::Obj(r) = recv {
                        if let Some(Obj::List(a)) = self.heap.get_mut(r) { a.push(val); } 
                        else { return Err(NsError::new(map, source_pos, "push() only on lists.", 1)); }
                    } else { return Err(NsError::new(map, source_pos, "push() on non-object.", 1)); }
                    self.pop(map, source_pos)?;
                }
                OpCode::ArrayPop => {
                    let recv = self.peek(0);
                    let popped = if let Value::Obj(r) = recv {
                        if let Some(Obj::List(a)) = self.heap.get_mut(r) { a.pop().unwrap_or(Value::Null) } 
                        else { return Err(NsError::new(map, source_pos, "pop() only on lists.", 1)); }
                    } else { return Err(NsError::new(map, source_pos, "pop() on non-object.", 1)); };
                    self.pop(map, source_pos)?;
                    self.push(popped);
                }
                OpCode::BuildList => {
                    let item_count = read_u16_ip!() as usize;
                    let mut list = Vec::with_capacity(item_count);
                    let stack_top = self.stack.len();
                    for i in 0..item_count {
                        list.push(self.stack[stack_top - item_count + i]);
                    }
                    self.stack.truncate(stack_top - item_count);
                    let list_ref = self.alloc(Obj::List(list));
                    self.push(Value::Obj(list_ref));
                }
                OpCode::BuildMap => {
                    let item_count = read_u16_ip!() as usize;
                    let mut map_obj = HashMap::with_capacity(item_count);
                    for _ in 0..item_count {
                        let val = self.pop(map, source_pos)?;
                        let key_val = self.pop(map, source_pos)?;
                        if let Value::Obj(r) = key_val {
                            if let Obj::String(s) = &self.heap[r] { map_obj.insert(s.clone(), val); } 
                            else { return Err(NsError::new(map, source_pos, "Map keys must be strings.", 1)); }
                        } else { return Err(NsError::new(map, source_pos, "Map keys must be strings.", 1)); }
                    }
                    let map_ref = self.alloc(Obj::Map(map_obj));
                    self.push(Value::Obj(map_ref));
                }
                OpCode::CreateModule => {
                    let module_fun_val = self.pop(map, source_pos)?;
                    let module_fun_obj = if let Value::Obj(r) = module_fun_val {
                        if let Obj::Function(f) = &self.heap[r] { f.clone() } 
                        else { return Err(NsError::new(map, source_pos, "Module must be a function object.", 1)); }
                    } else { return Err(NsError::new(map, source_pos, "Module must be a function object.", 1)); };

                    let mut module_vm = VM::new();
                    let new_module_fun_ref = {
                        let mut copied = HashMap::new();
                        let copied_val = module_vm.deep_copy_value_from(module_fun_val, self, &mut copied)
                            .map_err(|e| NsError::new(map, source_pos, e, 1))?;
                        if let Value::Obj(r) = copied_val { r } else { unreachable!() }
                    };

                    if let Some(Obj::Function(fun)) = module_vm.heap.get_mut(new_module_fun_ref) {
                        let mut chunk = std::mem::replace(&mut fun.chunk, Chunk::new());
                        module_vm.patch_globals_recursive(&mut chunk)?;
                        fun.chunk = chunk;
                    } else {
                        return Err(NsError::new(map, source_pos, "Module must be a function object.", 1));
                    }

                    module_vm.push(Value::Obj(new_module_fun_ref));
                    module_vm.call(new_module_fun_ref, 0).map_err(|e| NsError::new(map, source_pos, e, 1))?;
                    if let Err(e) = module_vm.run(map) {
                         return Err(NsError::new(map, source_pos, format!("Error in module:\n{}", e), 1));
                    }

                    let mut exports_map = HashMap::new();
                    for name in &module_fun_obj.exports {
                        if let Some(gid) = module_vm.intern.globals.get(name) {
                            let val = module_vm.globals[*gid as usize];
                            let mut copied = HashMap::new();
                            let copied_val = self.deep_copy_value_from(val, &module_vm, &mut copied)
                                .map_err(|e| NsError::new(map, source_pos, e, 1))?;
                            exports_map.insert(name.clone(), copied_val);
                        }
                    }

                    let map_ref = self.alloc(Obj::Map(exports_map));
                    self.push(Value::Obj(map_ref));
                }
                OpCode::MapKeys => {
                    let recv = self.peek(0);
                    let r = match recv { Value::Obj(r) => r, _ => return Err(NsError::new(map, source_pos, "keys() only supported on maps.", 1)) };
                    let string_keys: Vec<String> = match &self.heap[r] {
                        Obj::Map(m) => m.keys().cloned().collect(),
                        _ => return Err(NsError::new(map, source_pos, "keys() only supported on maps.", 1)),
                    };
                    self.pop(map, source_pos)?;
                    let mut value_keys = Vec::with_capacity(string_keys.len());
                    for k_str in string_keys {
                        value_keys.push(Value::Obj(self.alloc(Obj::String(k_str))));
                    }
                    let list_ref = self.alloc(Obj::List(value_keys));
                    self.push(Value::Obj(list_ref));
                }
                OpCode::MapValues => {
                    let recv = self.peek(0);
                    let r = match recv { Value::Obj(r) => r, _ => return Err(NsError::new(map, source_pos, "values() only supported on maps.", 1)) };
                    let values_list = match &self.heap[r] {
                        Obj::Map(m) => m.values().cloned().collect(),
                        _ => return Err(NsError::new(map, source_pos, "values() only supported on maps.", 1)),
                    };
                    self.pop(map, source_pos)?;
                    let list_ref = self.alloc(Obj::List(values_list));
                    self.push(Value::Obj(list_ref));
                }
                OpCode::StrBufNew | OpCode::StrBufAppend | OpCode::StrBufToString | OpCode::ToString => { return Err(NsError::new(map, source_pos, "String buffer ops are deprecated.", 1)); }
                OpCode::Equal => { let b = self.pop(map, source_pos)?; let a = self.pop(map, source_pos)?; self.push(Value::Bool(self.values_equal(a, b))); }
                OpCode::Greater => { let b = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Bool(a > b)); }
                OpCode::Less => { let b = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Bool(a < b)); }
                OpCode::Add => {
                    let b = self.peek(0); let a = self.peek(1);
                    if let (Value::Number(x), Value::Number(y)) = (a, b) {
                        self.pop(map, source_pos)?; self.pop(map, source_pos)?; self.push(Value::Number(x + y));
                    } else if let (Value::Obj(ar), Value::Obj(br)) = (a, b) {
                        let mut res = self.value_to_string(Value::Obj(ar))?;
                        res.push_str(&self.value_to_string(Value::Obj(br))?);
                        let s_ref = self.alloc(Obj::String(res));
                        self.pop(map, source_pos)?; self.pop(map, source_pos)?;
                        self.push(Value::Obj(s_ref));
                    } else { return Err(NsError::new(map, source_pos, "Operands must be two numbers or two strings.", 1)); }
                }
                OpCode::Sub => { let b = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Number(a - b)); }
                OpCode::Mul => { let b = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Number(a * b)); }
                OpCode::Div => { let b = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Number(a / b)); }
                OpCode::Not => { let v = self.pop(map, source_pos)?; self.push(Value::Bool(v.is_falsey())); }
                OpCode::Negate => { let a = VM::as_number(self.pop(map, source_pos)?, map, source_pos)?; self.push(Value::Number(-a)); }
                OpCode::Jump => { let off = read_u16_ip!() as usize; ip += off; }
                OpCode::JumpIfFalse => { let off = read_u16_ip!() as usize; if self.peek(0).is_falsey() { ip += off; } }
                OpCode::Loop => { let off = read_u16_ip!() as usize; ip -= off; }
                OpCode::Call => {
                    let argc = code[ip] as usize; ip += 1;
                    self.call_value(self.peek(argc), argc, map, source_pos)?;
                }
                OpCode::Return => {
                    let result = self.pop(map, source_pos)?;
                    let old_frame = self.frames.pop().unwrap();
                    self.stack.truncate(old_frame.base);
                    self.push(result);
                    if self.frames.is_empty() { return Ok(()); }
                }
            }
            if let Some(frame) = self.frames.last_mut() {
                frame.ip = ip;
            }
        }
    }
}
