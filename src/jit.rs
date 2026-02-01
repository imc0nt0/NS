use std::collections::HashMap;

use cranelift_codegen::ir::{self, types, AbiParam, InstBuilder, MemFlags};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::{opcode::OpCode, value::{FunctionObj, Value}};

pub type JitFn = unsafe extern "C" fn(*mut f64) -> f64;

#[derive(Clone, Copy)]
pub struct CompiledJit {
    pub f: JitFn,
    pub max_stack: usize,
}

pub struct JitEngine {
    module: JITModule,
    cache: HashMap<usize, CompiledJit>, // fun_ref -> compiled
}

impl JitEngine {
    pub fn new() -> Self {
        let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let module = JITModule::new(builder);
        Self { module, cache: HashMap::new() }
    }

    pub fn get_or_compile(&mut self, fun_ref: usize, fun: &FunctionObj) -> Option<CompiledJit> {
        if let Some(c) = self.cache.get(&fun_ref) {
            return Some(*c);
        }
        if !is_numeric_only(fun) {
            return None;
        }

        let max_stack = compute_max_stack(fun).ok()?;
        let fptr = self.compile_numeric(fun_ref, fun).ok()?;

        let compiled = CompiledJit { f: fptr, max_stack: max_stack.max(fun.arity + 4) };
        self.cache.insert(fun_ref, compiled);
        Some(compiled)
    }

    fn compile_numeric(&mut self, fun_ref: usize, fun: &FunctionObj) -> Result<JitFn, String> {
        // signature: fn(stack_ptr: *mut f64) -> f64
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.module.target_config().pointer_type()));
        sig.returns.push(AbiParam::new(types::F64));

        let name = format!("ns_jit_{}", fun_ref);
        let func_id = self.module.declare_function(&name, Linkage::Local, &sig).map_err(|e: cranelift_module::ModuleError| e.to_string())?;

        let mut ctx = self.module.make_context();
        ctx.func.signature = sig;

        let mut fb_ctx = FunctionBuilderContext::new();
        let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);

        let entry = b.create_block();
        b.append_block_params_for_function_params(entry);
        b.switch_to_block(entry);
        b.seal_block(entry);

        let stack_ptr = b.block_params(entry)[0];

        let code = &fun.chunk.code;
        let lines = &fun.chunk.lines;
        let constants = &fun.chunk.constants;

        let start = compute_instr_starts(code).map_err(|e| e)?;
        let blocks: Vec<ir::Block> = (0..=code.len()).map(|_| b.create_block()).collect();

        // 각 블록은 top(i64) 파라미터 1개를 받는다
        for ip in 0..=code.len() {
            b.append_block_param(blocks[ip], types::I64);
        }

        // entry에서 초기 top=arity+1로 시작 블록(0)으로 점프
        let top0 = b.ins().iconst(types::I64, (fun.arity as i64) + 1);
        b.ins().jump(blocks[0], &[ir::BlockArg::Value(top0)]);

        // 헬퍼: push/pop/peek 주소 계산
        let mem = MemFlags::new();

        let mut ip: usize = 0;
        while ip < code.len() {
            if !start[ip] {
                ip += 1;
                continue;
            }

            b.switch_to_block(blocks[ip]);
            let mut top = b.block_params(blocks[ip])[0];

            let op = OpCode::from_u8(code[ip]); ip += 1;
            match op {
                OpCode::Constant => {
                    let idx = read_u16(code, &mut ip) as usize;
                    let n = match constants[idx] {
                        Value::Number(x) => x,
                        _ => return Err("JIT expects number constants only".into()),
                    };
                    // push n
                    let v = b.ins().f64const(n);
                    push_f64(&mut b, stack_ptr, &mut top, v, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::GetLocal => {
                    let slot = read_u16(code, &mut ip) as i64;
                    let addr = b.ins().iadd_imm(stack_ptr, slot * 8);
                    let v = b.ins().load(types::F64, mem, addr, 0);
                    push_f64(&mut b, stack_ptr, &mut top, v, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::SetLocal => {
                    let slot = read_u16(code, &mut ip) as i64;
                    // peek
                    let v = peek_f64(&mut b, stack_ptr, top, 1, mem);
                    let addr = b.ins().iadd_imm(stack_ptr, slot * 8);
                    b.ins().store(mem, v, addr, 0);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::Pop => {
                    // pop discard
                    let _ = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::Add | OpCode::Sub | OpCode::Mul | OpCode::Div => {
                    let b1 = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let a1 = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let out = match op {
                        OpCode::Add => b.ins().fadd(a1, b1),
                        OpCode::Sub => b.ins().fsub(a1, b1),
                        OpCode::Mul => b.ins().fmul(a1, b1),
                        OpCode::Div => b.ins().fdiv(a1, b1),
                        _ => unreachable!(),
                    };
                    push_f64(&mut b, stack_ptr, &mut top, out, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::Less | OpCode::Greater | OpCode::Equal => {
                    let b1 = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let a1 = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let cc = match op {
                        OpCode::Less => ir::condcodes::FloatCC::LessThan,
                        OpCode::Greater => ir::condcodes::FloatCC::GreaterThan,
                        OpCode::Equal => ir::condcodes::FloatCC::Equal,
                        _ => unreachable!(),
                    };
                    let c = b.ins().fcmp(cc, a1, b1);
                    let one = b.ins().f64const(1.0);
                    let zero = b.ins().f64const(0.0);
                    let out = b.ins().select(c, one, zero);
                    push_f64(&mut b, stack_ptr, &mut top, out, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::Not => {
                    let v = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let zero = b.ins().f64const(0.0);
                    let is_zero = b.ins().fcmp(ir::condcodes::FloatCC::Equal, v, zero);
                    let one = b.ins().f64const(1.0);
                    let out = b.ins().select(is_zero, one, zero);
                    push_f64(&mut b, stack_ptr, &mut top, out, mem);
                    fallthrough(&mut b, blocks[ip], top);
                }

                OpCode::Jump => {
                    let off = read_u16(code, &mut ip) as usize;
                    let target = ip + off;
                    if target > code.len() || !start[target] {
                        return Err(format!("Bad jump target {}", target));
                    }
                    b.ins().jump(blocks[target], &[ir::BlockArg::Value(top)]);
                }

                OpCode::JumpIfFalse => {
                    let off = read_u16(code, &mut ip) as usize;
                    let target = ip + off;
                    if target > code.len() || !start[target] {
                        return Err(format!("Bad jif target {}", target));
                    }
                    // pop cond (0.0 = false)
                    let cond = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    let zero = b.ins().f64const(0.0);
                    let is_zero = b.ins().fcmp(ir::condcodes::FloatCC::Equal, cond, zero);
                    // true -> jump target, false -> fallthrough
                    b.ins().brif(is_zero, blocks[target], &[ir::BlockArg::Value(top)], blocks[ip], &[ir::BlockArg::Value(top)]);
                }

                OpCode::Loop => {
                    let off = read_u16(code, &mut ip) as usize;
                    let target = ip - off;
                    if !start[target] {
                        return Err(format!("Bad loop target {}", target));
                    }
                    b.ins().jump(blocks[target], &[ir::BlockArg::Value(top)]);
                }

                OpCode::Return => {
                    let v = pop_f64(&mut b, stack_ptr, &mut top, mem);
                    b.ins().return_(&[v]);
                }

                _ => {
                    // 숫자-only 밖
                    let line = lines.get(ip.saturating_sub(1)).copied().unwrap_or(0);
                    return Err(format!("JIT unsupported opcode {:?} at line {}", op, line));
                }
            }

            // seal은 마지막에 전체 sealer로 처리
        }

        // 모든 블록 seal
        for blk in blocks.iter() {
            b.seal_block(*blk);
        }

        b.finalize();

        self.module.define_function(func_id, &mut ctx).map_err(|e| e.to_string())?;
        self.module.clear_context(&mut ctx);
        self.module.finalize_definitions().map_err(|e| e.to_string())?;

        let ptr = self.module.get_finalized_function(func_id);
        Ok(unsafe { std::mem::transmute::<*const u8, JitFn>(ptr) })
    }
}

fn read_u16(code: &[u8], ip: &mut usize) -> u16 {
    let lo = code[*ip] as u16; *ip += 1;
    let hi = code[*ip] as u16; *ip += 1;
    lo | (hi << 8)
}

fn compute_instr_starts(code: &[u8]) -> Result<Vec<bool>, String> {
    let mut start = vec![false; code.len() + 1];
    let mut ip: usize = 0;
    while ip < code.len() {
        start[ip] = true;
        let op = OpCode::from_u8(code[ip]); ip += 1;
        match op {
            OpCode::Constant
            | OpCode::GetLocal
            | OpCode::SetLocal
            | OpCode::Jump
            | OpCode::JumpIfFalse
            | OpCode::Loop => ip += 2,
            OpCode::Call => ip += 1,
            _ => {}
        }
    }
    start[code.len()] = true;
    Ok(start)
}

fn is_numeric_only(fun: &FunctionObj) -> bool {
    // constants는 number만
    for c in fun.chunk.constants.iter() {
        if !matches!(c, Value::Number(_)) {
            return false;
        }
    }

    // 허용 opcode만
    let mut ip: usize = 0;
    while ip < fun.chunk.code.len() {
        let op = OpCode::from_u8(fun.chunk.code[ip]); ip += 1;
        match op {
            OpCode::Constant
            | OpCode::GetLocal
            | OpCode::SetLocal
            | OpCode::Jump
            | OpCode::JumpIfFalse
            | OpCode::Loop => ip += 2,

            OpCode::Pop
            | OpCode::Add | OpCode::Sub | OpCode::Mul | OpCode::Div
            | OpCode::Less | OpCode::Greater | OpCode::Equal
            | OpCode::Not
            | OpCode::Return => {}

            _ => return false,
        }
    }
    true
}

fn compute_max_stack(fun: &FunctionObj) -> Result<usize, String> {
    let code = &fun.chunk.code;
    let mut ip: usize = 0;
    let mut depth: i32 = (fun.arity as i32) + 1; // callee+args
    let mut maxd: i32 = depth;

    while ip < code.len() {
        let op = OpCode::from_u8(code[ip]); ip += 1;
        match op {
            OpCode::Constant => { ip += 2; depth += 1; }
            OpCode::GetLocal => { ip += 2; depth += 1; }
            OpCode::SetLocal => { ip += 2; /* peek */ }
            OpCode::Pop => { depth -= 1; }
            OpCode::Add | OpCode::Sub | OpCode::Mul | OpCode::Div
            | OpCode::Less | OpCode::Greater | OpCode::Equal => { depth -= 1; } // pop2 push1 => -1
            OpCode::Not => { /* pop1 push1 => 0 */ }
            OpCode::Jump | OpCode::JumpIfFalse | OpCode::Loop => { ip += 2; if matches!(op, OpCode::JumpIfFalse) { depth -= 1; } }
            OpCode::Return => { depth -= 1; }
            _ => return Err("compute_max_stack: unsupported opcode".into()),
        }
        if depth > maxd { maxd = depth; }
        if depth < 0 { return Err("stack underflow in analysis".into()); }
    }

    Ok(maxd as usize + 8)
}

fn fallthrough(b: &mut FunctionBuilder, next: ir::Block, top: ir::Value) {
    b.ins().jump(next, &[ir::BlockArg::Value(top)]);
}

fn push_f64(b: &mut FunctionBuilder, stack_ptr: ir::Value, top: &mut ir::Value, v: ir::Value, mem: MemFlags) {
    // store at stack[top], then top++
    let off = b.ins().imul_imm(*top, 8);
    let addr = b.ins().iadd(stack_ptr, off);
    b.ins().store(mem, v, addr, 0);
    *top = b.ins().iadd_imm(*top, 1);
}

fn pop_f64(b: &mut FunctionBuilder, stack_ptr: ir::Value, top: &mut ir::Value, mem: MemFlags) -> ir::Value {
    *top = b.ins().irsub_imm(*top, 1);
    let off = b.ins().imul_imm(*top, 8);
    let addr = b.ins().iadd(stack_ptr, off);
    b.ins().load(types::F64, mem, addr, 0)
}

fn peek_f64(b: &mut FunctionBuilder, stack_ptr: ir::Value, top: ir::Value, dist: i64, mem: MemFlags) -> ir::Value {
    let idx = b.ins().irsub_imm(top, dist);
    let off = b.ins().imul_imm(idx, 8);
    let addr = b.ins().iadd(stack_ptr, off);
    b.ins().load(types::F64, mem, addr, 0)
}