use crate::chunk::Chunk;
use crate::error::{NsError, NsResult, SourceMap};
use crate::lexer::{Lexer, Token, TokenKind};
use crate::opcode::OpCode;
use crate::value::{FunctionObj, Obj, Value};
use std::fs;
use std::path::Path;

#[derive(Clone)]
struct Local { name: String, depth: i32 }

struct Loop {
    start: usize,
    breaks: Vec<usize>,
}

pub struct Compiler<'a> {
    map: &'a SourceMap<'a>,
    lex: Lexer<'a>,
    cur: Token,
    prev: Token,

    fun: FunctionObj,
    locals: Vec<Local>,
    scope_depth: i32,
    loops: Vec<Loop>,
}

impl<'a> Compiler<'a> {
    pub fn compile(map: &'a SourceMap<'a>) -> NsResult<FunctionObj> {
        let mut c = Self {
            map,
            lex: Lexer::new(map),
            cur: Token { kind: TokenKind::Eof, start: 0, end: 0, line: 1 },
            prev: Token { kind: TokenKind::Eof, start: 0, end: 0, line: 1 },
            fun: FunctionObj {
                name: Some("<script>".to_string()),
                arity: 0,
                locals: 1,
                chunk: Chunk::new(),
                jit: false,
                exports: vec![],
                is_module: false,
            },
            locals: vec![Local { name: "<callee>".to_string(), depth: 0 }],
            scope_depth: 0,
            loops: vec![],
        };

        c.advance()?;
        while !c.check(TokenKind::Eof) {
            c.declaration()?;
        }

        c.emit(OpCode::Null);
        c.emit(OpCode::Return);
        c.fun.locals = c.locals.len().max(1);
        Ok(c.fun)
    }

    fn err<T>(&self, tok: &Token, msg: impl Into<String>, error_len: usize) -> NsResult<T> {
        Err(NsError::new(self.map, tok.start, msg, error_len))
    }

    fn text(&self, t: &Token) -> &str { &self.map.src[t.start..t.end] }

    fn advance(&mut self) -> NsResult<()> {
        self.prev = self.cur.clone();
        self.cur = self.lex.next()?;
        Ok(())
    }

    fn check(&self, k: TokenKind) -> bool { self.cur.kind == k }

    fn matches(&mut self, k: TokenKind) -> NsResult<bool> {
        if self.check(k) { self.advance()?; Ok(true) } else { Ok(false) }
    }

    fn consume(&mut self, k: TokenKind, msg: &str) -> NsResult<()> {
        if self.check(k) { self.advance()?; Ok(()) } else {
            let error_len = self.cur.end - self.cur.start;
            self.err(&self.cur, msg, error_len)
        }
    }

    fn line(&self) -> u32 { self.prev.line }

    fn emit(&mut self, op: OpCode) {
        self.fun.chunk.write_op(op, self.line());
        self.fun.chunk.source_offsets.push(self.prev.start);
    }

    fn emit_u8(&mut self, b: u8) {
        self.fun.chunk.write_u8(b, self.line());
        self.fun.chunk.source_offsets.push(self.prev.start);
    }

    fn emit_u16(&mut self, v: u16) {
        self.fun.chunk.write_u16(v, self.line());
        self.fun.chunk.source_offsets.push(self.prev.start);
        self.fun.chunk.source_offsets.push(self.prev.start);
    }

    fn emit_const(&mut self, v: Value) {
        let idx = self.fun.chunk.add_const(v);
        self.emit(OpCode::Constant);
        self.emit_u16(idx);
    }

    fn emit_string_obj_const(&mut self, s: String) {
        let r = self.fun.chunk.add_obj(Obj::String(s));
        let idx = self.fun.chunk.add_const(Value::Obj(r));
        self.emit(OpCode::Constant);
        self.emit_u16(idx);
    }

    fn global_name_id(&mut self, name: &str) -> u16 {
        self.fun.chunk.name_id(name)
    }

    fn declaration(&mut self) -> NsResult<()> {
        let mut jit = false;
        if self.matches(TokenKind::At)? {
            let at = self.prev.clone();
            if self.matches(TokenKind::Ident)? && self.text(&self.prev) == "jit" {
                jit = true;
            } else {
                return self.err(&at, "Expect 'jit' after '@' decorator.", at.end - at.start);
            }
        }

        let is_export = self.matches(TokenKind::Export)?;
        if is_export && jit {
            return self.err(&self.prev, "@jit cannot be used with 'export'.", self.prev.end - self.prev.start);
        }

        if self.matches(TokenKind::Fn)? {
            self.fun_decl(jit, is_export)
        } else if self.matches(TokenKind::Let)? {
            if jit { return self.err(&self.prev, "@jit can only be used on functions.", self.prev.end - self.prev.start); }
            self.let_decl(is_export)
        } else if self.matches(TokenKind::Import)? {
            if jit || is_export { return self.err(&self.prev, "Cannot use 'import' with '@jit' or 'export'.", self.prev.end - self.prev.start); }
            self.import_statement()
        } else {
            if jit || is_export { return self.err(&self.prev, "Expected a declaration after '@jit' or 'export'.", self.prev.end - self.prev.start); }
            self.statement()
        }
    }

    fn let_decl(&mut self, is_export: bool) -> NsResult<()> {
        let name_tok = self.cur.clone();
        self.consume(TokenKind::Ident, "Expect variable name.")?;
        let name = self.text(&name_tok).to_string();

        if self.scope_depth > 0 {
            if is_export {
                return self.err(&name_tok, "Cannot export a local variable.", name.len());
            }
            self.add_local(name.clone());
            if self.matches(TokenKind::Equal)? { self.expression()?; } else { self.emit(OpCode::Null); }
            self.matches(TokenKind::Semicolon)?;
            let slot = (self.locals.len() - 1) as u16;
            self.emit(OpCode::SetLocal); self.emit_u16(slot);
            self.emit(OpCode::Pop);
            Ok(())
        } else {
            if is_export {
                self.fun.exports.push(name.clone());
            }
            let gid = self.global_name_id(&name);
            if self.matches(TokenKind::Equal)? { self.expression()?; } else { self.emit(OpCode::Null); }
            self.matches(TokenKind::Semicolon)?;
            self.emit(OpCode::DefineGlobal); self.emit_u16(gid);
            Ok(())
        }
    }

    fn fun_decl(&mut self, jit: bool, is_export: bool) -> NsResult<()> {
        let name_tok = self.cur.clone();
        self.consume(TokenKind::Ident, "Expect function name.")?;
        let name = self.text(&name_tok).to_string();

        if is_export {
            if self.scope_depth > 0 {
                return self.err(&name_tok, "Cannot export a local function.", name.len());
            }
            self.fun.exports.push(name.clone());
        }

        let fobj = self.function(&name, jit)?;
        let r = self.fun.chunk.add_obj(Obj::Function(fobj));
        let cidx = self.fun.chunk.add_const(Value::Obj(r));

        if self.scope_depth > 0 {
            self.add_local(name.clone());
            self.emit(OpCode::Constant); self.emit_u16(cidx);
            let slot = (self.locals.len() - 1) as u16;
            self.emit(OpCode::SetLocal); self.emit_u16(slot);
            self.emit(OpCode::Pop);
        } else {
            let gid = self.global_name_id(&name);
            self.emit(OpCode::Constant); self.emit_u16(cidx);
            self.emit(OpCode::DefineGlobal); self.emit_u16(gid);
        }
        Ok(())
    }

    fn function(&mut self, name: &str, jit: bool) -> NsResult<FunctionObj> {
        self.consume(TokenKind::LParen, "Expect '(' after function name.")?;
        let mut params: Vec<String> = vec![];
        if !self.check(TokenKind::RParen) {
            loop {
                let p = self.cur.clone();
                self.consume(TokenKind::Ident, "Expect parameter name.")?;
                params.push(self.text(&p).to_string());
                if !self.matches(TokenKind::Comma)? { break; }
            }
        }
        self.consume(TokenKind::RParen, "Expect ')' after parameters.")?;
        self.consume(TokenKind::LBrace, "Expect '{' before function body.")?;

        let mut sub = SubCompiler::new(self, name, params, jit);
        sub.block_body()?;
        Ok(sub.finish())
    }

    fn statement(&mut self) -> NsResult<()> {
        if self.matches(TokenKind::If)? { self.if_stmt() }
        else if self.matches(TokenKind::While)? { self.while_stmt() }
        else if self.matches(TokenKind::For)? { self.for_statement() }
        else if self.matches(TokenKind::Return)? { self.return_stmt() }
        else if self.matches(TokenKind::Break)? { self.break_statement() }
        else if self.matches(TokenKind::Continue)? { self.continue_statement() }
        else if self.matches(TokenKind::LBrace)? { self.begin_scope(); self.block()?; self.end_scope(); Ok(()) }
        else { self.expr_stmt() }
    }

    fn import_statement(&mut self) -> NsResult<()> {
        let import_tok = self.prev.clone();
        self.consume(TokenKind::String, "Expect module path string after 'import'.")?;
        let path_str_raw = self.text(&self.prev).to_string();
        let path_str = &path_str_raw[1..path_str_raw.len() - 1];

        self.consume(TokenKind::As, "Expect 'as' after module path.")?;
        let name_tok = self.cur.clone();
        self.consume(TokenKind::Ident, "Expect namespace name after 'as'.")?;
        let name = self.text(&name_tok).to_string();

        self.consume(TokenKind::Semicolon, "Expect ';' after import statement.")?;

        let importer_path = Path::new(self.map.name); 
        let importer_dir = importer_path.parent().unwrap_or_else(|| Path::new(""));
        let module_path = importer_dir.join(path_str);

        let module_src = match fs::read_to_string(&module_path) {
            Ok(src) => src,
            Err(e) => return self.err(&import_tok, format!("Failed to read module '{}': {}", module_path.display(), e), import_tok.end - import_tok.start),
        };

        let module_path_str = module_path.to_str().unwrap_or("<module>");
        let module_map = SourceMap::new(module_path_str, &module_src);
        let mut module_fun = match Compiler::compile(&module_map) {
            Ok(fun) => fun,
            Err(e) => return self.err(&import_tok, format!("Failed to compile module '{}':\n{}", module_path.display(), e), import_tok.end - import_tok.start),
        };
        module_fun.is_module = true;

        let fun_obj_ref = self.fun.chunk.add_obj(Obj::Function(module_fun));
        let const_idx = self.fun.chunk.add_const(Value::Obj(fun_obj_ref));
        self.emit(OpCode::Constant);
        self.emit_u16(const_idx);
        self.emit(OpCode::CreateModule);

        let gid = self.global_name_id(name.as_str());
        self.emit(OpCode::DefineGlobal);
        self.emit_u16(gid);

        Ok(())
    }
    
    fn for_statement(&mut self) -> NsResult<()> {
        self.begin_scope();
        self.consume(TokenKind::LParen, "Expect '(' after 'for'.")?;

        if self.matches(TokenKind::Semicolon)? {
            // No initializer.
        } else if self.matches(TokenKind::Let)? {
            self.let_decl(false)?;
        } else {
            self.expr_stmt()?;
        }

        let mut loop_start = self.fun.chunk.code.len();
        let mut exit_jump = None;
        if !self.matches(TokenKind::Semicolon)? {
            self.expression()?;
            self.consume(TokenKind::Semicolon, "Expect ';' after loop condition.")?;
            exit_jump = Some(self.emit_jump(OpCode::JumpIfFalse));
            self.emit(OpCode::Pop);
        }

        let increment_start;
        if !self.matches(TokenKind::RParen)? {
            let body_jump = self.emit_jump(OpCode::Jump);
            increment_start = self.fun.chunk.code.len();
            self.expression()?;
            self.emit(OpCode::Pop);
            self.consume(TokenKind::RParen, "Expect ')' after for clauses.")?;
            self.emit(OpCode::Loop);
            self.emit_u16(((self.fun.chunk.code.len() + 2) - loop_start) as u16);
            loop_start = increment_start;
            self.patch_jump(body_jump);
        } else {
            increment_start = 0;
        }

        self.loops.push(Loop { start: loop_start, breaks: vec![] });
        self.statement()?;

        if increment_start != 0 {
            
        } else {
            self.emit(OpCode::Loop);
            self.emit_u16(((self.fun.chunk.code.len() + 2) - loop_start) as u16);
        }

        if let Some(exit) = exit_jump {
            self.patch_jump(exit);
            self.emit(OpCode::Pop);
        }

        let loop_info = self.loops.pop().unwrap();
        for break_jump in loop_info.breaks {
            self.patch_jump(break_jump);
        }

        self.end_scope();
        Ok(())
    }

    fn expr_stmt(&mut self) -> NsResult<()> {
        self.expression()?;
        self.matches(TokenKind::Semicolon)?;
        self.emit(OpCode::Pop);
        Ok(())
    }

    fn block(&mut self) -> NsResult<()> {
        while !self.check(TokenKind::RBrace) && !self.check(TokenKind::Eof) {
            self.declaration()?;
        }
        self.consume(TokenKind::RBrace, "Expect '}' after block.")?;
        Ok(())
    }

    fn if_stmt(&mut self) -> NsResult<()> {
        self.consume(TokenKind::LParen, "Expect '(' after 'if'.")?;
        self.expression()?;
        self.consume(TokenKind::RParen, "Expect ')' after condition.")?;
        let jif = self.emit_jump(OpCode::JumpIfFalse);
        self.emit(OpCode::Pop);
        self.statement()?;
        let jend = self.emit_jump(OpCode::Jump);
        self.patch_jump(jif);
        self.emit(OpCode::Pop);
        if self.matches(TokenKind::Else)? {
            self.statement()?;
        }
        self.patch_jump(jend);
        Ok(())
    }

    fn while_stmt(&mut self) -> NsResult<()> {
        let loop_start = self.fun.chunk.code.len();
        self.consume(TokenKind::LParen, "Expect '(' after 'while'.")?;
        self.expression()?;
        self.consume(TokenKind::RParen, "Expect ')' after condition.")?;
        let exit_jump = self.emit_jump(OpCode::JumpIfFalse);
        self.emit(OpCode::Pop);
        self.loops.push(Loop { start: loop_start, breaks: vec![] });
        self.statement()?;
        self.emit(OpCode::Loop);
        self.emit_u16(((self.fun.chunk.code.len() + 2) - loop_start) as u16);
        self.patch_jump(exit_jump);
        self.emit(OpCode::Pop);
        let loop_info = self.loops.pop().unwrap();
        for break_jump in loop_info.breaks {
            self.patch_jump(break_jump);
        }
        Ok(())
    }

    fn break_statement(&mut self) -> NsResult<()> {
        self.consume(TokenKind::Semicolon, "Expect ';' after 'break'.")?;
        if self.loops.is_empty() {
            return self.err(&self.prev, "Cannot use 'break' outside of a loop.", self.prev.end - self.prev.start);
        }
        let break_jump = self.emit_jump(OpCode::Jump);
        self.loops.last_mut().unwrap().breaks.push(break_jump);
        Ok(())
    }

    fn continue_statement(&mut self) -> NsResult<()> {
        self.consume(TokenKind::Semicolon, "Expect ';' after 'continue'.")?;
        if self.loops.is_empty() {
            return self.err(&self.prev, "Cannot use 'continue' outside of a loop.", self.prev.end - self.prev.start);
        }
        let loop_start = self.loops.last().unwrap().start;
        self.emit(OpCode::Loop);
        self.emit_u16(((self.fun.chunk.code.len() + 2) - loop_start) as u16);
        Ok(())
    }

    fn return_stmt(&mut self) -> NsResult<()> {
        if self.matches(TokenKind::Semicolon)? || self.check(TokenKind::Eof) || self.check(TokenKind::RBrace) {
            self.emit(OpCode::Null);
        } else {
            self.expression()?;
            self.matches(TokenKind::Semicolon)?;
        }
        self.emit(OpCode::Return);
        Ok(())
    }

    fn begin_scope(&mut self) { self.scope_depth += 1; }
    fn end_scope(&mut self) {
        self.scope_depth -= 1;
        while !self.locals.is_empty() && self.locals.last().unwrap().depth > self.scope_depth {
            self.emit(OpCode::Pop);
            self.locals.pop();
        }
    }

    fn add_local(&mut self, name: String) {
        self.locals.push(Local { name, depth: self.scope_depth });
    }

    fn resolve_local(&self, name: &str) -> Option<u16> {
        for (i, l) in self.locals.iter().enumerate().rev() {
            if l.name == name { return Some(i as u16); }
        }
        None
    }

    fn emit_jump(&mut self, op: OpCode) -> usize {
        self.emit(op);
        let at = self.fun.chunk.code.len();
        self.emit_u16(0xffff);
        at
    }

    fn patch_jump(&mut self, at: usize) {
        let jump = self.fun.chunk.code.len() - (at + 2);
        crate::chunk::Chunk::write_u16_at(&mut self.fun.chunk.code, at, jump as u16);
    }

    fn expression(&mut self) -> NsResult<()> { self.assignment() }

    fn assignment(&mut self) -> NsResult<()> {
        self.or()?;

        if self.matches(TokenKind::Equal)? {
            let name_tok = self.prev.clone();
            let target = self.last_assign_target().ok_or_else(|| NsError::new(self.map, name_tok.start, "Invalid assignment target.", name_tok.end - name_tok.start))?;

            match target {
                AssignTarget::Index => {
                    self.fun.chunk.code.pop();
                    self.fun.chunk.lines.pop();
                    self.fun.chunk.source_offsets.pop();
                }
                AssignTarget::Local(_) | AssignTarget::Global(_) => {
                    let len = self.fun.chunk.code.len();
                    self.fun.chunk.code.truncate(len - 3);
                    self.fun.chunk.lines.truncate(len - 3);
                    self.fun.chunk.source_offsets.truncate(len - 3);
                }
            }

            self.assignment()?;

            match target {
                AssignTarget::Local(slot) => { self.emit(OpCode::SetLocal); self.emit_u16(slot); }
                AssignTarget::Global(gid) => { self.emit(OpCode::SetGlobal); self.emit_u16(gid); }
                AssignTarget::Index => { self.emit(OpCode::ArraySet); }
            }
        } else if self.matches(TokenKind::PlusEqual)? {
            let name_tok = self.prev.clone();
            let target = self.last_assign_target().ok_or_else(|| NsError::new(self.map, name_tok.start, "Invalid assignment target.", name_tok.end - name_tok.start))?;
            
            match target {
                AssignTarget::Index => {
                    return self.err(&name_tok, "Compound assignment (e.g., '+=' is not yet supported for indexed or property assignments.", name_tok.end - name_tok.start);
                }
                AssignTarget::Local(slot) => {
                    self.expression()?;
                    self.emit(OpCode::Add);
                    self.emit(OpCode::SetLocal); self.emit_u16(slot);
                }
                AssignTarget::Global(gid) => {
                    self.expression()?;
                    self.emit(OpCode::Add);
                    self.emit(OpCode::SetGlobal); self.emit_u16(gid);
                }
            }
        }
        Ok(())
    }

    fn last_assign_target(&self) -> Option<AssignTarget> {
        let code = &self.fun.chunk.code;
        if code.is_empty() { return None; }

        if *code.last().unwrap() == OpCode::ArrayGet as u8 {
            return Some(AssignTarget::Index);
        }

        if code.len() < 3 { return None; }
        let op = OpCode::from_u8(code[code.len() - 3]);
        
        match op {
            OpCode::GetLocal => {
                let lo = code[code.len() - 2] as u16;
                let hi = code[code.len() - 1] as u16;
                let slot = lo | (hi << 8);
                Some(AssignTarget::Local(slot))
            }
            OpCode::GetGlobal => {
                let lo = code[code.len() - 2] as u16;
                let hi = code[code.len() - 1] as u16;
                let gid = lo | (hi << 8);
                Some(AssignTarget::Global(gid))
            }
            _ => None,
        }
    }

    fn or(&mut self) -> NsResult<()> {
        self.and()?;

        while self.matches(TokenKind::PipePipe)? {
            let else_jump = self.emit_jump(OpCode::JumpIfFalse);
            let end_jump = self.emit_jump(OpCode::Jump);

            self.patch_jump(else_jump);
            self.emit(OpCode::Pop);

            self.and()?;
            self.patch_jump(end_jump);
        }
        Ok(())
    }

    fn and(&mut self) -> NsResult<()> {
        self.equality()?;

        while self.matches(TokenKind::AmpAmp)? {
            let end_jump = self.emit_jump(OpCode::JumpIfFalse);

            self.emit(OpCode::Pop);

            self.equality()?;
            self.patch_jump(end_jump);
        }
        Ok(())
    }

    fn equality(&mut self) -> NsResult<()> {
        self.comparison()?;
        while self.matches(TokenKind::EqualEqual)? || self.matches(TokenKind::BangEqual)? {
            let op = self.prev.kind;
            self.comparison()?;
            match op {
                TokenKind::EqualEqual => self.emit(OpCode::Equal),
                TokenKind::BangEqual => { self.emit(OpCode::Equal); self.emit(OpCode::Not); },
                _ => {}
            }
        }
        Ok(())
    }

    fn comparison(&mut self) -> NsResult<()> {
        self.term()?;
        while matches!(self.cur.kind, TokenKind::Less|TokenKind::LessEqual|TokenKind::Greater|TokenKind::GreaterEqual) {
            let op = self.cur.kind;
            self.advance()?;
            self.term()?;
            match op {
                TokenKind::Less => self.emit(OpCode::Less),
                TokenKind::Greater => self.emit(OpCode::Greater),
                TokenKind::LessEqual => { self.emit(OpCode::Greater); self.emit(OpCode::Not); },
                TokenKind::GreaterEqual => { self.emit(OpCode::Less); self.emit(OpCode::Not); },
                _ => {}
            }
        }
        Ok(())
    }

    fn term(&mut self) -> NsResult<()> {
        self.factor()?;
        while self.matches(TokenKind::Plus)? || self.matches(TokenKind::Minus)? {
            let op = self.prev.kind;
            self.factor()?;
            match op {
                TokenKind::Plus => self.emit(OpCode::Add),
                TokenKind::Minus => self.emit(OpCode::Sub),
                _ => {}
            }
        }
        Ok(())
    }

    fn factor(&mut self) -> NsResult<()> {
        self.unary()?;
        while self.matches(TokenKind::Star)? || self.matches(TokenKind::Slash)? {
            let op = self.prev.kind;
            self.unary()?;
            match op {
                TokenKind::Star => self.emit(OpCode::Mul),
                TokenKind::Slash => self.emit(OpCode::Div),
                _ => {}
            }
        }
        Ok(())
    }

    fn unary(&mut self) -> NsResult<()> {
        if self.matches(TokenKind::Bang)? { self.unary()?; self.emit(OpCode::Not); return Ok(()); }
        if self.matches(TokenKind::Minus)? { self.unary()?; self.emit(OpCode::Negate); return Ok(()); }
        self.call()
    }

    fn call(&mut self) -> NsResult<()> {
        self.primary()?;
        loop {
            if self.matches(TokenKind::LParen)? {
                let mut argc: u8 = 0;
                if !self.check(TokenKind::RParen) {
                    loop {
                        self.expression()?;
                        argc = argc.wrapping_add(1);
                        if !self.matches(TokenKind::Comma)? { break; }
                    }
                }
                self.consume(TokenKind::RParen, "Expect ')' after arguments.")?;
                self.emit(OpCode::Call);
                self.emit_u8(argc);
            } else if self.matches(TokenKind::Dot)? {
                let name_tok = self.cur.clone();
                self.consume(TokenKind::Ident, "Expect property name after '.'")?;
                let prop = self.text(&name_tok).to_string();
                if self.check(TokenKind::LParen) {
                    self.advance()?;
                    let is_builtin = matches!(prop.as_str(), "push" | "pop" | "keys" | "values");
                    if !is_builtin {
                        self.emit_string_obj_const(prop.clone());
                        self.emit(OpCode::ArrayGet);
                    }
                    let mut argc: u8 = 0;
                    if !self.check(TokenKind::RParen) {
                        loop {
                            self.expression()?;
                            argc = argc.wrapping_add(1);
                            if !self.matches(TokenKind::Comma)? { break; }
                        }
                    }
                    self.consume(TokenKind::RParen, "Expect ')' after arguments.")?;
                    match prop.as_str() {
                        "push" => {
                            if argc != 1 { return self.err(&name_tok, "push(item) expects 1 argument.", name_tok.end - name_tok.start); }
                            self.emit(OpCode::ArrayPush);
                        }
                        "pop" => {
                            if argc != 0 { return self.err(&name_tok, "pop() expects 0 arguments.", name_tok.end - name_tok.start); }
                            self.emit(OpCode::ArrayPop);
                        }
                        "keys" => {
                            if argc != 0 { return self.err(&name_tok, "keys() expects 0 arguments.", name_tok.end - name_tok.start); }
                            self.emit(OpCode::MapKeys);
                        }
                        "values" => {
                            if argc != 0 { return self.err(&name_tok, "values() expects 0 arguments.", name_tok.end - name_tok.start); }
                            self.emit(OpCode::MapValues);
                        }
                        _ => {
                            self.emit(OpCode::Call);
                            self.emit_u8(argc);
                        }
                    }
                } else {
                    if prop == "length" {
                        self.emit(OpCode::ArrayLen);
                    } else {
                        self.emit_string_obj_const(prop);
                        self.emit(OpCode::ArrayGet);
                    }
                }
            } else if self.matches(TokenKind::LBracket)? {
                self.expression()?;
                self.consume(TokenKind::RBracket, "Expect ']' after index.")?;
                self.emit(OpCode::ArrayGet);
            } else {
                break;
            }
        }
        Ok(())
    }

    fn primary(&mut self) -> NsResult<()> {
        if self.matches(TokenKind::Number)? {
            let s = self.text(&self.prev);
            let n: f64 = s.parse().map_err(|_| NsError::new(self.map, self.prev.start, "Invalid number.", self.prev.end - self.prev.start))?;
            self.emit_const(Value::Number(n));
            return Ok(());
        }
        if self.matches(TokenKind::String)? {
            let raw = self.text(&self.prev);
            let inner = &raw[1..raw.len()-1];
            self.emit_string_obj_const(inner.to_string());
            return Ok(());
        }
        if self.matches(TokenKind::True)? { self.emit(OpCode::True); return Ok(()); }
        if self.matches(TokenKind::False)? { self.emit(OpCode::False); return Ok(()); }
        if self.matches(TokenKind::Null)? { self.emit(OpCode::Null); return Ok(()); }
        if self.matches(TokenKind::Ident)? {
            let name = self.text(&self.prev).to_string();
            if let Some(slot) = self.resolve_local(&name) {
                self.emit(OpCode::GetLocal);
                self.emit_u16(slot);
            } else {
                let gid = self.global_name_id(&name);
                self.emit(OpCode::GetGlobal);
                self.emit_u16(gid);
            }
            return Ok(())
        }
        if self.matches(TokenKind::LParen)? {
            self.expression()?;
            self.consume(TokenKind::RParen, "Expect ')' after expression.")?;
            return Ok(())
        }
        if self.matches(TokenKind::LBracket)? {
            let mut item_count = 0;
            if !self.check(TokenKind::RBracket) {
                loop {
                    self.expression()?;
                    item_count += 1;
                    if !self.matches(TokenKind::Comma)? {
                        break;
                    }
                }
            }
            self.consume(TokenKind::RBracket, "Expect ']' after list items.")?;
            self.emit(OpCode::BuildList);
            self.emit_u16(item_count);
            return Ok(())
        }
        if self.matches(TokenKind::LBrace)? {
            let mut item_count = 0;
            if !self.check(TokenKind::RBrace) {
                loop {
                    let key_tok = self.cur.clone();
                    self.consume(TokenKind::String, "Expect string literal as map key.")?;
                    let raw = self.text(&key_tok);
                    let inner = &raw[1..raw.len()-1];
                    self.emit_string_obj_const(inner.to_string());
        
                    self.consume(TokenKind::Colon, "Expect ':' after map key.")?;
        
                    self.expression()?;
                    item_count += 1;
        
                    if !self.matches(TokenKind::Comma)? {
                        break;
                    }
                }
            }
            self.consume(TokenKind::RBrace, "Expect '}' after map.")?;
            self.emit(OpCode::BuildMap);
            self.emit_u16(item_count);
            return Ok(())
        }
        self.err(&self.cur, "Expect expression.", self.cur.end - self.cur.start)
    }
}

#[derive(Clone, Copy)]
enum AssignTarget {
    Local(u16),
    Global(u16),
    Index,
}

struct SubCompiler<'a, 'b> {
    parent: &'a mut Compiler<'b>,
    old_fun: FunctionObj,
    old_locals: Vec<Local>,
    old_scope_depth: i32,
}

impl<'a, 'b> SubCompiler<'a, 'b> {
    fn new(parent: &'a mut Compiler<'b>, name: &str, params: Vec<String>, jit: bool) -> Self {
        let old_scope_depth = parent.scope_depth;
        parent.begin_scope();
        let fun = FunctionObj {
            name: Some(name.to_string()),
            arity: params.len(),
            locals: 0,
            chunk: Chunk::new(),
            jit,
            exports: vec![],
            is_module: false,
        };
        let old_fun = std::mem::replace(&mut parent.fun, fun);
        let old_locals = std::mem::replace(&mut parent.locals, vec![
            Local { name: name.to_string(), depth: parent.scope_depth }
        ]);
        for p in params {
            parent.add_local(p);
        }
        Self { parent, old_fun, old_locals, old_scope_depth }
    }

    fn block_body(&mut self) -> NsResult<()> {
        while !self.parent.check(TokenKind::RBrace) && !self.parent.check(TokenKind::Eof) {
            self.parent.declaration()?;
        }
        self.parent.consume(TokenKind::RBrace, "Expect '}' after function block.")?;
        Ok(())
    }

    fn finish(mut self) -> FunctionObj {
        self.parent.emit(OpCode::Null);
        self.parent.emit(OpCode::Return);
        self.parent.end_scope();
        let mut fun = std::mem::replace(&mut self.parent.fun, self.old_fun);
        let sub_locals = std::mem::replace(&mut self.parent.locals, self.old_locals);
        self.parent.scope_depth = self.old_scope_depth;
        fun.locals = sub_locals.len();
        fun
    }
}
