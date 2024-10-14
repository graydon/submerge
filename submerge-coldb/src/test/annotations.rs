use crate::ioutil::RangeExt;
use std::ops::Range;
use submerge_base::Result;

pub(crate) struct Annotations {
    context: Vec<String>,
    pub(crate) annotations: Vec<(Range<i64>, Vec<String>)>,
}

impl Annotations {
    pub(crate) fn new() -> Self {
        Annotations {
            context: Vec::new(),
            annotations: Vec::new(),
        }
    }
    pub(crate) fn push_context<T: ToString>(&mut self, context: T) {
        self.context.push(context.to_string());
    }
    pub(crate) fn pop_context(&mut self) {
        self.context.pop();
    }
    pub(crate) fn annotate<T: ToString>(&mut self, range: Range<i64>, name: T) {
        let mut ctx = self.context.clone();
        ctx.push(name.to_string());
        self.annotations.push((range, ctx));
    }
    #[cfg(test)]
    pub(crate) fn render_hexdump(&self, buf: &[u8]) -> Result<String> {
        use std::fmt::Write;
        let mut s = String::new();
        let mut pos = 0;
        for (r, name) in self.annotations.iter() {
            if r.is_empty() {
                continue;
            }
            let name = name.join(".");
            let (lo, hi) = (r.start, r.end - 1);
            let len = r.len();
            if lo < pos {
                writeln!(s, "- ERROR: out-of-order lo for {}", name)?;
            }
            if hi < pos {
                writeln!(s, "- ERROR: out-of-order hi for {}", name)?;
            }
            if lo > pos {
                writeln!(s, "- ERROR: unannotated ({} bytes)", lo - pos)?;
            }
            pos = hi + 1;
            writeln!(s, "- {} ({} bytes):", name, len)?;
            let lo_usz: usize = lo.try_into().expect("annotation exceeds usize");
            let hi_usz: usize = hi.try_into().expect("annotation exceeds usize");
            let bytes = &buf[lo_usz..=hi_usz];
            if bytes.is_empty() {
                continue;
            }
            let mut prev = [0u8; 16];
            let mut repeated = 0;
            let mut suppress_start = 0;
            const DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS: usize = 1;
            for (n, line) in bytes.chunks(16).enumerate() {
                if n > 0 && line.len() == 16 && prev == line {
                    if repeated == DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS {
                        suppress_start = lo_usz + (n * 16);
                    }
                    repeated += 1;
                    if repeated > DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS {
                        continue;
                    }
                }
                if line.len() == 16 {
                    prev.copy_from_slice(line);
                }
                if repeated > DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS {
                    writeln!(
                        s,
                        "\t {:08.8x} | ... previous line repeated {} times",
                        suppress_start,
                        (repeated - DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS)
                    )?;
                    repeated = 0;
                }
                write!(s, "\t {:08.8x} |", lo_usz + (n * 16))?;
                for group in line.chunks(4) {
                    s += "  ";
                    for byte in group {
                        write!(s, " {:02.2x}", byte)?;
                    }
                }
                for pad in 0..(16 - line.len()) {
                    s += "   ";
                    if pad & 3 == 3 {
                        s += "  ";
                    }
                }
                s += "   | ";
                for ch in line {
                    if ch.is_ascii_graphic() {
                        s.push(*ch as char);
                    } else {
                        s.push('.');
                    }
                }
                writeln!(s, "")?;
            }
            if repeated > DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS {
                writeln!(
                    s,
                    "\t {:08.8x} | ... previous line repeated {} times",
                    suppress_start,
                    (repeated - DISPLAYED_REPEAT_LIMIT_BEFORE_SUPPRESS)
                )?;
            }
        }
        Ok(s)
    }
}
