use submerge_base::Result;
use std::ops::Range;
use crate::ioutil::RangeExt;

pub struct Annotations {
    pub(crate) annotations: Vec<(Range<i64>, String)>,
}

impl Annotations {
    pub fn new() -> Self {
        Annotations {
            #[cfg(test)]
            annotations: Vec::new(),
        }
    }
    pub fn push(&mut self, range: Range<i64>, name: &str) {
        #[cfg(test)]
        self.annotations.push((range, name.to_string()));
    }
    #[cfg(test)]
    pub fn render_hexdump(&self, buf: &[u8]) -> Result<String> {
        use std::fmt::Write;
        let mut s = String::new();
        let mut pos = 0;
        for (r, name) in self.annotations.iter() {
            if r.is_empty() {
                continue;
            }
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
            let mut skipped = 0;
            let mut skipstart = 0;
            for (n, line) in bytes.chunks(16).enumerate() {
                if n > 0 && prev == line {
                    if skipped == 0 {
                        skipstart = lo_usz + (n * 16);
                    }
                    skipped += 1;
                    continue;
                }
                if line.len() == 16 {
                    prev.copy_from_slice(line);
                }
                if skipped > 0 {
                    writeln!(
                        s,
                        "\t {:08.8x} | ... previous line repeated {} times",
                        skipstart, skipped
                    )?;
                    skipped = 0;
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
            if skipped > 0 {
                writeln!(
                    s,
                    "\t {:08.8x} | ... previous line repeated {} times",
                    skipstart, skipped
                )?;
            }
        }
        Ok(s)
    }
}

trait AnnotateWriter {
}
