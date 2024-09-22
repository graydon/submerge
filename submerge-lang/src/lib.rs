#![allow(dead_code)]
use ordered_float::OrderedFloat;
use serde::{Serialize, Deserialize};




// When doing columnar evaluation
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Vals {
    I64s(Vec<i64>),
    F64s(Vec<OrderedFloat<f64>>),
    Bits(bs::Bs),
    Bins(Vec<Bin>),
    Rich(Box<Col>),           // Vals enriched with label, unit and form
    All(Vec<Vals>),           // Disjoint intersection (statically type-enforced)
    Any(Vec<i64>, Vec<Vals>), // Disjoint union (dynamically indexed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Bin {
    block: i64,
    entry: i64,
}

// A word is a bin that at least (a) is UTF-8 and (b) complies with UAX#31
// XID_Start XID_Continue* as well as as many restrictions as reasonable from
// UAX#39 (eg. single-script, general security profile, confusible) with an
// added ability to mark a realm, table or column as ASCII-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Word(Bin);

// A form describes additional representational details for a Val type, such as
// the data encoding of a Bin, or a decimal precision for a fixed-point I64.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Form(i64);

// A unit describes the physical, logical, or cultural units employed by the
// column if the column is numeric.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Unit(i64);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Col {
    name: Word,
    form: Form,
    unit: Unit,
    vals: Vals,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Tab {
    cols: Vec<Col>,
}

// A path designates a given Col within a (nested)
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Path(pub Vec<Word>);

// An Expr is an expresison in a modified Ei-calculus. It is tree-structured
// for ease of performing synchronous operations like typechecking.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Expr {
    Pass,
}

pub struct Insn {
    op: Opcode,
    // 10 bits binopcode + 6 = 2 bits per operand: literal/register and scalar/vector
    // 12 bits unopcode + 4 = 2 bits per operand: literal/register and scalar/vector
    a: Operand, // 16 bits lit-or-reg
    b: Operand, // 16 bits lit-or-reg
    c: Operand  // 16 bits lit-or-reg
}

// Insns are designed to pack/unpack to 64-bit words.
pub struct Operand(u16);

// An opcode is a single step in the evaluation of an Expr. They are
// not "lower level" than Expr nodes, just linearized so that there
// is an obvious way to step through an Expr in a Vm and interrupt
// the evaluation at any point.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Opcode {
    PrimBinOp(PrimBinOp),
    PrimUnOp(PrimUnOp),
    Literal(Vals),
    Path(Path),
    Reify, // Reify the environment
    Query, // Query the environment
    Merge, // Dependent merge of two values
    Cast,  // Cast value to type
    Eval,  // Binary evaluation of expression under environment
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PrimBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Cmp,
    Min,
    Max,
    Or,
    Xor,
    Shl,
    Shr,
    Asr,
    Rol,
    Ror,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PrimUnOp {
    Neg,
    Not,
    Abs,
    Sgn,
    Sqrt,
    Exp,
    Exp2,
    Exp10,
    Log,
    Log2,
    Log10,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Floor,
    Ceil,
    Trunc,
    Recip,
    Popcnt,
    Clz,
    Ctz,
    Bitrev,
    ByteSwap,
    BitCount,
    BitParity,
}

// A VM evaluates an Expr in a, interruptable way.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Vm {
    ops: Vec<Opcode>,
    stack: Vec<Frame>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Frame {
    ctx: Vec<Tab>,
    scalar_bit_regs: Vec<u64>,
    //vector_bit_regs: Vec<Box<dyn Iterator<Item=u64>>>,
    bin_regs: Vec<u64>,
    //flo_regs: Vec<f64>,
    int_regs: Vec<i64>,
    pc: usize,
}
