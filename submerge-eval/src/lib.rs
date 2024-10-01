#![allow(dead_code)]

// Eval is responsible for performing higher-level evaluation of Lang expressions.
//
// Each Lang expression is strictly terminating and in complexity class FO.
//
// Eval equips the system with a slightly richer complexity class, Dyn-FO, and
// additionally allows program _staging_ / metaprogramming.

use submerge_lang::{Tab, Vm};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Evaluator {
    tmp: Tab,
    new: Tab,
    seq: usize,
    cur: Vm,
}
