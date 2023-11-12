use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use serde::{Serialize,Deserialize};

#[derive(Debug,Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Default,Serialize,Deserialize)]
struct Brevet;

#[derive(Debug,Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Serialize,Deserialize)]
struct BrevetResult(Result<brevet::Expr, brevet::Error>);

impl Default for BrevetResult {
    fn default() -> Self {
        Self(Ok(brevet::Expr::default()))
    }
}

impl clepsydra::Lang for Brevet {
    type Key = String;
    type Val = BrevetResult;
    type Stmt = brevet::Expr;
    type Expr = brevet::Expr;
    fn get_write_set(s: &Self::Stmt) -> BTreeMap<Self::Key, Self::Expr> {
        BTreeMap::new()
    }
    fn get_read_set(e: &Self::Expr) -> BTreeSet<Self::Key> {
        BTreeSet::new()
    }
    fn get_eval_set(s: &Self::Stmt) -> BTreeSet<Self::Key> {
        BTreeSet::new()
    }

    fn eval_expr(
        e: &Self::Expr,
        vals: &[Self::Val],
        env: &BTreeMap<Self::Key, clepsydra::ExtVal<Self>>,
    ) -> clepsydra::ExtVal<Self> {
        let mut benv = brevet::Expr::Top;
        for (k, v) in env.iter() {
            if let clepsydra::ExtVal::Defined(BrevetResult(v)) = v {
                match v {
                    Ok(vgood) => benv = brevet::Expr::Merge(Arc::new(benv.clone()), Arc::new(vgood.clone())),
                    Err(bad) => return clepsydra::ExtVal::Defined(BrevetResult(Err(bad.clone())))
                }
            }
        }
        let mut ev = e.clone();
        while !ev.is_val() {
            match brevet::step(&benv, &ev) {
                Ok(good) => ev = good,
                Err(err) => return clepsydra::ExtVal::Defined(BrevetResult(Err(err)))
            }
        }
        clepsydra::ExtVal::Defined(BrevetResult(Ok(ev)))
    }
}

struct TieredStore {
    hot: redb::Database,
    // TODO: newel
    cold: BTreeMap<String, std::fs::File>,
}

impl clepsydra::Store<Brevet> for TieredStore {
    fn get_key_at_or_before_time(&self, kv: &clepsydra::KeyVer<Brevet>) -> Option<(clepsydra::GlobalTime, clepsydra::Entry<Brevet>)> {
        todo!()
    }

    fn put_key_at_time(&mut self, kv: &clepsydra::KeyVer<Brevet>, v: &clepsydra::Entry<Brevet>) {
        todo!()
    }

    fn get_delayed_watermark(&self) -> Option<clepsydra::Sdw> {
        todo!()
    }
}

struct System {
    db: clepsydra::Database<Brevet, TieredStore>
}