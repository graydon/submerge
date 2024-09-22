#![allow(dead_code)]

// The transaction system is based on a simplified variant of the
// OceanVista protocol.
//
// Simplifications:
//
//  - No quorum-writes or reads, always full replication.
//  - Local-only execution, only one watermark.
//  - No sharding, no datacenter-level organization.
//
// The general sequence looks like this:
//
//  1. Each tx begins at some coordinator node. That node is not
//     important for anything other than naming and driving the tx
//     forward: all nodes will eventually run all txs.
//
//  2. Each tx is assigned (without coordination) a global timestamp.
//     These have to be unique and ordered but there can be some clock
//     skew; any skew adds latency but doesn't change correctness.
//
//  3. The tx is replicated as a thunk-to-run to every other node, and
//     each node writes the tx timestamp (id) to _every record_ in its
//     multiversion store that is in the tx's static write footprint.
//     At that point new versions of those records have been allocated
//     and given globally-ordered timestamps but the thunks can't be
//     released to execution until we know no new earlier timestamps
//     are in flight. Hence watermarks.
//
//  4. Once all the nodes for a tx reply with an ack, the tx's
//     timestamp is released and the node's _local_ watermark advances
//     past it.
//
//  5. Nodes gossip their local watermarks periodically (as often as
//     practical) and observe the minimum-of-all-heard watermarks as
//     the global watermark.
//
//  6. When the global watermark passes a tx, it is released to
//     execution, as all of its predecessors have been replicated-in
//     to their timestamp-ordered positions.
//
//  7. Local execution runs as fast as it can, in as much parallelism
//     as it likes. Any read that depends on a not-yet-resolved thunk
//     waits for it (we might partition these into disjoint lanes to
//     simplify concurrency control at this level).
//
//  8. The only place this is "distributed" is in the replication
//     phase. If any write times out, the watermark will not advance
//     past the timed-out tx in that tx's epoch. The system enters
//     "reconfiguration", which is a single-decree paxos round that
//     votes on a new configuration containing a new nodeset, a new
//     epoch, and a final tx to accept-as-existing in the previous
//     epoch (killing all still-replicating txs past the final
//     one).
//

// Note this usage is a _very_ minimal use of paxos and so doesn't
// need anything fancy (logs, persistent leaders, view change phases,
// etc.) it's just single-decree paxos as implemented for example in
// stateright's example folder:
// https://github.com/stateright/stateright/blob/master/examples/paxos.rs
// (this is probably the best one to use!)
//
// Note that while reconfiguration only needs a quorum (of the old
// participants) to succeed, if it reconfigures to a new state with a
// faulty replica, it will not make progress on watermark advances as
// before, and have to reconfigure again!  i.e. the reconfiguration is
// quorum-y but the reconfigured-to state must be fully available.

// A configuration record contains:
//
//   - All the nodes in the current configuration
//   - The last timestamp in the previous configuration (after which
//     all timestamps are dead, until the new configuration starts).
//   - The first timestamp in the current configuration (when the
//     configuration was voted-on).
//
// A transaction enters a node and starts as unreplicated.
//
// It then tries to replicate to all nodes in the current configuration.
//
// If all replications succeed, the watermark advances and execution
// starts. Execution happens on all nodes _strictly locally_ and all
// nodes must arrive at the same result; if there is divergence the
// minority should halt, discard the segment it's trying to finalize,
// and reinitialize from the majority. If there's no majority the
// lower-numbered peers should copy from the higher-numbered. Both
// of these are serious events that should get a local log entry!
//
// If any replica R fails replication by a timeout, the client C
// waiting for R starts reconfiguration: a paxos vote is called to end
// the current configuration at the last consensus watermark advance,
// killing all the partially-replicated transactions after it, and to
// start a new configuration configuration excluding R and with a
// starting timestamp higher than any of the known timestamps
// (possibly with an epoch-counter advance). Because watermarks are
// based on knowing-what-others-know, (maps of each node's last-heard
// local watermark from all others), C will never propose invalidating
// a watermark anyone else considers committed-to, and if the vote
// goes through nobody will have any live timestamps in the range
// between the killed previous-config-end and the new config-start.
//
// Once reconfiguration completes, T is assigned a new transaction ID
// in the new epoch and replication begins again.
//
// Simultaneously, C notices that there are not enough replicas and
// begins bringing a new replica online to meet the target replica
// count.
//
// Whenever a new replica joins the network, it gossip-indicates
// readiness, and the highest-numbered node _in_ the existing
// configuration initiates a reconfiguration to expand the config to
// include it. Since the reconfig-proposer is an existing member, it
// knows it has not advanced its consensus watermark past some number
// N, and it can use that to seal off the previous config and propose
// the new one.


use std::collections::{BTreeMap, BTreeSet};

use serde::{Serialize, Deserialize};
use submerge_eval::Evaluator;
use submerge_lang::{Expr, Tab, Path, Vals};
use submerge_net::{NodeID, RealmTime, NodeTime, Duration};

use submerge_base::Error;

pub type NodeSet = BTreeSet<NodeID>; 

mod paxos;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Config {
    // The set of nodes to replicate transactions to
    nodes: NodeSet,
    // The number of times a replication-write should be retried
    retries: i64,
    // The number of milliseconds each attempt waits for an ack
    // before assuming it failed and retrying or giving up
    timeout: Duration
}

// A footprint indicates the set of keys that a given txn will read and write.
// The writes will all get thunks written to them pointing to this txn. The
// reads will all be considered dependencies of the txn, which it cannot execute
// before the resolution-of. Both reads and writes can indicate "an entire
// column", "an entire table", or "an entire database" (and may choose to do so
// if there is no way to statically bound the read or write set), though doing
// so will create an increasingly significant synchronization barrier, inhibiting
// parallel execution through it.

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
struct Footprint {
    reads: Vec<Path>,
    writes: Vec<Path>
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Thunk {
    vals: Tab,
    expr: Expr,
    foot: Footprint
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Transaction {
    time: RealmTime,
    thunk: Thunk,
    state: State
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Record {
    Resolved(Vals),
    Unresolved(Thunk)
}

pub trait Store {
    fn get(&self, path: Path) -> Result<Record, Error>;
    fn put(&self, path: Path, record: Record) -> Result<(), Error>;
    fn abort(&self, path: Path) -> Result<(), Error>;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
enum PutTry {
    Nothing,
    Attempt{count:i64, time:NodeTime},
    Success
}


#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum State {
    // Replicating thunks into nodes
    Put { nodes: BTreeMap<NodeID,PutTry> },
    // Replication failed with some set of timed-out nodes
    Err { nodes: NodeSet },
    // Waiting for the watermark to advance past us
    Seq,
    // Running the transaction thunk
    Run { eval: Evaluator },
    // Complete
    End
}

impl Transaction {
}


