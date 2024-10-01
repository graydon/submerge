use core::fmt::Debug;
use core::hash::Hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use submerge_base::{err, Error};
use submerge_lang::{Expr, Path, Vals};

pub trait Data: Clone + Debug + Eq + PartialEq + Ord + Hash {}
impl<T> Data for T where T: Clone + Debug + Eq + PartialEq + Ord + Hash {}

// A given Realm is a single, coherent, distributed system. It is composed of
// a set of Nodes, each of which has a unique NodeID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct NodeID(pub i64);

// NodeTime is a virtual time-point in signed 64-bit microseconds
// since the epoch. This is sufficient to span 292,471 years.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct NodeTime(i64);

// Duration is a time-span in signed 64-bit microseconds relative to
// some NodeTime or RealmTime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Duration(i64);

// RealmTimes are realm-local extended timestamps. The most
// significant (time) field stores a NodeTime (microsecond count), but
// this is then followed by both a NodeID and an event count allowing
// each node to label any event with a RealmTime without coordination
// with other Nodes, _and_ with essentially arbitrary numbers of
// sub-microsecond events without implying anything about real time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct RealmTime {
    time: NodeTime,
    node: NodeID,
    event: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SpecificMsg {
    Ping,
    Put(Expr, Vec<Path>),
    Ack,
}

// All inter-node communication takes the form of Messages. A message has
// a set of common fields, followed by a variable (enum) field for the
// specifics of a given type of message.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Msg {
    src: NodeID,
    dst: NodeID,
    txn_time: RealmTime, // Uniquely identifies transaction
    msg_time: RealmTime,
    sequence: i64,
    response: bool,
    specific: SpecificMsg,
}

// Each message sent or received turns into a single [u8] buffer added to
// the incoming or outgoing deque of the associated IOQueues. Transports
// then turn these into bytes-on-the-wire with whatever framing the transport
// finds necessary.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct IOQueues {
    outgoing: VecDeque<(NodeID, Box<[u8]>)>,
    incoming: VecDeque<(NodeID, Box<[u8]>)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Request {
    req: Box<Msg>,
    res: Option<Box<Msg>>,
}

// A Node organizes the communication for the process, in terms
// of sending and receiving messages with other nodes.
#[derive(Clone, Debug, Eq, PartialEq, Default, Hash)]
pub struct Node {
    /// The set of decoded incoming one-way messages awaiting consumption. The
    /// [`Node::recv_msg`] function will alternate messages between returning
    /// these and complete requests.
    incoming: VecDeque<Box<Msg>>,
    /// The set of request messages that have been sent but either not yet
    /// responded-to, or not yet consumed by [`Node::recv_msg`].
    requests: BTreeMap<i64, Request>,
    /// The set of decoded incoming request/response pairs awaiting consumption.
    complete: VecDeque<i64>,
    /// The set of incoming and outgoing serialized byte buffers associated with
    /// each peer node. [`Node::recv_bytes`] and [`Node::send_bytes`] operate on
    /// these.
    ioqueues: IOQueues,
}

#[derive(Clone, Debug, Eq, PartialEq, Default, Hash)]
pub enum RecvMsg {
    #[default]
    NoMsgs,
    Single(Box<Msg>),
    Paired {
        req: Box<Msg>,
        res: Box<Msg>,
    },
}

impl Node {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send_msg(&mut self, msg: Msg) -> Result<(), Error> {
        let dst = msg.dst;
        let buf = rmp_serde::to_vec(&msg)?;
        self.ioqueues
            .outgoing
            .push_back((dst, buf.into_boxed_slice()));
        Ok(())
    }

    pub fn maybe_pop_incoming_msg(&mut self) -> Option<Box<Msg>> {
        // When incoming and complete both have content, alternate
        // messages from one or the other.
        if self.incoming.len() + self.complete.len() & 1 == 0 {
            self.incoming.pop_front()
        } else {
            None
        }
    }

    pub fn recv_msg(&mut self) -> Result<RecvMsg, Error> {
        if self.incoming.is_empty() && self.complete.is_empty() {
            if let Some((src, buf)) = self.ioqueues.incoming.pop_front() {
                self.decode_msg(src, buf)?;
            }
        }

        if let Some(msg) = self.maybe_pop_incoming_msg() {
            Ok(RecvMsg::Single(msg))
        } else if let Some(id) = self.complete.pop_front() {
            if let Some(req) = self.requests.remove(&id) {
                if req.req.sequence != id {
                    return Err(err("Unexpected sequence"));
                }
                if req.req.response {
                    return Err(err("Request is a response"));
                }
                if let Some(res) = req.res {
                    if res.sequence != id {
                        return Err(err("Mismatched sequence"));
                    }
                    if !res.response {
                        return Err(err("Response is not a response"));
                    }
                    Ok(RecvMsg::Paired { req: req.req, res })
                } else {
                    Err(err("Missing response in complete request"))
                }
            } else {
                Err(err("Missing request"))
            }
        } else {
            Ok(RecvMsg::NoMsgs)
        }
    }

    pub fn recv_bytes(&mut self, src: NodeID, buf: Box<[u8]>) -> Result<(), Error> {
        self.ioqueues.incoming.push_back((src, buf));
        Ok(())
    }

    pub fn send_byes(&mut self) -> Result<Option<(NodeID, Box<[u8]>)>, Error> {
        if let Some((dst, buf)) = self.ioqueues.outgoing.pop_front() {
            Ok(Some((dst, buf)))
        } else {
            Ok(None)
        }
    }

    fn decode_msg(&mut self, src: NodeID, buf: Box<[u8]>) -> Result<(), Error> {
        let msg: Box<Msg> = Box::new(rmp_serde::from_slice(buf.as_ref())?);
        if msg.src != src {
            return Err(err("Mismatched source"));
        }
        if let Some(req) = self.requests.get_mut(&msg.sequence) {
            if req.res.is_none() {
                self.complete.push_back(msg.sequence);
                req.res = Some(msg);
            } else {
                return Err(err("Duplicate response"));
            }
        } else {
            self.incoming.push_back(msg);
        }
        Ok(())
    }
}
