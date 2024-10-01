// A server collects together all resources necessary to function as a replica
// of realm's tables and to support all necesary functions of the realm.
//
// A server may support one or more clients, or it may be configured strictly as
// an unloaded replica for redundancy.
//
// A server may be an active or passive replica. Active replicas participate in
// the replicated commit protocol, and therefore wait for one another (or at
// least a quorum of one another). Passive replicas can lag behind active
// replicas, can store and flood low-consistency data, but cannot initiate
// high-consistency write transactions.

pub enum ServerState {
    Idle,
    Running,
}

pub trait ServerTrait {}

struct ServerImpl {}

impl ServerTrait for ServerImpl {}

pub type Server = Box<dyn ServerTrait>;
