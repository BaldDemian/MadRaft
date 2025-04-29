#[macro_use]
extern crate log;

use madsim::net;
use serde::{Deserialize, Serialize};

pub mod kvraft;
pub mod raft;
pub mod shard_ctrler;
pub mod shardkv;

pub trait State: net::Message + Default {
    type Command: net::Message + Clone + CommandInfo;
    type Output: net::Message + Clone;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
    fn check_duplicate(&self, cmd: &Self::Command) -> Option<Self::Output>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request<T> {
    pub(crate) payload: T,
    pub(crate) sender: usize,
    pub(crate) seq: usize,
}

pub trait CommandInfo {
    fn sender(&self) -> usize;
    fn seq(&self) -> usize;
}

impl<T> CommandInfo for Request<T> {
    fn sender(&self) -> usize {
        self.sender
    }
    fn seq(&self) -> usize {
        self.seq
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Pair {
    pub(crate) sender: usize,
    pub(crate) seq: usize,
}
