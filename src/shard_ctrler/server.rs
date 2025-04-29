use super::msg::*;
use crate::kvraft::server::Server;
use crate::{Request, State};
use serde::{Deserialize, Serialize};

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardInfo {
    // Your data here.
}

impl State for ShardInfo {
    type Command = Request<Op>;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }

    fn check_duplicate(&self, cmd: &Self::Command) -> Option<Self::Output> {
        todo!()
    }
}
