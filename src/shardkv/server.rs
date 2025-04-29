use super::msg::*;
use crate::kvraft::server::Server;
use crate::shard_ctrler::client::Clerk as CtrlerClerk;
use crate::{Request, State};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        todo!("construct ShardKv");
        let inner = Server::new(servers, me, max_raft_state).await;
        Arc::new(ShardKvServer { inner })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    // Your data here.
}

impl State for ShardKv {
    type Command = Request<Op>;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }

    fn check_duplicate(&self, cmd: &Self::Command) -> Option<Self::Output> {
        todo!()
    }
}
