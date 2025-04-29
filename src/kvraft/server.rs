use super::msg::*;
use crate::raft::ApplyMsg;
use crate::{raft, CommandInfo, Pair, Request, State};
use futures::{channel::oneshot, lock::Mutex as AsyncMutex, select, FutureExt, StreamExt};
use madsim::net;
use madsim::task;
use madsim::time;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    state: Arc<AsyncMutex<S>>,
    max_raft_state: Option<usize>,
    notify: Arc<Mutex<HashMap<Pair, oneshot::Sender<S::Output>>>>,
}

impl<S: State> Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S> Server<S>
where
    S: State,
{
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let (rf, mut apply_ch) = raft::RaftHandle::new(servers, me).await;
        let server = Arc::new(Server {
            rf,
            me,
            state: Arc::new(AsyncMutex::new(S::default())),
            max_raft_state,
            notify: Arc::new(Mutex::new(HashMap::new())),
        });
        {
            let server = server.clone();
            task::spawn(async move {
                while let Some(msg) = apply_ch.next().await {
                    match msg {
                        ApplyMsg::Command { data, index } => {
                            let cmd: S::Command = bincode::deserialize(&data).unwrap();
                            let out = {
                                let mut st = server.state.lock().await;
                                if let Some(cached) = st.check_duplicate(&cmd) {
                                    cached
                                } else {
                                    st.apply(cmd.clone())
                                }
                            };
                            let p = Pair {
                                sender: cmd.sender(),
                                seq: cmd.seq(),
                            };
                            if let Some(tx) = server.notify.lock().unwrap().remove(&p) {
                                let _ = tx.send(out.clone());
                            }
                            if let Some(max) = server.max_raft_state {
                                if server.rf.raft_state_size() >= max {
                                    let snap = {
                                        let st = server.state.lock().await;
                                        bincode::serialize(&*st).unwrap()
                                    };
                                    let _ = server.rf.snapshot(index, &snap).await;
                                }
                            }
                        }
                        ApplyMsg::Snapshot {
                            data,
                            term: _,
                            index: _,
                        } => {
                            let snap: S = bincode::deserialize(&data).unwrap();
                            let mut st = server.state.lock().await;
                            *st = snap;
                        }
                    }
                }
            })
            .detach();
        }
        server.start_rpc_server();
        server
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.rf.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();
        let this = self.clone();
        net.add_rpc_handler(move |cmd: S::Command| {
            let this = this.clone();
            async move { this.apply_rpc(cmd).await }
        });
    }

    async fn apply_rpc(&self, cmd: S::Command) -> Result<S::Output, Error> {
        if let Some(res) = {
            let st = self.state.lock().await;
            st.check_duplicate(&cmd)
        } {
            return Ok(res);
        }
        let data = bincode::serialize(&cmd).unwrap();
        if let Err(_) = self.rf.start(&*data).await {
            return Err(Error::NotLeader { hint: 0 }); // err means that this peer is not a leader
        }

        let p = Pair {
            sender: cmd.sender(),
            seq: cmd.seq(),
        };
        let (tx, rx) = oneshot::channel();
        self.notify.lock().unwrap().insert(p, tx);
        let mut rx = rx.fuse();
        let mut delay = time::sleep(Duration::from_millis(20)).fuse();
        loop {
            select! {
                res = rx => return res.map_err(|_| Error::NotLeader { hint: 0 }),
                _ = delay => {
                    if !self.rf.is_leader() {
                        self.notify.lock().unwrap().remove(&p);
                        return Err(Error::NotLeader { hint: 0 });
                    }
                    delay = time::sleep(Duration::from_millis(20)).fuse();
                }
            }
        }
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    data: HashMap<String, String>,
    cache: HashMap<Pair, String>,
    handled: HashMap<usize, usize>,
}

impl State for Kv {
    type Command = Request<Op>;
    type Output = String;

    fn apply(&mut self, req: Self::Command) -> Self::Output {
        let p = Pair {
            sender: req.sender,
            seq: req.seq,
        };
        let (sender, seq) = (req.sender.clone(), req.seq.clone());
        let res = match req.payload {
            Op::Get { key } => self.data.get(&key).cloned().unwrap_or_default(),
            Op::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                "".to_string()
            }
            Op::Append { key, value } => {
                let old = self.data.get(&key).cloned().unwrap_or_default();
                let new = format!("{}{}", old, value);
                self.data.insert(key.clone(), new.clone());
                "".to_string()
            }
        };
        self.cache.insert(p.clone(), res.clone());
        self.handled.insert(req.sender, req.seq);
        self.cache
            .retain(|&pair, _| !(pair.sender == sender && pair.seq < seq));
        res
    }

    fn check_duplicate(&self, req: &Self::Command) -> Option<Self::Output> {
        let p = Pair {
            sender: req.sender,
            seq: req.seq,
        };
        if let Some(&last) = self.handled.get(&req.sender) {
            if req.seq <= last {
                return self.cache.get(&p).cloned();
            }
        }
        None
    }
}
