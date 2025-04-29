use super::msg::*;
use crate::Request;
use madsim::{net, time::*};
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Clerk {
    core: ClerkCore<Request<Op>, String>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.core.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.core.call(Op::Append { key, value }).await;
    }
}

pub struct ClerkCore<Req, Rsp> {
    servers: Vec<SocketAddr>,
    _mark: std::marker::PhantomData<(Req, Rsp)>,
    id: AtomicUsize,
    seq: AtomicUsize,
}

impl<Req, Rsp> ClerkCore<Request<Req>, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            servers,
            _mark: std::marker::PhantomData,
            id: AtomicUsize::new(rand_usize()),
            seq: AtomicUsize::new(1),
        }
    }

    pub async fn call(&self, args: Req) -> Rsp {
        let net = net::NetLocalHandle::current();
        loop {
            for i in 0..self.servers.len() {
                let ret = net
                    .call_timeout::<Request<Req>, Result<Rsp, Error>>(
                        self.servers[i],
                        Request {
                            payload: args.clone(),
                            sender: self.id.load(Ordering::Relaxed),
                            seq: self.seq.load(Ordering::Relaxed),
                        },
                        Duration::from_millis(500),
                    )
                    .await;
                if let Ok(Ok(resp)) = ret {
                    self.seq.fetch_add(1, Ordering::Relaxed);
                    return resp;
                }
                // keep trying forever facing all other errors: timeout, not leader...
            }
        }
    }
}

pub fn rand_usize() -> usize {
    madsim::rand::rng().gen()
}
