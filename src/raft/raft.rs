use futures::{channel::mpsc, stream::FuturesUnordered, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task,
    time::*,
};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

const INVALID_X_TERM: u64 = 0;
const INVALID_X_INDEX: u64 = 0;

const ELECTION_TIMEOUT_MIN_DURATION_MILLIS: u64 = 200;
const ELECTION_TIMEOUT_MAX_DURATION_MILLIS: u64 = 400;
const APPEND_ENTRIES_INTERVAL: Duration = Duration::from_millis(100);
const RECHECK_INTERVAL: Duration = Duration::from_millis(50);

// log entry struct
#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LogEntry {
    start: Start,
    command: Vec<u8>,
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Copy, Serialize, Deserialize)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,
    election_timeout_start: Instant,
    election_timeout_duration: Duration,
    state: State,
    log: Vec<LogEntry>,
    snapshot: Vec<u8>,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
struct State {
    // non-volatile states
    term: u64,
    voted_for: Option<usize>,
    last_included_index: u64,
    last_included_term: u64,
    // volatile states
    role: Role,
    commit_index: u64,
    last_applied: u64,
    // leader states
    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl State {
    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    term: u64,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    last_included_index: u64,
    last_included_term: u64,
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft({})", self.me)
    }
}

// HINT: put async functions here
impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            election_timeout_start: Instant::now(),
            election_timeout_duration: Default::default(),
            state: State::default(),
            log: vec![],
            snapshot: vec![],
        }));
        inner.lock().unwrap().log.push(LogEntry::default()); // add a sentinel
        inner.lock().unwrap().reset_election_timeout();
        let handle = RaftHandle { inner };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server(); // activate all RPC handlers
        let handle_clone = handle.clone();
        task::spawn(async move {
            handle_clone.append_entries_ticker().await;
        })
        .detach();
        let handle_clone = handle.clone();
        task::spawn(async move {
            handle_clone.election_ticker().await;
        })
        .detach();
        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise, start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let res = {
            let mut raft = self.inner.lock().unwrap();
            raft.start(cmd)
        };
        self.persist().await.expect("failed to persist");
        res
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.state.is_leader()
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't had more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        let raft = self.inner.lock().unwrap();
        if last_included_index < raft.state.last_included_index {
            return false;
        }
        if last_included_index >= raft.get_real_log_len() as u64 {
            return false;
        }
        true
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        let res = Ok(());
        {
            let mut raft = self.inner.lock().unwrap();
            if index <= raft.state.last_included_index {
                return res;
            }
            if index > raft.get_real_log_len() as u64 {
                return res;
            }
            if index > raft.state.commit_index {
                return res;
            }
            info!(
                "Server {} at term {} gets snapshot from the service with index {}",
                raft.me, raft.state.term, index
            );
            raft.snapshot = snapshot.to_vec();
            let mut tmp = vec![];
            tmp.push(LogEntry {
                start: Start {
                    index,
                    term: raft
                        .log
                        .get(raft.get_local_index(index) as usize)
                        .unwrap()
                        .start
                        .term,
                },
                command: vec![],
            });
            tmp.extend_from_slice(&raft.log[raft.get_local_index(index) as usize + 1..]);
            raft.log = tmp;
            if raft.log.len() == 0 {
                panic!("1")
            }
            raft.state.last_included_index = index;
            raft.state.last_included_term = raft
                .log
                .get(raft.get_local_index(index) as usize)
                .unwrap()
                .start
                .term;
        }
        self.persist().await.expect("failed to persist");
        res
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let (state, snapshot) = {
            let raft = self.inner.lock().unwrap();
            let persist: Persist = Persist {
                term: raft.state.term,
                voted_for: raft.state.voted_for,
                log: raft.log.clone(),
                last_included_index: raft.state.last_included_index,
                last_included_term: raft.state.last_included_term,
            };
            let snapshot: Vec<u8> = raft.snapshot.clone();
            (bincode::serialize(&persist).unwrap(), snapshot)
        };
        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                let mut raft = self.inner.lock().unwrap();
                raft.snapshot = snapshot.clone();
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                let mut raft = self.inner.lock().unwrap();
                raft.state.term = persist.term;
                raft.state.voted_for = persist.voted_for;
                raft.log = persist.log.clone();
                raft.state.last_included_index = persist.last_included_index;
                raft.state.last_included_term = persist.last_included_term;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut raft = self.inner.lock().unwrap();
        raft.state.last_applied = max(raft.state.last_applied, raft.state.last_included_index);
        raft.state.commit_index = max(raft.state.commit_index, raft.state.last_included_index);
        Ok(())
    }

    // add RPC handlers here
    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current(); // this is a simulation net

        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.request_vote(args).await.unwrap() }
        });

        let this = self.clone();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let this = this.clone();
            async move { this.append_entries(args).await.unwrap() }
        });

        let this = self.clone();
        net.add_rpc_handler(move |args: InstallSnapshotArgs| {
            let this = this.clone();
            async move { this.install_entries(args).await.unwrap() }
        });
        // add more RPC handlers here
    }

    // async RPC handlers, since persist() requires async operations(file I/O)
    async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }
    async fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.append_entries(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }
    async fn install_entries(&self, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.install_snapshot(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    async fn election_ticker(&self) {
        loop {
            let ok = {
                let raft = self.inner.lock().unwrap();
                raft.state.role != Role::Leader
                    && raft.election_timeout_start.elapsed() >= raft.election_timeout_duration
            }; // make sure 'raft'(the lock) is scoped
            if ok {
                let self_clone = self.clone();
                // 'self' only lives inside this method, so we should pass its clone to the async task
                task::spawn(async move {
                    self_clone
                        .inner
                        .lock()
                        .unwrap()
                        .send_vote_request(self_clone.inner.clone());
                    self_clone.persist().await.expect("failed to persist");
                })
                .detach();
            }
            sleep(Duration::from_millis(rand::rng().gen_range(50..350))).await;
        }
    }

    async fn append_entries_ticker(&self) {
        loop {
            if self.inner.lock().unwrap().state.role != Role::Leader {
                sleep(Duration::from(RECHECK_INTERVAL)).await;
                continue;
            }
            let self_clone = self.clone();
            task::spawn(async move {
                {
                    self_clone
                        .inner
                        .lock()
                        .unwrap()
                        .send_append_entries(self_clone.inner.clone());
                    self_clone.persist().await.expect("failed to persist");
                }
            })
            .detach();
            sleep(Duration::from(APPEND_ENTRIES_INTERVAL)).await;
        }
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn become_follower(&mut self, term: u64) {
        self.state.role = Role::Follower;
        self.state.term = term;
        self.state.voted_for = None;
        // we will persist states in async methods
    }
    fn become_candidate(&mut self) {
        self.state.role = Role::Candidate;
        self.state.term += 1;
        self.state.voted_for = Some(self.me); // self vote
    }
    fn become_leader(&mut self) {
        // only a candidate can become a leader
        self.state.role = Role::Leader;
        // init next_index and match_index everytime elected as a leader
        self.state.next_index = vec![self.get_real_log_len() as u64; self.peers.len()];
        self.state.match_index = vec![0; self.peers.len()];
    }
    fn get_local_index(&self, index: u64) -> u64 {
        if index < self.state.last_included_index {
            panic!()
        }
        index - self.state.last_included_index
    }
    fn get_real_log_len(&self) -> usize {
        self.log.len() + self.state.last_included_index as usize
    }
    fn get_first_index(&self, term: u64) -> u64 {
        // get the first entry's index with the given term
        for (_, en) in self.log.iter().enumerate() {
            if en.start.term == term {
                return en.start.index;
            }
        }
        INVALID_X_INDEX
    }
    fn get_last_index(&self, term: u64) -> u64 {
        // get the last entry's index with the given term
        let mut ans = INVALID_X_INDEX;
        for (_, en) in self.log.iter().enumerate() {
            if en.start.term == term {
                ans = en.start.index;
            }
            if en.start.term > term {
                break;
            }
        }
        ans
    }
    fn get_quorum_index(&self) -> u64 {
        let mut tmp: Vec<u64> = self.state.match_index.clone();
        tmp.sort_unstable();
        tmp[(tmp.len() - 1) / 2]
    }
    fn reset_election_timeout(&mut self) {
        self.election_timeout_start = Instant::now();
        self.election_timeout_duration =
            Duration::from_millis(rand::rng().gen_range(
                ELECTION_TIMEOUT_MIN_DURATION_MILLIS..ELECTION_TIMEOUT_MAX_DURATION_MILLIS,
            ))
    }

    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }
        let start = Start {
            index: self.get_real_log_len() as u64,
            term: self.state.term,
        };
        self.log.push(LogEntry {
            start,
            command: data.to_vec(),
        });
        info!(
            "Server {} at term {} gets a new command {} from the service",
            self.me,
            self.state.term,
            self.get_real_log_len()
        );
        Ok(start)
    }

    // apply committed message.
    fn apply_entries(&mut self) {
        let mut cnt = 0;
        // commit log[last_applied+1, commit_index]
        for i in self.state.last_applied + 1..=self.state.commit_index {
            if i >= self.get_real_log_len() as u64 {
                break;
            }
            if i < self.state.last_included_index {
                continue;
            }
            let local_index = self.get_local_index(i);
            if local_index == 0 {
                continue;
            }
            let msg = ApplyMsg::Command {
                data: self.log.get(local_index as usize).unwrap().command.clone(),
                index: self.log.get(local_index as usize).unwrap().start.index,
            };
            info!(
                "Server {} at term {} applies entry {}",
                self.me, self.state.term, i
            );
            self.apply_ch.unbounded_send(msg).unwrap();
            cnt += 1;
        }
        self.state.last_applied += cnt;
    }

    fn apply_snapshot(&mut self) {
        let msg = ApplyMsg::Snapshot {
            data: self.snapshot.clone(),
            term: self.state.last_included_term,
            index: self.state.last_included_index,
        };
        info!(
            "Server {} at term {} applies snapshot {}",
            self.me, self.state.term, self.state.last_included_index
        );
        self.state.last_applied = max(self.state.last_applied, self.state.last_included_index);
        self.state.commit_index = max(self.state.commit_index, self.state.last_included_index);
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    // true RPC handlers
    fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        // Reply false if args.term < self term
        if args.term < self.state.term {
            reply.term = self.state.term;
            return reply;
        }
        // become follower when seeing a higher term
        if args.term > self.state.term {
            self.become_follower(args.term)
        }
        self.reset_election_timeout();
        // now the term is aligned
        // if votedFor is null or candidate_id, and candidate's log is at
        // least as up-to-date as receiver's log, grant vote
        let last_entry = self.log.last().unwrap();
        if (self.state.voted_for.is_none() || self.state.voted_for.unwrap() == args.candidate_id)
            && (last_entry.start.term < args.last_log_term
                || (last_entry.start.term == args.last_log_term
                    && last_entry.start.index <= args.last_log_index))
        {
            self.state.voted_for = Some(args.candidate_id);
            reply.vote_granted = true;
            reply.term = self.state.term;
        }
        reply
    }

    fn append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut reply = AppendEntriesReply {
            term: 0,
            success: false,
            follower_id: self.me,
            prev_log_index: args.prev_log_index,
            entries_len: args.entries.len(),
            args_term: args.term,
            x_term: 0,
            x_index: 0,
            x_len: 0,
        };
        // Reply false if term < self term
        if args.term < self.state.term {
            reply.term = self.state.term;
            reply.success = false;
            return reply;
        }
        // become follower when seeing a higher term
        if args.term >= self.state.term {
            self.become_follower(args.term)
        }
        // receive a heartbeat from the current leader, ok to reset the election timer
        self.reset_election_timeout();
        reply.term = self.state.term;
        if args.prev_log_index < self.state.last_included_index {
            return reply;
        }
        if args.prev_log_index < self.get_real_log_len() as u64
            && (self
                .log
                .get(self.get_local_index(args.prev_log_index) as usize)
                .unwrap()
                .start
                .term
                == args.prev_log_term)
        {
            for (i, en) in args.entries.iter().enumerate() {
                // check if the follower's log entries are in sync with the leader's
                let idx = self.get_local_index(args.prev_log_index) + 1 + i as u64;
                if idx < self.log.len() as u64
                    && en.start.term != self.log.get(idx as usize).unwrap().start.term
                {
                    // delete the conflicting one and entries following that
                    self.log.truncate(idx as usize);
                }
                // case 1: in sync
                // case 2: no this entry at all
                // case 3: the above if stmt is executed and the log is truncated
                // for case 2 and case 3, we know idx >= len(rf.Log) is true, so we append this entry
                if idx >= self.log.len() as u64 {
                    self.log.push(en.clone());
                }
                // for case 1, we do nothing, since the entries are already in sync
            }
            // we will persist later
            if args.leader_commit > self.state.commit_index {
                let tmp = self.state.commit_index;
                if args.entries.len() == 0 {
                    self.state.commit_index = max(self.state.commit_index, args.leader_commit);
                } else {
                    self.state.commit_index = max(
                        self.state.commit_index,
                        min(args.leader_commit, args.entries.last().unwrap().start.index),
                    );
                }
                if self.state.commit_index > tmp {
                    self.apply_entries();
                }
            }
            reply.success = true;
        } else {
            reply.success = false;
            if args.prev_log_index < self.get_real_log_len() as u64 {
                reply.x_term = self
                    .log
                    .get(self.get_local_index(args.prev_log_index) as usize)
                    .unwrap()
                    .start
                    .term;
            } else {
                reply.x_term = INVALID_X_TERM
            }
            reply.x_index = self.get_first_index(args.prev_log_term);
            reply.x_len = self.get_real_log_len();
        }
        reply
    }

    fn install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        let mut reply = InstallSnapshotReply {
            term: 0,
            args_term: args.term,
            args_last_included_index: args.last_included_index,
            follower_id: self.me,
        };
        if args.term < self.state.term {
            reply.term = self.state.term;
            return reply;
        }
        if args.last_included_index <= self.state.last_included_index {
            reply.term = self.state.term;
            return reply;
        }
        self.become_follower(args.term);
        self.reset_election_timeout();
        reply.term = self.state.term;
        info!(
            "Server {} at term {} gets the snapshot with index {}",
            self.me, self.state.term, args.last_included_index
        );
        // Rule5: save snapshot file, discard any existing or partial snapshot with a smaller index
        self.snapshot = args.data;
        // Rule6: If existing log entry has same index and term as snapshot’s
        // last included entry, retain log entries following it and reply
        let local_index = self.get_local_index(args.last_included_index);
        self.state.last_included_term = args.last_included_term;
        self.state.last_included_index = args.last_included_index;
        if local_index < self.log.len() as u64
            && self.log.get(local_index as usize).unwrap().start.term == args.last_included_term
        {
            // todo: should we add the sentinel?
            let mut tmp = vec![];
            tmp.push(LogEntry {
                start: Start {
                    index: self.state.last_included_index,
                    term: self.state.last_included_term,
                },
                command: vec![],
            });
            tmp.extend_from_slice(&self.log[local_index as usize + 1..]);
            self.log = tmp;
            self.apply_snapshot();
            return reply;
        }
        // Rule7: discard all log
        self.log = vec![];
        // still we need a sentinel
        self.log.push(LogEntry {
            start: Start {
                index: self.state.last_included_index,
                term: self.state.last_included_term,
            },
            command: vec![], // nil
        });
        self.apply_snapshot();
        reply
    }

    // generate random number.
    fn generate_rpc_timeout() -> Duration {
        Duration::from_millis(40)
    }

    fn send_vote_request(&mut self, arc: Arc<Mutex<Raft>>) {
        self.become_candidate();
        self.reset_election_timeout();
        let args: RequestVoteArgs = RequestVoteArgs {
            term: self.state.term,
            candidate_id: self.me,
            last_log_index: self.log.last().unwrap().start.index,
            last_log_term: self.log.last().unwrap().start.term,
        };
        let timeout = Self::generate_rpc_timeout();
        let net = net::NetLocalHandle::current();

        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            // NOTE: `call` function takes ownerships
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<RequestVoteArgs, RequestVoteReply>(peer, args, timeout)
                    .await
            });
        }
        let mut votes: i64 = 1; // self vote
                                // spawn a concurrent task
        let inner = arc.clone();
        task::spawn(async move {
            // handle RPC tasks in completion order
            while let Some(res) = rpcs.next().await {
                let mut raft = inner.lock().unwrap();
                if raft.state.role != Role::Candidate {
                    break; // give up
                }
                if let Ok(resp) = res {
                    if resp.term > raft.state.term {
                        raft.become_follower(resp.term);
                        break;
                    }
                    if resp.term < raft.state.term || args.term != raft.state.term {
                        continue; // this response is outdated
                    }
                    if resp.term == raft.state.term && resp.vote_granted {
                        votes += 1;
                        if votes >= (raft.peers.len() / 2 + 1) as i64
                            && raft.state.role == Role::Candidate
                        {
                            // become leader
                            info!(
                                "Node {} at term {} becomes the leader.",
                                raft.me, raft.state.term
                            );
                            raft.become_leader();
                            // don't care about other votes
                            break;
                        }
                    }
                } else {
                    continue;
                }
            }
        })
        .detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop
    }

    fn send_append_entries(&mut self, arc: Arc<Mutex<Raft>>) {
        let timeout = Self::generate_rpc_timeout();
        let net = net::NetLocalHandle::current();
        let mut append_entries_rpcs = FuturesUnordered::new();
        let mut install_snapshot_rpcs = FuturesUnordered::new();
        let term = self.state.term;
        for (i, &peer) in self.peers.iter().enumerate() {
            if self.state.role != Role::Leader || term != self.state.term {
                return;
            }
            if i == self.me {
                self.state.match_index[i] = self.get_real_log_len() as u64 - 1;
                self.state.next_index[i] = self.get_real_log_len() as u64;
                continue;
            }
            // if last log index >= next_index for a follower: send
            // AppendEntries RPC with log entries starting at nextIndex
            let pre_index = max(0, self.state.next_index[i] - 1);
            if pre_index < self.state.last_included_index {
                // PartD: if a follower falls so far behind that
                // the leader has discarded the log entries it needs to catch up,
                // the leader must then send a snapshot plus the log starting at the time of the snapshot
                let net = net.clone();
                let args: InstallSnapshotArgs = InstallSnapshotArgs {
                    term,
                    leader_id: self.me,
                    last_included_index: self.state.last_included_index,
                    last_included_term: self.state.last_included_term,
                    data: self.snapshot.clone(),
                };
                install_snapshot_rpcs.push(async move {
                    net.call_timeout::<InstallSnapshotArgs, InstallSnapshotReply>(
                        peer, args, timeout,
                    )
                    .await
                });
            } else {
                // pre_index >= self.state.last_included_index
                let mut entries: Vec<LogEntry> = vec![];
                let local_index = self.get_local_index(pre_index);
                if self.log.last().unwrap().start.index >= self.state.next_index[i] {
                    entries.extend_from_slice(&self.log[local_index as usize + 1..]);
                }
                let net = net.clone();
                let args: AppendEntriesArgs = AppendEntriesArgs {
                    term,
                    leader_id: self.me,
                    prev_log_index: pre_index,
                    prev_log_term: self.log.get(local_index as usize).unwrap().start.term,
                    entries,
                    leader_commit: self.state.commit_index,
                };
                append_entries_rpcs.push(async move {
                    net.call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, args, timeout)
                        .await
                });
            }
        }
        // leader handles AppendEntriesReply
        let inner = arc.clone();
        task::spawn(async move {
            while let Some(res) = append_entries_rpcs.next().await {
                let mut raft = inner.lock().unwrap(); // this is the latest info
                if raft.state.role != Role::Leader {
                    break; // give up
                }
                if let Ok(resp) = res {
                    if resp.args_term != raft.state.term {
                        continue;
                    }
                    if resp.success {
                        raft.state.match_index[resp.follower_id] = max(
                            raft.state.match_index[resp.follower_id],
                            resp.prev_log_index + resp.entries_len as u64,
                        );
                        raft.state.next_index[resp.follower_id] = max(
                            raft.state.next_index[resp.follower_id],
                            raft.state.match_index[resp.follower_id] + 1,
                        );
                        let quorum = raft.get_quorum_index();
                        if quorum < raft.state.last_included_index {
                            continue;
                        }
                        let local_quorum_index = raft.get_local_index(quorum);
                        if local_quorum_index >= raft.log.len() as u64 {
                            continue;
                        }
                        if quorum > raft.state.commit_index
                            && raft
                                .log
                                .get(local_quorum_index as usize)
                                .unwrap()
                                .start
                                .term
                                == raft.state.term
                        {
                            raft.state.commit_index = max(raft.state.commit_index, quorum);
                            // ok to commit new entries
                            raft.apply_entries();
                        }
                    } else {
                        if resp.term > raft.state.term {
                            raft.become_follower(resp.term);
                            break;
                        }
                        // decrement next_index and wait for next AppendEntries
                        // the following line is too slow!
                        // raft.state.next_index[resp.follower_id] = max(1, raft.state.next_index[resp.follower_id]-1);
                        if resp.x_term == INVALID_X_TERM {
                            // Case 3: follower's log is too short, then nextIndex = XLen
                            raft.state.next_index[resp.follower_id] = max(1, resp.x_len as u64);
                        } else {
                            let idx = raft.get_last_index(resp.x_term);
                            if idx == INVALID_X_INDEX {
                                // Case 1: leader doesn't have XTerm, then nextIndex = XIndex
                                raft.state.next_index[resp.follower_id] = max(1, resp.x_index);
                            } else {
                                // Case 2: leader has XTerm, then nextIndex = leader's last entry for XTerm
                                raft.state.next_index[resp.follower_id] = max(1, idx);
                            }
                        }
                    }
                }
            }
        })
        .detach();
        let inner = arc.clone();
        task::spawn(async move {
            while let Some(res) = install_snapshot_rpcs.next().await {
                let mut raft = inner.lock().unwrap();
                if raft.state.role != Role::Leader {
                    break; // give up
                }
                if let Ok(resp) = res {
                    if resp.args_term != raft.state.term {
                        continue;
                    }
                    if resp.term > raft.state.term {
                        raft.become_follower(resp.term);
                        break;
                    }
                    raft.state.match_index[resp.follower_id] = resp.args_last_included_index;
                    raft.state.next_index[resp.follower_id] = resp.args_last_included_index + 1;
                }
            }
        })
        .detach();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    term: u64,
    candidate_id: usize,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    term: u64,
    vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: u64,
    leader_id: usize,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: u64,
    success: bool,
    // the following three fields can be optimized someday...
    follower_id: usize,
    prev_log_index: u64,
    entries_len: usize,
    args_term: u64,
    // for quick rollback matchIndex and nextIndex
    x_term: u64,
    x_index: u64,
    x_len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstallSnapshotArgs {
    term: u64,
    leader_id: usize,
    last_included_index: u64,
    last_included_term: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstallSnapshotReply {
    term: u64,
    // the following two could be optimized...
    args_term: u64,
    args_last_included_index: u64,
    follower_id: usize,
}
