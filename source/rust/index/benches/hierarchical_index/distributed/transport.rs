//! Bounded TCP transport for the two-machine fixture. Each call owns a connection,
//! so concurrent workers do not serialize on a shared stream. Separate listeners
//! reserve control-handler capacity independently from construction requests.
//!
//! The token authenticates a dedicated private sandbox run; this is not an
//! encrypted or production transport. Operation identity and deduplication belong
//! to the owner engine: a retry reaches that engine unchanged.

use super::observability::{WorkProfile, WorkSnapshot, Scope, detailed};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Read once when binding so comparisons can use the same executable. Readiness
/// waiting removes the idle accept delay without changing admission or workers.
#[derive(Clone, Copy)]
enum AcceptMode {
    Readiness,
    Poll1ms,
}
impl AcceptMode {
    fn from_environment() -> Result<Self, TransportError> {
        match std::env::var("SPANN_ACCEPT_MODE") {
            Ok(mode) if mode == "readiness" => Ok(Self::Readiness),
            Ok(mode) if mode == "poll_1ms" => Ok(Self::Poll1ms),
            Err(std::env::VarError::NotPresent) => Ok(Self::Readiness),
            _ => Err(TransportError::InvalidConfig),
        }
    }

    fn wait(self, listener: &TcpListener) -> io::Result<()> {
        match self {
            Self::Poll1ms => thread::sleep(Duration::from_millis(1)),
            Self::Readiness => {
                #[cfg(unix)]
                {
                    use std::os::fd::AsRawFd;
                    let mut descriptor = libc::pollfd {
                        fd: listener.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // The listener remains owned by this thread for the entire
                    // call. One initialized descriptor is valid for nfds=1.
                    // The timeout bounds idle shutdown even without arrivals.
                    let result = unsafe { libc::poll(&mut descriptor, 1, 10) };
                    if result < 0 {
                        let error = io::Error::last_os_error();
                        if error.kind() != io::ErrorKind::Interrupted {
                            return Err(error);
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = listener;
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub max_frame_bytes: usize,
    pub max_data_handlers: usize,
    pub max_control_handlers: usize,
    /// Caps both client calls and accepted connections, including idle peers.
    pub max_operation_time: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_data_handlers: 64,
            max_control_handlers: 16,
            max_operation_time: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RequestClass {
    Data,
    Control,
}

#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
    Deadline,
    FrameTooLarge,
    ReplyTooLarge,
    Codec(String),
    Remote(String),
    InvalidConfig,
    Shutdown,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "transport I/O: {e}"),
            Self::Deadline => write!(f, "transport operation deadline expired"),
            Self::FrameTooLarge => write!(f, "transport frame exceeds configured limit"),
            Self::ReplyTooLarge => write!(f, "transport reply exceeds configured limit"),
            Self::Codec(e) => write!(f, "transport encoding: {e}"),
            Self::Remote(e) => write!(f, "remote operation: {e}"),
            Self::InvalidConfig => write!(f, "invalid transport configuration"),
            Self::Shutdown => write!(f, "transport server is shutting down"),
        }
    }
}
impl std::error::Error for TransportError {}
impl From<io::Error> for TransportError {
    fn from(e: io::Error) -> Self {
        if matches!(
            e.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) {
            Self::Deadline
        } else {
            Self::Io(e)
        }
    }
}

#[derive(Default)]
pub struct TransportCounters {
    profile: WorkProfile,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    rejected_connections: AtomicU64,
    failed_connections: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransportSnapshot {
    pub diagnostics: WorkSnapshot,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub rejected_connections: u64,
    pub failed_connections: u64,
}

impl TransportCounters {
    pub fn snapshot(&self) -> TransportSnapshot {
        TransportSnapshot {
            diagnostics: self.profile.snapshot(),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<Q> {
    token: String,
    budget_millis: u64,
    request: Q,
}

fn validate(config: &TransportConfig, token: &str) -> Result<(), TransportError> {
    if token.is_empty()
        || config.max_frame_bytes == 0
        || config.max_frame_bytes > u32::MAX as usize
        || config.max_data_handlers == 0
        || config.max_control_handlers == 0
        || config.max_operation_time.is_zero()
        || Instant::now()
            .checked_add(config.max_operation_time)
            .is_none()
    {
        return Err(TransportError::InvalidConfig);
    }
    Ok(())
}

fn remaining(deadline: Instant) -> Result<Duration, TransportError> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(TransportError::Deadline)
    } else {
        Ok(left)
    }
}

fn read_exact_until(
    stream: &mut TcpStream,
    mut buf: &mut [u8],
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<(), TransportError> {
    while !buf.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => {
                counters
                    .bytes_received
                    .fetch_add(n as u64, Ordering::Relaxed);
                buf = &mut buf[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    remaining(deadline)?;
    Ok(())
}

fn write_all_until(
    stream: &mut TcpStream,
    mut buf: &[u8],
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<(), TransportError> {
    while !buf.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Ok(n) => {
                counters.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                buf = &buf[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    remaining(deadline)?;
    Ok(())
}

fn receive<T: DeserializeOwned>(
    stream: &mut TcpStream,
    limit: usize,
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<T, TransportError> {
    let mut prefix = [0; 4];
    read_exact_until(stream, &mut prefix, deadline, counters)?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len > limit {
        return Err(TransportError::FrameTooLarge);
    }
    let mut body = vec![0; len];
    read_exact_until(stream, &mut body, deadline, counters)?;
    let decode = detailed().then(|| counters.profile.operation(Scope::TransportDeserialize));
    let value = serde_json::from_slice(&body).map_err(|error| {
        #[cfg(test)]
        eprintln!("RPC JSON decoding failed: {error}");
        #[cfg(not(test))]
        let _ = error;
        TransportError::Codec("invalid JSON frame".into())
    })?;
    remaining(deadline)?;
    drop(decode);
    counters.messages_received.fetch_add(1, Ordering::Relaxed);
    Ok(value)
}

fn send<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    limit: usize,
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<(), TransportError> {
    remaining(deadline)?;
    // The writer refuses oversized values while encoding, before allocating an
    // unbounded serialization buffer. The receiver validates before allocating.
    let mut body = LimitedBuffer {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    let encode = detailed().then(|| counters.profile.operation(Scope::TransportSerialize));
    serde_json::to_writer(&mut body, value).map_err(|error| {
        // The expansion codec enforces its independent field ceiling before
        // encoding. Only these exact size failures are safe to subdivide;
        // invalid data and allocation failures remain ordinary codec failures.
        let message = error.to_string();
        if body.exceeded
            || message == "expansion payload exceeds decoded byte limit"
            || message == "expansion payload size overflow"
        {
            TransportError::FrameTooLarge
        } else {
            TransportError::Codec("cannot encode JSON frame".into())
        }
    })?;
    remaining(deadline)?;
    drop(encode);
    write_all_until(
        stream,
        &(body.bytes.len() as u32).to_be_bytes(),
        deadline,
        counters,
    )?;
    write_all_until(stream, &body.bytes, deadline, counters)?;
    counters.messages_sent.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl Write for LimitedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let available = self.limit.saturating_sub(self.bytes.len());
        if buf.len() > available {
            self.exceeded = true;
            // Retain the bound without confusing an exactly full valid prefix
            // with a serializer's unrelated later error.
            self.bytes.extend_from_slice(&buf[..available]);
            return Err(io::Error::other("frame limit"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Clone this client into workers. Calls use independent sockets; there is no
/// transport-level retry because only the engine knows whether mutation applied.
#[derive(Clone)]
pub struct RpcClient {
    data_addr: SocketAddr,
    control_addr: SocketAddr,
    token: Arc<String>,
    config: TransportConfig,
    counters: Arc<TransportCounters>,
}

const OVERSIZED_REPLY: &str = "__spann_transport_reply_too_large_v1__";

impl RpcClient {
    pub(super) fn bounded_deadline(&self, requested: Instant) -> Instant {
        requested.min(Instant::now() + self.config.max_operation_time)
    }
    pub fn new(
        data_addr: SocketAddr,
        control_addr: SocketAddr,
        token: String,
        config: TransportConfig,
    ) -> Result<Self, TransportError> {
        validate(&config, &token)?;
        Ok(Self {
            data_addr,
            control_addr,
            token: Arc::new(token),
            config,
            counters: Arc::default(),
        })
    }
    pub fn counters(&self) -> TransportSnapshot {
        self.counters.snapshot()
    }
    pub fn call<Q: Serialize, R: DeserializeOwned>(
        &self,
        request: &Q,
        class: RequestClass,
        deadline: Instant,
    ) -> Result<R, TransportError> {
        self.call_with_timeout(request, class, deadline, self.config.max_operation_time)
    }
    /// The engine uses this only for a decoded save operation with its own budget.
    pub(super) fn call_with_timeout<Q: Serialize, R: DeserializeOwned>(
        &self,
        request: &Q,
        class: RequestClass,
        deadline: Instant,
        cap: Duration,
    ) -> Result<R, TransportError> {
        let _elapsed = self.counters.profile.operation(Scope::TransportClientElapsed);
        let deadline = deadline.min(Instant::now() + cap);
        let addr = match class {
            RequestClass::Data => self.data_addr,
            RequestClass::Control => self.control_addr,
        };
        let result = (|| {
            let connect = detailed().then(|| self.counters.profile.operation(Scope::TransportConnect));
            let mut stream = TcpStream::connect_timeout(&addr, remaining(deadline)?)?;
            drop(connect);
            stream.set_nodelay(true)?;
            let millis = remaining(deadline)?.as_millis().min(u64::MAX as u128) as u64;
            if millis == 0 {
                return Err(TransportError::Deadline);
            }
            let envelope = Envelope {
                token: self.token.as_str().to_owned(),
                budget_millis: millis,
                request,
            };
            send(
                &mut stream,
                &envelope,
                self.config.max_frame_bytes,
                deadline,
                &self.counters,
            )?;
            let reply: Result<R, String> = receive(
                &mut stream,
                self.config.max_frame_bytes,
                deadline,
                &self.counters,
            )?;
            reply.map_err(|error| {
                if error == OVERSIZED_REPLY {
                    TransportError::ReplyTooLarge
                } else {
                    TransportError::Remote(error)
                }
            })
        })();
        if result.is_err() {
            self.counters
                .failed_connections
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

/// A bounded server with independent data/control admission. Handlers must bound
/// their own computation/engine waits: transport deadlines bound socket I/O, and
/// shutdown joins admitted handlers rather than abandoning mutable operations.
pub struct RpcServer {
    data_addr: SocketAddr,
    control_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    counters: Arc<TransportCounters>,
    listeners: Vec<JoinHandle<()>>,
}

impl RpcServer {
    pub fn bind<Q, R, F>(
        data_bind: SocketAddr,
        control_bind: SocketAddr,
        token: String,
        config: TransportConfig,
        handler: Arc<F>,
    ) -> Result<Self, TransportError>
    where
        Q: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(Q) -> Result<R, String> + Send + Sync + 'static,
    {
        let cap = config.max_operation_time;
        Self::bind_with_request_timeout(
            data_bind,
            control_bind,
            token,
            config,
            handler,
            Arc::new(move |_: &Q| cap),
        )
    }
    /// Classify only an authenticated, decoded request. Frame admission retains
    /// the ordinary timeout. The callback must return a bounded operation cap.
    pub fn bind_with_request_timeout<Q, R, F, T>(
        data_bind: SocketAddr,
        control_bind: SocketAddr,
        token: String,
        config: TransportConfig,
        handler: Arc<F>,
        timeout: Arc<T>,
    ) -> Result<Self, TransportError>
    where
        Q: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(Q) -> Result<R, String> + Send + Sync + 'static,
        T: Fn(&Q) -> Duration + Send + Sync + 'static,
    {
        validate(&config, &token)?;
        let accept_mode = AcceptMode::from_environment()?;
        let data = TcpListener::bind(data_bind)?;
        let control = TcpListener::bind(control_bind)?;
        data.set_nonblocking(true)?;
        control.set_nonblocking(true)?;
        let mut server = Self {
            data_addr: data.local_addr()?,
            control_addr: control.local_addr()?,
            stop: Arc::default(),
            counters: Arc::default(),
            listeners: Vec::new(),
        };
        for (listener, capacity) in [
            (data, config.max_data_handlers),
            (control, config.max_control_handlers),
        ] {
            let stop = server.stop.clone();
            let counters = server.counters.clone();
            let handler = handler.clone();
            let timeout = timeout.clone();
            let token = token.clone();
            let config = config.clone();
            server.listeners.push(thread::spawn(move || {
                let active = Arc::new(AtomicUsize::new(0));
                let mut workers: Vec<JoinHandle<()>> = Vec::new();
                while !stop.load(Ordering::Acquire) {
                    let mut i = 0;
                    while i < workers.len() {
                        if workers[i].is_finished() {
                            let _ = workers.swap_remove(i).join();
                        } else {
                            i += 1;
                        }
                    }
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if active.load(Ordering::Acquire) >= capacity {
                                counters
                                    .rejected_connections
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            active.fetch_add(1, Ordering::AcqRel);
                            let active = active.clone();
                            let handler = handler.clone();
                            let timeout = timeout.clone();
                            let counters = counters.clone();
                            let token = token.clone();
                            let config = config.clone();
                            let spawn_counters = counters.clone();
                            let _spawn = detailed().then(|| spawn_counters.profile.operation(Scope::TransportThreadSpawn));
                            let thread_wait = counters.clone();
                            let queued_at = Instant::now();
                            workers.push(thread::spawn(move || {
                                if detailed() { thread_wait.profile.record_wall(Scope::TransportThreadStartWait, queued_at.elapsed().as_nanos() as u64); }
                                struct Slot(Arc<AtomicUsize>);
                                impl Drop for Slot {
                                    fn drop(&mut self) {
                                        self.0.fetch_sub(1, Ordering::AcqRel);
                                    }
                                }
                                let _slot = Slot(active);
                                let accepted = Instant::now();
                                let deadline = accepted + config.max_operation_time;
                                #[cfg(test)]
                                let mut failure_stage = "request frame or envelope";
                                let result = (|| -> Result<(), TransportError> {
                                    // Accepted sockets can inherit the listener's nonblocking
                                    // mode. Socket timeouts require blocking reads and writes.
                                    stream.set_nonblocking(false)?;
                                    stream.set_nodelay(true)?;
                                    let envelope: Envelope<Q> = receive(
                                        &mut stream,
                                        config.max_frame_bytes,
                                        deadline,
                                        &counters,
                                    )?;
                                    if !token_matches(token.as_bytes(), envelope.token.as_bytes()) {
                                        return Err(TransportError::Remote(
                                            "unauthorized run".into(),
                                        ));
                                    }
                                    let budget = Duration::from_millis(envelope.budget_millis);
                                    let deadline = accepted
                                        .checked_add(budget.min(timeout(&envelope.request)))
                                        .ok_or(TransportError::InvalidConfig)?;
                                    remaining(deadline)?;
                                    #[cfg(test)]
                                    {
                                        failure_stage = "handler reply serialization or send";
                                    }
                                    let execution = counters.profile.operation(Scope::TransportServerExecution);
                                    let reply = handler(envelope.request);
                                    drop(execution);
                                    match send(
                                        &mut stream,
                                        &reply,
                                        config.max_frame_bytes,
                                        deadline,
                                        &counters,
                                    ) {
                                        Err(TransportError::FrameTooLarge) => {
                                            // Encoding finishes before a prefix is sent.
                                            // Tell the client the known size condition;
                                            // this does not imply a mutation was unapplied.
                                            let rejection: Result<(), String> =
                                                Err(OVERSIZED_REPLY.into());
                                            send(
                                                &mut stream,
                                                &rejection,
                                                config.max_frame_bytes,
                                                deadline,
                                                &counters,
                                            )
                                        }
                                        result => result,
                                    }
                                })();
                                if result.is_err() {
                                    counters.failed_connections.fetch_add(1, Ordering::Relaxed);
                                    #[cfg(test)]
                                    if let Err(error) = &result {
                                        // Never print the authenticated envelope or token.
                                        eprintln!(
                                            "RPC connection failed during {failure_stage}: {error}"
                                        );
                                    }
                                }
                            }));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            if accept_mode.wait(&listener).is_err() {
                                counters.failed_connections.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => {
                            counters.failed_connections.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                drop(listener);
                for worker in workers {
                    let _ = worker.join();
                }
            }));
        }
        Ok(server)
    }
    pub fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }
    pub fn control_addr(&self) -> SocketAddr {
        self.control_addr
    }
    pub fn counters(&self) -> TransportSnapshot {
        self.counters.snapshot()
    }
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        for listener in self.listeners.drain(..) {
            let _ = listener.join();
        }
    }
}
impl Drop for RpcServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn token_matches(expected: &[u8], received: &[u8]) -> bool {
    if expected.len() != received.len() {
        return false;
    }
    expected
        .iter()
        .zip(received)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
