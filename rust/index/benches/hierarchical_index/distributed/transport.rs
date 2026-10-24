//! Persistent TCP transport for the two-machine fixture. Each in-flight call owns
//! its stream exclusively. A readiness reactor parks idle connections outside the
//! fixed execution pools. Separate data/control pools reserve dependency capacity.
//!
//! The token authenticates a dedicated private sandbox run; this is not an
//! encrypted or production transport. Operation identity and deduplication belong
//! to the owner engine: a retry reaches that engine unchanged.

use super::observability::{detailed, RemoteRequestTiming, Scope, WorkProfile, WorkSnapshot};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc,
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
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    /// Maximum request payload and individual reply chunk, in bytes.
    /// Large replies are streamed in chunks; this does not bound decoded values.
    pub max_frame_bytes: usize,
    pub max_data_handlers: usize,
    pub max_control_handlers: usize,
    /// Bounds each request and the lifetime of an idle cached socket.
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

/// Data work has separate execution capacity for nested operations. A construction
/// handler can wait for publication or export, and export can wait for a chunk.
/// None of these waits can consume the capacity needed by the next dependency.
#[derive(Clone, Debug, Serialize)]
pub struct TransportCapacity {
    pub data_execution_lanes: [usize; 3],
    pub control_execution_lanes: [usize; 1],
    pub data_connections: usize,
    pub control_connections: usize,
    pub frame_bytes: usize,
    pub data_decoders: usize,
    pub control_decoders: usize,
}
impl TransportConfig {
    pub fn capacity(&self) -> TransportCapacity {
        TransportCapacity {
            data_execution_lanes: [self.max_data_handlers; 3],
            control_execution_lanes: [self.max_control_handlers],
            data_connections: self.max_data_handlers.saturating_mul(6),
            control_connections: self.max_control_handlers.saturating_mul(2),
            frame_bytes: self.max_frame_bytes,
            data_decoders: self.max_data_handlers.min(8),
            control_decoders: self.max_control_handlers.min(8),
        }
    }
}
fn execution_lane<Q: Any>(request: &Q) -> usize {
    use super::engine::EngineRequest;
    use super::placement_runtime::PlacementRequest;
    match (request as &dyn Any).downcast_ref::<EngineRequest>() {
        Some(EngineRequest::Placement(PlacementRequest::Chunk { .. })) => 2,
        Some(EngineRequest::Structure(_))
        | Some(EngineRequest::Shared(super::shared_runtime::SharedRequest::RefreshCentroid {
            ..
        }))
        | Some(EngineRequest::Placement(PlacementRequest::Export { .. })) => 1,
        _ => 0,
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
    by_request: parking_lot::Mutex<BTreeMap<String, TransportSnapshot>>,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    rejected_connections: AtomicU64,
    failed_connections: AtomicU64,
    opened_connections: AtomicU64,
    reused_connections: AtomicU64,
    accepted_connections: AtomicU64,
    spawned_workers: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransportSnapshot {
    pub diagnostics: WorkSnapshot,
    /// False means fine scopes and request attribution were not collected.
    #[serde(default)]
    pub detailed_enabled: bool,
    /// Fixed enum names only; unauthenticated frames are never classified.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_request: BTreeMap<String, TransportSnapshot>,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub rejected_connections: u64,
    pub failed_connections: u64,
    #[serde(default)]
    pub opened_connections: u64,
    #[serde(default)]
    pub reused_connections: u64,
    #[serde(default)]
    pub accepted_connections: u64,
    #[serde(default)]
    pub spawned_workers: u64,
}

impl TransportCounters {
    pub fn snapshot(&self) -> TransportSnapshot {
        let mut snapshot = TransportSnapshot {
            diagnostics: self.profile.snapshot(),
            detailed_enabled: detailed(),
            by_request: BTreeMap::new(),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),
            opened_connections: self.opened_connections.load(Ordering::Relaxed),
            reused_connections: self.reused_connections.load(Ordering::Relaxed),
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            spawned_workers: self.spawned_workers.load(Ordering::Relaxed),
        };
        snapshot.by_request = self.by_request.lock().clone();
        for entry in snapshot.by_request.values().cloned().collect::<Vec<_>>() {
            merge_snapshot(&mut snapshot, &entry);
        }
        snapshot
    }
    fn record_request(&self, kind: &str, local: &TransportCounters) {
        let snapshot = TransportSnapshot {
            diagnostics: local.profile.snapshot_nonzero(),
            detailed_enabled: true,
            bytes_sent: local.bytes_sent.load(Ordering::Relaxed),
            bytes_received: local.bytes_received.load(Ordering::Relaxed),
            messages_sent: local.messages_sent.load(Ordering::Relaxed),
            messages_received: local.messages_received.load(Ordering::Relaxed),
            failed_connections: local.failed_connections.load(Ordering::Relaxed),
            opened_connections: local.opened_connections.load(Ordering::Relaxed),
            reused_connections: local.reused_connections.load(Ordering::Relaxed),
            accepted_connections: local.accepted_connections.load(Ordering::Relaxed),
            spawned_workers: local.spawned_workers.load(Ordering::Relaxed),

            ..TransportSnapshot::default()
        };
        merge_snapshot(
            self.by_request.lock().entry(kind.to_owned()).or_default(),
            &snapshot,
        );
    }
}

// Merge completed connections only. In-flight work appears when its connection
// finishes; callers take phase snapshots after joining the phase's operations.
fn merge_snapshot(target: &mut TransportSnapshot, source: &TransportSnapshot) {
    target.detailed_enabled |= source.detailed_enabled;
    target.bytes_sent += source.bytes_sent;
    target.bytes_received += source.bytes_received;
    target.messages_sent += source.messages_sent;
    target.messages_received += source.messages_received;
    target.rejected_connections += source.rejected_connections;
    target.failed_connections += source.failed_connections;
    target.opened_connections += source.opened_connections;
    target.reused_connections += source.reused_connections;
    target.accepted_connections += source.accepted_connections;
    target.spawned_workers += source.spawned_workers;

    for (key, value) in &source.diagnostics.remote_requests {
        target
            .diagnostics
            .remote_requests
            .entry(key.clone())
            .or_default()
            .merge(value);
    }
    for (key, value) in &source.diagnostics.counters {
        *target.diagnostics.counters.entry(key.clone()).or_default() += value;
    }
    for (key, value) in &source.diagnostics.inclusive_scopes {
        let total = target
            .diagnostics
            .inclusive_scopes
            .entry(key.clone())
            .or_default();
        total.calls += value.calls;
        total.wall_nanos += value.wall_nanos;
        total.thread_cpu_nanos += value.thread_cpu_nanos;
        total.thread_cpu_supported_calls += value.thread_cpu_supported_calls;
        total.max_wall_nanos = total.max_wall_nanos.max(value.max_wall_nanos);
        total.max_thread_cpu_nanos = total.max_thread_cpu_nanos.max(value.max_thread_cpu_nanos);
        for (a, b) in total.wall_histogram.iter_mut().zip(value.wall_histogram) {
            *a += b;
        }
    }
}

fn request_kind<Q: Any>(request: &Q) -> &'static str {
    use super::engine::EngineRequest::*;
    let Some(request) = (request as &dyn Any).downcast_ref::<super::engine::EngineRequest>() else {
        return "Other";
    };
    match request {
        Expand(..) => "Expand",
        OwnerTransfer(request) => match request {
            super::batched_transfer::BatchRequest::Prepare { .. } => "OwnerTransfer.Prepare",
            super::batched_transfer::BatchRequest::Step { .. } => "OwnerTransfer.Step",
            super::batched_transfer::BatchRequest::Status { .. } => "OwnerTransfer.Status",
        },
        CurrentRoot => "CurrentRoot",
        CurrentView => "CurrentView",
        CurrentViewIfChanged { .. } => "CurrentViewIfChanged",
        Placement(request) => match request {
            super::placement_runtime::PlacementRequest::Export { .. } => "Placement.Export",
            super::placement_runtime::PlacementRequest::Chunk { .. } => "Placement.Chunk",
            super::placement_runtime::PlacementRequest::InstallView { .. } => {
                "Placement.InstallView"
            }
            _ => "Placement",
        },
        Shared(request) => match request {
            super::shared_runtime::SharedRequest::RefreshCentroid { .. } => {
                "Shared.RefreshCentroid"
            }
            super::shared_runtime::SharedRequest::Prepare { .. } => "Shared.Prepare",
            super::shared_runtime::SharedRequest::Finish { .. } => "Shared.Finish",
            super::shared_runtime::SharedRequest::Status { .. } => "Shared.Status",
        },
        BoundaryReady(..) => "BoundaryReady",
        BoundaryFinish { .. } => "BoundaryFinish",
        BoundaryStatus { .. } => "BoundaryStatus",
        NeighborReassign { .. } => "NeighborReassign",
        BalanceJob(..) => "BalanceJob",
        ReassignJob(..) => "ReassignJob",
        EnqueueReassign(..) => "EnqueueReassign",
        EnqueueNavigationRetry { .. } => "EnqueueNavigationRetry",
        ScheduledJob(..) => "ScheduledJob",
        Eligible => "Eligible",
        Enqueue(..) => "Enqueue",
        EnqueueNeighbor(..) => "EnqueueNeighbor",
        Structure(..) => "Structure",
        Summary { .. } => "Summary",
        Handoff { .. } => "Handoff",
        Append { .. } => "Append",
        PrepareTransfer { .. } => "PrepareTransfer",
        TransferStep { .. } => "TransferStep",
        TransferStatus { .. } => "TransferStatus",
        Root { .. } => "Root",
        Node { .. } => "Node",
        Counts => "Counts",
        Membership => "Membership",
        ValidateWriter => "ValidateWriter",
        Snapshot => "Snapshot",
        SaveOwner(..) => "SaveOwner",
        WriterCheckpoint { .. } => "WriterCheckpoint",
        ReadSavedObject(..) => "ReadSavedObject",
        ListNodes => "ListNodes",
        SnapshotNodes { .. } => "SnapshotNodes",
        OpenEpoch { .. } => "OpenEpoch",
        CloseEpoch { .. } => "CloseEpoch",
        SealEpoch { .. } => "SealEpoch",
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<Q> {
    token: String,
    budget_millis: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    diagnostics: bool,
    request: Q,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct ServerTiming {
    queue_timings_available: bool,
    decode_queue_nanos: u64,
    decode_nanos: u64,
    execution_queue_nanos: u64,
    handler_nanos: u64,
}
#[derive(Serialize, Deserialize)]
struct TimedReply<R> {
    result: Result<R, String>,
    timing: ServerTiming,
}
#[derive(Serialize)]
struct TimedReplyRef<'a, R> {
    result: &'a Result<R, String>,
    timing: &'a ServerTiming,
}
fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// Where the calling thread's most recent `RpcClient::call` spent its time,
/// in nanoseconds. Always collected; it costs a few clock reads per call.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct CallTiming {
    /// Taking a cached socket (with its liveness probe) or connecting.
    pub connect_ns: u64,
    /// Serializing the request envelope to JSON.
    pub encode_ns: u64,
    /// Writing the request frame to the socket.
    pub write_ns: u64,
    /// From the last request byte written to the reply's length prefix
    /// read: the network both ways plus all server-side work.
    pub first_byte_wait_ns: u64,
    /// Reading the rest of the reply frame.
    pub read_ns: u64,
    /// Decoding the reply JSON.
    pub decode_ns: u64,
}
/// Server-side times of the request a handler is running, in nanoseconds,
/// available to the handler through `current_server_request`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ServerRequestTiming {
    /// First request byte read to the whole frame read.
    pub read_ns: u64,
    /// Whole frame read to a decoder thread picking it up.
    pub decode_queue_ns: u64,
    /// Envelope JSON decoding and the token check.
    pub decode_ns: u64,
    /// Decoded to the reactor handing the request to an execution worker.
    pub dispatch_ns: u64,
    /// Handed off to the worker thread starting it.
    pub worker_wake_ns: u64,
    /// First request byte read to the handler starting.
    pub before_handler_ns: u64,
}
thread_local! {
    static LAST_CALL: std::cell::Cell<CallTiming> = const { std::cell::Cell::new(CallTiming {
        connect_ns: 0, encode_ns: 0, write_ns: 0, first_byte_wait_ns: 0, read_ns: 0, decode_ns: 0,
    }) };
    static SERVER_REQUEST: std::cell::Cell<Option<ServerRequestTiming>> = const { std::cell::Cell::new(None) };
}
fn update_call(f: impl FnOnce(&mut CallTiming)) {
    LAST_CALL.with(|cell| {
        let mut timing = cell.get();
        f(&mut timing);
        cell.set(timing);
    });
}
/// Timing of this thread's most recent client call.
pub fn last_call_timing() -> CallTiming {
    LAST_CALL.with(|cell| cell.get())
}
/// Timing of the request the calling handler thread is serving, if any.
pub fn current_server_request() -> Option<ServerRequestTiming> {
    SERVER_REQUEST.with(|cell| cell.get())
}

fn validate(config: &TransportConfig, token: &str) -> Result<(), TransportError> {
    if token.is_empty()
        || config.max_frame_bytes == 0
        || config.max_frame_bytes >= u32::MAX as usize
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
    let waiting = Instant::now();
    let mut prefix = [0; 4];
    read_exact_until(stream, &mut prefix, deadline, counters)?;
    let reading = Instant::now();
    update_call(|t| t.first_byte_wait_ns = nanos(reading - waiting));
    let len = u32::from_be_bytes(prefix) as usize;
    if len == CHUNKED_REPLY as usize {
        let value = receive_chunked(stream, limit, deadline, counters);
        // Chunked replies interleave reads with decoding.
        update_call(|t| t.decode_ns = nanos(reading.elapsed()));
        return value;
    }
    if len > limit {
        return Err(TransportError::FrameTooLarge);
    }
    let mut body = vec![0; len];
    read_exact_until(stream, &mut body, deadline, counters)?;
    let decoding = Instant::now();
    update_call(|t| t.read_ns = nanos(decoding - reading));
    let decode = detailed().then(|| counters.profile.operation(Scope::TransportDeserialize));
    let value = serde_json::from_slice(&body).map_err(|error| {
        #[cfg(test)]
        eprintln!("RPC JSON decoding failed: {error}");
        #[cfg(not(test))]
        let _ = error;
        TransportError::Codec("invalid JSON frame".into())
    })?;
    update_call(|t| t.decode_ns = nanos(decoding.elapsed()));
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
    // The first four bytes hold the length prefix, so prefix and body leave
    // in one write and, with Nagle off, usually one segment.
    let mut body = LimitedBuffer {
        bytes: vec![0; 4],
        limit: limit + 4,
        exceeded: false,
    };
    let encoding = Instant::now();
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
    let writing = Instant::now();
    update_call(|t| t.encode_ns = nanos(writing - encoding));
    let len = (body.bytes.len() - 4) as u32;
    body.bytes[..4].copy_from_slice(&len.to_be_bytes());
    write_all_until(stream, &body.bytes, deadline, counters)?;
    update_call(|t| t.write_ns = nanos(writing.elapsed()));
    counters.messages_sent.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// Ordinary replies retain their length-prefixed JSON encoding. A reserved prefix
// introduces a large reply: nonempty length-prefixed chunks followed by a zero
// length terminator. TCP supplies ordering. Both peers must support this framing.
const CHUNKED_REPLY: u32 = u32::MAX;

/// Serialize directly into one bounded buffer. Only a successful serialization
/// gets a terminator; a partial response always causes the connection to close.
fn send_reply<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    limit: usize,
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<(), TransportError> {
    remaining(deadline)?;
    let mut writer = ReplyWriter {
        stream,
        limit,
        deadline,
        counters,
        buffer: Vec::new(),
        started: false,
        error: None,
    };
    // Streaming serialization includes socket writes once the first chunk fills.
    let encode = detailed().then(|| counters.profile.operation(Scope::TransportSerialize));
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if let Some(error) = writer.error.take() {
            return Err(error);
        }
        let message = error.to_string();
        // Only errors before any bytes are sent can use the existing explicit
        // size rejection. Never append a second response after a partial reply.
        if !writer.started
            && (message == "expansion payload exceeds decoded byte limit"
                || message == "expansion payload size overflow")
        {
            return Err(TransportError::FrameTooLarge);
        }
        return Err(TransportError::Codec("cannot encode JSON reply".into()));
    }
    remaining(deadline)?;
    drop(encode);
    if writer.started {
        writer.emit_chunk()?;
        write_all_until(writer.stream, &0u32.to_be_bytes(), deadline, counters)?;
    } else {
        // Small replies use the original framing, with no extra round trip.
        writer.emit_chunk()?;
    }
    counters.messages_sent.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

struct ReplyWriter<'a> {
    stream: &'a mut TcpStream,
    limit: usize,
    deadline: Instant,
    counters: &'a TransportCounters,
    buffer: Vec<u8>,
    started: bool,
    error: Option<TransportError>,
}
impl ReplyWriter<'_> {
    fn emit_chunk(&mut self) -> Result<(), TransportError> {
        // Small chunks (every ordinary reply) leave with their prefix in one
        // write; copying a large chunk would cost more than the extra write.
        if self.buffer.len() <= 64 * 1024 {
            let mut frame = Vec::with_capacity(self.buffer.len() + 4);
            frame.extend_from_slice(&(self.buffer.len() as u32).to_be_bytes());
            frame.extend_from_slice(&self.buffer);
            write_all_until(self.stream, &frame, self.deadline, self.counters)?;
            self.buffer.clear();
            return Ok(());
        }
        write_all_until(
            self.stream,
            &(self.buffer.len() as u32).to_be_bytes(),
            self.deadline,
            self.counters,
        )?;
        write_all_until(self.stream, &self.buffer, self.deadline, self.counters)?;
        self.buffer.clear();
        Ok(())
    }
    fn append(&mut self, mut bytes: &[u8]) -> Result<(), TransportError> {
        while !bytes.is_empty() {
            if self.buffer.len() == self.limit {
                if !self.started {
                    // Mark started before writing: even a partial prefix forbids
                    // sending a replacement reply or automatically replaying work.
                    self.started = true;
                    write_all_until(
                        self.stream,
                        &CHUNKED_REPLY.to_be_bytes(),
                        self.deadline,
                        self.counters,
                    )?;
                }
                self.emit_chunk()?;
            }
            let count = bytes.len().min(self.limit - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
        }
        Ok(())
    }
}
impl Write for ReplyWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.append(bytes) {
            self.error = Some(error);
            return Err(io::Error::other("reply chunk write failed"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ReplyReader<'a> {
    stream: &'a mut TcpStream,
    limit: usize,
    deadline: Instant,
    counters: &'a TransportCounters,
    buffer: Vec<u8>,
    offset: usize,
    done: bool,
    error: Option<TransportError>,
}
impl ReplyReader<'_> {
    fn read_chunk(&mut self) -> Result<(), TransportError> {
        let mut prefix = [0; 4];
        read_exact_until(self.stream, &mut prefix, self.deadline, self.counters)?;
        let len = u32::from_be_bytes(prefix) as usize;
        if len > self.limit {
            return Err(TransportError::FrameTooLarge);
        }
        self.done = len == 0;
        self.buffer.resize(len, 0);
        self.offset = 0;
        read_exact_until(self.stream, &mut self.buffer, self.deadline, self.counters)
    }
}
impl Read for ReplyReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.done {
            return Ok(0);
        }
        if self.offset == self.buffer.len() {
            if let Err(error) = self.read_chunk() {
                self.error = Some(error);
                return Err(io::Error::other("reply chunk read failed"));
            }
        }
        let count = out.len().min(self.buffer.len() - self.offset);
        out[..count].copy_from_slice(&self.buffer[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

fn receive_chunked<T: DeserializeOwned>(
    stream: &mut TcpStream,
    limit: usize,
    deadline: Instant,
    counters: &TransportCounters,
) -> Result<T, TransportError> {
    let mut reader = ReplyReader {
        stream,
        limit,
        deadline,
        counters,
        buffer: Vec::new(),
        offset: 0,
        done: false,
        error: None,
    };
    // Decode incrementally instead of allocating the entire serialized reply.
    // from_reader checks the end of JSON and consumes the mandatory terminator
    // before this connection can be reused. Decoded application data still grows.
    let decode = detailed().then(|| counters.profile.operation(Scope::TransportDeserialize));
    let result = serde_json::from_reader(&mut reader);
    if let Some(error) = reader.error.take() {
        return Err(error);
    }
    let value = result.map_err(|_| TransportError::Codec("invalid chunked JSON reply".into()))?;
    remaining(deadline)?;
    drop(decode);
    counters.messages_received.fetch_add(1, Ordering::Relaxed);
    Ok(value)
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

/// Clone this client into workers. Calls exclusively borrow cached sockets; there is no
/// transport-level retry because only the engine knows whether mutation applied.
#[derive(Clone)]
pub struct RpcClient {
    data_addr: SocketAddr,
    control_addr: SocketAddr,
    token: Arc<String>,
    config: TransportConfig,
    counters: Arc<TransportCounters>,
    idle: Arc<parking_lot::Mutex<[Vec<(TcpStream, Instant)>; 2]>>,
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
            idle: Arc::new(parking_lot::Mutex::new([Vec::new(), Vec::new()])),
        })
    }
    pub fn counters(&self) -> TransportSnapshot {
        self.counters.snapshot()
    }
    pub fn call<Q: Serialize + 'static, R: DeserializeOwned>(
        &self,
        request: &Q,
        class: RequestClass,
        deadline: Instant,
    ) -> Result<R, TransportError> {
        self.call_with_timeout(request, class, deadline, self.config.max_operation_time)
    }
    /// The engine uses this only for a decoded save operation with its own budget.
    pub(super) fn call_with_timeout<Q: Serialize + 'static, R: DeserializeOwned>(
        &self,
        request: &Q,
        class: RequestClass,
        deadline: Instant,
        cap: Duration,
    ) -> Result<R, TransportError> {
        let call_started = detailed().then(Instant::now);
        let mut server_timing = None;
        let local = detailed().then(|| Arc::new(TransportCounters::default()));
        let counters = local.as_ref().unwrap_or(&self.counters);
        let elapsed = counters.profile.operation(Scope::TransportClientElapsed);
        let deadline = deadline.min(Instant::now() + cap);
        let addr = match class {
            RequestClass::Data => self.data_addr,
            RequestClass::Control => self.control_addr,
        };
        LAST_CALL.with(|cell| cell.set(CallTiming::default()));
        let connecting = Instant::now();
        let result = (|| {
            let slot = match class {
                RequestClass::Data => 0,
                RequestClass::Control => 1,
            };
            let mut stream = loop {
                let cached = self.idle.lock()[slot].pop();
                match cached {
                    Some((stream, since))
                        if since.elapsed() < self.config.max_operation_time / 2 =>
                    {
                        // Probe only before sending. Never replay a request after any write.
                        stream.set_nonblocking(true)?;
                        let mut byte = [0u8];
                        let healthy = matches!(stream.peek(&mut byte), Err(e) if e.kind() == io::ErrorKind::WouldBlock);
                        stream.set_nonblocking(false)?;
                        if healthy {
                            counters.reused_connections.fetch_add(1, Ordering::Relaxed);
                            break stream;
                        }
                    }
                    Some(_) => {}
                    None => {
                        let connect =
                            detailed().then(|| counters.profile.operation(Scope::TransportConnect));
                        let stream = TcpStream::connect_timeout(&addr, remaining(deadline)?)?;
                        counters.opened_connections.fetch_add(1, Ordering::Relaxed);
                        drop(connect);
                        break stream;
                    }
                }
            };
            stream.set_nodelay(true)?;
            update_call(|t| t.connect_ns = nanos(connecting.elapsed()));
            let millis = remaining(deadline)?.as_millis().min(u64::MAX as u128) as u64;
            if millis == 0 {
                return Err(TransportError::Deadline);
            }
            let envelope = Envelope {
                token: self.token.as_str().to_owned(),
                budget_millis: millis,
                diagnostics: detailed(),
                request,
            };
            send(
                &mut stream,
                &envelope,
                self.config.max_frame_bytes,
                deadline,
                counters,
            )
            .map_err(|error| {
                if matches!(error, TransportError::FrameTooLarge) {
                    eprintln!(
                        "transport_size_failure stage=request_encode kind={} limit={}",
                        request_kind(request),
                        self.config.max_frame_bytes
                    );
                }
                error
            })?;
            let received = if detailed() {
                receive::<TimedReply<R>>(
                    &mut stream,
                    self.config.max_frame_bytes,
                    deadline,
                    counters,
                )
                .map(|reply| {
                    server_timing = Some(reply.timing);
                    reply.result
                })
            } else {
                receive::<Result<R, String>>(
                    &mut stream,
                    self.config.max_frame_bytes,
                    deadline,
                    counters,
                )
            };
            let reply = received.map_err(|error| {
                if matches!(error, TransportError::FrameTooLarge) {
                    eprintln!(
                        "transport_size_failure stage=reply_frame_prefix kind={} limit={}",
                        request_kind(request),
                        self.config.max_frame_bytes
                    );
                }
                error
            })?;
            let capacity = match class {
                RequestClass::Data => self.config.max_data_handlers,
                RequestClass::Control => self.config.max_control_handlers,
            };
            let mut idle = self.idle.lock();
            if idle[slot].len() < capacity {
                idle[slot].push((stream, Instant::now()));
            }
            drop(idle);
            reply.map_err(|error| {
                if error == OVERSIZED_REPLY {
                    TransportError::ReplyTooLarge
                } else {
                    TransportError::Remote(error)
                }
            })
        })();
        if result.is_err() {
            counters.failed_connections.fetch_add(1, Ordering::Relaxed);
        }
        drop(elapsed);
        if let Some(local) = local {
            let client_elapsed = nanos(call_started.unwrap().elapsed());
            let decode = local
                .profile
                .snapshot_nonzero()
                .inclusive_scopes
                .get("TransportDeserialize")
                .map(|t| t.wall_nanos)
                .unwrap_or(0);
            let timed_replies = u64::from(server_timing.is_some());
            let server = server_timing.unwrap_or_default();
            local.profile.record_remote(
                request_kind(request),
                RemoteRequestTiming {
                    calls: 1,
                    errors: u64::from(result.is_err()),
                    timed_replies,
                    server_queue_timed_replies: u64::from(server.queue_timings_available),
                    client_elapsed_nanos: client_elapsed,
                    max_client_elapsed_nanos: client_elapsed,
                    client_decode_nanos: decode,
                    server_decode_queue_nanos: server.decode_queue_nanos,
                    server_decode_nanos: server.decode_nanos,
                    server_execution_queue_nanos: server.execution_queue_nanos,
                    server_handler_nanos: server.handler_nanos,
                    bytes_sent: local.bytes_sent.load(Ordering::Relaxed),
                    bytes_received: local.bytes_received.load(Ordering::Relaxed),
                },
            );
            self.counters.record_request(request_kind(request), &local);
        }
        result
    }
}

struct PendingFrame {
    stream: TcpStream,
    prefix: [u8; 4],
    prefix_read: usize,
    body: Vec<u8>,
    started: Instant,
    queued: Instant,
    /// Always-collected instants for `ServerRequestTiming`.
    complete: Instant,
    decode_started: Instant,
    decoded: Instant,
    decode_ready: Option<Instant>,
    decode_finished: Option<Instant>,
    timing: ServerTiming,
    counters: Arc<TransportCounters>,
}
impl PendingFrame {
    fn new(stream: TcpStream, aggregate: &Arc<TransportCounters>) -> Self {
        Self {
            stream,
            prefix: [0; 4],
            prefix_read: 0,
            body: Vec::new(),
            started: Instant::now(),
            queued: Instant::now(),
            complete: Instant::now(),
            decode_started: Instant::now(),
            decoded: Instant::now(),
            decode_ready: None,
            decode_finished: None,
            timing: ServerTiming {
                queue_timings_available: detailed(),
                ..ServerTiming::default()
            },
            counters: if detailed() {
                Arc::default()
            } else {
                aggregate.clone()
            },
        }
    }
    fn read(&mut self, limit: usize, cap: Duration) -> Result<bool, TransportError> {
        remaining(self.started + cap)?;
        for _ in 0..8 {
            remaining(self.started + cap)?;
            let mut chunk = [0u8; 16384];
            let target = if self.prefix_read < 4 {
                &mut self.prefix[self.prefix_read..]
            } else {
                let len = u32::from_be_bytes(self.prefix) as usize;
                if len > limit {
                    return Err(TransportError::FrameTooLarge);
                }
                if self.body.len() == len {
                    return Ok(true);
                }
                let count = chunk.len().min(len - self.body.len());
                &mut chunk[..count]
            };
            match self.stream.read(target) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
                Ok(n) => {
                    // Each request receives a fresh budget from its first byte;
                    // idle expiry remains bounded independently.
                    if self.prefix_read == 0 {
                        self.started = Instant::now();
                    }
                    self.counters
                        .bytes_received
                        .fetch_add(n as u64, Ordering::Relaxed);
                    if self.prefix_read < 4 {
                        self.prefix_read += n;
                    } else {
                        self.body.extend_from_slice(&chunk[..n]);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        if self.prefix_read == 4 && u32::from_be_bytes(self.prefix) as usize > limit {
            return Err(TransportError::FrameTooLarge);
        }
        Ok(self.prefix_read == 4 && self.body.len() == u32::from_be_bytes(self.prefix) as usize)
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
        for (listener, capacity, lane_count) in [
            (data, config.max_data_handlers, 3),
            (control, config.max_control_handlers, 1),
        ] {
            let stop = server.stop.clone();
            let counters = server.counters.clone();
            let handler = handler.clone();
            let timeout = timeout.clone();
            let token = token.clone();
            let config = config.clone();
            server.listeners.push(thread::spawn(move || {
                let mut jobs = Vec::new();
                let mut receivers = Vec::new();
                for _ in 0..lane_count {
                    let (sender, receiver) = mpsc::sync_channel::<(PendingFrame, Envelope<Q>)>(capacity);
                    jobs.push(sender);
                    receivers.push(Arc::new(parking_lot::Mutex::new(receiver)));
                }
                let (finished, completed) = mpsc::channel();
                #[cfg(unix)]
                let (mut wake_read, wake_write) = std::os::unix::net::UnixStream::pair().expect("reactor wake socket");
                #[cfg(unix)]
                wake_read.set_nonblocking(true).expect("nonblocking wake socket");
                #[cfg(unix)]
                let wake_write = { wake_write.set_nonblocking(true).expect("nonblocking wake socket"); Arc::new(wake_write) };
                let mut workers = Vec::new();
                for (lane, receiver) in receivers.into_iter().enumerate() {
                  for _ in 0..capacity {
                    let receiver = receiver.clone();
                    let finished = finished.clone();
                    let aggregate = counters.clone();
                    let handler = handler.clone();
                    let timeout = timeout.clone();
                    let config = config.clone();
                    #[cfg(unix)]
                    let wake_write = wake_write.clone();
                    counters.spawned_workers.fetch_add(1, Ordering::Relaxed);
                    workers.push(thread::spawn(move || loop {
                        let job = receiver.lock().recv();
                        let Ok((mut frame, envelope)) = job else { break };
                        let counters = &frame.counters;
                        let kind = request_kind(&envelope.request);
                        let result = (|| -> Result<(), TransportError> {
                            frame.stream.set_nonblocking(false)?;
                            if detailed() {
                                counters.profile.record_wall(Scope::TransportThreadStartWait, frame.queued.elapsed().as_nanos() as u64);
                            }
                            let deadline = frame.started.checked_add(Duration::from_millis(envelope.budget_millis).min(timeout(&envelope.request)))
                                .ok_or(TransportError::InvalidConfig)?;
                            remaining(deadline)?;
                            let execution = counters.profile.operation(Scope::TransportServerExecution);
                            if envelope.diagnostics {
                                frame.timing.execution_queue_nanos = frame.decode_finished.map(|t| nanos(t.elapsed())).unwrap_or(0);
                            }
                            let handler_started = envelope.diagnostics.then(Instant::now);
                            let starting = Instant::now();
                            let since = |later: Instant, earlier: Instant| nanos(later.saturating_duration_since(earlier));
                            SERVER_REQUEST.with(|cell| cell.set(Some(ServerRequestTiming {
                                read_ns: since(frame.complete, frame.started),
                                decode_queue_ns: since(frame.decode_started, frame.complete),
                                decode_ns: since(frame.decoded, frame.decode_started),
                                dispatch_ns: since(frame.queued, frame.decoded),
                                worker_wake_ns: since(starting, frame.queued),
                                before_handler_ns: since(starting, frame.started),
                            })));
                            let reply = handler(envelope.request);
                            SERVER_REQUEST.with(|cell| cell.set(None));
                            if let Some(started) = handler_started { frame.timing.handler_nanos = nanos(started.elapsed()); }
                            drop(execution);
                            let sent = if envelope.diagnostics {
                                send_reply(&mut frame.stream, &TimedReplyRef { result: &reply, timing: &frame.timing }, config.max_frame_bytes, deadline, counters)
                            } else {
                                send_reply(&mut frame.stream, &reply, config.max_frame_bytes, deadline, counters)
                            };
                            match sent {
                                Err(TransportError::FrameTooLarge) => {
                                    let rejection: Result<(), String> = Err(OVERSIZED_REPLY.into());
                                    if envelope.diagnostics {
                                        send_reply(&mut frame.stream, &TimedReply { result: rejection, timing: frame.timing.clone() }, config.max_frame_bytes, deadline, counters)
                                    } else { send_reply(&mut frame.stream, &rejection, config.max_frame_bytes, deadline, counters) }
                                }
                                result => result,
                            }
                        })();
                        if result.is_err() { counters.failed_connections.fetch_add(1, Ordering::Relaxed); }
                        if detailed() { aggregate.record_request(kind, counters); }
                        let stream = result.ok().map(|_| frame.stream);
                        if finished.send((lane, stream)).is_err() { break; }
                        #[cfg(unix)]
                        { let _ = (&*wake_write).write(&[1]); }
                    }));
                  }
                }
                let (decode_sender, decode_receiver) = mpsc::sync_channel::<PendingFrame>(capacity * lane_count);
                let decode_receiver = Arc::new(parking_lot::Mutex::new(decode_receiver));
                let (decoded_sender, decoded_receiver) = mpsc::channel();
                let mut decoders = Vec::new();
                for _ in 0..capacity.min(8) {
                    let receiver = decode_receiver.clone();
                    let sender = decoded_sender.clone();
                    let token = token.clone();
                    #[cfg(unix)]
                    let wake_write = wake_write.clone();
                    counters.spawned_workers.fetch_add(1, Ordering::Relaxed);
                    decoders.push(thread::spawn(move || loop {
                        let Ok(mut frame) = receiver.lock().recv() else { break };
                        frame.decode_started = Instant::now();
                        let decoding_started = detailed().then_some(frame.decode_started);
                        if let Some(ready) = frame.decode_ready { frame.timing.decode_queue_nanos = nanos(ready.elapsed()); }
                        let decode = detailed().then(|| frame.counters.profile.operation(Scope::TransportDeserialize));
                        let envelope = serde_json::from_slice::<Envelope<Q>>(&frame.body)
                            .map_err(|_| TransportError::Codec("invalid JSON frame".into()))
                            .and_then(|envelope| {
                                frame.counters.messages_received.fetch_add(1, Ordering::Relaxed);
                                if token_matches(token.as_bytes(), envelope.token.as_bytes()) { Ok(envelope) }
                                else { Err(TransportError::Remote("unauthorized run".into())) }
                            });
                        drop(decode);
                        frame.decoded = Instant::now();
                        if let Some(started) = decoding_started {
                            frame.timing.decode_nanos = nanos(started.elapsed());
                            frame.decode_finished = Some(Instant::now());
                        }
                        frame.body = Vec::new();
                        if sender.send((frame, envelope)).is_err() { break; }
                        #[cfg(unix)]
                        { let _ = (&*wake_write).write(&[1]); }
                    }));
                }
                let mut decoding = 0usize;
                let mut pending = Vec::<PendingFrame>::new();
                let mut active = vec![0usize; lane_count];
                while !stop.load(Ordering::Acquire) {
                    while let Ok((lane, stream)) = completed.try_recv() {
                        active[lane] -= 1;
                        if let Some(stream) = stream {
                            if stream.set_nonblocking(true).is_ok() {
                                pending.push(PendingFrame::new(stream, &counters));
                            }
                        }
                    }
                    while let Ok((mut frame, envelope)) = decoded_receiver.try_recv() {
                        decoding -= 1;
                        let envelope = match envelope {
                            Ok(envelope) => envelope,
                            Err(_) => {
                                frame.counters.failed_connections.fetch_add(1, Ordering::Relaxed);
                                if detailed() { counters.record_request("UnauthenticatedOrUndecodable", &frame.counters); }
                                continue;
                            }
                        };
                        let lane = execution_lane(&envelope.request).min(lane_count - 1);
                        if active[lane] >= capacity {
                            counters.rejected_connections.fetch_add(1, Ordering::Relaxed);
                            if detailed() { counters.record_request(request_kind(&envelope.request), &frame.counters); }
                        } else {
                            frame.queued = Instant::now();
                            active[lane] += 1;
                            if jobs[lane].send((frame, envelope)).is_err() { stop.store(true, Ordering::Release); break; }
                        }
                    }
                    for _ in 0..32 {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                counters.accepted_connections.fetch_add(1, Ordering::Relaxed);
                                // Bound idle sockets and partially assembled frames separately
                                // from executing handlers. Admission never waits on a data pool.
                                if pending.len() + decoding + active.iter().sum::<usize>() >= capacity.saturating_mul(lane_count).saturating_mul(2) {
                                    counters.rejected_connections.fetch_add(1, Ordering::Relaxed);
                                } else if stream.set_nonblocking(true).and_then(|_| stream.set_nodelay(true)).is_ok() {
                                    pending.push(PendingFrame::new(stream, &counters));
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(_) => { stop.store(true, Ordering::Release); break; }
                        }
                    }
                    let mut i = 0;
                    while i < pending.len() {
                        match pending[i].read(config.max_frame_bytes, config.max_operation_time) {
                            Ok(false) => i += 1,
                            Ok(true) => {
                                let mut frame = pending.swap_remove(i);
                                frame.complete = Instant::now();
                                frame.decode_ready = detailed().then_some(frame.complete);
                                match decode_sender.try_send(frame) {
                                    Ok(()) => decoding += 1,
                                    Err(mpsc::TrySendError::Full(frame)) | Err(mpsc::TrySendError::Disconnected(frame)) => {
                                        counters.rejected_connections.fetch_add(1, Ordering::Relaxed);
                                        if detailed() { counters.record_request("UnauthenticatedOrUndecodable", &frame.counters); }
                                    }
                                }
                            }
                            Err(error) => {
                                let frame = pending.swap_remove(i);
                                // A clean close of an idle reusable socket is normal.
                                if !matches!(error, TransportError::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof && frame.prefix_read == 0) {
                                    frame.counters.failed_connections.fetch_add(1, Ordering::Relaxed);
                                }
                                if detailed() { counters.record_request("UnauthenticatedOrUndecodable", &frame.counters); }
                            }
                        }
                    }
                    #[cfg(unix)]
                    if matches!(accept_mode, AcceptMode::Readiness) {
                        use std::os::fd::AsRawFd;
                        let mut fds = Vec::with_capacity(pending.len() + 2);
                        fds.push(libc::pollfd { fd: listener.as_raw_fd(), events: libc::POLLIN, revents: 0 });
                        fds.push(libc::pollfd { fd: wake_read.as_raw_fd(), events: libc::POLLIN, revents: 0 });
                        fds.extend(pending.iter().map(|frame| libc::pollfd { fd: frame.stream.as_raw_fd(), events: libc::POLLIN, revents: 0 }));
                        // All descriptors remain owned until poll returns. Ten milliseconds
                        // bounds shutdown; arrivals and worker returns wake immediately.
                        unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, 10); }
                        let mut bytes = [0u8; 256];
                        while wake_read.read(&mut bytes).is_ok_and(|n| n > 0) {}
                    } else { thread::sleep(Duration::from_millis(1)); }
                    #[cfg(not(unix))]
                    { let _ = accept_mode; thread::sleep(Duration::from_millis(1)); }
                }
                drop(listener);
                drop(pending);
                drop(decode_sender);
                for decoder in decoders { let _ = decoder.join(); }
                drop(jobs);
                // Handlers retain their own deadline contract; join mutations before exit.
                for worker in workers { let _ = worker.join(); }

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
