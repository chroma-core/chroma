use super::*;
use std::sync::{Condvar, Mutex};

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
fn config() -> TransportConfig {
    TransportConfig {
        max_frame_bytes: 4096,
        max_data_handlers: 2,
        max_control_handlers: 1,
        max_operation_time: Duration::from_secs(3),
    }
}
fn client(server: &RpcServer) -> RpcClient {
    RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        config(),
    )
    .unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[test]
fn parallel_roundtrips_preserve_duplicate_requests_and_count_wire_bytes() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(move |q: u64| {
            observed.lock().unwrap().push(q);
            Ok(q + 1)
        }),
    )
    .unwrap();
    let client = client(&server);
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let client = client.clone();
            thread::spawn(move || {
                client
                    .call::<_, u64>(&7u64, RequestClass::Data, deadline())
                    .unwrap()
            })
        })
        .collect();
    for worker in threads {
        assert_eq!(worker.join().unwrap(), 8);
    }
    server.shutdown();
    assert_eq!(*seen.lock().unwrap(), vec![7, 7]);
    let sent = client.counters();
    let received = server.counters();
    assert_eq!(sent.messages_sent, 2);
    assert_eq!(sent.messages_received, 2);
    assert_eq!(sent.bytes_sent, received.bytes_received);
    assert_eq!(sent.bytes_received, received.bytes_sent);
    assert!(sent.bytes_sent > 8);
    assert_eq!(sent.failed_connections, 0);
    assert_eq!(sent.detailed_enabled, detailed());
    if detailed() {
        let kind = &sent.by_request["Other"];
        assert_eq!(kind.bytes_sent, sent.bytes_sent);
        assert_eq!(kind.messages_sent, 2);
        assert_eq!(
            kind.diagnostics.inclusive_scopes["TransportConnect"].calls,
            2
        );
        assert_eq!(
            received.by_request["Other"].diagnostics.inclusive_scopes["TransportServerExecution"]
                .calls,
            2
        );
        assert_eq!(
            received.by_request["Other"].diagnostics.inclusive_scopes["TransportThreadStartWait"]
                .calls,
            2
        );
    } else {
        assert!(sent.by_request.is_empty());
    }
}

#[test]
fn fragmented_frames_work_and_oversized_prefixes_do_not_reach_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(move |q: u64| {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(q)
        }),
    )
    .unwrap();
    let mut stream = TcpStream::connect(server.data_addr()).unwrap();
    stream.set_nodelay(true).unwrap();
    let body = serde_json::to_vec(&Envelope {
        diagnostics: false,
        token: "test-run-token".to_string(),
        budget_millis: 2000,
        request: 42u64,
    })
    .unwrap();
    for byte in (body.len() as u32).to_be_bytes().iter().chain(body.iter()) {
        stream.write_all(&[*byte]).unwrap();
    }
    let reply: Result<u64, String> =
        receive(&mut stream, 4096, deadline(), &TransportCounters::default()).unwrap();
    assert_eq!(reply.unwrap(), 42);
    let mut oversized = TcpStream::connect(server.data_addr()).unwrap();
    oversized.write_all(&4097u32.to_be_bytes()).unwrap();
    oversized
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut byte = [0];
    assert!(matches!(oversized.read(&mut byte), Ok(0) | Err(_)));
    server.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(server.counters().failed_connections, 1);
}

#[test]
fn wrong_run_token_never_dispatches_and_errors_do_not_contain_token() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(move |q: u64| {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(q)
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "wrong-secret-token".into(),
        config(),
    )
    .unwrap();
    let error = client
        .call::<_, u64>(&7u64, RequestClass::Control, deadline())
        .unwrap_err();
    assert!(!format!("{error:?}").contains("secret-token"));
    server.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn control_capacity_survives_saturated_data_handlers() {
    let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
    let observed = gate.clone();
    let mut cfg = config();
    cfg.max_data_handlers = 1;
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        cfg.clone(),
        Arc::new(move |q: u64| {
            if q == 1 {
                let (lock, changed) = &*observed;
                let mut state = lock.lock().unwrap();
                *state = 1;
                changed.notify_all();
                while *state == 1 {
                    state = changed.wait(state).unwrap();
                }
            }
            Ok(q)
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg,
    )
    .unwrap();
    let data_client = client.clone();
    let data =
        thread::spawn(move || data_client.call::<_, u64>(&1u64, RequestClass::Data, deadline()));
    {
        let (lock, changed) = &*gate;
        let state = lock.lock().unwrap();
        let (_state, timed) = changed
            .wait_timeout_while(state, Duration::from_secs(2), |s| *s == 0)
            .unwrap();
        assert!(!timed.timed_out());
    }
    assert!(client
        .call::<_, u64>(&3u64, RequestClass::Data, deadline())
        .is_err());
    assert_eq!(
        client
            .call::<_, u64>(&2u64, RequestClass::Control, deadline())
            .unwrap(),
        2
    );
    {
        let (lock, changed) = &*gate;
        *lock.lock().unwrap() = 2;
        changed.notify_all();
    }
    assert_eq!(data.join().unwrap().unwrap(), 1);
    server.shutdown();
    assert_eq!(server.counters().rejected_connections, 1);
}

#[test]
fn absolute_client_deadline_and_idle_server_shutdown_are_bounded() {
    let mut timeout_config = config();
    timeout_config.max_operation_time = Duration::from_millis(300);
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        timeout_config,
        Arc::new(|q: u64| {
            thread::sleep(Duration::from_millis(120));
            Ok(q)
        }),
    )
    .unwrap();
    let client = client(&server);
    let started = Instant::now();
    assert!(matches!(
        client.call::<_, u64>(
            &1u64,
            RequestClass::Data,
            started + Duration::from_millis(35)
        ),
        Err(TransportError::Deadline)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
    let idle = TcpStream::connect(server.control_addr()).unwrap();
    let addr = server.data_addr();
    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpStream::connect(addr).is_err());
    drop(idle);
    assert!(client
        .call::<_, u64>(&1u64, RequestClass::Data, deadline())
        .is_err());
}

#[test]
fn client_frame_limit_and_expired_deadline_fail_before_dispatch() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: String| Ok(q)),
    )
    .unwrap();
    let client = client(&server);
    assert!(matches!(
        client.call::<_, String>(&"x".repeat(5000), RequestClass::Data, deadline()),
        Err(TransportError::FrameTooLarge)
    ));
    assert!(matches!(
        client.call::<_, String>(&"x", RequestClass::Control, Instant::now()),
        Err(TransportError::Deadline)
    ));
    server.shutdown();
    assert_eq!(server.counters().messages_received, 0);
}

#[test]
fn accepted_connection_waits_for_delayed_request_bytes_within_deadline() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q + 1)),
    )
    .unwrap();
    let mut stream = TcpStream::connect(server.data_addr()).unwrap();
    // Give the listener time to accept before any bytes arrive. A nonblocking
    // accepted socket would return WouldBlock and close this valid connection.
    thread::sleep(Duration::from_millis(100));
    send(
        &mut stream,
        &Envelope {
            diagnostics: false,
            token: "test-run-token".into(),
            budget_millis: 2000,
            request: 41u64,
        },
        4096,
        deadline(),
        &TransportCounters::default(),
    )
    .unwrap();
    let reply: Result<u64, String> =
        receive(&mut stream, 4096, deadline(), &TransportCounters::default()).unwrap();
    assert_eq!(reply.unwrap(), 42);
    server.shutdown();
    assert_eq!(server.counters().failed_connections, 0);
}

#[test]
fn exact_limit_non_size_serializer_failure_remains_codec_error() {
    struct ExactThenInvalid;
    impl Serialize for ExactThenInvalid {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_str("xx")?; // Four JSON bytes exactly fit.
            Err(serde::ser::Error::custom(
                "controlled non-size serialization failure",
            ))
        }
    }
    let listener = TcpListener::bind(loopback()).unwrap();
    let mut stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_peer, _) = listener.accept().unwrap();
    let counters = TransportCounters::default();
    let result = send(&mut stream, &ExactThenInvalid, 4, deadline(), &counters);
    assert!(
        matches!(result, Err(TransportError::Codec(_))),
        "filling a buffer does not prove a size rejection: {result:?}"
    );
    assert_eq!(counters.snapshot().bytes_sent, 0);
    let overflow = send(&mut stream, &"xxx", 4, deadline(), &counters);
    assert!(matches!(overflow, Err(TransportError::FrameTooLarge)));
    assert_eq!(counters.snapshot().bytes_sent, 0);
}

#[test]
fn save_budget_extends_only_classified_requests_and_preserves_absolute_deadline() {
    let mut cfg = config();
    cfg.max_operation_time = Duration::from_millis(40);
    let cap = Duration::from_millis(400);
    let mut server = RpcServer::bind_with_request_timeout(
        loopback(),
        loopback(),
        "test-run-token".into(),
        cfg.clone(),
        Arc::new(|save: bool| {
            thread::sleep(Duration::from_millis(100));
            Ok(save)
        }),
        Arc::new(move |save: &bool| {
            if *save {
                cap
            } else {
                Duration::from_millis(40)
            }
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg,
    )
    .unwrap();
    let saved: bool = client
        .call_with_timeout(&true, RequestClass::Data, Instant::now() + cap, cap)
        .unwrap();
    assert!(saved);
    let ordinary: Result<bool, _> = client.call(&false, RequestClass::Data, Instant::now() + cap);
    assert!(matches!(ordinary, Err(TransportError::Deadline)));
    let short: Result<bool, _> = client.call_with_timeout(
        &true,
        RequestClass::Data,
        Instant::now() + Duration::from_millis(20),
        cap,
    );
    assert!(matches!(short, Err(TransportError::Deadline)));
    // Even a caller requesting a longer budget cannot extend a normal handler.
    let unclassified: Result<bool, _> =
        client.call_with_timeout(&false, RequestClass::Data, Instant::now() + cap, cap);
    assert!(unclassified.is_err());
    server.shutdown();
}

#[test]
fn idle_listeners_shutdown_without_new_connections() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q)),
    )
    .unwrap();
    // Let each listener enter its empty-listener wait before shutting down.
    thread::sleep(Duration::from_millis(30));
    let (sent, received) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        server.shutdown();
        server.shutdown();
        sent.send(server.counters()).unwrap();
    });
    let counters = received.recv_timeout(Duration::from_secs(2)).unwrap();
    worker.join().unwrap();
    assert_eq!(counters.messages_received, 0);
    assert_eq!(counters.failed_connections, 0);
}

/// Run separately with SPANN_ACCEPT_MODE=readiness and poll_1ms. This reports a
/// distribution, rather than asserting a timing threshold on shared machines.
#[test]
#[ignore = "loopback latency experiment; run each accept mode separately"]
fn sequential_loopback_accept_latency() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q + 1)),
    )
    .unwrap();
    let client = client(&server);
    let mut micros = Vec::new();
    for q in 0..1100_u64 {
        let started = Instant::now();
        let reply: u64 = client.call(&q, RequestClass::Data, deadline()).unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1e6;
        assert_eq!(reply, q + 1);
        if q >= 100 {
            micros.push(elapsed);
        }
    }
    server.shutdown();
    micros.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({
            "accept_mode": std::env::var("SPANN_ACCEPT_MODE").unwrap_or("readiness".into()),
            "samples": micros.len(),
            "mean_us": micros.iter().sum::<f64>() / micros.len() as f64,
            "p50_us": micros[499],
            "p95_us": micros[949],
            "p99_us": micros[989],
            "failed_connections": server.counters().failed_connections,
        })
    );
    assert_eq!(server.counters().failed_connections, 0);
}

#[test]
fn request_kinds_are_bounded_names_without_request_values() {
    assert_eq!(
        request_kind(&super::super::engine::EngineRequest::CurrentViewIfChanged {
            known_sequence: 917
        }),
        "CurrentViewIfChanged"
    );
    assert_eq!(request_kind(&"secret payload".to_string()), "Other");
}

#[test]
fn request_snapshot_merge_keeps_totals_and_maxima() {
    let mut a = TransportSnapshot::default();
    let mut b = TransportSnapshot::default();
    b.bytes_sent = 19;
    b.diagnostics.inclusive_scopes.insert(
        "TransportConnect".into(),
        super::super::observability::TimeTotal {
            calls: 1,
            wall_nanos: 7,
            max_wall_nanos: 7,
            thread_cpu_nanos: 3,
            thread_cpu_supported_calls: 1,
            ..Default::default()
        },
    );
    merge_snapshot(&mut a, &b);
    merge_snapshot(&mut a, &b);
    assert_eq!(a.bytes_sent, 38);
    assert_eq!(a.diagnostics.inclusive_scopes["TransportConnect"].calls, 2);
    assert_eq!(
        a.diagnostics.inclusive_scopes["TransportConnect"].wall_nanos,
        14
    );
    assert_eq!(
        a.diagnostics.inclusive_scopes["TransportConnect"].max_wall_nanos,
        7
    );
    assert_eq!(
        a.diagnostics.inclusive_scopes["TransportConnect"].thread_cpu_supported_calls,
        2
    );
}

#[test]
fn persistent_calls_reuse_socket_and_worker_without_pinning_idle_capacity() {
    let threads = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let observed = threads.clone();
    let mut cfg = config();
    cfg.max_data_handlers = 1;
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        cfg.clone(),
        Arc::new(move |q: u64| {
            observed.lock().unwrap().insert(thread::current().id());
            Ok(q + 1)
        }),
    )
    .unwrap();
    let first = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg.clone(),
    )
    .unwrap();
    for q in 0u64..30 {
        assert_eq!(
            first
                .call::<_, u64>(&q, RequestClass::Data, deadline())
                .unwrap(),
            q + 1
        );
    }
    assert_eq!(first.idle.lock()[0].len(), 1);
    assert_eq!(first.counters().opened_connections, 1);
    assert_eq!(first.counters().reused_connections, 29);
    let snapshot = first.counters();
    if detailed() {
        let timing = &snapshot.diagnostics.remote_requests["Other"];
        assert_eq!(timing.calls, 30);
        assert_eq!(timing.timed_replies, 30);
        assert_eq!(timing.server_queue_timed_replies, 30);
        assert!(timing.server_decode_queue_nanos > 0);
        assert!(timing.server_execution_queue_nanos > 0);
        assert_eq!(timing.errors, 0);
        assert!(timing.client_elapsed_nanos >= timing.server_handler_nanos);
        assert!(timing.client_decode_nanos > 0);
        assert_eq!(
            snapshot.by_request["Other"].diagnostics.remote_requests["Other"].calls,
            30
        );
    } else {
        assert!(snapshot.diagnostics.remote_requests.is_empty());
    }
    // A second client runs while the first client's connection remains idle.
    let second = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg,
    )
    .unwrap();
    assert_eq!(
        second
            .call::<_, u64>(&99u64, RequestClass::Data, deadline())
            .unwrap(),
        100
    );
    assert_eq!(threads.lock().unwrap().len(), 1);
    assert_eq!(server.counters().spawned_workers, 6);
    assert_eq!(server.counters().accepted_connections, 2);
    if detailed() {
        assert_eq!(
            first.counters().by_request["Other"]
                .diagnostics
                .inclusive_scopes["TransportConnect"]
                .calls,
            1
        );
    }
    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn closed_cached_connection_is_replaced_before_sending() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q)),
    )
    .unwrap();
    let client = client(&server);
    assert_eq!(
        client
            .call::<_, u64>(&1u64, RequestClass::Data, deadline())
            .unwrap(),
        1
    );
    client.idle.lock()[0][0]
        .0
        .shutdown(std::net::Shutdown::Both)
        .unwrap();
    assert_eq!(
        client
            .call::<_, u64>(&2u64, RequestClass::Data, deadline())
            .unwrap(),
        2
    );
    server.shutdown();
}

#[test]
fn lost_reply_never_replays_a_mutation() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _: Envelope<u64> =
            receive(&mut stream, 4096, deadline(), &TransportCounters::default()).unwrap();
        observed.fetch_add(1, Ordering::Relaxed);
        // Apply the request, then lose its reply. The client must return uncertainty.
        drop(stream);
        listener.set_nonblocking(true).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(matches!(listener.accept(), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
    });
    let client = RpcClient::new(addr, addr, "test-run-token".into(), config()).unwrap();
    assert!(client
        .call::<_, u64>(&1u64, RequestClass::Data, deadline())
        .is_err());
    server.join().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(client.idle.lock()[0].is_empty());
}

#[test]
fn idle_socket_admission_is_bounded_and_does_not_block_control() {
    let mut cfg = config();
    cfg.max_data_handlers = 1;
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        cfg.clone(),
        Arc::new(|q: u64| Ok(q)),
    )
    .unwrap();
    let idle: Vec<_> = (0..7)
        .map(|_| TcpStream::connect(server.data_addr()).unwrap())
        .collect();
    let until = Instant::now() + Duration::from_secs(1);
    while server.counters().rejected_connections == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(server.counters().rejected_connections, 1);
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg,
    )
    .unwrap();
    assert_eq!(
        client
            .call::<_, u64>(&7u64, RequestClass::Control, deadline())
            .unwrap(),
        7
    );
    server.shutdown();
    drop(idle);
}

#[test]
fn saturated_primary_workers_allow_reciprocal_export_and_chunk_dependencies() {
    use super::super::{
        engine::EngineRequest,
        placement_runtime::PlacementRequest,
        protocol::{envelope::Envelope as EngineEnvelope, handoff::HandoffPayload, AttemptId},
        routing::NodeRef,
    };
    let envelope = EngineEnvelope {
        run: 1,
        epoch: 1,
        attempt: AttemptId {
            authority: 1,
            sequence: 1,
        },
        kind: "transport-test".into(),
        placements: Vec::new(),
    };
    let export = EngineRequest::Placement(PlacementRequest::Export {
        envelope: envelope.clone(),
        root: NodeRef {
            id: 1,
            authority: 1,
            placement: 1,
        },
        destination: 2,
        new_placement: 2,
    });
    let chunk = EngineRequest::Placement(PlacementRequest::Chunk {
        envelope,
        payload: HandoffPayload {
            root: 1,
            source: 1,
            destination: 2,
            old_placement: 1,
            new_placement: 2,
            vectors: 0,
            bytes: 0,
            manifest_digest: [0; 32],
        },
        index: 0,
        nodes: Vec::new(),
    });
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let destinations: Vec<_> = (0..2)
        .map(|_| Arc::new(std::sync::OnceLock::<RpcClient>::new()))
        .collect();
    let mut servers = Vec::new();
    let mut cfg = config();
    cfg.max_data_handlers = 1;
    for destination in &destinations {
        let destination = destination.clone();
        let barrier = barrier.clone();
        let export = export.clone();
        let chunk = chunk.clone();
        servers.push(
            RpcServer::bind(
                loopback(),
                loopback(),
                "test-run-token".into(),
                cfg.clone(),
                Arc::new(move |q: EngineRequest| -> Result<u64, String> {
                    match q {
                        EngineRequest::CurrentRoot => {
                            // Both primary workers stay occupied while nested requests cross back.
                            barrier.wait();
                            destination
                                .get()
                                .unwrap()
                                .call(&export, RequestClass::Data, deadline())
                                .map_err(|e| e.to_string())
                        }
                        EngineRequest::Placement(PlacementRequest::Export { .. }) => destination
                            .get()
                            .unwrap()
                            .call(&chunk, RequestClass::Data, deadline())
                            .map_err(|e| e.to_string()),
                        EngineRequest::Placement(PlacementRequest::Chunk { .. }) => Ok(7),
                        _ => Err("unexpected request".into()),
                    }
                }),
            )
            .unwrap(),
        );
    }
    for i in 0..2 {
        let target = &servers[1 - i];
        assert!(destinations[i]
            .set(
                RpcClient::new(
                    target.data_addr(),
                    target.control_addr(),
                    "test-run-token".into(),
                    cfg.clone()
                )
                .unwrap()
            )
            .is_ok());
    }
    let callers: Vec<_> = servers
        .iter()
        .map(|server| {
            let client = RpcClient::new(
                server.data_addr(),
                server.control_addr(),
                "test-run-token".into(),
                cfg.clone(),
            )
            .unwrap();
            thread::spawn(move || {
                client
                    .call::<_, u64>(&EngineRequest::CurrentRoot, RequestClass::Data, deadline())
                    .unwrap()
            })
        })
        .collect();
    for caller in callers {
        assert_eq!(caller.join().unwrap(), 7);
    }
    for server in &mut servers {
        server.shutdown();
        assert_eq!(server.counters().rejected_connections, 0);
    }
}

#[test]
fn partial_large_frame_does_not_block_small_same_class_request() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q)),
    )
    .unwrap();
    let mut partial = TcpStream::connect(server.data_addr()).unwrap();
    partial.write_all(&4096u32.to_be_bytes()).unwrap();
    partial.write_all(&[b' '; 1024]).unwrap();
    let client = client(&server);
    assert_eq!(
        client
            .call::<_, u64>(&7u64, RequestClass::Data, deadline())
            .unwrap(),
        7
    );
    server.shutdown();
}

#[test]
fn reused_connection_budget_starts_with_new_request_and_replies_stay_isolated() {
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(|q: u64| Ok(q + 1)),
    )
    .unwrap();
    let client = client(&server);
    client
        .call::<_, u64>(&0u64, RequestClass::Data, deadline())
        .unwrap();
    thread::sleep(Duration::from_millis(250));
    assert_eq!(
        client
            .call::<_, u64>(
                &1u64,
                RequestClass::Data,
                Instant::now() + Duration::from_millis(150)
            )
            .unwrap(),
        2
    );
    let workers: Vec<_> = (0..2)
        .map(|worker| {
            let client = client.clone();
            thread::spawn(move || {
                for n in 0u64..100 {
                    let request = worker * 1000 + n;
                    assert_eq!(
                        client
                            .call::<_, u64>(&request, RequestClass::Data, deadline())
                            .unwrap(),
                        request + 1
                    );
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    server.shutdown();
    assert!(client.counters().opened_connections <= 2);
    assert!(client.counters().reused_connections >= 200);
}

#[test]
fn oversized_request_on_reused_socket_sends_no_partial_frame_and_does_not_poison_next_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(move |q: String| {
            seen.fetch_add(1, Ordering::Relaxed);
            Ok(q.len())
        }),
    )
    .unwrap();
    let client = client(&server);
    assert_eq!(
        client
            .call::<_, usize>(&"small".to_string(), RequestClass::Data, deadline())
            .unwrap(),
        5
    );
    assert!(matches!(
        client.call::<_, usize>(&"x".repeat(8192), RequestClass::Data, deadline()),
        Err(TransportError::FrameTooLarge)
    ));
    assert_eq!(
        client
            .call::<_, usize>(&"next".to_string(), RequestClass::Data, deadline())
            .unwrap(),
        4
    );
    server.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(client.counters().opened_connections, 2);
}

#[test]
fn promoted_shared_view_compact_wire_fits_default_frame_limit() {
    use super::super::{
        construction::BoundaryView,
        engine::EngineRequest,
        placement_runtime::PlacementRequest,
        protocol::publication::PublishedView,
        routing::{Child, NodeRef, RoutingNode},
    };
    let target = |id| NodeRef {
        id,
        authority: 1,
        placement: 0,
    };
    let centroid = vec![-0.123046875f32; 768];
    let make_node = |id, parent, children| RoutingNode {
        target: target(id),
        parent,
        centroid: centroid.clone(),
        centroid_code: vec![0; 384],
        children,
        leaf: false,
        revision: 1,
        navigation: Default::default(),
    };
    let mut units = BTreeMap::new();
    let mut shared = BTreeMap::new();
    let mut root_children = Vec::new();
    let mut next = 100u128;
    for branch in 2..34u128 {
        let count = if branch == 33 { 85 } else { 66 };
        let mut children = Vec::new();
        for _ in 0..count {
            units.insert(next, target(next));
            children.push(Child {
                target: target(next),
                centroid: centroid.clone(),
                leaf: true,
            });
            next += 1;
        }
        shared.insert(branch, make_node(branch, Some(target(1)), children));
        root_children.push(Child {
            target: target(branch),
            centroid: centroid.clone(),
            leaf: false,
        });
    }
    assert_eq!(units.len(), 2131);
    let root = make_node(1, None, root_children);
    shared.insert(1, root.clone());
    let request = EngineRequest::Placement(PlacementRequest::InstallView {
        run: 1,
        view: PublishedView {
            sequence: 2,
            attempt: None,
            value: BoundaryView {
                navigation_topology: None,
                root,
                shared,
                units,
            },
        },
    });
    let bytes = serde_json::to_vec(&Envelope {
        diagnostics: false,
        token: "test-run-token".to_string(),
        budget_millis: 30000,
        request: &request,
    })
    .unwrap()
    .len();
    eprintln!(
        "promoted_shared_view_request_bytes={bytes} frame_limit={}",
        TransportConfig::default().max_frame_bytes
    );
    assert!(bytes < TransportConfig::default().max_frame_bytes);
    let mut cfg = config();
    cfg.max_frame_bytes = TransportConfig::default().max_frame_bytes;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let expected_wire = serde_json::to_vec(&request).unwrap();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        cfg.clone(),
        Arc::new(move |decoded: EngineRequest| {
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), expected_wire);
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "test-run-token".into(),
        cfg,
    )
    .unwrap();
    client
        .call::<_, ()>(
            &request,
            RequestClass::Control,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    server.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn requested_reply_timing_preserves_errors_and_full_width_values() {
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "test-run-token".into(),
        config(),
        Arc::new(|value: u128| -> Result<u128, String> {
            thread::sleep(Duration::from_millis(2));
            if value == 0 {
                Err("expected rejection".into())
            } else {
                Ok(value)
            }
        }),
    )
    .unwrap();
    let mut stream = TcpStream::connect(server.data_addr()).unwrap();
    for value in [u128::MAX, 0, u128::MAX - 1] {
        let counters = TransportCounters::default();
        send(
            &mut stream,
            &Envelope {
                token: "test-run-token".into(),
                budget_millis: 2000,
                diagnostics: true,
                request: value,
            },
            4096,
            deadline(),
            &counters,
        )
        .unwrap();
        let reply: TimedReply<u128> = receive(&mut stream, 4096, deadline(), &counters).unwrap();
        assert!(reply.timing.handler_nanos >= 1_000_000);
        assert_eq!(reply.timing.queue_timings_available, detailed());
        if value == 0 {
            assert_eq!(reply.result, Err("expected rejection".into()));
        } else {
            assert_eq!(reply.result.unwrap(), value);
        }
    }
    server.shutdown();
}

#[test]
fn large_replies_are_chunked_and_connection_is_reused_without_replaying_work() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut server = RpcServer::bind(
        loopback(),
        loopback(),
        "test-run-token".into(),
        config(),
        Arc::new(move |q: usize| {
            seen.fetch_add(1, Ordering::Relaxed);
            Ok("é🦀".repeat(q))
        }),
    )
    .unwrap();
    let client = client(&server);
    for count in [4000usize, 1, 4096, 0] {
        let reply: String = client.call(&count, RequestClass::Data, deadline()).unwrap();
        assert_eq!(reply, "é🦀".repeat(count));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 4);
    assert_eq!(client.counters().opened_connections, 1);
    assert_eq!(client.counters().reused_connections, 3);
    assert_eq!(client.counters().messages_received, 4);
    server.shutdown();
}

#[test]
fn reply_chunk_wire_ceiling_and_exact_boundary() {
    // Include boundaries, JSON escapes, and UTF-8 split across chunks.
    for value in ["xx".to_owned(), "xxx".to_owned(), "é🦀\n".repeat(100)] {
        let listener = TcpListener::bind(loopback()).unwrap();
        let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        send_reply(
            &mut sender,
            &value,
            4,
            deadline(),
            &TransportCounters::default(),
        )
        .unwrap();
        let expected = serde_json::to_vec(&value).unwrap();
        let mut prefix = [0; 4];
        peer.read_exact(&mut prefix).unwrap();
        let mut body = Vec::new();
        if expected.len() <= 4 {
            assert_eq!(u32::from_be_bytes(prefix) as usize, expected.len());
            body.resize(expected.len(), 0);
            peer.read_exact(&mut body).unwrap();
        } else {
            assert_eq!(u32::from_be_bytes(prefix), CHUNKED_REPLY);
            loop {
                peer.read_exact(&mut prefix).unwrap();
                let len = u32::from_be_bytes(prefix) as usize;
                if len == 0 {
                    break;
                }
                assert!(len <= 4);
                let start = body.len();
                body.resize(start + len, 0);
                peer.read_exact(&mut body[start..]).unwrap();
            }
        }
        assert_eq!(body, expected);
    }
}

fn read_test_reply(bytes: Vec<u8>) -> Result<String, TransportError> {
    let listener = TcpListener::bind(loopback()).unwrap();
    let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    sender.write_all(&bytes).unwrap();
    sender.shutdown(std::net::Shutdown::Write).unwrap();
    receive(&mut peer, 16, deadline(), &TransportCounters::default())
}

#[test]
fn chunked_reply_requires_complete_json_terminator_and_bounded_chunks() {
    let marker = CHUNKED_REPLY.to_be_bytes();
    let valid = [
        marker.as_slice(),
        &4u32.to_be_bytes(),
        b"\"ok\"",
        &0u32.to_be_bytes(),
    ]
    .concat();
    assert_eq!(read_test_reply(valid.clone()).unwrap(), "ok");
    // Every truncation fails, including EOF after complete JSON but before its terminator.
    for length in 0..valid.len() {
        assert!(
            read_test_reply(valid[..length].to_vec()).is_err(),
            "length={length}"
        );
    }
    let oversized = [marker.as_slice(), &17u32.to_be_bytes()].concat();
    assert!(matches!(
        read_test_reply(oversized),
        Err(TransportError::FrameTooLarge)
    ));
    let trailing = [
        marker.as_slice(),
        &5u32.to_be_bytes(),
        b"\"ok\"x",
        &0u32.to_be_bytes(),
    ]
    .concat();
    assert!(matches!(
        read_test_reply(trailing),
        Err(TransportError::Codec(_))
    ));
    let premature = [marker.as_slice(), &0u32.to_be_bytes()].concat();
    assert!(matches!(
        read_test_reply(premature),
        Err(TransportError::Codec(_))
    ));
}

#[test]
fn incomplete_chunked_reply_keeps_absolute_deadline() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    sender.write_all(&CHUNKED_REPLY.to_be_bytes()).unwrap();
    sender.write_all(&4u32.to_be_bytes()).unwrap();
    sender.write_all(b"\"ok\"").unwrap();
    let result = receive::<String>(
        &mut peer,
        16,
        Instant::now() + Duration::from_millis(40),
        &TransportCounters::default(),
    );
    assert!(matches!(result, Err(TransportError::Deadline)));
}

#[test]
fn serialization_failure_after_chunk_emission_is_not_retryable_size_rejection() {
    struct InvalidReply;
    impl Serialize for InvalidReply {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_str(&"x".repeat(100))?;
            Err(serde::ser::Error::custom(
                "expansion payload exceeds decoded byte limit",
            ))
        }
    }
    let listener = TcpListener::bind(loopback()).unwrap();
    let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let result = send_reply(
        &mut sender,
        &InvalidReply,
        16,
        deadline(),
        &TransportCounters::default(),
    );
    assert!(matches!(result, Err(TransportError::Codec(_))));
    sender.shutdown(std::net::Shutdown::Write).unwrap();
    assert!(receive::<String>(&mut peer, 16, deadline(), &TransportCounters::default()).is_err());
}

#[test]
fn reply_larger_than_default_sixteen_mib_roundtrips() {
    let limit = TransportConfig::default().max_frame_bytes;
    assert_eq!(limit, 16 * 1024 * 1024);
    let listener = TcpListener::bind(loopback()).unwrap();
    let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    let worker = thread::spawn(move || {
        send_reply(
            &mut sender,
            &"x".repeat(limit + 1024),
            limit,
            until,
            &TransportCounters::default(),
        )
        .unwrap();
    });
    let reply: String = receive(&mut peer, limit, until, &TransportCounters::default()).unwrap();
    worker.join().unwrap();
    assert_eq!(reply.len(), limit + 1024);
    assert!(reply.bytes().all(|b| b == b'x'));
}
