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
