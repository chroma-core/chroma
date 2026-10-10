//! Exercise the actual gRPC client with a secondary SysDB configured.
use super::*;
use parking_lot::Mutex;
use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{
    body::Body,
    codegen::{http, BoxFuture},
    server::{NamedService, UnaryService},
};

#[derive(Clone)]
struct MockSysDb {
    rows: Arc<Vec<chroma_proto::Database>>,
    requests: Arc<Mutex<Vec<chroma_proto::ListDatabasesRequest>>>,
    counts: Arc<Mutex<usize>>,
    paginate: bool,
}

impl MockSysDb {
    fn new(names: impl Iterator<Item = String>, paginate: bool) -> Self {
        Self {
            rows: Arc::new(
                names
                    .map(|name| chroma_proto::Database {
                        id: Uuid::new_v4().to_string(),
                        name,
                        tenant: "47cf80f0-3906-4c59-98ef-6d817ad4397c".into(),
                    })
                    .collect(),
            ),
            requests: Default::default(),
            counts: Default::default(),
            paginate,
        }
    }

    async fn start(
        &self,
    ) -> (
        SysDbClient<chroma_tracing::GrpcClientTraceService<Channel>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let service = self.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            SysDbClient::new(
                ServiceBuilder::new()
                    .layer(chroma_tracing::GrpcClientTraceLayer)
                    .service(channel),
            ),
            task,
        )
    }
}

impl UnaryService<chroma_proto::ListDatabasesRequest> for MockSysDb {
    type Response = chroma_proto::ListDatabasesResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, req: tonic::Request<chroma_proto::ListDatabasesRequest>) -> Self::Future {
        let req = req.into_inner();
        self.requests.lock().push(req.clone());
        let offset = if self.paginate {
            req.offset.unwrap_or(0) as usize
        } else {
            0
        };
        let limit = if self.paginate {
            req.limit.map(|v| v as usize).unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        let databases = self.rows.iter().skip(offset).take(limit).cloned().collect();
        Box::pin(async {
            Ok(tonic::Response::new(chroma_proto::ListDatabasesResponse {
                databases,
            }))
        })
    }
}

impl UnaryService<chroma_proto::CountDatabasesRequest> for MockSysDb {
    type Response = chroma_proto::CountDatabasesResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<chroma_proto::CountDatabasesRequest>) -> Self::Future {
        *self.counts.lock() += 1;
        let count = self.rows.len() as u64;
        Box::pin(async move {
            Ok(tonic::Response::new(chroma_proto::CountDatabasesResponse {
                count,
            }))
        })
    }
}

impl NamedService for MockSysDb {
    const NAME: &'static str = "chroma.SysDB";
}
impl tower::Service<http::Request<Body>> for MockSysDb {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            match req.uri().path() {
                "/chroma.SysDB/ListDatabases" => {
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        chroma_proto::ListDatabasesResponse,
                        chroma_proto::ListDatabasesRequest,
                    >::default())
                    .unary(service, req)
                    .await)
                }
                "/chroma.SysDB/CountDatabases" => {
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        chroma_proto::CountDatabasesResponse,
                        chroma_proto::CountDatabasesRequest,
                    >::default())
                    .unary(service, req)
                    .await)
                }
                path => panic!("unexpected RPC: {path}"),
            }
        })
    }
}

#[tokio::test]
async fn database_pagination_stays_bounded_with_secondary_configured() {
    // An unbounded response for this tenant exceeds tonic's default 4 MiB.
    let primary = MockSysDb::new((0..50_001).map(|i| format!("scale-database-{i:05}")), true);
    assert!(
        prost::Message::encoded_len(&chroma_proto::ListDatabasesResponse {
            databases: primary.rows.as_ref().clone(),
        }) > 4 * 1024 * 1024
    );
    let secondary = MockSysDb::new(std::iter::empty(), false);
    let (client, primary_task) = primary.start().await;
    let (secondary_client, secondary_task) = secondary.start().await;
    let mut sysdb = GrpcSysDb {
        client,
        _mcmr_client: Some(secondary_client),
    };
    for offset in [0, 40, 49_960] {
        let page = sysdb
            .list_databases("tenant".into(), Some(40), offset)
            .await
            .unwrap();
        assert_eq!(page.len(), 40);
        assert_eq!(page[0].name, format!("scale-database-{offset:05}"));
    }
    assert!(secondary.requests.lock().is_empty());
    assert_eq!(*primary.counts.lock(), 0);
    let requests = primary.requests.lock();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|req| req.limit == Some(40)));
    assert_eq!(
        requests.iter().map(|req| req.offset).collect::<Vec<_>>(),
        vec![Some(0), Some(40), Some(49_960)]
    );
    primary_task.abort();
    secondary_task.abort();
}

#[tokio::test]
async fn database_pagination_preserves_combined_order_at_boundaries() {
    for primary_count in [0, 5] {
        let primary = MockSysDb::new((0..primary_count).map(|i| format!("db-{i}")), true);
        // Deliberately unsorted: preserve the existing stable topology ordering.
        let secondary = MockSysDb::new(
            ["b+two", "a+one", "b+three"].into_iter().map(String::from),
            false,
        );
        let (client, primary_task) = primary.start().await;
        let (secondary_client, secondary_task) = secondary.start().await;
        let mut sysdb = GrpcSysDb {
            client,
            _mcmr_client: Some(secondary_client),
        };
        let all: Vec<String> = (0..primary_count)
            .map(|i| format!("db-{i}"))
            .chain(["a+one", "b+two", "b+three"].into_iter().map(String::from))
            .collect();
        for offset in 0..=10 {
            for limit in [Some(0), Some(1), Some(3), Some(20), None] {
                let before_count = *primary.counts.lock();
                let before_secondary = secondary.requests.lock().len();
                let page = sysdb
                    .list_databases("tenant".into(), limit, offset)
                    .await
                    .unwrap();
                let expected: Vec<_> = all
                    .iter()
                    .map(String::as_str)
                    .skip(offset as usize)
                    .take(limit.map(|v| v as usize).unwrap_or(usize::MAX))
                    .collect();
                assert_eq!(
                    page.iter().map(|db| db.name.as_str()).collect::<Vec<_>>(),
                    expected,
                    "offset={offset}, limit={limit:?}"
                );
                let expected_count =
                    usize::from(offset >= primary_count && offset > 0 && limit != Some(0));
                assert_eq!(*primary.counts.lock() - before_count, expected_count);
                if limit == Some(0) {
                    assert_eq!(secondary.requests.lock().len(), before_secondary);
                }
            }
        }
        primary_task.abort();
        secondary_task.abort();
    }
}
