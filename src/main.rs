use axum::{
    routing::{get, put},
    http::StatusCode,
    Router, extract,
};
use serde::{Deserialize};
use tokio::sync::{RwLock, Mutex, mpsc};
use std::{net::SocketAddr, sync::Arc, collections::HashMap, time::Duration};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = Queues::default();
    let app = Router::new()
        .route("/:queue", put(queue_push))
        .route("/:queue", get(queue_pop))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

type Message = String;

struct Queue { //FIFO Message async Queue
    recv: Mutex<mpsc::Receiver<Message>>, // Using Mutex to guarantee receiver is FIFO
    send: mpsc::Sender<Message>,          // Sender uses Semaphore, as such its already fair
}

impl Default for Queue {
    fn default() -> Self {
        let (send, recv) = mpsc::channel::<Message>(256);
        Self {
            recv: Mutex::new(recv),
            send,
        }
    }
}

#[derive(Clone, Default)]
struct Queues {
    queues: Arc<RwLock<HashMap<String, Arc<Queue>>>>,
}

impl Queues {
    pub async fn get(&self, queue_name: String) -> Arc<Queue> {
        let qr = self.queues.read().await;
        match qr.get(&queue_name) {
            Some(val) => val.clone(),
            None => {
                drop(qr);
                let mut qw = self.queues.write().await;
                qw.entry(queue_name).or_insert_with(|| Arc::new(Queue::default())).clone()
            },
        }
    }
}

#[derive(Deserialize)]
struct PushRequest {
    #[serde(rename = "v")]
    value: Message,
}

async fn queue_push(
    extract::Path(queue_name): extract::Path<String>,
    extract::Query(req): extract::Query<PushRequest>,
    extract::State(queues): extract::State<Queues>,
) -> StatusCode {
    let queue = queues.get(queue_name).await;
    queue.send.send(req.value).await.expect("send shouldn't fail");
    StatusCode::OK
}

#[derive(Deserialize)]
struct PopRequest {
    timeout: Option<u64>,
}

async fn queue_pop(
    extract::Path(queue_name): extract::Path<String>,
    extract::Query(req): extract::Query<PopRequest>,
    extract::State(queues): extract::State<Queues>,
) -> (StatusCode, Message) {
    let queue = queues.get(queue_name).await;
    if let Some(timeout) = req.timeout {
        let f = async move {
            let mut queue = queue.recv.lock().await;
            queue.recv().await
        };
        let val = tokio::time::timeout(Duration::from_secs(timeout), f).await;
        match val {
            Ok(Some(val)) => (StatusCode::OK, val),
            _ => (StatusCode::NOT_FOUND, "".into()),
        }
    } else {
        let val =
            queue.recv.try_lock()
            .map(|mut x| x.try_recv());
        match val {
            Ok(Ok(val)) => (StatusCode::OK, val),
            _ => (StatusCode::NOT_FOUND, "".into()),
        }
    }
}
