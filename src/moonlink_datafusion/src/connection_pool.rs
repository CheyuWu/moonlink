use std::sync::{LazyLock, Mutex};

use tokio::net::UnixStream;

static CONN_POOL: LazyLock<Mutex<Vec<UnixStream>>> = LazyLock::new(|| Mutex::new(Vec::new()));

pub(crate) async fn get_stream(uri: &str) -> std::io::Result<UnixStream> {
    if let Some(stream) = CONN_POOL.lock().unwrap().pop() {
        return Ok(stream);
    }
    UnixStream::connect(uri).await
}

pub(crate) async fn return_stream(stream: UnixStream) {
    let mut pool = CONN_POOL.lock().unwrap();
    pool.push(stream);
}
