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

#[cfg(test)]
mod tests {
    use crate::connection_pool::{get_stream, return_stream};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn test_connection_pool_basic() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("test_basic.sock");
        let uri_str = uri.to_str().unwrap().to_string();

        let listener = UnixListener::bind(&uri_str).expect("failed to bind socket");
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        let stream1 = get_stream(&uri_str).await.expect("should connect");
        return_stream(stream1).await;

        let stream2 = get_stream(&uri_str).await.expect("should reuse from pool");
        assert!(stream2.into_std().is_ok());
    }
}
