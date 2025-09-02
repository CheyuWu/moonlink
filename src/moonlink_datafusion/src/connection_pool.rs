use std::{collections::HashMap, sync::LazyLock};
use tokio::{net::UnixStream, sync::Mutex};

#[derive(Debug)]
pub struct PooledStream {
    pub uri: String,
    pub stream: Option<UnixStream>,
}

impl PooledStream {
    pub fn new(uri: String, stream: UnixStream) -> Self {
        Self {
            uri,
            stream: Some(stream),
        }
    }
    pub fn stream_mut(&mut self) -> &mut UnixStream {
        self.stream.as_mut().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            let uri = self.uri.clone();

            tokio::spawn(async move {
                let mut pool = POOL.lock().await;
                pool.entry(uri).or_default().lock().await.push(stream);
            });
        }
    }
}

static POOL: LazyLock<Mutex<HashMap<String, Mutex<Vec<UnixStream>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) async fn get_stream(uri: &str) -> crate::Result<PooledStream> {
    {
        let mut pool = POOL.lock().await;
        if let Some(mutex_vec) = pool.get_mut(uri) {
            let mut vec = mutex_vec.lock().await;
            if let Some(stream) = vec.pop() {
                return Ok(PooledStream::new(uri.to_string(), stream));
            }
        }
    }
    let stream = UnixStream::connect(uri).await?;
    Ok(PooledStream::new(uri.to_string(), stream))
}

#[cfg(test)]
mod tests {
    use crate::connection_pool::get_stream;
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
        drop(stream1);

        let mut stream2 = get_stream(&uri_str).await.expect("should reuse from pool");
        let unix_stream = stream2.stream.take().unwrap();
        assert!(unix_stream.into_std().is_ok());
    }

    #[tokio::test]
    async fn test_pool_concurrent_multiple_uris() {
        let dir = tempdir().unwrap();

        // URI 1
        let path1 = dir.path().join("test1.sock");
        let uri1 = path1.to_str().unwrap();
        let listener1 = UnixListener::bind(uri1).unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener1.accept().await;
            }
        });

        // URI 2
        let path2 = dir.path().join("test2.sock");
        let uri2 = path2.to_str().unwrap();
        let listener2 = UnixListener::bind(uri2).unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener2.accept().await;
            }
        });

        // Retrieve streams for URI1 and URI2 concurrently
        let (stream1, stream2) = tokio::join!(get_stream(uri1), get_stream(uri2));

        let mut stream1 = stream1.expect("connect URI1");
        let mut stream2 = stream2.expect("connect URI2");

        let addr1 = stream1.stream_mut().peer_addr().unwrap();
        let addr2 = stream2.stream_mut().peer_addr().unwrap();

        drop(stream1);
        drop(stream2);

        // Retrieve streams for both URIs again; should reuse connections from the pool
        let (stream1b, stream2b) = tokio::join!(get_stream(uri1), get_stream(uri2));

        let addr1b = stream1b.unwrap().stream_mut().peer_addr().unwrap();
        let addr2b = stream2b.unwrap().stream_mut().peer_addr().unwrap();

        assert_eq!(
            addr1.as_pathname(),
            addr1b.as_pathname(),
            "URI1 should reuse its connection"
        );
        assert_eq!(
            addr2.as_pathname(),
            addr2b.as_pathname(),
            "URI2 should reuse its connection"
        );
    }
}
