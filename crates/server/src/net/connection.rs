use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::command::Command;
use crate::metrics::collector::MetricsCollector;
use crate::tracing_config::RequestContext;
use eagle_core::resp::RespValue;
use eagle_core::resp::encoder::Encoder;
use eagle_core::resp::parser::RespParser;
use eagle_core::store::Store;

pub struct Connection {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    peer_addr: SocketAddr,
    store: Store,
    metrics: Arc<MetricsCollector>,
    read_timeout: Duration,
    encoder: Encoder,
    aof_encoder: Encoder,
}

impl Connection {
    pub fn new(
        socket: TcpStream,
        peer_addr: SocketAddr,
        store: Store,
        metrics: Arc<MetricsCollector>,
        read_timeout_secs: u64,
    ) -> Self {
        let (reader, writer) = socket.into_split();
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            peer_addr,
            store,
            metrics,
            read_timeout: Duration::from_secs(read_timeout_secs),
            encoder: Encoder::with_capacity(4096), // Reusable encoder buffer
            aof_encoder: Encoder::with_capacity(4096),
        }
    }

    /// Encode a response and write it - takes encoder and writer separately to avoid borrow conflicts
    #[inline]
    async fn encode_and_write(
        encoder: &mut Encoder,
        writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
        response: RespValue,
    ) -> Result<()> {
        match response {
            RespValue::StreamArray(len, mut rx) => {
                encoder.clear();
                encoder.encode_array_header(len);
                writer.write_all(encoder.as_bytes()).await?;

                while let Some(item_res) = rx.recv().await {
                    if let Ok(item) = item_res {
                        encoder.clear();
                        encoder.encode_resp(&item)?;
                        writer.write_all(encoder.as_bytes()).await?;
                    }
                }
                writer.flush().await?;
            }
            other => {
                encoder.clear();
                encoder.encode_resp(&other)?;
                writer.write_all(encoder.as_bytes()).await?;
                writer.flush().await?;
            }
        }
        Ok(())
    }

    pub async fn handle(&mut self) -> Result<()> {
        // Create a connection span for all operations on this connection
        let conn_span = info_span!(
            "connection",
            peer = %self.peer_addr,
        );

        async {
            info!("Connection established");
            let mut parser = RespParser::new(&mut self.reader);

            loop {
                let start = Instant::now();

                // Apply read timeout to prevent idle connections from consuming resources
                let parse_result = timeout(self.read_timeout, parser.parse()).await;

                let value = match parse_result {
                    Ok(Ok(value)) => value,
                    Ok(Err(e)) => {
                        let err_msg = e.to_string();
                        // Connection closed is a normal client disconnect, not an error
                        if err_msg.contains("Connection closed") {
                            debug!("Client disconnected gracefully");
                            break;
                        }
                        error!(error = %e, "Protocol parsing error");
                        let error_resp = RespValue::Error(err_msg);
                        Self::encode_and_write(&mut self.encoder, &mut self.writer, error_resp)
                            .await?;
                        self.metrics.record_error();
                        break;
                    }
                    Err(_) => {
                        warn!(
                            timeout_secs = self.read_timeout.as_secs(),
                            "Read timeout exceeded"
                        );
                        break;
                    }
                };

                let mut aof_payload = None;
                if self.store.aof_enabled() && Command::is_write_resp(&value)? {
                    self.aof_encoder.clear();
                    self.aof_encoder.encode_resp(&value)?;
                    aof_payload = Some(self.aof_encoder.as_bytes().to_vec());
                }

                match Command::from_resp(value) {
                    Ok(cmd) => {
                        // Create a request context for this command
                        let req_ctx = RequestContext::for_client(&self.peer_addr.to_string())
                            .with_command(cmd.name());
                        let req_span = req_ctx.span();

                        async {
                            let result = cmd.execute(&self.store).await;
                            let duration = start.elapsed();
                            let cmd_name = cmd.name();

                            // Record command with latency
                            self.metrics.record_command_latency(cmd_name, duration);

                            match result {
                                Ok(response) => {
                                    debug!(
                                        latency_us = duration.as_micros() as u64,
                                        "Command executed successfully"
                                    );
                                    if let Some(payload) = &aof_payload
                                        && cmd.should_log_aof(&response)
                                        && let Err(e) = self.store.append_aof(payload).await
                                    {
                                        error!(error = %e, "Failed to append to AOF");
                                    }
                                    Self::encode_and_write(
                                        &mut self.encoder,
                                        &mut self.writer,
                                        response,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        latency_us = duration.as_micros() as u64,
                                        "Command execution failed"
                                    );
                                    let error_resp = RespValue::Error(e.to_string());
                                    Self::encode_and_write(
                                        &mut self.encoder,
                                        &mut self.writer,
                                        error_resp,
                                    )
                                    .await?;
                                    self.metrics.record_command_error(cmd_name);
                                }
                            }

                            // Also record to legacy operation-specific metrics
                            match cmd_name {
                                "GET" => self.metrics.record_get(duration),
                                "SET" => self.metrics.record_set(duration),
                                "DEL" => self.metrics.record_del(duration),
                                _ => {}
                            }

                            Ok::<_, anyhow::Error>(())
                        }
                        .instrument(req_span)
                        .await?;
                    }
                    Err(e) => {
                        error!(error = %e, "Command parsing error");
                        let error_resp = RespValue::Error(e.to_string());
                        Self::encode_and_write(&mut self.encoder, &mut self.writer, error_resp)
                            .await?;
                        self.metrics.record_error();
                    }
                }
            }

            info!("Connection closed");
            self.metrics.record_connection_closed();
            Ok(())
        }
        .instrument(conn_span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_connection_handler() -> Result<()> {
        let store = Store::new_memory()?;
        let metrics = Arc::new(MetricsCollector::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Start server in background
        tokio::spawn(async move {
            if let Ok((socket, peer_addr)) = listener.accept().await {
                let mut conn = Connection::new(socket, peer_addr, store, metrics, 30);
                if let Err(e) = conn.handle().await {
                    error!("Connection error: {}", e);
                }
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test client connection
        let mut stream = TcpStream::connect(addr).await?;

        // Test SET command
        let set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        stream.write_all(set_cmd.as_bytes()).await?;

        // Read response
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        assert!(n > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_hgetall_streaming() -> Result<()> {
        let store = Store::new_memory()?;
        let metrics = Arc::new(MetricsCollector::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Add data
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        // Start server in background
        tokio::spawn(async move {
            if let Ok((socket, peer_addr)) = listener.accept().await {
                let mut conn = Connection::new(socket, peer_addr, store, metrics, 30);
                if let Err(e) = conn.handle().await {
                    error!("Connection error: {}", e);
                }
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let mut stream = TcpStream::connect(addr).await?;

        // Send HGETALL
        let cmd = "*2\r\n$7\r\nHGETALL\r\n$6\r\nmyhash\r\n";
        stream.write_all(cmd.as_bytes()).await?;

        // Read response
        // Expected: *4\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let resp = std::str::from_utf8(&buf[..n])?;

        assert!(resp.starts_with("*4\r\n"));
        // Check for fields (order may vary)
        assert!(resp.contains("field1"));
        assert!(resp.contains("value1"));
        assert!(resp.contains("field2"));
        assert!(resp.contains("value2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_hkeys_streaming() -> Result<()> {
        let store = Store::new_memory()?;
        let metrics = Arc::new(MetricsCollector::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Add data
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        // Start server in background
        tokio::spawn(async move {
            if let Ok((socket, peer_addr)) = listener.accept().await {
                let mut conn = Connection::new(socket, peer_addr, store, metrics, 30);
                if let Err(e) = conn.handle().await {
                    error!("Connection error: {}", e);
                }
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let mut stream = TcpStream::connect(addr).await?;

        // Send HKEYS
        let cmd = "*2\r\n$5\r\nHKEYS\r\n$6\r\nmyhash\r\n";
        stream.write_all(cmd.as_bytes()).await?;

        // Read response
        // Expected: *2\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let resp = std::str::from_utf8(&buf[..n])?;

        assert!(resp.starts_with("*2\r\n"));
        assert!(resp.contains("field1"));
        assert!(resp.contains("field2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_hvals_streaming() -> Result<()> {
        let store = Store::new_memory()?;
        let metrics = Arc::new(MetricsCollector::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Add data
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        // Start server in background
        tokio::spawn(async move {
            if let Ok((socket, peer_addr)) = listener.accept().await {
                let mut conn = Connection::new(socket, peer_addr, store, metrics, 30);
                if let Err(e) = conn.handle().await {
                    error!("Connection error: {}", e);
                }
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect
        let mut stream = TcpStream::connect(addr).await?;

        // Send HVALS
        let cmd = "*2\r\n$5\r\nHVALS\r\n$6\r\nmyhash\r\n";
        stream.write_all(cmd.as_bytes()).await?;

        // Read response
        // Expected: *2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let resp = std::str::from_utf8(&buf[..n])?;

        assert!(resp.starts_with("*2\r\n"));
        assert!(resp.contains("value1"));
        assert!(resp.contains("value2"));

        Ok(())
    }
}
