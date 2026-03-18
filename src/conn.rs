use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::ready;

use futures::stream::Stream;
use futures::task::{Context, Poll};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{tungstenite::protocol::WebSocketConfig, WebSocketStream};

use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{CallId, EventMessage, Message, MethodCall, MethodId};

use crate::error::CdpError;
use crate::error::Result;

type ConnectStream = MaybeTlsStream<tokio::net::TcpStream>;

/// Exchanges the messages with the websocket
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
pub struct Connection<T: EventMessage> {
    /// Queue of commands to send.
    pending_commands: VecDeque<MethodCall>,
    /// The websocket of the chromium instance
    ws: WebSocketStream<ConnectStream>,
    /// The identifier for a specific command
    next_id: usize,
    /// A flush is required.
    needs_flush: bool,
    /// The message that is currently being proceessed
    pending_flush: Option<MethodCall>,
    /// The phantom marker.
    _marker: PhantomData<T>,
}

lazy_static::lazy_static! {
    /// Nagle's algorithm disabled?
    static ref DISABLE_NAGLE: bool = match std::env::var("DISABLE_NAGLE") {
        Ok(disable_nagle) => disable_nagle == "true",
        _ => true
    };
    /// Websocket config defaults
    static ref WEBSOCKET_DEFAULTS: bool = match std::env::var("WEBSOCKET_DEFAULTS") {
        Ok(d) => d == "true",
        _ => false
    };
}

impl<T: EventMessage + Unpin> Connection<T> {
    pub async fn connect(debug_ws_url: impl AsRef<str>) -> Result<Self> {
        let mut config = WebSocketConfig::default();

        if !*WEBSOCKET_DEFAULTS {
            config.max_message_size = None;
            config.max_frame_size = None;
        }

        let ws = if crate::uring_fs::is_enabled() {
            Self::connect_uring(debug_ws_url.as_ref(), config).await?
        } else {
            Self::connect_default(debug_ws_url.as_ref(), config).await?
        };

        Ok(Self {
            pending_commands: Default::default(),
            ws,
            next_id: 0,
            needs_flush: false,
            pending_flush: None,
            _marker: Default::default(),
        })
    }

    /// Default path: let tokio-tungstenite handle TCP connect + WS handshake.
    async fn connect_default(
        url: &str,
        config: WebSocketConfig,
    ) -> Result<WebSocketStream<ConnectStream>> {
        let (ws, _) =
            tokio_tungstenite::connect_async_with_config(url, Some(config), *DISABLE_NAGLE).await?;
        Ok(ws)
    }

    /// io_uring path: pre-connect the TCP socket via io_uring, then do WS
    /// handshake over the pre-connected stream.
    async fn connect_uring(
        url: &str,
        config: WebSocketConfig,
    ) -> Result<WebSocketStream<ConnectStream>> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let request = url.into_client_request()?;
        let host = request
            .uri()
            .host()
            .ok_or_else(|| CdpError::msg("no host in CDP WebSocket URL"))?;
        let port = request.uri().port_u16().unwrap_or(9222);

        // Resolve host → SocketAddr (CDP is always localhost, so this is fast).
        let addr_str = format!("{}:{}", host, port);
        let addr: std::net::SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(_) => {
                // Hostname needs DNS — fall back to default path.
                return Self::connect_default(url, config).await;
            }
        };

        // TCP connect via io_uring.
        let std_stream = crate::uring_fs::tcp_connect(addr)
            .await
            .map_err(CdpError::Io)?;

        // Set non-blocking + Nagle.
        std_stream.set_nonblocking(true).map_err(CdpError::Io)?;
        if *DISABLE_NAGLE {
            let _ = std_stream.set_nodelay(true);
        }

        // Wrap in tokio TcpStream.
        let tokio_stream = tokio::net::TcpStream::from_std(std_stream).map_err(CdpError::Io)?;

        // WebSocket handshake over the pre-connected stream.
        let (ws, _) = tokio_tungstenite::client_async_with_config(
            request,
            MaybeTlsStream::Plain(tokio_stream),
            Some(config),
        )
        .await?;

        Ok(ws)
    }
}

impl<T: EventMessage> Connection<T> {
    fn next_call_id(&mut self) -> CallId {
        let id = CallId::new(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Queue in the command to send over the socket and return the id for this
    /// command
    pub fn submit_command(
        &mut self,
        method: MethodId,
        session_id: Option<SessionId>,
        params: serde_json::Value,
    ) -> serde_json::Result<CallId> {
        let id = self.next_call_id();
        let call = MethodCall {
            id,
            method,
            session_id: session_id.map(Into::into),
            params,
        };
        self.pending_commands.push_back(call);
        Ok(id)
    }

    /// Flush any processed messages and drain queued commands in a burst.
    fn start_send_next(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if self.needs_flush {
            if let Poll::Ready(Ok(())) = self.ws.poll_flush_unpin(cx) {
                self.needs_flush = false;
            }
        }
        if self.pending_flush.is_none() && !self.needs_flush {
            // Drain as many queued commands as the sink will accept.
            let mut sent_any = false;
            while let Some(cmd) = self.pending_commands.pop_front() {
                tracing::trace!("Sending {:?}", cmd);
                let msg = serde_json::to_string(&cmd)?;
                self.ws.start_send_unpin(msg.into())?;
                sent_any = true;
                // Check if the sink is still ready for more.
                if !self.ws.poll_ready_unpin(cx).is_ready() {
                    self.pending_flush = Some(cmd);
                    return Ok(());
                }
            }
            if sent_any {
                self.needs_flush = true;
            }
        }
        Ok(())
    }
}

impl<T: EventMessage + Unpin> Stream for Connection<T> {
    type Item = Result<Box<Message<T>>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        // flush pending outgoing messages
        loop {
            if let Err(err) = pin.start_send_next(cx) {
                return Poll::Ready(Some(Err(err)));
            }

            if let Some(call) = pin.pending_flush.take() {
                if pin.ws.poll_ready_unpin(cx).is_ready() {
                    pin.needs_flush = true;
                    // try another flush in this same poll
                    continue;
                } else {
                    pin.pending_flush = Some(call);
                }
            }

            break;
        }

        // read from the websocket
        match ready!(pin.ws.poll_next_unpin(cx)) {
            Some(Ok(WsMessage::Text(text))) => {
                match decode_message::<T>(text.as_bytes(), Some(&text)) {
                    Ok(msg) => Poll::Ready(Some(Ok(msg))),
                    Err(err) => {
                        tracing::debug!(
                            target: "chromiumoxide::conn::raw_ws::parse_errors",
                            "Dropping malformed text WS frame: {err}",
                        );
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Some(Ok(WsMessage::Binary(buf))) => match decode_message::<T>(&buf, None) {
                Ok(msg) => Poll::Ready(Some(Ok(msg))),
                Err(err) => {
                    tracing::debug!(
                        target: "chromiumoxide::conn::raw_ws::parse_errors",
                        "Dropping malformed binary WS frame: {err}",
                    );
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
            Some(Ok(WsMessage::Close(_))) => Poll::Ready(None),
            // ignore ping and pong
            Some(Ok(WsMessage::Ping(_))) | Some(Ok(WsMessage::Pong(_))) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Ok(msg)) => {
                // Unexpected WS message type, but not fatal.
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    "Unexpected WS message type: {:?}",
                    msg
                );
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Some(Err(CdpError::Ws(err)))),
            None => {
                // ws connection closed
                Poll::Ready(None)
            }
        }
    }
}

/// Shared decode path for both text and binary WS frames.
/// `raw_text_for_logging` is only provided for textual frames so we can log the original
/// payload on parse failure if desired.
#[cfg(not(feature = "serde_stacker"))]
fn decode_message<T: EventMessage>(
    bytes: &[u8],
    raw_text_for_logging: Option<&str>,
) -> Result<Box<Message<T>>> {
    match serde_json::from_slice::<Box<Message<T>>>(bytes) {
        Ok(msg) => {
            tracing::trace!("Received {:?}", msg);
            Ok(msg)
        }
        Err(err) => {
            if let Some(txt) = raw_text_for_logging {
                let preview = &txt[..txt.len().min(512)];
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    msg_len = txt.len(),
                    "Skipping unrecognized WS message {err} preview={preview}",
                );
            } else {
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    "Skipping unrecognized binary WS message {err}",
                );
            }
            Err(err.into())
        }
    }
}

/// Shared decode path for both text and binary WS frames.
/// `raw_text_for_logging` is only provided for textual frames so we can log the original
/// payload on parse failure if desired.
#[cfg(feature = "serde_stacker")]
fn decode_message<T: EventMessage>(
    bytes: &[u8],
    raw_text_for_logging: Option<&str>,
) -> Result<Box<Message<T>>> {
    use serde::Deserialize;
    let mut de = serde_json::Deserializer::from_slice(bytes);

    de.disable_recursion_limit();

    let de = serde_stacker::Deserializer::new(&mut de);

    match Box::<Message<T>>::deserialize(de) {
        Ok(msg) => {
            tracing::trace!("Received {:?}", msg);
            Ok(msg)
        }
        Err(err) => {
            if let Some(txt) = raw_text_for_logging {
                let preview = &txt[..txt.len().min(512)];
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    msg_len = txt.len(),
                    "Skipping unrecognized WS message {err} preview={preview}",
                );
            } else {
                tracing::debug!(
                    target: "chromiumoxide::conn::raw_ws::parse_errors",
                    "Skipping unrecognized binary WS message {err}",
                );
            }
            Err(err.into())
        }
    }
}
