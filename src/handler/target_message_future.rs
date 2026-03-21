use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};

use crate::handler::target::TargetMessage;
use crate::{error::Result, ArcHttpRequest};

/// Convenience alias for sending messages to a Target task/actor.
///
/// This channel is typically owned by the Target event loop and accepts
/// `TargetMessage` commands to be processed serially.
type TargetSender = mpsc::Sender<TargetMessage>;

pin_project! {
    pub struct TargetMessageFuture<T> {
        #[pin]
        rx_request: oneshot::Receiver<T>,
        target_sender: mpsc::Sender<TargetMessage>,
        message: Option<TargetMessage>,
    }
}

impl<T> TargetMessageFuture<T> {
    pub fn new(
        target_sender: TargetSender,
        message: TargetMessage,
        rx_request: oneshot::Receiver<T>,
    ) -> Self {
        Self {
            target_sender,
            rx_request,
            message: Some(message),
        }
    }

    /// Helper to build a `TargetMessageFuture<ArcHttpRequest>` for any
    /// "wait" style target message (navigation, network idle, etc.).
    ///
    /// The `make_msg` closure receives the `oneshot::Sender<ArcHttpRequest>` and
    /// must wrap it into the appropriate `TargetMessage` variant
    /// (e.g. `TargetMessage::WaitForNavigation(tx)`).
    pub(crate) fn wait(
        target_sender: TargetSender,
        make_msg: impl FnOnce(oneshot::Sender<ArcHttpRequest>) -> TargetMessage,
    ) -> TargetMessageFuture<ArcHttpRequest> {
        let (tx, rx_request) = oneshot::channel();
        let message = make_msg(tx);
        TargetMessageFuture::new(target_sender, message, rx_request)
    }

    /// Wait for the main-frame navigation to finish.
    ///
    /// This triggers a `TargetMessage::WaitForNavigation` and resolves with
    /// the final `ArcHttpRequest` associated with that navigation (if any).
    pub fn wait_for_navigation(target_sender: TargetSender) -> TargetMessageFuture<ArcHttpRequest> {
        Self::wait(target_sender, TargetMessage::WaitForNavigation)
    }

    /// Wait until the main frame reaches `networkIdle`.
    ///
    /// This triggers a `TargetMessage::WaitForNetworkIdle` and resolves with
    /// the `ArcHttpRequest` associated with the navigation that led to the
    /// idle state (if any).
    pub fn wait_for_network_idle(
        target_sender: TargetSender,
    ) -> TargetMessageFuture<ArcHttpRequest> {
        Self::wait(target_sender, TargetMessage::WaitForNetworkIdle)
    }

    /// Wait until the main frame reaches `networkAlmostIdle`.
    ///
    /// This triggers a `TargetMessage::WaitForNetworkAlmostIdle` and resolves
    /// with the `ArcHttpRequest` associated with that navigation (if any).
    pub fn wait_for_network_almost_idle(
        target_sender: TargetSender,
    ) -> TargetMessageFuture<ArcHttpRequest> {
        Self::wait(target_sender, TargetMessage::WaitForNetworkAlmostIdle)
    }
}

impl<T> Future for TargetMessageFuture<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if this.message.is_some() {
            let message = this.message.take().expect("existence checked above");
            match this.target_sender.try_send(message) {
                Ok(()) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
                    *this.message = Some(msg);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e.into())),
            }
        } else {
            this.rx_request.as_mut().poll(cx).map_err(Into::into)
        }
    }
}
