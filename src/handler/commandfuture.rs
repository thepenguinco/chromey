use pin_project_lite::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};

use crate::cmd::{to_command_response, CommandMessage};
use crate::error::Result;
use crate::handler::target::TargetMessage;
use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{Command, CommandResponse, MethodId, Response};

pin_project! {

    pub struct CommandFuture<T, M = Result<Response>> {
        #[pin]
        rx_command: oneshot::Receiver<M>,
        target_sender: mpsc::Sender<TargetMessage>,
        #[pin]
        delay: tokio::time::Sleep,

        message: Option<TargetMessage>,

        method: MethodId,

        _marker: PhantomData<T>
    }
}

impl<T: Command> CommandFuture<T> {
    /// A new command future.
    pub fn new(
        cmd: T,
        target_sender: mpsc::Sender<TargetMessage>,
        session: Option<SessionId>,
        request_timeout: std::time::Duration,
    ) -> Result<Self> {
        let (tx, rx_command) = oneshot::channel::<Result<Response>>();
        let method = cmd.identifier();

        let message = Some(TargetMessage::Command(CommandMessage::with_session(
            cmd, tx, session,
        )?));

        let delay = tokio::time::sleep(request_timeout);

        Ok(Self {
            target_sender,
            rx_command,
            message,
            delay,
            method,
            _marker: PhantomData,
        })
    }
}

impl<T> Future for CommandFuture<T>
where
    T: Command,
{
    type Output = Result<CommandResponse<T::Response>>;

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
        } else if this.delay.poll(cx).is_ready() {
            Poll::Ready(Err(crate::error::CdpError::Timeout))
        } else {
            match this.rx_command.as_mut().poll(cx) {
                Poll::Ready(Ok(Ok(response))) => {
                    Poll::Ready(to_command_response::<T>(response, this.method.clone()))
                }
                Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
