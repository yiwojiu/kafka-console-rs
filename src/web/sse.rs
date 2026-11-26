use std::convert::Infallible;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::core::messages::StreamEvent;

pub fn message_stream(
    rx: tokio::sync::mpsc::Receiver<StreamEvent>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let stream = ReceiverStream::new(rx).map(|event| {
        let e = match event {
            StreamEvent::Message(record) => Event::default()
                .event("message")
                .json_data(record)
                .unwrap_or_else(|_| Event::default().event("message").data("serialization error")),
            StreamEvent::Done { sent } => Event::default()
                .event("done")
                .json_data(serde_json::json!({ "sent": sent }))
                .unwrap_or_else(|_| Event::default().event("done").data("{}")),
        };
        Ok::<Event, Infallible>(e)
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("ping"))
}
