use crate::scanner::ScanMessage;
use crossterm::event::{Event as CrosstermEvent, EventStream, KeyEvent};
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum AppEvent {
    Key(KeyEvent),
    Resize,
    Tick,
    Scan(ScanMessage),
}

pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<AppEvent>,
    _task: tokio::task::JoinHandle<()>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration, mut scan_rx: mpsc::UnboundedReceiver<ScanMessage>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(async move {
            let mut reader = EventStream::new();
            let mut tick_interval = tokio::time::interval(tick_rate);

            loop {
                let tick = tick_interval.tick();
                let crossterm_event = reader.next();

                tokio::select! {
                    maybe_event = crossterm_event => {
                        match maybe_event {
                            Some(Ok(CrosstermEvent::Key(key))) => {
                                if tx.send(AppEvent::Key(key)).is_err() {
                                    break;
                                }
                            }
                            Some(Ok(CrosstermEvent::Resize(_, _))) => {
                                let _ = tx.send(AppEvent::Resize);
                            }
                            Some(Err(_)) | None => break,
                            _ => {}
                        }
                    }
                    _ = tick => {
                        if tx.send(AppEvent::Tick).is_err() {
                            break;
                        }
                    }
                    maybe_scan = scan_rx.recv() => {
                        if let Some(msg) = maybe_scan {
                            if tx.send(AppEvent::Scan(msg)).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self { rx, _task: task }
    }

    pub async fn next(&mut self) -> Option<AppEvent> {
        self.rx.recv().await
    }
}
