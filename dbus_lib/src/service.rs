//! The `dev.mandre.rquicksharex.Share` object on the session bus.
//!
//! The app wires it with the channels it already has from `rqs_lib`: the
//! `EndpointInfo` broadcast of `RQS::discovery`, the `mpsc::Sender<SendInfo>`
//! returned by `RQS::run`, and `RQS::message_sender`. Starting and stopping
//! discovery needs `&mut RQS`, so that part is delegated through
//! [`DiscoveryControl`].

use std::sync::Arc;

use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage};
use rqs_lib::{EndpointInfo, SendInfo};
use tokio::sync::{broadcast, mpsc, RwLock};
use zbus::object_server::SignalEmitter;

use crate::devices::{Device, DeviceBook};
use crate::outbound::{transfer_event, TransferEvent};
use crate::send::send_info_for;

pub const BUS_NAME: &str = "dev.mandre.rquicksharex";
pub const OBJECT_PATH: &str = "/dev/mandre/rquicksharex";
pub const INTERFACE: &str = "dev.mandre.rquicksharex.Share";

/// How the app starts and stops `RQS::discovery`.
pub trait DiscoveryControl: Send + Sync + 'static {
    fn start(&self) -> Result<(), String>;
    fn stop(&self);
}

/// `(id, name, kind)` as it goes on the wire.
type DeviceTuple = (String, String, String);

fn tuples(devices: Vec<Device>) -> Vec<DeviceTuple> {
    devices
        .into_iter()
        .map(|d| (d.id, d.name, d.kind))
        .collect()
}

pub struct ShareService {
    book: Arc<RwLock<DeviceBook>>,
    control: Arc<dyn DiscoveryControl>,
    sender_file: mpsc::Sender<SendInfo>,
    message_sender: broadcast::Sender<ChannelMessage>,
}

#[zbus::interface(name = "dev.mandre.rquicksharex.Share")]
impl ShareService {
    /// Starts looking for nearby devices (same as the Send tab of the app).
    async fn start_discovery(&self) -> zbus::fdo::Result<()> {
        self.control.start().map_err(zbus::fdo::Error::Failed)
    }

    async fn stop_discovery(&self) {
        self.control.stop();
    }

    /// The devices seen since discovery started: `(id, name, kind)`.
    async fn list_devices(&self) -> Vec<DeviceTuple> {
        tuples(self.book.read().await.list())
    }

    /// Sends `paths` (absolute) to `device_id`; returns the transfer id used
    /// in the `TransferProgress` / `TransferFinished` signals.
    async fn send_files(&self, device_id: String, paths: Vec<String>) -> zbus::fdo::Result<String> {
        let info = {
            let book = self.book.read().await;
            let device = book.get(&device_id).ok_or_else(|| {
                zbus::fdo::Error::InvalidArgs(format!("unknown device: {device_id}"))
            })?;
            send_info_for(device, &paths)
                .map_err(|e| zbus::fdo::Error::InvalidArgs(e.to_string()))?
        };
        let id = info.id.clone();
        self.sender_file
            .send(info)
            .await
            .map_err(|e| zbus::fdo::Error::Failed(format!("rqs_lib is not running: {e}")))?;
        Ok(id)
    }

    async fn cancel_transfer(&self, transfer_id: String) -> zbus::fdo::Result<()> {
        self.message_sender
            .send(ChannelMessage {
                id: transfer_id,
                direction: ChannelDirection::FrontToLib,
                action: Some(ChannelAction::CancelTransfer),
                ..Default::default()
            })
            .map(|_| ())
            .map_err(|e| zbus::fdo::Error::Failed(format!("rqs_lib is not running: {e}")))
    }

    #[zbus(signal)]
    async fn devices_changed(
        emitter: &SignalEmitter<'_>,
        devices: Vec<DeviceTuple>,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn transfer_progress(
        emitter: &SignalEmitter<'_>,
        id: &str,
        state: &str,
        ack_bytes: u64,
        total_bytes: u64,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn transfer_finished(
        emitter: &SignalEmitter<'_>,
        id: &str,
        ok: bool,
        reason: &str,
    ) -> zbus::Result<()>;
}

/// Exports the object, takes the bus name and spawns the two forwarding
/// tasks (devices and outbound transfers). Returns once everything is up;
/// the tasks live as long as `connection`.
pub async fn serve(
    connection: zbus::Connection,
    control: Arc<dyn DiscoveryControl>,
    mut devices: broadcast::Receiver<EndpointInfo>,
    sender_file: mpsc::Sender<SendInfo>,
    message_sender: broadcast::Sender<ChannelMessage>,
) -> zbus::Result<()> {
    let book = Arc::new(RwLock::new(DeviceBook::default()));
    let mut transfers = message_sender.subscribe();
    let service = ShareService {
        book: book.clone(),
        control,
        sender_file,
        message_sender,
    };
    connection.object_server().at(OBJECT_PATH, service).await?;
    connection.request_name(BUS_NAME).await?;

    let iface = connection
        .object_server()
        .interface::<_, ShareService>(OBJECT_PATH)
        .await?;

    let devices_iface = iface.clone();
    tokio::spawn(async move {
        loop {
            match devices.recv().await {
                Ok(info) => {
                    let changed = book.write().await.update(info);
                    if changed {
                        let list = tuples(book.read().await.list());
                        if let Err(e) =
                            ShareService::devices_changed(devices_iface.signal_emitter(), list)
                                .await
                        {
                            log::warn!("DevicesChanged not emitted: {e}");
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("missed {n} discovery updates")
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    tokio::spawn(async move {
        loop {
            match transfers.recv().await {
                Ok(msg) => {
                    let emitter = iface.signal_emitter();
                    let result = match transfer_event(&msg) {
                        Some(TransferEvent::Progress {
                            id,
                            state,
                            ack_bytes,
                            total_bytes,
                        }) => {
                            ShareService::transfer_progress(
                                emitter,
                                &id,
                                &state,
                                ack_bytes,
                                total_bytes,
                            )
                            .await
                        }
                        Some(TransferEvent::Finished { id, ok, reason }) => {
                            ShareService::transfer_finished(emitter, &id, ok, &reason).await
                        }
                        None => Ok(()),
                    };
                    if let Err(e) = result {
                        log::warn!("transfer signal not emitted: {e}");
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("missed {n} transfer messages")
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    Ok(())
}
