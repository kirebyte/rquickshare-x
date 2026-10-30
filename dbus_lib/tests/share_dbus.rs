//! End-to-end test of the Share interface on a private session bus.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage, TransferType};
use rqs_lib::{DeviceType, EndpointInfo, OutboundPayload, State};
use tokio::sync::{broadcast, mpsc};

use rqs_dbus::service::{serve, DiscoveryControl, BUS_NAME, OBJECT_PATH};

struct PrivateBus {
    address: String,
    pid: u32,
}

impl PrivateBus {
    fn start() -> Self {
        let out = Command::new("dbus-daemon")
            .args(["--session", "--fork", "--print-address=1", "--print-pid=1"])
            .stdout(Stdio::piped())
            .output()
            .expect("dbus-daemon must be installed");
        let text = String::from_utf8(out.stdout).unwrap();
        let mut lines = text.lines();
        let address = lines.next().expect("bus address").to_string();
        let pid = lines.next().expect("bus pid").trim().parse().unwrap();
        Self { address, pid }
    }

    async fn connect(&self) -> zbus::Connection {
        zbus::connection::Builder::address(self.address.as_str())
            .unwrap()
            .build()
            .await
            .unwrap()
    }
}

impl Drop for PrivateBus {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg(self.pid.to_string()).status();
    }
}

#[derive(Default)]
struct FakeControl {
    starts: AtomicUsize,
    stops: AtomicUsize,
}

impl DiscoveryControl for FakeControl {
    fn start(&self) -> Result<(), String> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn stop(&self) {
        self.stops.fetch_add(1, Ordering::SeqCst);
    }
}

#[zbus::proxy(
    interface = "dev.mandre.rquicksharex.Share",
    default_service = "dev.mandre.rquicksharex",
    default_path = "/dev/mandre/rquicksharex"
)]
trait Share {
    fn start_discovery(&self) -> zbus::Result<()>;
    fn stop_discovery(&self) -> zbus::Result<()>;
    fn list_devices(&self) -> zbus::Result<Vec<(String, String, String)>>;
    fn send_files(&self, device_id: &str, paths: Vec<String>) -> zbus::Result<String>;
    fn cancel_transfer(&self, transfer_id: &str) -> zbus::Result<()>;
    #[zbus(signal)]
    fn devices_changed(&self, devices: Vec<(String, String, String)>) -> zbus::Result<()>;
    #[zbus(signal)]
    fn transfer_progress(
        &self,
        id: String,
        state: String,
        ack_bytes: u64,
        total_bytes: u64,
    ) -> zbus::Result<()>;
    #[zbus(signal)]
    fn transfer_finished(&self, id: String, ok: bool, reason: String) -> zbus::Result<()>;
}

fn phone() -> EndpointInfo {
    EndpointInfo {
        fullname: "x._FC9F5ED42C8A._tcp.local.".to_string(),
        id: "192.168.0.185:53601".to_string(),
        name: Some("Kira Foxybyte".to_string()),
        ip: Some("192.168.0.185".to_string()),
        port: Some("53601".to_string()),
        rtype: Some(DeviceType::Phone),
        present: Some(true),
    }
}

async fn within<T>(fut: impl std::future::Future<Output = Option<T>>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(5), fut)
        .await
        .expect("must arrive in time")
        .expect("stream still open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_share_interface_round_trips_to_rqs_lib_channels() {
    assert_eq!(BUS_NAME, "dev.mandre.rquicksharex");
    assert_eq!(OBJECT_PATH, "/dev/mandre/rquicksharex");

    let bus = PrivateBus::start();
    let control = Arc::new(FakeControl::default());
    let (dch_sender, _) = broadcast::channel::<EndpointInfo>(8);
    let (sender_file, mut files_rx) = mpsc::channel(4);
    let (message_sender, _) = broadcast::channel::<ChannelMessage>(8);
    let mut to_lib = message_sender.subscribe();

    serve(
        bus.connect().await,
        control.clone(),
        dch_sender.subscribe(),
        sender_file,
        message_sender.clone(),
    )
    .await
    .unwrap();

    let proxy = ShareProxy::new(&bus.connect().await).await.unwrap();
    let mut devices_changed = proxy.receive_devices_changed().await.unwrap();
    let mut progress = proxy.receive_transfer_progress().await.unwrap();
    let mut finished = proxy.receive_transfer_finished().await.unwrap();

    // Discovery is delegated to the app.
    proxy.start_discovery().await.unwrap();
    assert_eq!(control.starts.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.list_devices().await.unwrap(), vec![]);

    // A device shows up in the broadcast → signal + list.
    dch_sender.send(phone()).unwrap();
    let signal = within(devices_changed.next()).await;
    let devices = signal.args().unwrap().devices;
    assert_eq!(
        devices,
        vec![(
            "192.168.0.185:53601".to_string(),
            "Kira Foxybyte".to_string(),
            "Phone".to_string()
        )]
    );
    assert_eq!(proxy.list_devices().await.unwrap(), devices);

    // SendFiles builds the SendInfo the app would.
    let id = proxy
        .send_files("192.168.0.185:53601", vec!["/tmp/a.jpg".to_string()])
        .await
        .unwrap();
    assert_eq!(id, "192.168.0.185:53601");
    let info = within(files_rx.recv()).await;
    assert_eq!(info.addr, "192.168.0.185:53601");
    assert_eq!(info.name, "Kira Foxybyte");
    let OutboundPayload::Files(files) = info.ob;
    assert_eq!(files, vec!["/tmp/a.jpg".to_string()]);

    // Unknown device and relative paths are D-Bus errors.
    let err = proxy
        .send_files("nope", vec!["/tmp/a".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown device"), "{err}");
    let err = proxy
        .send_files("192.168.0.185:53601", vec!["a.jpg".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not absolute"), "{err}");

    // Outbound progress and the end of the transfer come back as signals.
    let mut msg = ChannelMessage {
        id: "192.168.0.185:53601".to_string(),
        direction: ChannelDirection::LibToFront,
        rtype: Some(TransferType::Outbound),
        state: Some(State::SendingFiles),
        ..Default::default()
    };
    {
        let meta = msg.meta.get_or_insert_with(Default::default);
        meta.ack_bytes = 10;
        meta.total_bytes = 100;
    }
    message_sender.send(msg.clone()).unwrap();
    let p = within(progress.next()).await;
    let args = p.args().unwrap();
    assert_eq!(
        (args.state.as_str(), args.ack_bytes, args.total_bytes),
        ("SendingFiles", 10, 100)
    );
    msg.state = Some(State::Finished);
    message_sender.send(msg).unwrap();
    let f = within(finished.next()).await;
    let args = f.args().unwrap();
    assert_eq!(
        (args.id.as_str(), args.ok, args.reason.as_str()),
        ("192.168.0.185:53601", true, "")
    );

    // CancelTransfer goes to rqs_lib as a FrontToLib message.
    proxy.cancel_transfer("192.168.0.185:53601").await.unwrap();
    let cancel = loop {
        let m = within(async { to_lib.recv().await.ok() }).await;
        if matches!(m.direction, ChannelDirection::FrontToLib) {
            break m;
        }
    };
    assert!(matches!(cancel.action, Some(ChannelAction::CancelTransfer)));

    // The device leaves → signal with an empty list.
    dch_sender
        .send(EndpointInfo {
            id: "192.168.0.185:53601".to_string(),
            ..Default::default()
        })
        .unwrap();
    let signal = within(devices_changed.next()).await;
    assert_eq!(signal.args().unwrap().devices, vec![]);

    proxy.stop_discovery().await.unwrap();
    assert_eq!(control.stops.load(Ordering::SeqCst), 1);
}
