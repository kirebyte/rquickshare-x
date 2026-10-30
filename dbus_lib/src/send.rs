//! Building the `SendInfo` for a `SendFiles` call.

use std::fmt;
use std::path::Path;

use rqs_lib::{EndpointInfo, OutboundPayload, SendInfo};

/// Why a `SendFiles` call is refused before reaching `rqs_lib`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    NoFiles,
    NotAbsolute(String),
    NoAddress,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::NoFiles => f.write_str("no files given"),
            SendError::NotAbsolute(path) => write!(f, "path is not absolute: {path}"),
            SendError::NoAddress => f.write_str("device has no address"),
        }
    }
}

impl std::error::Error for SendError {}

/// The `SendInfo` the app's `send_payload` command would build for `device`.
pub fn send_info_for(device: &EndpointInfo, paths: &[String]) -> Result<SendInfo, SendError> {
    if paths.is_empty() {
        return Err(SendError::NoFiles);
    }
    if let Some(relative) = paths.iter().find(|p| !Path::new(p).is_absolute()) {
        return Err(SendError::NotAbsolute(relative.clone()));
    }
    let (Some(ip), Some(port)) = (&device.ip, &device.port) else {
        return Err(SendError::NoAddress);
    };
    Ok(SendInfo {
        id: device.id.clone(),
        name: device.name.clone().unwrap_or_default(),
        addr: format!("{ip}:{port}"),
        ob: OutboundPayload::Files(paths.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rqs_lib::{DeviceType, EndpointInfo, OutboundPayload};

    fn phone() -> EndpointInfo {
        EndpointInfo {
            fullname: "x".to_string(),
            id: "192.168.0.185:53601".to_string(),
            name: Some("Kira Foxybyte".to_string()),
            ip: Some("192.168.0.185".to_string()),
            port: Some("53601".to_string()),
            rtype: Some(DeviceType::Phone),
            present: Some(true),
        }
    }

    #[test]
    fn absolute_paths_to_a_known_device_make_a_send_info() {
        let info = send_info_for(
            &phone(),
            &["/tmp/a.jpg".to_string(), "/tmp/b c.txt".to_string()],
        )
        .unwrap();
        assert_eq!(info.id, "192.168.0.185:53601");
        assert_eq!(info.name, "Kira Foxybyte");
        assert_eq!(info.addr, "192.168.0.185:53601");
        match info.ob {
            OutboundPayload::Files(files) => {
                assert_eq!(
                    files,
                    vec!["/tmp/a.jpg".to_string(), "/tmp/b c.txt".to_string()]
                )
            }
        }
    }

    #[test]
    fn relative_or_empty_paths_are_refused() {
        assert_eq!(
            send_info_for(&phone(), &[]).unwrap_err(),
            SendError::NoFiles
        );
        assert_eq!(
            send_info_for(&phone(), &["a.jpg".to_string()]).unwrap_err(),
            SendError::NotAbsolute("a.jpg".to_string())
        );
    }

    #[test]
    fn a_device_without_address_is_refused() {
        let mut nowhere = phone();
        nowhere.ip = None;
        assert_eq!(
            send_info_for(&nowhere, &["/tmp/a".to_string()]).unwrap_err(),
            SendError::NoAddress
        );
    }

    #[test]
    fn errors_are_dbus_friendly() {
        assert_eq!(SendError::NoFiles.to_string(), "no files given");
        assert_eq!(
            SendError::NotAbsolute("a".to_string()).to_string(),
            "path is not absolute: a"
        );
        assert_eq!(SendError::NoAddress.to_string(), "device has no address");
    }
}
