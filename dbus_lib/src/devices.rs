//! Bookkeeping of the devices currently visible, fed by the `EndpointInfo`
//! broadcast of `RQS::discovery`.

use std::collections::HashMap;

use rqs_lib::EndpointInfo;

/// What `ListDevices` returns for one device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// `ip:port`, the same id the app uses.
    pub id: String,
    pub name: String,
    /// `Phone`, `Tablet`, `Laptop` or `Unknown`.
    pub kind: String,
}

impl From<&EndpointInfo> for Device {
    fn from(info: &EndpointInfo) -> Self {
        Self {
            id: info.id.clone(),
            name: info.name.clone().unwrap_or_default(),
            kind: info
                .rtype
                .as_ref()
                .map(|t| format!("{t:?}"))
                .unwrap_or_else(|| "Unknown".to_string()),
        }
    }
}

/// The devices currently visible, by id.
#[derive(Debug, Default)]
pub struct DeviceBook {
    devices: HashMap<String, EndpointInfo>,
}

impl DeviceBook {
    /// Applies one `EndpointInfo` from discovery and says whether the list
    /// changed. `ServiceRemoved` arrives as an info with only the id set.
    pub fn update(&mut self, info: EndpointInfo) -> bool {
        if info.present != Some(true) {
            return self.devices.remove(&info.id).is_some();
        }
        let same = self.devices.get(&info.id).is_some_and(|old| {
            Device::from(old) == Device::from(&info) && old.ip == info.ip && old.port == info.port
        });
        if same {
            return false;
        }
        self.devices.insert(info.id.clone(), info);
        true
    }

    pub fn get(&self, id: &str) -> Option<&EndpointInfo> {
        self.devices.get(id)
    }

    /// Sorted by name, then id, so the order is stable.
    pub fn list(&self) -> Vec<Device> {
        let mut list: Vec<Device> = self.devices.values().map(Device::from).collect();
        list.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        list
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rqs_lib::{DeviceType, EndpointInfo};

    fn endpoint(id: &str, name: &str, present: Option<bool>) -> EndpointInfo {
        EndpointInfo {
            fullname: format!("{id}._FC9F5ED42C8A._tcp.local."),
            id: id.to_string(),
            name: Some(name.to_string()),
            ip: Some(id.split(':').next().unwrap().to_string()),
            port: Some(id.split(':').nth(1).unwrap().to_string()),
            rtype: Some(DeviceType::Phone),
            present,
        }
    }

    #[test]
    fn present_endpoints_are_listed_sorted_by_name_and_absent_ones_removed() {
        let mut book = DeviceBook::default();
        assert!(book.update(endpoint("192.168.0.185:53601", "Kira Foxybyte", Some(true))));
        assert!(book.update(endpoint("192.168.0.31:53601", "Alpha", Some(true))));
        assert_eq!(
            book.list(),
            vec![
                Device {
                    id: "192.168.0.31:53601".to_string(),
                    name: "Alpha".to_string(),
                    kind: "Phone".to_string(),
                },
                Device {
                    id: "192.168.0.185:53601".to_string(),
                    name: "Kira Foxybyte".to_string(),
                    kind: "Phone".to_string(),
                },
            ]
        );
        // The same endpoint again with the same data changes nothing.
        assert!(!book.update(endpoint("192.168.0.31:53601", "Alpha", Some(true))));
        // A renamed endpoint is a change.
        assert!(book.update(endpoint(
            "192.168.0.31:53601",
            "Alpha (Android_X)",
            Some(true)
        )));
        // ServiceRemoved comes as an EndpointInfo with only the id.
        assert!(book.update(EndpointInfo {
            id: "192.168.0.31:53601".to_string(),
            ..Default::default()
        }));
        assert_eq!(book.list().len(), 1);
        assert!(!book.update(EndpointInfo {
            id: "nunca".to_string(),
            ..Default::default()
        }));
    }

    #[test]
    fn an_endpoint_without_a_name_or_type_still_lists() {
        let mut book = DeviceBook::default();
        book.update(EndpointInfo {
            id: "10.0.0.2:1".to_string(),
            present: Some(true),
            ..Default::default()
        });
        assert_eq!(
            book.list(),
            vec![Device {
                id: "10.0.0.2:1".to_string(),
                name: String::new(),
                kind: "Unknown".to_string(),
            }]
        );
        assert_eq!(
            book.get("10.0.0.2:1").map(|e| e.id.clone()),
            Some("10.0.0.2:1".to_string())
        );
        assert!(book.get("x").is_none());
    }
}
