//! D-Bus interface for RQuickShare-X: list discovered devices and send files
//! without the window. Bus name `dev.mandre.rquicksharex`, object
//! `/dev/mandre/rquicksharex`, interface `dev.mandre.rquicksharex.Share`.

pub mod devices;
pub mod outbound;
pub mod send;
