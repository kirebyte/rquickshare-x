//! Translation of outbound `ChannelMessage`s into the transfer signals.

use rqs_lib::channel::{ChannelDirection, ChannelMessage, TransferType};
use rqs_lib::State;

/// What the `TransferProgress` / `TransferFinished` signals carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferEvent {
    Progress {
        id: String,
        /// The `rqs_lib::State` name, e.g. `SendingFiles`.
        state: String,
        ack_bytes: u64,
        total_bytes: u64,
    },
    Finished {
        id: String,
        ok: bool,
        /// Empty when `ok`; else `rejected`, `cancelled` or `disconnected`.
        reason: String,
    },
}

/// Translates an outbound `ChannelMessage` from `rqs_lib`; `None` for
/// inbound transfers and for messages we sent ourselves.
pub fn transfer_event(msg: &ChannelMessage) -> Option<TransferEvent> {
    if !matches!(msg.direction, ChannelDirection::LibToFront)
        || !matches!(msg.rtype, Some(TransferType::Outbound))
    {
        return None;
    }
    let id = msg.id.clone();
    let finished = |ok: bool, reason: &str| {
        Some(TransferEvent::Finished {
            id: id.clone(),
            ok,
            reason: reason.to_string(),
        })
    };
    match msg.state.as_ref()? {
        State::Finished => finished(true, ""),
        State::Rejected => finished(false, "rejected"),
        State::Cancelled => finished(false, "cancelled"),
        State::Disconnected => finished(false, "disconnected"),
        state => Some(TransferEvent::Progress {
            id,
            state: format!("{state:?}"),
            ack_bytes: msg.meta.as_ref().map(|m| m.ack_bytes).unwrap_or(0),
            total_bytes: msg.meta.as_ref().map(|m| m.total_bytes).unwrap_or(0),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rqs_lib::channel::{ChannelDirection, ChannelMessage, TransferType};
    use rqs_lib::State;

    fn outbound(state: State, ack: u64, total: u64) -> ChannelMessage {
        let mut msg = ChannelMessage {
            id: "t1".to_string(),
            direction: ChannelDirection::LibToFront,
            rtype: Some(TransferType::Outbound),
            state: Some(state),
            ..Default::default()
        };
        let meta = msg.meta.get_or_insert_with(Default::default);
        meta.ack_bytes = ack;
        meta.total_bytes = total;
        msg
    }

    #[test]
    fn sending_states_become_progress() {
        assert_eq!(
            transfer_event(&outbound(State::SendingFiles, 10, 100)),
            Some(TransferEvent::Progress {
                id: "t1".to_string(),
                state: "SendingFiles".to_string(),
                ack_bytes: 10,
                total_bytes: 100,
            })
        );
        assert_eq!(
            transfer_event(&outbound(State::SentIntroduction, 0, 100)),
            Some(TransferEvent::Progress {
                id: "t1".to_string(),
                state: "SentIntroduction".to_string(),
                ack_bytes: 0,
                total_bytes: 100,
            })
        );
    }

    #[test]
    fn terminal_states_become_finished_with_a_reason() {
        assert_eq!(
            transfer_event(&outbound(State::Finished, 100, 100)),
            Some(TransferEvent::Finished {
                id: "t1".to_string(),
                ok: true,
                reason: String::new(),
            })
        );
        for (state, reason) in [
            (State::Rejected, "rejected"),
            (State::Cancelled, "cancelled"),
            (State::Disconnected, "disconnected"),
        ] {
            assert_eq!(
                transfer_event(&outbound(state, 5, 100)),
                Some(TransferEvent::Finished {
                    id: "t1".to_string(),
                    ok: false,
                    reason: reason.to_string(),
                }),
                "{reason}"
            );
        }
    }

    #[test]
    fn inbound_and_front_to_lib_messages_are_ignored() {
        let mut inbound = outbound(State::ReceivingFiles, 1, 2);
        inbound.rtype = Some(TransferType::Inbound);
        assert_eq!(transfer_event(&inbound), None);
        let mut ours = outbound(State::Initial, 0, 0);
        ours.direction = ChannelDirection::FrontToLib;
        assert_eq!(transfer_event(&ours), None);
        let mut no_state = outbound(State::Initial, 0, 0);
        no_state.state = None;
        assert_eq!(transfer_event(&no_state), None);
    }
}
