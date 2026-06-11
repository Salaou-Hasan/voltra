//! Redis pub/sub: channel and pattern subscription registry.
//!
//! Messages are fire-and-forget (exactly like Redis): no persistence, no
//! delivery guarantee to disconnected clients.

use super::util::glob_match;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone, Debug)]
pub enum PubMsg {
    Message { channel: Bytes, payload: Bytes },
    PMessage { pattern: Bytes, channel: Bytes, payload: Bytes },
}

#[derive(Default)]
pub struct PubSub {
    channels: DashMap<Bytes, DashMap<u64, UnboundedSender<PubMsg>>>,
    patterns: DashMap<Bytes, DashMap<u64, UnboundedSender<PubMsg>>>,
}

impl PubSub {
    pub fn subscribe(&self, conn_id: u64, channel: Bytes, tx: UnboundedSender<PubMsg>) {
        self.channels.entry(channel).or_default().insert(conn_id, tx);
    }

    pub fn unsubscribe(&self, conn_id: u64, channel: &Bytes) {
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.remove(&conn_id);
        }
        self.channels.retain(|_, subs| !subs.is_empty());
    }

    pub fn psubscribe(&self, conn_id: u64, pattern: Bytes, tx: UnboundedSender<PubMsg>) {
        self.patterns.entry(pattern).or_default().insert(conn_id, tx);
    }

    pub fn punsubscribe(&self, conn_id: u64, pattern: &Bytes) {
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&conn_id);
        }
        self.patterns.retain(|_, subs| !subs.is_empty());
    }

    /// Remove a disconnected client everywhere.
    pub fn drop_conn(&self, conn_id: u64) {
        for entry in self.channels.iter() {
            entry.value().remove(&conn_id);
        }
        self.channels.retain(|_, subs| !subs.is_empty());
        for entry in self.patterns.iter() {
            entry.value().remove(&conn_id);
        }
        self.patterns.retain(|_, subs| !subs.is_empty());
    }

    /// Deliver to all channel + matching pattern subscribers.
    /// Returns the number of clients the message was delivered to.
    pub fn publish(&self, channel: &Bytes, payload: &Bytes) -> usize {
        let mut delivered = 0;
        if let Some(subs) = self.channels.get(channel) {
            for s in subs.iter() {
                if s.value()
                    .send(PubMsg::Message { channel: channel.clone(), payload: payload.clone() })
                    .is_ok()
                {
                    delivered += 1;
                }
            }
        }
        for entry in self.patterns.iter() {
            if glob_match(entry.key(), channel) {
                for s in entry.value().iter() {
                    if s.value()
                        .send(PubMsg::PMessage {
                            pattern: entry.key().clone(),
                            channel: channel.clone(),
                            payload: payload.clone(),
                        })
                        .is_ok()
                    {
                        delivered += 1;
                    }
                }
            }
        }
        delivered
    }

    /// Active channels (with ≥1 subscriber), optionally glob-filtered.
    pub fn channels_list(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = self
            .channels
            .iter()
            .filter(|e| !e.value().is_empty())
            .filter(|e| pattern.map(|p| glob_match(p, e.key())).unwrap_or(true))
            .map(|e| e.key().clone())
            .collect();
        out.sort();
        out
    }

    pub fn numsub(&self, channel: &Bytes) -> usize {
        self.channels.get(channel).map(|s| s.len()).unwrap_or(0)
    }

    pub fn numpat(&self) -> usize {
        self.patterns.iter().filter(|e| !e.value().is_empty()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn publish_reaches_channel_and_pattern_subscribers() {
        let ps = PubSub::default();
        let (tx1, mut rx1) = unbounded_channel();
        let (tx2, mut rx2) = unbounded_channel();
        ps.subscribe(1, b("news.tech"), tx1);
        ps.psubscribe(2, b("news.*"), tx2);

        let n = ps.publish(&b("news.tech"), &b("hello"));
        assert_eq!(n, 2);
        assert!(matches!(rx1.try_recv().unwrap(), PubMsg::Message { .. }));
        assert!(matches!(rx2.try_recv().unwrap(), PubMsg::PMessage { .. }));

        ps.drop_conn(1);
        assert_eq!(ps.publish(&b("news.tech"), &b("again")), 1);
        assert_eq!(ps.numpat(), 1);
        assert_eq!(ps.channels_list(None).len(), 0); // channel emptied after drop
    }
}
