use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

// ─── Message Types ───────────────────────────────────────────────────────────

/// A chat message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub message_id: String,
    pub room_id: String,
    pub sender_id: String,
    pub content: String,
    pub message_type: MessageType,
    pub thread_id: Option<String>,
    pub reply_to: Option<String>,
    pub timestamp_ms: u64,
    pub edited_at_ms: Option<u64>,
    pub deleted: bool,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    Text,
    System,
    Action,
    Image,
    Custom(String),
}

/// A reaction on a message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reaction {
    pub emoji: String,
    pub user_ids: Vec<String>,
}

// ─── Room Types ──────────────────────────────────────────────────────────────

/// Chat room configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatRoom {
    pub room_id: String,
    pub name: String,
    pub room_type: RoomType,
    pub created_by: String,
    pub created_at_ms: u64,
    pub members: Vec<String>,
    pub max_members: usize,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoomType {
    Public,
    Private,
    Direct,
    Group,
}

// ─── Moderation ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModerationAction {
    pub action_id: String,
    pub action_type: ModerationType,
    pub target_user_id: String,
    pub room_id: String,
    pub moderator_id: String,
    pub reason: String,
    pub timestamp_ms: u64,
    pub expires_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModerationType {
    Mute,
    Ban,
    Kick,
    Warn,
    MessageDelete,
}

// ─── Pagination ──────────────────────────────────────────────────────────────

/// Cursor-based pagination result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagePage {
    pub messages: Vec<ChatMessage>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub total_count: usize,
}

// ─── Chat Engine ─────────────────────────────────────────────────────────────

pub struct ChatEngine {
    /// room_id -> ChatRoom
    rooms: DashMap<String, ChatRoom>,
    /// room_id -> message history (bounded ring buffer per room)
    messages: DashMap<String, Mutex<VecDeque<ChatMessage>>>,
    /// message_id -> Vec<Reaction>
    reactions: DashMap<String, Vec<Reaction>>,
    /// room_id -> moderation actions
    moderation: DashMap<String, Vec<ModerationAction>>,
    /// user_id -> map of room_id -> last_read_message_id
    read_cursors: DashMap<String, DashMap<String, String>>,
    /// Maximum messages stored per room (ring buffer).
    max_history_per_room: usize,
    /// ID generator.
    next_id: AtomicU64,
}

impl ChatEngine {
    pub fn new(max_history_per_room: usize) -> Self {
        Self {
            rooms: DashMap::new(),
            messages: DashMap::new(),
            reactions: DashMap::new(),
            moderation: DashMap::new(),
            read_cursors: DashMap::new(),
            max_history_per_room,
            next_id: AtomicU64::new(1),
        }
    }

    // ─── Room Management ────────────────────────────────────────────────────

    pub fn create_room(
        &self,
        name: &str,
        room_type: RoomType,
        created_by: &str,
        max_members: usize,
        now_ms: u64,
    ) -> ChatRoom {
        let room_id = self.generate_id();
        let room = ChatRoom {
            room_id: room_id.clone(),
            name: name.to_string(),
            room_type,
            created_by: created_by.to_string(),
            created_at_ms: now_ms,
            members: vec![created_by.to_string()],
            max_members,
            metadata: None,
        };
        self.rooms.insert(room_id.clone(), room.clone());
        self.messages
            .insert(room_id, Mutex::new(VecDeque::new()));
        room
    }

    pub fn get_room(&self, room_id: &str) -> Option<ChatRoom> {
        self.rooms.get(room_id).map(|r| r.clone())
    }

    pub fn join_room(&self, room_id: &str, user_id: &str) -> Result<(), String> {
        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| "Room not found".to_string())?;

        // Check if banned
        if self.is_banned(room_id, user_id) {
            return Err("User is banned from this room".to_string());
        }

        // Check if already a member
        if room.members.contains(&user_id.to_string()) {
            return Ok(());
        }

        // Direct rooms cannot have more than 2 members
        if room.room_type == RoomType::Direct && room.members.len() >= 2 {
            return Err("Direct message rooms are limited to 2 members".to_string());
        }

        // Check max members
        if room.max_members > 0 && room.members.len() >= room.max_members {
            return Err("Room is full".to_string());
        }

        room.members.push(user_id.to_string());
        Ok(())
    }

    pub fn leave_room(&self, room_id: &str, user_id: &str) -> Result<(), String> {
        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| "Room not found".to_string())?;

        let pos = room
            .members
            .iter()
            .position(|m| m == user_id)
            .ok_or_else(|| "User is not a member of this room".to_string())?;

        room.members.remove(pos);
        Ok(())
    }

    pub fn list_rooms(&self) -> Vec<ChatRoom> {
        self.rooms.iter().map(|r| r.value().clone()).collect()
    }

    pub fn user_rooms(&self, user_id: &str) -> Vec<ChatRoom> {
        self.rooms
            .iter()
            .filter(|r| r.value().members.contains(&user_id.to_string()))
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn is_member(&self, room_id: &str, user_id: &str) -> bool {
        self.rooms
            .get(room_id)
            .map(|r| r.members.contains(&user_id.to_string()))
            .unwrap_or(false)
    }

    // ─── Messaging ──────────────────────────────────────────────────────────

    /// Send a message to a room.
    /// Returns Err if user is not a member, is muted, or is banned.
    pub fn send_message(
        &self,
        room_id: &str,
        sender_id: &str,
        content: &str,
        message_type: MessageType,
        thread_id: Option<&str>,
        reply_to: Option<&str>,
        now_ms: u64,
    ) -> Result<ChatMessage, String> {
        // Check membership
        if !self.is_member(room_id, sender_id) {
            return Err("User is not a member of this room".to_string());
        }

        // Check moderation (mute/ban)
        if self.is_muted(room_id, sender_id, now_ms) {
            return Err("User is muted in this room".to_string());
        }
        if self.is_banned(room_id, sender_id) {
            return Err("User is banned from this room".to_string());
        }

        let message_id = self.generate_id();
        let msg = ChatMessage {
            message_id: message_id.clone(),
            room_id: room_id.to_string(),
            sender_id: sender_id.to_string(),
            content: content.to_string(),
            message_type,
            thread_id: thread_id.map(|s| s.to_string()),
            reply_to: reply_to.map(|s| s.to_string()),
            timestamp_ms: now_ms,
            edited_at_ms: None,
            deleted: false,
            metadata: None,
        };

        // Insert into room history (ring buffer)
        let entry = self
            .messages
            .entry(room_id.to_string())
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut history = entry.value().lock().unwrap();
        history.push_back(msg.clone());
        if history.len() > self.max_history_per_room {
            history.pop_front();
        }

        Ok(msg)
    }

    /// Edit a message (only the sender can edit).
    pub fn edit_message(
        &self,
        message_id: &str,
        sender_id: &str,
        new_content: &str,
        now_ms: u64,
    ) -> Result<ChatMessage, String> {
        let room_id = self
            .find_message_room(message_id)
            .ok_or_else(|| "Message not found".to_string())?;

        let entry = self
            .messages
            .get(&room_id)
            .ok_or_else(|| "Room history not found".to_string())?;
        let mut history = entry.value().lock().unwrap();

        let msg = history
            .iter_mut()
            .find(|m| m.message_id == message_id)
            .ok_or_else(|| "Message not found".to_string())?;

        if msg.sender_id != sender_id {
            return Err("Only the sender can edit this message".to_string());
        }

        if msg.deleted {
            return Err("Cannot edit a deleted message".to_string());
        }

        msg.content = new_content.to_string();
        msg.edited_at_ms = Some(now_ms);

        Ok(msg.clone())
    }

    /// Soft-delete a message (marks as deleted, content replaced with "[deleted]").
    pub fn delete_message(&self, message_id: &str, user_id: &str) -> Result<(), String> {
        let room_id = self
            .find_message_room(message_id)
            .ok_or_else(|| "Message not found".to_string())?;

        let entry = self
            .messages
            .get(&room_id)
            .ok_or_else(|| "Room history not found".to_string())?;
        let mut history = entry.value().lock().unwrap();

        let msg = history
            .iter_mut()
            .find(|m| m.message_id == message_id)
            .ok_or_else(|| "Message not found".to_string())?;

        if msg.sender_id != user_id {
            return Err("Only the sender can delete this message".to_string());
        }

        msg.deleted = true;
        msg.content = "[deleted]".to_string();

        Ok(())
    }

    /// Get message history with cursor-based pagination.
    /// `before_id`: get messages older than this ID (None = most recent).
    /// `limit`: max messages to return.
    ///
    /// Pagination algorithm:
    /// 1. Lock the room's VecDeque (the ring buffer).
    /// 2. If `before_id` is None, start from the newest message (back of deque).
    /// 3. If `before_id` is Some(id), find the index of that message, then take
    ///    messages strictly before that index.
    /// 4. Take up to `limit` messages from the slice (working backwards from the cursor).
    /// 5. Return them in chronological order (oldest first).
    /// 6. `has_more` is true if there are messages before the returned window.
    /// 7. `next_cursor` is the message_id of the oldest returned message (to fetch the
    ///    next older page, pass this as `before_id`).
    pub fn get_messages(
        &self,
        room_id: &str,
        before_id: Option<&str>,
        limit: usize,
    ) -> Result<MessagePage, String> {
        let entry = self
            .messages
            .get(room_id)
            .ok_or_else(|| "Room not found".to_string())?;
        let history = entry.value().lock().unwrap();

        let total_count = history.len();

        // Determine the end index (exclusive) for our window.
        let end_idx = match before_id {
            Some(id) => {
                // Find the position of the cursor message.
                history
                    .iter()
                    .position(|m| m.message_id == id)
                    .ok_or_else(|| "Cursor message not found".to_string())?
            }
            None => total_count,
        };

        // Take up to `limit` messages before end_idx.
        let start_idx = if end_idx > limit {
            end_idx - limit
        } else {
            0
        };

        let messages: Vec<ChatMessage> = history
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx)
            .cloned()
            .collect();

        let has_more = start_idx > 0;
        let next_cursor = if has_more {
            messages.first().map(|m| m.message_id.clone())
        } else {
            None
        };

        Ok(MessagePage {
            messages,
            has_more,
            next_cursor,
            total_count,
        })
    }

    /// Get messages in a thread.
    pub fn get_thread(
        &self,
        room_id: &str,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, String> {
        let entry = self
            .messages
            .get(room_id)
            .ok_or_else(|| "Room not found".to_string())?;
        let history = entry.value().lock().unwrap();

        let thread_messages: Vec<ChatMessage> = history
            .iter()
            .filter(|m| m.thread_id.as_deref() == Some(thread_id))
            .cloned()
            .collect();

        // Return the most recent `limit` messages from the thread.
        let start = if thread_messages.len() > limit {
            thread_messages.len() - limit
        } else {
            0
        };

        Ok(thread_messages[start..].to_vec())
    }

    // ─── Reactions ──────────────────────────────────────────────────────────

    pub fn add_reaction(
        &self,
        message_id: &str,
        user_id: &str,
        emoji: &str,
    ) -> Result<(), String> {
        // Verify the message exists.
        if self.find_message_room(message_id).is_none() {
            return Err("Message not found".to_string());
        }

        let mut entry = self
            .reactions
            .entry(message_id.to_string())
            .or_insert_with(Vec::new);

        // Find or create the reaction entry for this emoji.
        if let Some(reaction) = entry.value_mut().iter_mut().find(|r| r.emoji == emoji) {
            if !reaction.user_ids.contains(&user_id.to_string()) {
                reaction.user_ids.push(user_id.to_string());
            }
        } else {
            entry.value_mut().push(Reaction {
                emoji: emoji.to_string(),
                user_ids: vec![user_id.to_string()],
            });
        }

        Ok(())
    }

    pub fn remove_reaction(
        &self,
        message_id: &str,
        user_id: &str,
        emoji: &str,
    ) -> Result<(), String> {
        let mut entry = self
            .reactions
            .get_mut(message_id)
            .ok_or_else(|| "No reactions on this message".to_string())?;

        let reactions = entry.value_mut();
        if let Some(reaction) = reactions.iter_mut().find(|r| r.emoji == emoji) {
            reaction.user_ids.retain(|u| u != user_id);
            if reaction.user_ids.is_empty() {
                reactions.retain(|r| r.emoji != emoji);
            }
            Ok(())
        } else {
            Err("Reaction not found".to_string())
        }
    }

    pub fn get_reactions(&self, message_id: &str) -> Vec<Reaction> {
        self.reactions
            .get(message_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    // ─── Unread Tracking ────────────────────────────────────────────────────

    /// Mark messages in a room as read up to a specific message.
    pub fn mark_read(&self, user_id: &str, room_id: &str, message_id: &str) {
        let user_cursors = self
            .read_cursors
            .entry(user_id.to_string())
            .or_insert_with(DashMap::new);
        user_cursors.insert(room_id.to_string(), message_id.to_string());
    }

    /// Get unread count for a user in a specific room.
    pub fn unread_count(&self, user_id: &str, room_id: &str) -> usize {
        let last_read = self
            .read_cursors
            .get(user_id)
            .and_then(|cursors| cursors.get(room_id).map(|v| v.clone()));

        let entry = match self.messages.get(room_id) {
            Some(e) => e,
            None => return 0,
        };
        let history = entry.value().lock().unwrap();

        match last_read {
            None => {
                // Never read anything in this room — all messages are unread.
                history.len()
            }
            Some(cursor_id) => {
                // Count messages after the cursor.
                let cursor_pos = history.iter().position(|m| m.message_id == cursor_id);
                match cursor_pos {
                    Some(pos) => history.len() - pos - 1,
                    None => history.len(), // cursor evicted from ring buffer
                }
            }
        }
    }

    /// Get unread counts for all rooms a user is in.
    pub fn all_unread_counts(&self, user_id: &str) -> Vec<(String, usize)> {
        let rooms = self.user_rooms(user_id);
        rooms
            .into_iter()
            .map(|room| {
                let count = self.unread_count(user_id, &room.room_id);
                (room.room_id, count)
            })
            .filter(|(_, count)| *count > 0)
            .collect()
    }

    // ─── Moderation ─────────────────────────────────────────────────────────

    /// Mute a user in a room.
    pub fn mute_user(
        &self,
        room_id: &str,
        target_user_id: &str,
        moderator_id: &str,
        reason: &str,
        duration_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<ModerationAction, String> {
        if self.get_room(room_id).is_none() {
            return Err("Room not found".to_string());
        }

        let action = ModerationAction {
            action_id: self.generate_id(),
            action_type: ModerationType::Mute,
            target_user_id: target_user_id.to_string(),
            room_id: room_id.to_string(),
            moderator_id: moderator_id.to_string(),
            reason: reason.to_string(),
            timestamp_ms: now_ms,
            expires_at_ms: duration_ms.map(|d| now_ms + d),
        };

        self.moderation
            .entry(room_id.to_string())
            .or_insert_with(Vec::new)
            .push(action.clone());

        Ok(action)
    }

    /// Ban a user from a room.
    pub fn ban_user(
        &self,
        room_id: &str,
        target_user_id: &str,
        moderator_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<ModerationAction, String> {
        if self.get_room(room_id).is_none() {
            return Err("Room not found".to_string());
        }

        let action = ModerationAction {
            action_id: self.generate_id(),
            action_type: ModerationType::Ban,
            target_user_id: target_user_id.to_string(),
            room_id: room_id.to_string(),
            moderator_id: moderator_id.to_string(),
            reason: reason.to_string(),
            timestamp_ms: now_ms,
            expires_at_ms: None, // bans are permanent until lifted
        };

        self.moderation
            .entry(room_id.to_string())
            .or_insert_with(Vec::new)
            .push(action.clone());

        // Also remove the user from the room if they are a member.
        let _ = self.leave_room(room_id, target_user_id);

        Ok(action)
    }

    /// Check if a user is muted in a room (considering expiration).
    pub fn is_muted(&self, room_id: &str, user_id: &str, now_ms: u64) -> bool {
        self.moderation
            .get(room_id)
            .map(|actions| {
                actions.iter().any(|a| {
                    a.action_type == ModerationType::Mute
                        && a.target_user_id == user_id
                        && match a.expires_at_ms {
                            Some(exp) => now_ms < exp,
                            None => true, // permanent mute
                        }
                })
            })
            .unwrap_or(false)
    }

    /// Check if a user is banned from a room.
    pub fn is_banned(&self, room_id: &str, user_id: &str) -> bool {
        self.moderation
            .get(room_id)
            .map(|actions| {
                actions
                    .iter()
                    .any(|a| a.action_type == ModerationType::Ban && a.target_user_id == user_id)
            })
            .unwrap_or(false)
    }

    // ─── Internal ───────────────────────────────────────────────────────────

    fn generate_id(&self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("msg_{:016x}", id)
    }

    /// Find which room a message belongs to.
    fn find_message_room(&self, message_id: &str) -> Option<String> {
        for entry in self.messages.iter() {
            let history = entry.value().lock().unwrap();
            if history.iter().any(|m| m.message_id == message_id) {
                return Some(entry.key().clone());
            }
        }
        None
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> ChatEngine {
        ChatEngine::new(1000)
    }

    #[test]
    fn test_create_room() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        assert_eq!(room.name, "General");
        assert_eq!(room.room_type, RoomType::Public);
        assert_eq!(room.created_by, "alice");
        assert_eq!(room.members, vec!["alice"]);
        assert!(e.get_room(&room.room_id).is_some());
    }

    #[test]
    fn test_join_and_leave_room() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        // Join
        e.join_room(&room.room_id, "bob").unwrap();
        let updated = e.get_room(&room.room_id).unwrap();
        assert!(updated.members.contains(&"bob".to_string()));
        assert_eq!(updated.members.len(), 2);

        // Leave
        e.leave_room(&room.room_id, "bob").unwrap();
        let updated = e.get_room(&room.room_id).unwrap();
        assert!(!updated.members.contains(&"bob".to_string()));
        assert_eq!(updated.members.len(), 1);
    }

    #[test]
    fn test_send_message() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        let msg = e
            .send_message(
                &room.room_id,
                "alice",
                "Hello world!",
                MessageType::Text,
                None,
                None,
                2000,
            )
            .unwrap();

        assert_eq!(msg.content, "Hello world!");
        assert_eq!(msg.sender_id, "alice");
        assert_eq!(msg.room_id, room.room_id);
        assert_eq!(msg.timestamp_ms, 2000);
        assert!(!msg.deleted);
    }

    #[test]
    fn test_send_message_not_member_rejected() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        let result = e.send_message(
            &room.room_id,
            "bob", // not a member
            "Hello",
            MessageType::Text,
            None,
            None,
            2000,
        );

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "User is not a member of this room");
    }

    #[test]
    fn test_message_pagination() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        // Send 25 messages
        let mut msg_ids = Vec::new();
        for i in 0..25 {
            let msg = e
                .send_message(
                    &room.room_id,
                    "alice",
                    &format!("Message {}", i),
                    MessageType::Text,
                    None,
                    None,
                    1000 + i as u64,
                )
                .unwrap();
            msg_ids.push(msg.message_id);
        }

        // Get last 10 (most recent)
        let page1 = e.get_messages(&room.room_id, None, 10).unwrap();
        assert_eq!(page1.messages.len(), 10);
        assert!(page1.has_more);
        assert_eq!(page1.total_count, 25);
        assert_eq!(page1.messages.last().unwrap().content, "Message 24");
        assert_eq!(page1.messages.first().unwrap().content, "Message 15");

        // Get next page using cursor
        let cursor = page1.next_cursor.unwrap();
        let page2 = e.get_messages(&room.room_id, Some(&cursor), 10).unwrap();
        assert_eq!(page2.messages.len(), 10);
        assert!(page2.has_more);
        assert_eq!(page2.messages.last().unwrap().content, "Message 14");
        assert_eq!(page2.messages.first().unwrap().content, "Message 5");

        // Get last page
        let cursor2 = page2.next_cursor.unwrap();
        let page3 = e.get_messages(&room.room_id, Some(&cursor2), 10).unwrap();
        assert_eq!(page3.messages.len(), 5);
        assert!(!page3.has_more);
        assert!(page3.next_cursor.is_none());
        assert_eq!(page3.messages.first().unwrap().content, "Message 0");
    }

    #[test]
    fn test_edit_message_by_sender() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        let msg = e
            .send_message(
                &room.room_id,
                "alice",
                "Original",
                MessageType::Text,
                None,
                None,
                2000,
            )
            .unwrap();

        let edited = e
            .edit_message(&msg.message_id, "alice", "Edited content", 3000)
            .unwrap();

        assert_eq!(edited.content, "Edited content");
        assert_eq!(edited.edited_at_ms, Some(3000));
    }

    #[test]
    fn test_edit_message_by_other_rejected() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room.room_id, "bob").unwrap();

        let msg = e
            .send_message(
                &room.room_id,
                "alice",
                "Alice's message",
                MessageType::Text,
                None,
                None,
                2000,
            )
            .unwrap();

        let result = e.edit_message(&msg.message_id, "bob", "Hacked!", 3000);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Only the sender can edit this message"
        );
    }

    #[test]
    fn test_delete_message() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);

        let msg = e
            .send_message(
                &room.room_id,
                "alice",
                "To be deleted",
                MessageType::Text,
                None,
                None,
                2000,
            )
            .unwrap();

        e.delete_message(&msg.message_id, "alice").unwrap();

        // Verify content is replaced
        let page = e.get_messages(&room.room_id, None, 10).unwrap();
        assert_eq!(page.messages[0].content, "[deleted]");
        assert!(page.messages[0].deleted);
    }

    #[test]
    fn test_thread_messages() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room.room_id, "bob").unwrap();

        // Top-level message (acts as thread root)
        let root = e
            .send_message(
                &room.room_id,
                "alice",
                "Thread topic",
                MessageType::Text,
                None,
                None,
                1000,
            )
            .unwrap();

        // Thread replies
        e.send_message(
            &room.room_id,
            "bob",
            "Reply 1",
            MessageType::Text,
            Some(&root.message_id),
            None,
            2000,
        )
        .unwrap();

        e.send_message(
            &room.room_id,
            "alice",
            "Reply 2",
            MessageType::Text,
            Some(&root.message_id),
            None,
            3000,
        )
        .unwrap();

        let thread = e
            .get_thread(&room.room_id, &root.message_id, 10)
            .unwrap();
        assert_eq!(thread.len(), 2);
        assert_eq!(thread[0].content, "Reply 1");
        assert_eq!(thread[1].content, "Reply 2");
    }

    #[test]
    fn test_add_and_remove_reaction() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room.room_id, "bob").unwrap();

        let msg = e
            .send_message(
                &room.room_id,
                "alice",
                "React to me!",
                MessageType::Text,
                None,
                None,
                2000,
            )
            .unwrap();

        // Add reactions
        e.add_reaction(&msg.message_id, "alice", "thumbsup").unwrap();
        e.add_reaction(&msg.message_id, "bob", "thumbsup").unwrap();
        e.add_reaction(&msg.message_id, "alice", "heart").unwrap();

        let reactions = e.get_reactions(&msg.message_id);
        assert_eq!(reactions.len(), 2);

        let thumbsup = reactions.iter().find(|r| r.emoji == "thumbsup").unwrap();
        assert_eq!(thumbsup.user_ids.len(), 2);

        // Remove reaction
        e.remove_reaction(&msg.message_id, "bob", "thumbsup").unwrap();
        let reactions = e.get_reactions(&msg.message_id);
        let thumbsup = reactions.iter().find(|r| r.emoji == "thumbsup").unwrap();
        assert_eq!(thumbsup.user_ids.len(), 1);
        assert_eq!(thumbsup.user_ids[0], "alice");
    }

    #[test]
    fn test_unread_count() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room.room_id, "bob").unwrap();

        // Send 5 messages
        for i in 0..5 {
            e.send_message(
                &room.room_id,
                "alice",
                &format!("Msg {}", i),
                MessageType::Text,
                None,
                None,
                1000 + i as u64,
            )
            .unwrap();
        }

        // Bob has never read — all 5 unread
        assert_eq!(e.unread_count("bob", &room.room_id), 5);
    }

    #[test]
    fn test_mark_read_resets_count() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room.room_id, "bob").unwrap();

        let mut msg_ids = Vec::new();
        for i in 0..5 {
            let msg = e
                .send_message(
                    &room.room_id,
                    "alice",
                    &format!("Msg {}", i),
                    MessageType::Text,
                    None,
                    None,
                    1000 + i as u64,
                )
                .unwrap();
            msg_ids.push(msg.message_id);
        }

        // Mark read up to message 2 (index 2)
        e.mark_read("bob", &room.room_id, &msg_ids[2]);
        assert_eq!(e.unread_count("bob", &room.room_id), 2); // msgs 3 and 4

        // Mark read up to last message
        e.mark_read("bob", &room.room_id, &msg_ids[4]);
        assert_eq!(e.unread_count("bob", &room.room_id), 0);
    }

    #[test]
    fn test_mute_prevents_sending() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "admin", 0, 1000);
        e.join_room(&room.room_id, "troll").unwrap();

        // Mute for 60 seconds
        e.mute_user(&room.room_id, "troll", "admin", "Spamming", Some(60_000), 1000)
            .unwrap();

        // Try to send at t=2000 (still muted, expires at t=61000)
        let result = e.send_message(
            &room.room_id,
            "troll",
            "spam",
            MessageType::Text,
            None,
            None,
            2000,
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "User is muted in this room");

        // After expiry, should be able to send
        let result = e.send_message(
            &room.room_id,
            "troll",
            "I'm back",
            MessageType::Text,
            None,
            None,
            62_000,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ban_prevents_joining() {
        let e = engine();
        let room = e.create_room("General", RoomType::Public, "admin", 0, 1000);
        e.join_room(&room.room_id, "troll").unwrap();

        // Ban
        e.ban_user(&room.room_id, "troll", "admin", "Toxic behavior", 2000)
            .unwrap();

        // Verify removed from room
        assert!(!e.is_member(&room.room_id, "troll"));

        // Try to re-join
        let result = e.join_room(&room.room_id, "troll");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "User is banned from this room");
    }

    #[test]
    fn test_direct_message_room() {
        let e = engine();
        let room = e.create_room("DM", RoomType::Direct, "alice", 2, 1000);

        // Second member joins
        e.join_room(&room.room_id, "bob").unwrap();

        // Third member cannot join
        let result = e.join_room(&room.room_id, "charlie");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Direct message rooms are limited to 2 members"
        );
    }

    #[test]
    fn test_max_history_ring_buffer() {
        let e = ChatEngine::new(10); // only keep 10 messages
        let room = e.create_room("Small", RoomType::Public, "alice", 0, 1000);

        // Send 15 messages
        for i in 0..15 {
            e.send_message(
                &room.room_id,
                "alice",
                &format!("Msg {}", i),
                MessageType::Text,
                None,
                None,
                1000 + i as u64,
            )
            .unwrap();
        }

        // Should only have 10 messages (oldest 5 evicted)
        let page = e.get_messages(&room.room_id, None, 100).unwrap();
        assert_eq!(page.messages.len(), 10);
        assert_eq!(page.total_count, 10);
        assert_eq!(page.messages.first().unwrap().content, "Msg 5");
        assert_eq!(page.messages.last().unwrap().content, "Msg 14");
    }

    #[test]
    fn test_list_user_rooms() {
        let e = engine();
        let room1 = e.create_room("Room A", RoomType::Public, "alice", 0, 1000);
        let room2 = e.create_room("Room B", RoomType::Public, "alice", 0, 2000);
        let _room3 = e.create_room("Room C", RoomType::Public, "bob", 0, 3000);

        // Alice is in room1 and room2 (as creator)
        let alice_rooms = e.user_rooms("alice");
        assert_eq!(alice_rooms.len(), 2);
        let room_ids: Vec<&str> = alice_rooms.iter().map(|r| r.room_id.as_str()).collect();
        assert!(room_ids.contains(&room1.room_id.as_str()));
        assert!(room_ids.contains(&room2.room_id.as_str()));

        // Bob is only in room3
        let bob_rooms = e.user_rooms("bob");
        assert_eq!(bob_rooms.len(), 1);
    }

    #[test]
    fn test_all_unread_counts() {
        let e = engine();
        let room1 = e.create_room("Room A", RoomType::Public, "alice", 0, 1000);
        let room2 = e.create_room("Room B", RoomType::Public, "alice", 0, 1000);
        e.join_room(&room1.room_id, "bob").unwrap();
        e.join_room(&room2.room_id, "bob").unwrap();

        // Send messages in both rooms
        for i in 0..3 {
            e.send_message(
                &room1.room_id,
                "alice",
                &format!("R1 Msg {}", i),
                MessageType::Text,
                None,
                None,
                2000 + i as u64,
            )
            .unwrap();
        }
        for i in 0..2 {
            e.send_message(
                &room2.room_id,
                "alice",
                &format!("R2 Msg {}", i),
                MessageType::Text,
                None,
                None,
                3000 + i as u64,
            )
            .unwrap();
        }

        // Bob has 3 unread in room1, 2 in room2
        let unread = e.all_unread_counts("bob");
        assert_eq!(unread.len(), 2);
        let r1_count = unread.iter().find(|(id, _)| id == &room1.room_id).unwrap().1;
        let r2_count = unread.iter().find(|(id, _)| id == &room2.room_id).unwrap().1;
        assert_eq!(r1_count, 3);
        assert_eq!(r2_count, 2);
    }
}
