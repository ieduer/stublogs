PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS site_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  post_slug TEXT NOT NULL,
  post_title TEXT NOT NULL DEFAULT '',
  actor_name TEXT NOT NULL DEFAULT '',
  actor_site_slug TEXT NOT NULL DEFAULT '',
  content_preview TEXT NOT NULL DEFAULT '',
  reaction_key TEXT NOT NULL DEFAULT '',
  reaction_label TEXT NOT NULL DEFAULT '',
  target_path TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  read_at TEXT,
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_notifications_site_created
ON site_notifications(site_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_site_notifications_site_read
ON site_notifications(site_id, read_at, created_at DESC);

CREATE TABLE IF NOT EXISTS site_telegram_settings (
  site_id INTEGER PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 0,
  notify_comments INTEGER NOT NULL DEFAULT 1,
  notify_reactions INTEGER NOT NULL DEFAULT 1,
  telegram_chat_id TEXT NOT NULL DEFAULT '',
  telegram_bot_token_enc TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_telegram_settings_updated
ON site_telegram_settings(updated_at DESC);
