PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  post_slug TEXT NOT NULL,
  author_name TEXT NOT NULL,
  author_site_slug TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_comments_site_post_created
ON comments(site_id, post_slug, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_comments_site_created
ON comments(site_id, created_at DESC);
