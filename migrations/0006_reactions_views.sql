PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS reactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  post_slug TEXT NOT NULL,
  reaction_key TEXT NOT NULL,
  actor_token TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  UNIQUE(site_id, post_slug, reaction_key, actor_token),
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_reactions_site_post
ON reactions(site_id, post_slug, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_reactions_actor
ON reactions(site_id, post_slug, actor_token);

CREATE TABLE IF NOT EXISTS page_views (
  site_id INTEGER NOT NULL,
  resource_type TEXT NOT NULL,
  resource_key TEXT NOT NULL,
  view_count INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY(site_id, resource_type, resource_key),
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_page_views_site_type
ON page_views(site_id, resource_type, updated_at DESC);
