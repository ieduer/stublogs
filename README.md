# Stublogs（Cloudflare + GitHub）

`Stublogs` 是面向 `bdfz.net` 的多租戶極簡部落格平台：

- 入口主頁：`https://blog.bdfz.net`（Cloudflare Pages）
- API 入口：`https://app.bdfz.net`（Cloudflare Worker）
- 使用者站點：`https://xxx.bdfz.net`
- 編輯後台：`https://xxx.bdfz.net/admin`

## 現行能力

- 註冊（需邀請碼）
- 即時 slug 可用性檢查
- Bear 風格後台編輯器（支援草稿暫存、快捷鍵儲存）
- 站點設定自訂（主色、首頁標題、副標、外部連結、頁尾文字）
- 全站公開站點列表 / 全站公開文章流
- 新站註冊即時 Telegram 通知
- 匯出站點資料（JSON）

## DNS 與路由

- `app.bdfz.net CNAME stublogs.bdfz.workers.dev`（Proxied）
- `*.bdfz.net CNAME app.bdfz.net`（Proxied）
- Worker route：`*.bdfz.net/*`
- Pages custom domain：`blog.bdfz.net`

> 保留詞命中時，Worker 會 `fetch(request)` 轉回既有 origin，不劫持既有服務。

## API（重點）

- `GET /api/check-slug?slug=xxx`
- `POST /api/register`
- `GET /api/public-sites`
- `GET /api/public-feed`
- `GET /api/site-settings`（需登入）
- `POST /api/site-settings`（需登入）
- `GET /api/list-posts`
- `GET /api/posts/:postSlug`
- `POST /api/posts`
- `GET /api/export`

## 註冊要求

- 邀請碼：`suen`（透過 `INVITE_CODES` secret 設定）
- slug 規則：
  - 全小寫
  - 僅允許 `a-z 0-9 -`
  - 長度 `2~30`
  - 不可 `-` 開頭/結尾
  - 不可包含 `--`
  - 不可與保留詞衝突

## 環境變數

### `wrangler.toml` vars

- `BASE_DOMAIN=bdfz.net`
- `API_ENTRY_SLUG=app`
- `RESERVED_SLUGS=...`
- `GITHUB_BRANCH=main`
- `CORS_ALLOWED_ORIGINS=https://blog.bdfz.net`

### Worker secrets

- `SESSION_SECRET`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_TOKEN`
- `INVITE_CODES`（目前值：`suen`）
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## 本地開發

```bash
npm install
npx wrangler d1 create stublogs-db
npx wrangler d1 migrations apply stublogs-db --local
cp .dev.vars.example .dev.vars
npm run dev
```

## 部署

```bash
npx wrangler secret put SESSION_SECRET
npx wrangler secret put GITHUB_OWNER
npx wrangler secret put GITHUB_REPO
npx wrangler secret put GITHUB_TOKEN
npx wrangler secret put INVITE_CODES
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_CHAT_ID
npx wrangler d1 migrations apply stublogs-db --remote
npx wrangler deploy
npx wrangler pages deploy pages --project-name stublogs-home
```

## 目錄

- `src/index.js`：Worker 主程式（API + 前後台渲染）
- `pages/`：`blog.bdfz.net` 主頁
- `migrations/`：D1 schema
- `tests/slug.test.js`：slug/host 規則測試
