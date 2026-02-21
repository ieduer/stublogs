const API_ORIGIN = "https://app.bdfz.net";

const POLL_MS = 15000;

const form = document.getElementById("register-form");
const statusEl = document.getElementById("status");
const slugInput = document.getElementById("slug");
const nameInput = document.getElementById("displayName");
const siteCountEl = document.getElementById("site-count");
const latestSitesEl = document.getElementById("latest-sites");
const allSitesEl = document.getElementById("all-sites");
const globalFeedEl = document.getElementById("global-feed");
const siteFilterInput = document.getElementById("site-filter");

let timer = null;
let pollTimer = null;
let allSites = [];

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#ab3720" : "#655e52";
}

function normalizeSlug(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "")
    .replace(/--+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 30);
}

function escapeText(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleString("zh-Hant", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

async function fetchJson(path, options) {
  const response = await fetch(`${API_ORIGIN}${path}`, options);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Request failed");
  }
  return payload;
}

function renderSites(sites) {
  const keyword = String(siteFilterInput.value || "").trim().toLowerCase();
  const filtered = keyword
    ? sites.filter((site) => {
        const hay = `${site.slug} ${site.displayName} ${site.description}`.toLowerCase();
        return hay.includes(keyword);
      })
    : sites;

  allSitesEl.innerHTML = filtered.length
    ? filtered
        .map(
          (site) =>
            `<li><a href="${escapeText(site.url)}" target="_blank" rel="noreferrer noopener">${escapeText(
              site.displayName
            )}</a><div class="meta">${escapeText(site.slug)}.bdfz.net · ${escapeText(
              site.description || ""
            )}</div></li>`
        )
        .join("")
    : `<li class="meta">沒有符合條件的站點</li>`;

  latestSitesEl.innerHTML = sites.length
    ? sites
        .slice(0, 8)
        .map(
          (site) =>
            `<li><a href="${escapeText(site.url)}" target="_blank" rel="noreferrer noopener">${escapeText(
              site.displayName
            )}</a><div class="meta">${escapeText(site.slug)}.bdfz.net · ${formatDate(
              site.createdAt
            )}</div></li>`
        )
        .join("")
    : `<li class="meta">尚無已註冊站點</li>`;
}

async function refreshSites() {
  const payload = await fetchJson("/api/public-sites");
  allSites = payload.sites || [];
  siteCountEl.textContent = String(payload.total || allSites.length || 0);
  renderSites(allSites);
}

async function refreshFeed() {
  const payload = await fetchJson("/api/public-feed");
  const posts = payload.posts || [];

  globalFeedEl.innerHTML = posts.length
    ? posts
        .slice(0, 8)
        .map(
          (post) =>
            `<li><a href="${escapeText(post.url)}" target="_blank" rel="noreferrer noopener">${escapeText(
              post.title
            )}</a><div class="meta">${escapeText(post.siteName)} · ${formatDate(post.updatedAt)}</div></li>`
        )
        .join("")
    : `<li class="meta">尚無公開文章</li>`;
}

async function checkSlug() {
  const slug = normalizeSlug(slugInput.value);
  slugInput.value = slug;

  if (!slug) {
    setStatus("");
    return;
  }

  try {
    const payload = await fetchJson(`/api/check-slug?slug=${encodeURIComponent(slug)}`);
    if (payload.available) {
      setStatus(`可使用：${slug}.bdfz.net`);
      return;
    }

    setStatus(`不可使用（${payload.reason || "unknown"}）`, true);
  } catch {
    setStatus("暫時無法檢查 slug，請稍後再試", true);
  }
}

slugInput.addEventListener("input", () => {
  slugInput.value = normalizeSlug(slugInput.value);
  if (!nameInput.value.trim()) {
    nameInput.value = slugInput.value;
  }

  clearTimeout(timer);
  timer = setTimeout(checkSlug, 240);
});

siteFilterInput.addEventListener("input", () => {
  renderSites(allSites);
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const payload = {
    slug: normalizeSlug(slugInput.value),
    displayName: nameInput.value.trim(),
    adminPassword: document.getElementById("adminPassword").value,
    inviteCode: document.getElementById("inviteCode").value.trim(),
    description: document.getElementById("description").value.trim(),
  };

  if (!payload.slug) {
    setStatus("請輸入有效的子域名", true);
    return;
  }

  setStatus("正在建立站點...");

  try {
    const result = await fetchJson("/api/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    setStatus("建立成功，跳轉中...");
    await Promise.all([refreshSites(), refreshFeed()]);
    window.location.href = `${result.siteUrl}/admin`;
  } catch (error) {
    setStatus(error.message || "建立失敗", true);
  }
});

async function refreshAll() {
  try {
    await Promise.all([refreshSites(), refreshFeed()]);
  } catch (error) {
    setStatus(error.message || "資料載入失敗", true);
  }
}

refreshAll();
pollTimer = setInterval(refreshAll, POLL_MS);

window.addEventListener("beforeunload", () => {
  clearInterval(pollTimer);
});
