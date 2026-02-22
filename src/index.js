import { scryptSync } from "node:crypto";

const DEFAULT_RESERVED_SLUGS = [
  "app",
  "www",
  "api",
  "admin",
  "assets",
  "static",
  "img",
  "files",
  "forum",
  "mail",
  "mx",
  "ftp",
  "ssh",
  "vpn",
  "cdn",
  "docs",
  "status",
  "blog",
  "dashboard",
];

const SESSION_COOKIE = "stublogs_session";
const SESSION_TTL_SECONDS = 60 * 60 * 24 * 7;
const DEFAULT_API_ENTRY_SLUG = "app";
const SITE_CONFIG_VERSION = 3;
const LEGACY_FOOTER_NOTE = "在這裡，把語文寫成你自己。";
const POSTS_PAGE_SIZE = 10;
const COMMENTS_PAGE_SIZE = 20;
const DEFAULT_FAVICON_URL = "https://img.bdfz.net/20250503004.webp";
const PASSWORD_SCRYPT_N = 1 << 14;
const PASSWORD_SCRYPT_R = 8;
const PASSWORD_SCRYPT_P = 1;
const PASSWORD_SCRYPT_KEYLEN = 32;
const CUSTOM_CSS_MAX_LENGTH = 64000;
const PUBLIC_SSR_CACHE_CONTROL = "public, s-maxage=60, stale-while-revalidate=120";
const PRIVATE_NO_CACHE_CONTROL = "private, no-cache";
const REACTOR_COOKIE = "stublogs_reactor";
const REACTOR_COOKIE_TTL_SECONDS = 60 * 60 * 24 * 365 * 2;
const HOME_VIEW_KEY = "__home__";
const NOTIFICATION_PAGE_SIZE = 40;

const LOGIN_RATE_WINDOW_MS = 15 * 60 * 1000;
const LOGIN_RATE_MAX_ATTEMPTS = 5;
const COMMENT_RATE_WINDOW_MS = 60 * 1000;
const COMMENT_RATE_MAX_ATTEMPTS = 6;
const REACTION_RATE_WINDOW_MS = 60 * 1000;
const REACTION_RATE_MAX_ATTEMPTS = 30;
const VIEW_RATE_WINDOW_MS = 10 * 1000;
const VIEW_RATE_MAX_ATTEMPTS = 2;
let commentsTableReadyPromise = null;
let rateLimitsTableReadyPromise = null;
let reactionsTableReadyPromise = null;
let viewsTableReadyPromise = null;
let notificationTablesReadyPromise = null;
const postsColumnsPromiseByDb = new WeakMap();

const REACTION_PRESETS = Object.freeze([
  { key: "lion", icon: "🦁", label: "獅王" },
  { key: "dragon", icon: "🐉", label: "金龍" },
  { key: "hummingbird", icon: "🐦", label: "蜂鳥" },
  { key: "deer", icon: "🦌", label: "冰鹿" },
  { key: "eagle", icon: "🦅", label: "蒼鷹" },
  { key: "wolf", icon: "🐺", label: "紫狼" },
  { key: "unicorn", icon: "🦄", label: "獨角獸" },
  { key: "phoenix", icon: "🐦‍🔥", label: "鳳凰" },
  { key: "orca", icon: "🐋", label: "逆戟鯨" },
  { key: "fire", icon: "🔥", label: "燃" },
  { key: "rocket", icon: "🚀", label: "起飛" },
  { key: "spark", icon: "✨", label: "閃亮" },
  { key: "mindblown", icon: "🤯", label: "炸裂" },
  { key: "respect", icon: "🫶", label: "超讚" },
]);
const REACTION_PRESET_MAP = new Map(
  REACTION_PRESETS.map((item) => [item.key, item])
);

export default {
  async fetch(request, env, ctx) {
    try {
      return await handleRequest(request, env, ctx);
    } catch (error) {
      console.error("Unhandled error", error);
      return json({ error: "Internal server error" }, 500);
    }
  },
};

export function getReservedSlugs(env) {
  const configured = String(env.RESERVED_SLUGS || "")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);

  return new Set([...DEFAULT_RESERVED_SLUGS, ...configured]);
}

export function validateSlug(rawSlug, reservedSlugs = new Set()) {
  const slug = String(rawSlug || "").trim().toLowerCase();

  if (slug.length < 2 || slug.length > 30) {
    return { ok: false, reason: "slug-length" };
  }

  if (!/^[a-z0-9-]+$/.test(slug)) {
    return { ok: false, reason: "slug-charset" };
  }

  if (slug.startsWith("-") || slug.endsWith("-")) {
    return { ok: false, reason: "slug-edge-dash" };
  }

  if (slug.includes("--")) {
    return { ok: false, reason: "slug-double-dash" };
  }

  const blockedTerms = ["official", "support", "staff", "security"];
  if (blockedTerms.includes(slug)) {
    return { ok: false, reason: "slug-sensitive" };
  }

  if (reservedSlugs.has(slug)) {
    return { ok: false, reason: "slug-reserved" };
  }

  return { ok: true, slug };
}

export function validatePostSlug(rawSlug) {
  const slug = String(rawSlug || "").trim().toLowerCase();

  if (slug.length < 1 || slug.length > 80) {
    return { ok: false, reason: "post-slug-length" };
  }

  if (!/^[a-z0-9-]+$/.test(slug)) {
    return { ok: false, reason: "post-slug-charset" };
  }

  if (slug.startsWith("-") || slug.endsWith("-")) {
    return { ok: false, reason: "post-slug-edge-dash" };
  }

  if (slug.includes("--")) {
    return { ok: false, reason: "post-slug-double-dash" };
  }

  return { ok: true, slug };
}

export function getHostSlug(hostname, baseDomain) {
  const host = String(hostname || "").toLowerCase();
  const base = String(baseDomain || "").toLowerCase();

  if (host === base) {
    return null;
  }

  if (!host.endsWith(`.${base}`)) {
    return null;
  }

  const prefix = host.slice(0, host.length - base.length - 1);
  if (!prefix) {
    return null;
  }

  const labels = prefix.split(".").filter(Boolean);
  return labels.length ? labels[0] : null;
}

export function slugifyValue(value) {
  const raw = String(value || "").toLowerCase().trim();
  const slug = raw
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);

  if (slug) {
    return slug;
  }
  if (!raw) {
    return "";
  }
  return `post-${stableShortHash(raw)}`.slice(0, 80);
}

function stableShortHash(input) {
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(36).padStart(6, "0").slice(0, 8);
}

async function handleRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = normalizePath(url.pathname);
  const hostHeader = request.headers.get("host") || url.host;
  const hostname = hostHeader.split(":")[0].toLowerCase();
  const baseDomain = String(env.BASE_DOMAIN || "bdfz.net").toLowerCase();
  const apiEntrySlug = String(env.API_ENTRY_SLUG || DEFAULT_API_ENTRY_SLUG)
    .trim()
    .toLowerCase();
  const hostSlug = getHostSlug(hostname, baseDomain);
  const reservedSlugs = getReservedSlugs(env);

  if (path === "/healthz") {
    return text("ok");
  }

  if (path.startsWith("/api/")) {
    if (request.method === "OPTIONS") {
      return buildApiPreflightResponse(request, env);
    }

    const response = await handleApi(request, env, ctx, {
      path,
      url,
      hostSlug,
      baseDomain,
      reservedSlugs,
      apiEntrySlug,
    });
    return withCors(response, request, env);
  }

  if (hostSlug === null) {
    if (path === "/") {
      return html(renderRootPage(baseDomain), 200);
    }

    if (path === "/admin") {
      const site = String(url.searchParams.get("site") || "")
        .trim()
        .toLowerCase();
      const validation = validateSlug(site, reservedSlugs);
      if (validation.ok) {
        return Response.redirect(`https://${site}.${baseDomain}/admin`, 302);
      }
      return html(renderRootAdminHelp(baseDomain), 200);
    }

    if (path === "/robots.txt") {
      return text("User-agent: *\nAllow: /\n");
    }

    return notFound();
  }

  if (reservedSlugs.has(hostSlug) && hostSlug !== apiEntrySlug) {
    // Reserved slugs are owned by platform/system services.
    // Let Cloudflare continue to the configured origin for those hosts.
    return fetch(request);
  }

  if (hostSlug === apiEntrySlug && path === "/") {
    return html(renderRootPage(baseDomain), 200);
  }

  if (hostSlug === apiEntrySlug && path === "/admin") {
    return html(renderRootAdminHelp(baseDomain), 200);
  }

  const site = await getSiteBySlug(env, hostSlug);

  if (!site) {
    if (path === "/" || path === "/admin") {
      return html(renderClaimPage(hostSlug, baseDomain), 200);
    }
    return notFound("Site not found");
  }

  if (path === "/admin") {
    const authed = await isSiteAuthenticated(request, env, site.slug);
    const siteConfig = await getSiteConfig(env, site);
    return html(
      renderAdminPage(site, siteConfig, authed, baseDomain),
      200,
      { "Cache-Control": PRIVATE_NO_CACHE_CONTROL }
    );
  }

  if (path === "/feed.xml") {
    const siteConfig = await getSiteConfig(env, site);
    const posts = await listPosts(env, site.id, false);
    return xml(
      renderSiteRssXml(site, siteConfig, posts, baseDomain),
      200,
      { "Cache-Control": PUBLIC_SSR_CACHE_CONTROL }
    );
  }

  if (path === "/sitemap.xml") {
    const [posts, pages] = await Promise.all([
      listPosts(env, site.id, false),
      listSitePages(env, site.id, 200),
    ]);
    return xml(
      renderSiteSitemapXml(site, posts, pages, baseDomain),
      200,
      { "Cache-Control": PUBLIC_SSR_CACHE_CONTROL }
    );
  }

  if (path === "/") {
    const siteConfig = await getSiteConfig(env, site);
    const page = parsePositiveInt(url.searchParams.get("page"), 1, 1, 9999);
    const [homeViewCount, postsPage, sitePages, communitySites, campusFeed] = await Promise.all([
      listPageViewCounts(env, site.id, "home", [HOME_VIEW_KEY]).then(
        (map) => Math.max(Number(map.get(HOME_VIEW_KEY) || 0), 0)
      ),
      listPostsPage(env, site.id, page, POSTS_PAGE_SIZE),
      listSitePages(env, site.id, 20),
      siteConfig.hideCommunitySites ? Promise.resolve([]) : listCommunitySites(env, site.slug, 12),
      siteConfig.hideCampusFeed ? Promise.resolve([]) : listCampusFeed(env, site.id, 18),
    ]);
    const postViewMap = await listPageViewCounts(
      env,
      site.id,
      "post",
      postsPage.posts.map((post) => post.postSlug)
    );
    const postsWithViews = postsPage.posts.map((post) => ({
      ...post,
      viewCount: Math.max(Number(postViewMap.get(post.postSlug) || 0), 0),
    }));
    return html(
      renderSiteHomePage(
        site,
        siteConfig,
        postsWithViews,
        sitePages,
        communitySites,
        campusFeed,
        baseDomain,
        postsPage,
        homeViewCount
      ),
      200,
      { "Cache-Control": PUBLIC_SSR_CACHE_CONTROL }
    );
  }

  if (path.startsWith("/preview/")) {
    const previewSlug = path.slice("/preview/".length).toLowerCase();
    if (!previewSlug || previewSlug.includes("/")) {
      return notFound("Preview not found");
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return html(renderSimpleMessage("401", "Preview requires login"), 401);
    }

    const post = await getPostMeta(env, site.id, previewSlug, true);
    if (!post) {
      return notFound("Preview not found");
    }

    const file = await githubReadFile(env, getPostFilePath(site.slug, post.postSlug));
    if (!file) {
      return notFound("Post content missing");
    }

    const siteConfig = await getSiteConfig(env, site);
    const [communitySites, sitePages] = await Promise.all([
      siteConfig.hideCommunitySites
        ? Promise.resolve([])
        : listCommunitySites(env, site.slug, 8),
      listSitePages(env, site.id, 20),
    ]);
    const commentPage = parsePositiveInt(url.searchParams.get("cpage"), 1, 1, 9999);
    const [commentsData, postViewCount] = await Promise.all([
      siteConfig.commentsEnabled
        ? listPostComments(env, site.id, post.postSlug, commentPage, COMMENTS_PAGE_SIZE)
        : Promise.resolve({ comments: [], page: 1, totalPages: 1, total: 0 }),
      listPageViewCounts(env, site.id, "post", [post.postSlug]).then(
        (map) => Math.max(Number(map.get(post.postSlug) || 0), 0)
      ),
    ]);
    const articleHtml = renderMarkdown(file.content);
    return html(
      renderPostPage(site, siteConfig, post, articleHtml, communitySites, sitePages, baseDomain, {
        previewMode: true,
        comments: commentsData.comments,
        commentsPage: commentsData.page,
        commentsTotalPages: commentsData.totalPages,
        commentsEnabled: siteConfig.commentsEnabled,
        commentsTotal: commentsData.total,
        commentBasePath: `/preview/${encodeURIComponent(post.postSlug)}`,
        postViewCount,
        reactionsEnabled: false,
      }),
      200
    );
  }

  if (path === "/robots.txt") {
    return text("User-agent: *\nAllow: /\n");
  }

  const segments = path.slice(1).split("/").filter(Boolean);
  if (segments.length !== 1) {
    return notFound();
  }

  const postSlug = segments[0].toLowerCase();
  const post = await getPostMeta(env, site.id, postSlug, false);
  if (!post) {
    return notFound("Post not found");
  }

  const file = await githubReadFile(env, getPostFilePath(site.slug, post.postSlug));
  if (!file) {
    return notFound("Post content missing");
  }

  const siteConfig = await getSiteConfig(env, site);
  const [communitySites, sitePages] = await Promise.all([
    siteConfig.hideCommunitySites
      ? Promise.resolve([])
      : listCommunitySites(env, site.slug, 8),
    listSitePages(env, site.id, 20),
  ]);
  const commentPage = parsePositiveInt(url.searchParams.get("cpage"), 1, 1, 9999);
  const reactor = await resolveReactorToken(request, env);
  const [commentsData, postViewCount, reactionSnapshot] = await Promise.all([
    siteConfig.commentsEnabled
      ? listPostComments(env, site.id, post.postSlug, commentPage, COMMENTS_PAGE_SIZE)
      : Promise.resolve({ comments: [], page: 1, totalPages: 1, total: 0 }),
    listPageViewCounts(env, site.id, "post", [post.postSlug]).then(
      (map) => Math.max(Number(map.get(post.postSlug) || 0), 0)
    ),
    listPostReactionSnapshot(env, site.id, post.postSlug, reactor.token),
  ]);
  const articleHtml = renderMarkdown(file.content);
  let response = html(
    renderPostPage(site, siteConfig, post, articleHtml, communitySites, sitePages, baseDomain, {
      comments: commentsData.comments,
      commentsPage: commentsData.page,
      commentsTotalPages: commentsData.totalPages,
      commentsEnabled: siteConfig.commentsEnabled,
      commentsTotal: commentsData.total,
      commentBasePath: `/${encodeURIComponent(post.postSlug)}`,
      postViewCount,
      reactionSnapshot,
    }),
    200,
    { "Cache-Control": PUBLIC_SSR_CACHE_CONTROL }
  );
  if (reactor.shouldSetCookie) {
    response = withCookie(response, buildReactorCookie(reactor.token));
  }
  return response;
}

async function handleApi(request, env, ctx, context) {
  const { path, url, hostSlug, baseDomain, reservedSlugs, apiEntrySlug } = context;

  if (request.method === "GET" && path === "/api/check-slug") {
    const slug = String(url.searchParams.get("slug") || "")
      .trim()
      .toLowerCase();
    const validation = validateSlug(slug, reservedSlugs);
    if (!validation.ok) {
      return json({ available: false, reason: validation.reason }, 200);
    }

    const existing = await getSiteBySlug(env, slug);
    if (existing) {
      return json({ available: false, reason: "slug-already-exists" }, 200);
    }

    return json({ available: true }, 200);
  }

  if (request.method === "GET" && path === "/api/public-sites") {
    const sites = await listPublicSites(env, 1000);
    return json(
      {
        generatedAt: new Date().toISOString(),
        total: sites.length,
        sites,
      },
      200
    );
  }

  if (request.method === "GET" && path === "/api/public-feed") {
    const feed = await listCampusFeed(env, null, 80);
    return json(
      {
        generatedAt: new Date().toISOString(),
        total: feed.length,
        posts: feed,
      },
      200
    );
  }

  if (request.method === "POST" && path === "/api/register") {
    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const registerRate = await consumeRateLimit(
      env,
      `${clientIp}:register`,
      10 * 60 * 1000,
      8,
      ctx
    );
    if (!registerRate.allowed) {
      return json(
        { error: "Too many registration attempts, please try later" },
        429,
        { "Retry-After": String(Math.ceil(registerRate.retryAfterMs / 1000)) }
      );
    }

    const body = await readJson(request);

    const slug = String(body.slug || "")
      .trim()
      .toLowerCase();
    const displayName = sanitizeName(body.displayName || slug) || slug;
    const description = sanitizeDescription(body.description || "");
    const adminPassword = String(body.adminPassword || "");
    const inviteCode = String(body.inviteCode || "").trim();

    const inviteCodes = getInviteCodes(env);
    if (!inviteCodes.size) {
      return json({ error: "Invite codes are not configured" }, 503);
    }

    if (!inviteCodes.has(inviteCode)) {
      return json({ error: "Invalid invite code" }, 403);
    }

    if (hostSlug && hostSlug !== apiEntrySlug && !reservedSlugs.has(hostSlug) && slug !== hostSlug) {
      return json({ error: "Slug must match current hostname" }, 400);
    }

    const validation = validateSlug(slug, reservedSlugs);
    if (!validation.ok) {
      return json({ error: "Invalid slug", reason: validation.reason }, 400);
    }

    if (adminPassword.length < 8) {
      return json({ error: "Password must be at least 8 characters" }, 400);
    }

    const existing = await getSiteBySlug(env, slug);
    if (existing) {
      return json({ error: "Slug already exists" }, 409);
    }

    const now = new Date().toISOString();
    const passwordHash = await createPasswordHash(adminPassword, env);

    let siteId = null;
    try {
      const insert = await env.DB.prepare(
        `INSERT INTO sites (slug, display_name, description, admin_secret_hash, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
        .bind(slug, displayName, description, passwordHash, now, now)
        .run();

      siteId = Number(insert.meta?.last_row_id || 0);
      if (!siteId) {
        const createdSite = await getSiteBySlug(env, slug);
        siteId = Number(createdSite?.id || 0);
      }

      const siteConfig = JSON.stringify(
        normalizeSiteConfig(
          {
            slug,
            displayName,
            description,
            heroTitle: "",
            heroSubtitle: "",
            colorTheme: "default",
            footerNote: "",
            customCss: "",
            faviconUrl: DEFAULT_FAVICON_URL,
            headerLinks: [],
            hideCommunitySites: false,
            hideCampusFeed: false,
            commentsEnabled: true,
            createdAt: now,
            updatedAt: now,
            exportVersion: SITE_CONFIG_VERSION,
          },
          {
            slug,
            displayName,
            description,
            createdAt: now,
            updatedAt: now,
          }
        ),
        null,
        2
      );

      await githubWriteFile(
        env,
        getSiteConfigPath(slug),
        `${siteConfig}\n`,
        `feat(${slug}): initialize blog`
      );

      const welcomeSlug = "hello-world";
      await githubWriteFile(
        env,
        getPostFilePath(slug, welcomeSlug),
        buildWelcomePost(slug, displayName, baseDomain),
        `feat(${slug}): add welcome post`
      );

      await upsertPostMeta(
        env,
        siteId,
        welcomeSlug,
        "Hello World",
        "你的第一篇文章，開始編輯它吧。",
        1,
        now,
        now,
        { excludeFromCampusFeed: true, isPage: false }
      );

      const notifyTask = notifyTelegramNewSite(env, {
        slug,
        displayName,
        siteUrl: `https://${slug}.${baseDomain}`,
        createdAt: now,
      }).catch((error) => {
        console.error("Telegram notify failed", error);
      });

      if (ctx && typeof ctx.waitUntil === "function") {
        ctx.waitUntil(notifyTask);
      } else {
        await notifyTask;
      }
    } catch (error) {
      console.error("Failed to register site", error);

      if (siteId) {
        await env.DB.batch([
          env.DB.prepare("DELETE FROM posts WHERE site_id = ?").bind(siteId),
          env.DB.prepare("DELETE FROM sites WHERE id = ?").bind(siteId),
        ]);
      }

      return json(
        {
          error: "Failed to initialize site",
          detail: String(error && error.message ? error.message : error),
        },
        502
      );
    }

    return json(
      {
        ok: true,
        site: slug,
        siteUrl: `https://${slug}.${baseDomain}`,
      },
      201
    );
  }

  if (request.method === "GET" && path === "/api/site-settings") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const config = await getSiteConfig(env, site);
    return json({ site: formatSiteForClient(site), config }, 200);
  }

  if (request.method === "POST" && path === "/api/site-settings") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const body = await readJson(request);
    const currentConfig = await getSiteConfig(env, site);
    const now = new Date().toISOString();

    const displayName = sanitizeName(
      body.displayName ?? currentConfig.displayName ?? site.displayName
    ) || site.slug;
    const description = sanitizeDescription(
      body.description ?? currentConfig.description ?? ""
    );
    const heroTitle = sanitizeTitle(
      body.heroTitle ?? currentConfig.heroTitle ?? ""
    );
    const heroSubtitle = sanitizeDescription(
      body.heroSubtitle ?? currentConfig.heroSubtitle ?? ""
    );
    const colorTheme = sanitizeColorTheme(
      body.colorTheme ?? currentConfig.colorTheme ?? "default"
    );
    const footerNote = sanitizeDescription(
      body.footerNote ?? currentConfig.footerNote ?? ""
    );
    const customCss = sanitizeCustomCss(
      body.customCss ?? currentConfig.customCss ?? ""
    );
    const faviconUrl = sanitizeFaviconUrl(
      body.faviconUrl ?? currentConfig.faviconUrl ?? DEFAULT_FAVICON_URL
    );
    const headerLinks = Array.isArray(body.headerLinks)
      ? sanitizeHeaderLinks(body.headerLinks)
      : sanitizeHeaderLinks(currentConfig.headerLinks || []);
    const hideCommunitySites = body.hideCommunitySites === undefined
      ? Boolean(currentConfig.hideCommunitySites)
      : Boolean(body.hideCommunitySites);
    const hideCampusFeed = body.hideCampusFeed === undefined
      ? Boolean(currentConfig.hideCampusFeed)
      : Boolean(body.hideCampusFeed);
    const commentsEnabled = body.commentsEnabled === undefined
      ? Boolean(currentConfig.commentsEnabled)
      : Boolean(body.commentsEnabled);

    const nextConfig = normalizeSiteConfig(
      {
        slug: site.slug,
        displayName,
        description,
        heroTitle,
        heroSubtitle,
        colorTheme,
        footerNote,
        customCss,
        faviconUrl,
        headerLinks,
        hideCommunitySites,
        hideCampusFeed,
        commentsEnabled,
        createdAt: site.createdAt,
        updatedAt: now,
      },
      site
    );

    try {
      await env.DB.prepare(
        `UPDATE sites
         SET display_name = ?, description = ?, updated_at = ?
         WHERE id = ?`
      )
        .bind(displayName, description, now, site.id)
        .run();

      await githubWriteFile(
        env,
        getSiteConfigPath(site.slug),
        `${JSON.stringify(nextConfig, null, 2)}\n`,
        `feat(${site.slug}): update site settings`
      );
    } catch (error) {
      console.error("Failed to save site settings", error);
      return json(
        {
          error: "Failed to save site settings",
          detail: String(error && error.message ? error.message : error),
        },
        502
      );
    }

    const updatedSite = await getSiteBySlug(env, site.slug);
    return json(
      {
        ok: true,
        site: formatSiteForClient(updatedSite || site),
        config: nextConfig,
      },
      200
    );
  }

  if (request.method === "POST" && path === "/api/login") {
    if (!hostSlug) {
      return json({ error: "Login must happen on site subdomain" }, 400);
    }

    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const rateKey = `${clientIp}:${hostSlug}`;
    const rateResult = await consumeRateLimit(
      env,
      rateKey,
      LOGIN_RATE_WINDOW_MS,
      LOGIN_RATE_MAX_ATTEMPTS,
      ctx
    );
    if (!rateResult.allowed) {
      return json(
        { error: "Too many login attempts, please try later" },
        429,
        { "Retry-After": String(Math.ceil(rateResult.retryAfterMs / 1000)) }
      );
    }

    const body = await readJson(request);
    const password = String(body.password || "");
    const slug = String(body.slug || hostSlug)
      .trim()
      .toLowerCase();

    if (slug !== hostSlug) {
      return json({ error: "Slug mismatch" }, 400);
    }

    const site = await getSiteBySlug(env, slug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const verified = await verifyPassword(password, site.adminSecretHash, env);
    if (!verified) {
      return json({ error: "Invalid credentials" }, 401);
    }

    if (isLegacyPasswordHash(site.adminSecretHash)) {
      try {
        const upgradedHash = await createPasswordHash(password, env);
        await env.DB.prepare(
          `UPDATE sites
           SET admin_secret_hash = ?, updated_at = ?
           WHERE id = ?`
        )
          .bind(upgradedHash, new Date().toISOString(), site.id)
          .run();
      } catch (error) {
        console.error("Failed to upgrade legacy password hash", error);
      }
    }

    await clearRateLimit(env, rateKey);
    const token = await createSessionToken(site.slug, env);
    const response = json({ ok: true }, 200);
    return withCookie(response, buildSessionCookie(token));
  }

  if (request.method === "POST" && path === "/api/logout") {
    const response = json({ ok: true }, 200);
    return withCookie(response, buildClearSessionCookie());
  }

  if (request.method === "POST" && path === "/api/change-password") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const rateKey = `${clientIp}:${hostSlug}:change-password`;
    const rateResult = await consumeRateLimit(
      env,
      rateKey,
      15 * 60 * 1000,
      6,
      ctx
    );
    if (!rateResult.allowed) {
      return json(
        { error: "Too many password change attempts, please try later" },
        429,
        { "Retry-After": String(Math.ceil(rateResult.retryAfterMs / 1000)) }
      );
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const body = await readJson(request);
    const currentPassword = String(body.currentPassword || "");
    const newPassword = String(body.newPassword || "");

    if (newPassword.length < 8) {
      return json({ error: "New password must be at least 8 characters" }, 400);
    }

    const validCurrent = await verifyPassword(currentPassword, site.adminSecretHash, env);
    if (!validCurrent) {
      return json({ error: "Current password is incorrect" }, 403);
    }

    const nextHash = await createPasswordHash(newPassword, env);
    const now = new Date().toISOString();
    await env.DB.prepare(
      `UPDATE sites
       SET admin_secret_hash = ?, updated_at = ?
       WHERE id = ?`
    )
      .bind(nextHash, now, site.id)
      .run();

    await clearRateLimit(env, rateKey);
    return json({ ok: true }, 200);
  }

  if (request.method === "GET" && path === "/api/list-posts") {
    let slug = hostSlug;
    if (!slug) {
      slug = String(url.searchParams.get("slug") || "")
        .trim()
        .toLowerCase();
    }

    if (!slug) {
      return json({ error: "Missing slug" }, 400);
    }

    const site = await getSiteBySlug(env, slug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    let includeDrafts = url.searchParams.get("includeDrafts") === "1";
    if (includeDrafts) {
      const authed = await isSiteAuthenticated(request, env, site.slug);
      if (!authed) {
        includeDrafts = false;
      }
    }

    const posts = await listPosts(env, site.id, includeDrafts);
    return json(
      {
        site: {
          slug: site.slug,
          displayName: site.displayName,
          description: site.description,
        },
        posts,
      },
      200
    );
  }

  if (request.method === "POST" && path === "/api/view") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const body = await readJson(request);
    const resourceType = normalizeViewResourceType(body.resourceType || body.type || "post");
    const resourceKeyInput = String(body.resourceKey || body.postSlug || "").trim().toLowerCase();
    const resourceKey = resourceType === "home" ? HOME_VIEW_KEY : resourceKeyInput;
    if (!resourceKey) {
      return json({ error: "Missing resource key" }, 400);
    }
    if (resourceType === "post") {
      const post = await getPostMeta(env, site.id, resourceKey, false);
      if (!post) {
        return json({ error: "Post not found" }, 404);
      }
    }

    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const viewRate = await consumeRateLimit(
      env,
      `${clientIp}:${site.slug}:view:${resourceType}:${resourceKey}`,
      VIEW_RATE_WINDOW_MS,
      VIEW_RATE_MAX_ATTEMPTS,
      ctx
    );
    if (!viewRate.allowed) {
      const viewMap = await listPageViewCounts(env, site.id, resourceType, [resourceKey]);
      const currentCount = Math.max(Number(viewMap.get(resourceKey) || 0), 0);
      return json(
        { ok: true, throttled: true, count: currentCount },
        200
      );
    }

    const count = await incrementPageViewCount(env, site.id, resourceType, resourceKey);
    return json(
      {
        ok: true,
        resourceType,
        resourceKey,
        count,
      },
      200
    );
  }

  if (request.method === "GET" && path === "/api/admin/comments") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    await ensureCommentsTable(env);
    const postSlug = String(url.searchParams.get("postSlug") || "").trim().toLowerCase();
    const page = parsePositiveInt(url.searchParams.get("page"), 1, 1, 9999);
    const pageSize = COMMENTS_PAGE_SIZE;

    if (postSlug) {
      const commentsData = await listPostComments(env, site.id, postSlug, page, pageSize);
      return json(
        {
          postSlug,
          comments: commentsData.comments,
          page: commentsData.page,
          totalPages: commentsData.totalPages,
          total: commentsData.total,
        },
        200
      );
    }

    const commentsData = await listSiteComments(env, site.id, page, pageSize);
    return json(
      {
        comments: commentsData.comments,
        page: commentsData.page,
        totalPages: commentsData.totalPages,
        total: commentsData.total,
      },
      200
    );
  }

  if (request.method === "GET" && path === "/api/admin/notify-settings") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }
    const settings = await getSiteNotifySettings(env, site.id, { includeBotToken: false });
    return json({ settings }, 200);
  }

  if (request.method === "POST" && path === "/api/admin/notify-settings") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const body = await readJson(request);
    const settings = await upsertSiteNotifySettings(env, site.id, {
      enabled: body.enabled,
      notifyComments: body.notifyComments,
      notifyReactions: body.notifyReactions,
      telegramChatId: body.telegramChatId,
      telegramBotToken: body.telegramBotToken,
      clearBotToken: body.clearBotToken,
    });
    return json({ ok: true, settings }, 200);
  }

  if (request.method === "GET" && path === "/api/admin/notifications") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const page = parsePositiveInt(url.searchParams.get("page"), 1, 1, 9999);
    const notificationsData = await listSiteNotifications(
      env,
      site.id,
      page,
      NOTIFICATION_PAGE_SIZE
    );
    return json(notificationsData, 200);
  }

  if (request.method === "POST" && path === "/api/admin/notifications/read") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const body = await readJson(request);
    await markSiteNotificationsRead(env, site.id, {
      all: body.all === true,
      ids: Array.isArray(body.ids) ? body.ids : [],
    });
    const notificationsData = await listSiteNotifications(
      env,
      site.id,
      1,
      NOTIFICATION_PAGE_SIZE
    );
    return json({ ok: true, unread: notificationsData.unread }, 200);
  }

  if (request.method === "GET" && path === "/api/reactions") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const postSlug = String(url.searchParams.get("postSlug") || "").trim().toLowerCase();
    if (!postSlug) {
      return json({ error: "Missing post slug" }, 400);
    }
    const post = await getPostMeta(env, site.id, postSlug, false);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }
    const reactor = await resolveReactorToken(request, env);
    const snapshot = await listPostReactionSnapshot(env, site.id, postSlug, reactor.token);
    let response = json(
      {
        ok: true,
        postSlug,
        total: snapshot.total,
        selectedKeys: snapshot.selectedKeys,
        reactions: snapshot.items.map((item) => ({
          key: item.key,
          icon: item.icon,
          label: item.label,
          count: item.count,
          selected: item.selected,
        })),
      },
      200
    );
    if (reactor.shouldSetCookie) {
      response = withCookie(response, buildReactorCookie(reactor.token));
    }
    return response;
  }

  if (request.method === "POST" && path === "/api/reactions") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const reactionRate = await consumeRateLimit(
      env,
      `${clientIp}:${site.slug}:reactions`,
      REACTION_RATE_WINDOW_MS,
      REACTION_RATE_MAX_ATTEMPTS,
      ctx
    );
    if (!reactionRate.allowed) {
      return json(
        { error: "Too many reaction requests, please try later" },
        429,
        { "Retry-After": String(Math.ceil(reactionRate.retryAfterMs / 1000)) }
      );
    }

    const body = await readJson(request);
    const postSlug = String(body.postSlug || "").trim().toLowerCase();
    const reactionKey = sanitizeReactionKey(body.reactionKey || "");
    if (!postSlug) {
      return json({ error: "Missing post slug" }, 400);
    }
    if (!reactionKey) {
      return json({ error: "Invalid reaction key" }, 400);
    }

    const post = await getPostMeta(env, site.id, postSlug, false);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }

    const reactor = await resolveReactorToken(request, env);
    const active = await togglePostReaction(env, site.id, postSlug, reactionKey, reactor.token);
    if (active) {
      const reactionPreset = REACTION_PRESET_MAP.get(reactionKey);
      queueSiteNotificationEvent(env, ctx, site, {
        eventType: "reaction",
        postSlug,
        postTitle: post.title || postSlug,
        reactionKey,
        reactionLabel: reactionPreset
          ? `${reactionPreset.icon} ${reactionPreset.label}`
          : reactionKey,
        targetPath: `/${encodeURIComponent(postSlug)}#reactions`,
      });
    }
    const snapshot = await listPostReactionSnapshot(env, site.id, postSlug, reactor.token);
    let response = json(
      {
        ok: true,
        postSlug,
        reactionKey,
        active,
        total: snapshot.total,
        selectedKeys: snapshot.selectedKeys,
        reactions: snapshot.items.map((item) => ({
          key: item.key,
          icon: item.icon,
          label: item.label,
          count: item.count,
          selected: item.selected,
        })),
      },
      200
    );
    if (reactor.shouldSetCookie) {
      response = withCookie(response, buildReactorCookie(reactor.token));
    }
    return response;
  }

  if (request.method === "GET" && path === "/api/comments") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const postSlug = String(url.searchParams.get("postSlug") || "").trim().toLowerCase();
    if (!postSlug) {
      return json({ error: "Missing post slug" }, 400);
    }

    const post = await getPostMeta(env, site.id, postSlug, false);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }

    const siteConfig = await getSiteConfig(env, site);
    if (!siteConfig.commentsEnabled) {
      return json({ comments: [], page: 1, totalPages: 1, total: 0 }, 200);
    }

    const page = parsePositiveInt(url.searchParams.get("page"), 1, 1, 9999);
    const commentsData = await listPostComments(env, site.id, postSlug, page, COMMENTS_PAGE_SIZE);
    return json(
      {
        comments: commentsData.comments,
        page: commentsData.page,
        totalPages: commentsData.totalPages,
        total: commentsData.total,
      },
      200
    );
  }

  if (request.method === "POST" && path === "/api/comments") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const siteConfig = await getSiteConfig(env, site);
    if (!siteConfig.commentsEnabled) {
      return json({ error: "Comments are disabled for this site" }, 403);
    }

    const clientIp = request.headers.get("cf-connecting-ip") || "unknown";
    const rateKey = `${clientIp}:${site.slug}:comments`;
    const rateResult = await consumeRateLimit(
      env,
      rateKey,
      COMMENT_RATE_WINDOW_MS,
      COMMENT_RATE_MAX_ATTEMPTS,
      ctx
    );
    if (!rateResult.allowed) {
      return json(
        { error: "Too many comments, please try later" },
        429,
        { "Retry-After": String(Math.ceil(rateResult.retryAfterMs / 1000)) }
      );
    }

    const body = await readJson(request);
    const postSlug = String(body.postSlug || "").trim().toLowerCase();
    const authorName = sanitizeCommentAuthor(body.authorName || "");
    const authorSite = sanitizeOptionalSiteSlug(body.authorSiteSlug || "");
    const content = sanitizeCommentContent(body.content || "");
    if (!postSlug) {
      return json({ error: "Missing post slug" }, 400);
    }
    if (!authorName) {
      return json({ error: "Name is required" }, 400);
    }
    if (!content) {
      return json({ error: "Comment content is required" }, 400);
    }

    const post = await getPostMeta(env, site.id, postSlug, false);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }

    const created = await createComment(env, site.id, postSlug, authorName, authorSite, content);
    queueSiteNotificationEvent(env, ctx, site, {
      eventType: "comment",
      postSlug,
      postTitle: post.title || postSlug,
      actorName: created.authorName,
      actorSiteSlug: created.authorSiteSlug,
      contentPreview: created.content,
      targetPath: `/${encodeURIComponent(postSlug)}#comments`,
      createdAt: created.createdAt,
    });
    return json({ ok: true, comment: created }, 201);
  }

  if (request.method === "DELETE" && path.startsWith("/api/comments/")) {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }
    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }
    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const rawId = path.slice("/api/comments/".length);
    const id = Number(rawId);
    if (!Number.isInteger(id) || id <= 0) {
      return json({ error: "Invalid comment id" }, 400);
    }
    const deleted = await deleteComment(env, site.id, id);
    if (!deleted) {
      return json({ error: "Comment not found" }, 404);
    }
    return json({ ok: true }, 200);
  }

  if (request.method === "GET" && path.startsWith("/api/posts/")) {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const postSlug = decodeURIComponent(path.slice("/api/posts/".length)).toLowerCase();
    if (!postSlug || postSlug.includes("/")) {
      return json({ error: "Invalid post slug" }, 400);
    }

    const post = await getPostMeta(env, site.id, postSlug, true);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }

    const file = await githubReadFile(env, getPostFilePath(site.slug, post.postSlug));
    return json(
      {
        post: {
          ...post,
          content: file ? file.content : "",
        },
      },
      200
    );
  }

  if (request.method === "DELETE" && path.startsWith("/api/posts/")) {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const postSlug = decodeURIComponent(path.slice("/api/posts/".length)).toLowerCase();
    if (!postSlug || postSlug.includes("/")) {
      return json({ error: "Invalid post slug" }, 400);
    }

    const post = await getPostMeta(env, site.id, postSlug, true);
    if (!post) {
      return json({ error: "Post not found" }, 404);
    }

    try {
      await githubDeleteFile(
        env,
        getPostFilePath(site.slug, post.postSlug),
        `feat(${site.slug}): delete post ${post.postSlug}`
      );
      await deletePostMeta(env, site.id, post.postSlug);
      await deleteCommentsByPost(env, site.id, post.postSlug);
      await deleteReactionsByPost(env, site.id, post.postSlug);
    } catch (error) {
      console.error("Failed to delete post", error);
      return json(
        {
          error: "Failed to delete post",
          detail: String(error && error.message ? error.message : error),
        },
        502
      );
    }

    return json({ ok: true, postSlug: post.postSlug }, 200);
  }

  if (request.method === "POST" && path === "/api/posts") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const body = await readJson(request);

    const title = sanitizeTitle(body.title || "");
    const requestedSlug = String(body.postSlug || "").trim().toLowerCase();
    const postSlug = requestedSlug || slugifyValue(title);
    const previousSlugRaw = String(body.previousSlug || "").trim().toLowerCase();
    const previousSlug = previousSlugRaw && !previousSlugRaw.includes("/")
      ? previousSlugRaw
      : "";
    const description = sanitizeDescription(body.description || "");
    const content = String(body.content || "");
    const published = Boolean(body.published) ? 1 : 0;
    const isPage = Boolean(body.isPage) ? 1 : 0;

    if (!title) {
      return json({ error: "Title is required" }, 400);
    }

    const validation = validatePostSlug(postSlug);
    if (!validation.ok) {
      return json({ error: "Invalid post slug", reason: validation.reason }, 400);
    }

    const now = new Date().toISOString();
    const isRenaming = Boolean(previousSlug && previousSlug !== postSlug);

    try {
      let existingPost = null;
      let previousPost = null;

      if (isRenaming) {
        previousPost = await getPostMeta(env, site.id, previousSlug, true);
        if (!previousPost) {
          return json({ error: "Original post not found" }, 404);
        }
      }

      existingPost = await getPostMeta(env, site.id, postSlug, true);
      if (existingPost && isRenaming) {
        return json({ error: "Target post slug already exists" }, 409);
      }

      await githubWriteFile(
        env,
        getPostFilePath(site.slug, postSlug),
        content,
        `feat(${site.slug}): update post ${postSlug}`
      );

      if (isRenaming) {
        await githubDeleteFile(
          env,
          getPostFilePath(site.slug, previousSlug),
          `feat(${site.slug}): rename post ${previousSlug} -> ${postSlug}`
        );
        await deletePostMeta(env, site.id, previousSlug);
        await moveCommentsToPost(env, site.id, previousSlug, postSlug);
        await moveReactionsToPost(env, site.id, previousSlug, postSlug);
      }

      const createdAt = previousPost?.createdAt || existingPost?.createdAt || now;
      await upsertPostMeta(
        env,
        site.id,
        postSlug,
        title,
        description,
        published,
        now,
        createdAt,
        { isPage: isPage === 1 }
      );
    } catch (error) {
      if (error && error.status === 409) {
        return json({ error: error.userMessage || "文章已被其他人修改，請重新載入後再儲存。" }, 409);
      }
      console.error("Failed to save post", error);
      return json(
        {
          error: "Failed to save post",
          detail: String(error && error.message ? error.message : error),
        },
        502
      );
    }

    return json(
      {
        ok: true,
        post: {
          postSlug,
          title,
          description,
          published,
          isPage,
          updatedAt: now,
        },
      },
      200
    );
  }

  if (request.method === "GET" && path === "/api/export") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    const posts = await listPosts(env, site.id, true);
    const config = await getSiteConfig(env, site);
    const files = await Promise.all(
      posts.map((post) =>
        githubReadFile(env, getPostFilePath(site.slug, post.postSlug)).catch((error) => {
          console.error("Failed to read post during export", post.postSlug, error);
          return null;
        })
      )
    );
    const exportedPosts = posts.map((post, index) => ({
      ...post,
      content: files[index] ? files[index].content : "",
    }));

    const payload = {
      exportedAt: new Date().toISOString(),
      site: {
        slug: site.slug,
        displayName: site.displayName,
        description: site.description,
        createdAt: site.createdAt,
      },
      config,
      posts: exportedPosts,
    };

    const filename = `${site.slug}-export-${new Date().toISOString().slice(0, 10)}.json`;
    return new Response(JSON.stringify(payload, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  }

  if (request.method === "POST" && path === "/api/import") {
    if (!hostSlug) {
      return json({ error: "Missing site context" }, 400);
    }

    const site = await getSiteBySlug(env, hostSlug);
    if (!site) {
      return json({ error: "Site not found" }, 404);
    }

    const authed = await isSiteAuthenticated(request, env, site.slug);
    if (!authed) {
      return json({ error: "Unauthorized" }, 401);
    }

    let formData;
    try {
      formData = await request.formData();
    } catch {
      return json({ error: "Invalid form data" }, 400);
    }

    const file = formData.get("file");
    if (!file || typeof file.text !== "function") {
      return json({ error: "No file uploaded" }, 400);
    }

    let csvText;
    try {
      csvText = await file.text();
    } catch {
      return json({ error: "Failed to read file" }, 400);
    }

    if (!csvText || csvText.length < 10) {
      return json({ error: "File is empty or too small" }, 400);
    }

    if (csvText.length > 10 * 1024 * 1024) {
      return json({ error: "File too large (max 10MB)" }, 400);
    }

    const parsed = parseCSV(csvText);
    if (!parsed.headers.length || !parsed.rows.length) {
      return json({ error: "No valid rows found in CSV" }, 400);
    }

    const imported = [];
    const skipped = [];
    const errors = [];

    for (const row of parsed.rows) {
      try {
        const title = String(row.title || "").trim();
        if (!title) {
          skipped.push({ reason: "missing title" });
          continue;
        }

        const isPage = String(row.is_page || "").toLowerCase() === "true" ? 1 : 0;

        let rawSlug = String(row.slug || row.link || "").trim();
        rawSlug = rawSlug.replace(/^\/+/, "");
        let postSlug = rawSlug ? slugifyValue(rawSlug) : slugifyValue(title);
        if (!postSlug) {
          postSlug = slugifyValue(title);
        }
        if (!postSlug) {
          skipped.push({ title, reason: "cannot derive slug" });
          continue;
        }

        const slugCheck = validatePostSlug(postSlug);
        if (!slugCheck.ok) {
          postSlug = slugifyValue(title);
          const recheck = validatePostSlug(postSlug);
          if (!recheck.ok) {
            skipped.push({ title, reason: "invalid slug" });
            continue;
          }
          postSlug = recheck.slug;
        } else {
          postSlug = slugCheck.slug;
        }

        const content = String(row.content || "").trim();
        const description = String(row.meta_description || "").trim().slice(0, 240);

        let publishedDate = String(row.published_date || "").trim();
        let createdAt;
        if (publishedDate) {
          const d = new Date(publishedDate);
          createdAt = Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
        } else {
          createdAt = new Date().toISOString();
        }

        const discoverable = String(row.make_discoverable || "true").toLowerCase();
        const published = discoverable === "false" ? 0 : 1;

        await githubWriteFile(
          env,
          getPostFilePath(site.slug, postSlug),
          content || `# ${title}\n`,
          `import(${site.slug}): ${postSlug} from BearBlog`
        );

        await upsertPostMeta(
          env,
          site.id,
          postSlug,
          sanitizeTitle(title),
          sanitizeDescription(description),
          published,
          createdAt,
          createdAt,
          { isPage: isPage === 1 }
        );

        imported.push({ title, postSlug, isPage: isPage === 1 });
      } catch (error) {
        errors.push({ title: row.title || "unknown", error: error.message || "unknown error" });
      }
    }

    return json({
      ok: true,
      imported: imported.length,
      skipped: skipped.length,
      errors: errors.length,
      details: { imported, skipped, errors },
    });
  }

  return notFound();
}

async function getSiteBySlug(env, slug) {
  return env.DB.prepare(
    `SELECT
      id,
      slug,
      display_name AS displayName,
      description,
      admin_secret_hash AS adminSecretHash,
      created_at AS createdAt,
      updated_at AS updatedAt
    FROM sites
    WHERE slug = ?
    LIMIT 1`
  )
    .bind(slug)
    .first();
}

function formatSiteForClient(site) {
  if (!site) {
    return null;
  }

  return {
    id: site.id,
    slug: site.slug,
    displayName: site.displayName,
    description: site.description,
    createdAt: site.createdAt,
    updatedAt: site.updatedAt,
    url: `https://${site.slug}.bdfz.net`,
  };
}

async function listPublicSites(env, limit = 500) {
  const safeLimit = Math.min(Math.max(Number(limit) || 100, 1), 1000);
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  const postCountClause = hasIsPageColumn
    ? "p.site_id = s.id AND p.published = 1 AND p.is_page = 0"
    : "p.site_id = s.id AND p.published = 1";
  const result = await env.DB.prepare(
    `SELECT
      s.slug AS slug,
      s.display_name AS displayName,
      s.description AS description,
      s.created_at AS createdAt,
      s.updated_at AS updatedAt,
      (
        SELECT COUNT(*)
        FROM posts p
        WHERE ${postCountClause}
      ) AS postCount
    FROM sites s
    ORDER BY s.created_at DESC
    LIMIT ?`
  )
    .bind(safeLimit)
    .all();

  const rows = result.results || [];
  return rows.map((site) => ({
    slug: site.slug,
    displayName: site.displayName,
    description: site.description || "",
    createdAt: site.createdAt,
    updatedAt: site.updatedAt,
    postCount: Number(site.postCount || 0),
    url: `https://${site.slug}.bdfz.net`,
  }));
}

async function listCommunitySites(env, currentSlug, limit = 12) {
  const safeLimit = Math.min(Math.max(Number(limit) || 12, 1), 60);
  const result = await env.DB.prepare(
    `SELECT
      slug,
      display_name AS displayName,
      description,
      created_at AS createdAt
    FROM sites
    WHERE slug != ?
    ORDER BY created_at DESC
    LIMIT ?`
  )
    .bind(currentSlug, safeLimit)
    .all();

  const rows = result.results || [];
  return rows.map((site) => ({
    slug: site.slug,
    displayName: site.displayName,
    description: site.description || "",
    createdAt: site.createdAt,
    url: `https://${site.slug}.bdfz.net`,
  }));
}

async function listCampusFeed(env, excludeSiteId = null, limit = 24) {
  const safeLimit = Math.min(Math.max(Number(limit) || 24, 1), 120);
  const hasExcludeColumn = await hasPostsColumn(env, "exclude_from_campus_feed");
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  const feedVisibilityClause = hasExcludeColumn
    ? "p.published = 1 AND p.exclude_from_campus_feed = 0"
    : "p.published = 1 AND NOT (p.post_slug = 'hello-world' AND p.title = 'Hello World')";
  const pageExclusionClause = hasIsPageColumn ? "AND p.is_page = 0" : "";
  const visibilityClause = `${feedVisibilityClause} ${pageExclusionClause}`.trim();
  const sql = excludeSiteId
    ? `SELECT
        p.post_slug AS postSlug,
        p.title AS title,
        p.description AS description,
        p.updated_at AS updatedAt,
        s.slug AS siteSlug,
        s.display_name AS siteName
      FROM posts p
      INNER JOIN sites s ON p.site_id = s.id
      WHERE ${visibilityClause} AND p.site_id != ?
      ORDER BY p.updated_at DESC
      LIMIT ?`
    : `SELECT
        p.post_slug AS postSlug,
        p.title AS title,
        p.description AS description,
        p.updated_at AS updatedAt,
        s.slug AS siteSlug,
        s.display_name AS siteName
      FROM posts p
      INNER JOIN sites s ON p.site_id = s.id
      WHERE ${visibilityClause}
      ORDER BY p.updated_at DESC
      LIMIT ?`;

  const statement = excludeSiteId
    ? env.DB.prepare(sql).bind(excludeSiteId, safeLimit)
    : env.DB.prepare(sql).bind(safeLimit);
  const result = await statement.all();
  const rows = result.results || [];

  return rows.map((post) => ({
    siteSlug: post.siteSlug,
    siteName: post.siteName,
    postSlug: post.postSlug,
    title: post.title,
    description: post.description || "",
    updatedAt: post.updatedAt,
    url: `https://${post.siteSlug}.bdfz.net/${post.postSlug}`,
  }));
}

async function listPostsPage(env, siteId, page = 1, pageSize = POSTS_PAGE_SIZE) {
  const safePageSize = Math.min(Math.max(Number(pageSize) || POSTS_PAGE_SIZE, 1), 60);
  const safePage = Math.max(Number(page) || 1, 1);
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  const isPageSelect = hasIsPageColumn ? "is_page AS isPage" : "0 AS isPage";
  const pageFilter = hasIsPageColumn ? "AND is_page = 0" : "";

  const totalResult = await env.DB.prepare(
    `SELECT COUNT(*) AS total
     FROM posts
     WHERE site_id = ? AND published = 1 ${pageFilter}`
  )
    .bind(siteId)
    .first();

  const total = Math.max(Number(totalResult?.total || 0), 0);
  const totalPages = Math.max(1, Math.ceil(total / safePageSize));
  const boundedPage = Math.min(safePage, totalPages);
  const offset = (boundedPage - 1) * safePageSize;
  const rowsResult = await env.DB.prepare(
    `SELECT
      post_slug AS postSlug,
      title,
      description,
      published,
      ${isPageSelect},
      created_at AS createdAt,
      updated_at AS updatedAt
    FROM posts
    WHERE site_id = ? AND published = 1 ${pageFilter}
    ORDER BY updated_at DESC
    LIMIT ? OFFSET ?`
  )
    .bind(siteId, safePageSize, offset)
    .all();

  return {
    posts: rowsResult.results || [],
    total,
    page: boundedPage,
    pageSize: safePageSize,
    totalPages,
    hasPrev: boundedPage > 1,
    hasNext: boundedPage < totalPages,
  };
}

async function listPosts(env, siteId, includeDrafts = false) {
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  const isPageSelect = hasIsPageColumn ? "is_page AS isPage" : "0 AS isPage";
  const publishedFilter = includeDrafts ? "" : "AND published = 1";
  const pageFilter = includeDrafts
    ? ""
    : (hasIsPageColumn ? "AND is_page = 0" : "");

  const sql = `SELECT
      post_slug AS postSlug,
      title,
      description,
      published,
      ${isPageSelect},
      created_at AS createdAt,
      updated_at AS updatedAt
    FROM posts
    WHERE site_id = ? ${publishedFilter} ${pageFilter}
    ORDER BY updated_at DESC`;

  const result = await env.DB.prepare(sql).bind(siteId).all();
  return result.results || [];
}

async function listSitePages(env, siteId, limit = 20) {
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  if (!hasIsPageColumn) {
    return [];
  }

  const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), 100);
  const result = await env.DB.prepare(
    `SELECT
      post_slug AS postSlug,
      title,
      description,
      created_at AS createdAt,
      updated_at AS updatedAt
    FROM posts
    WHERE site_id = ? AND published = 1 AND is_page = 1
    ORDER BY updated_at DESC
    LIMIT ?`
  )
    .bind(siteId, safeLimit)
    .all();

  return result.results || [];
}

async function ensureCommentsTable(env) {
  if (!commentsTableReadyPromise) {
    commentsTableReadyPromise = (async () => {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS comments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_id INTEGER NOT NULL,
          post_slug TEXT NOT NULL,
          author_name TEXT NOT NULL,
          author_site_slug TEXT NOT NULL DEFAULT '',
          content TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_comments_site_post_created
         ON comments(site_id, post_slug, created_at DESC)`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_comments_site_created
         ON comments(site_id, created_at DESC)`
      ).run();
    })().catch((error) => {
      commentsTableReadyPromise = null;
      throw error;
    });
  }
  return commentsTableReadyPromise;
}

async function ensureReactionsTable(env) {
  if (!reactionsTableReadyPromise) {
    reactionsTableReadyPromise = (async () => {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS reactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_id INTEGER NOT NULL,
          post_slug TEXT NOT NULL,
          reaction_key TEXT NOT NULL,
          actor_token TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          UNIQUE(site_id, post_slug, reaction_key, actor_token),
          FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_reactions_site_post
         ON reactions(site_id, post_slug, created_at DESC)`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_reactions_actor
         ON reactions(site_id, post_slug, actor_token)`
      ).run();
    })().catch((error) => {
      reactionsTableReadyPromise = null;
      throw error;
    });
  }
  return reactionsTableReadyPromise;
}

async function ensureViewsTable(env) {
  if (!viewsTableReadyPromise) {
    viewsTableReadyPromise = (async () => {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS page_views (
          site_id INTEGER NOT NULL,
          resource_type TEXT NOT NULL,
          resource_key TEXT NOT NULL,
          view_count INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          PRIMARY KEY(site_id, resource_type, resource_key),
          FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_page_views_site_type
         ON page_views(site_id, resource_type, updated_at DESC)`
      ).run();
    })().catch((error) => {
      viewsTableReadyPromise = null;
      throw error;
    });
  }
  return viewsTableReadyPromise;
}

async function ensureNotificationTables(env) {
  if (!notificationTablesReadyPromise) {
    notificationTablesReadyPromise = (async () => {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS site_notifications (
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
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_site_notifications_site_created
         ON site_notifications(site_id, created_at DESC)`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_site_notifications_site_read
         ON site_notifications(site_id, read_at, created_at DESC)`
      ).run();
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS site_telegram_settings (
          site_id INTEGER PRIMARY KEY,
          enabled INTEGER NOT NULL DEFAULT 0,
          notify_comments INTEGER NOT NULL DEFAULT 1,
          notify_reactions INTEGER NOT NULL DEFAULT 1,
          telegram_chat_id TEXT NOT NULL DEFAULT '',
          telegram_bot_token_enc TEXT NOT NULL DEFAULT '',
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_site_telegram_settings_updated
         ON site_telegram_settings(updated_at DESC)`
      ).run();
    })().catch((error) => {
      notificationTablesReadyPromise = null;
      throw error;
    });
  }
  return notificationTablesReadyPromise;
}

async function ensureRateLimitsTable(env) {
  if (!rateLimitsTableReadyPromise) {
    rateLimitsTableReadyPromise = (async () => {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS rate_limits (
          rate_key TEXT PRIMARY KEY,
          window_start_ms INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )`
      ).run();
      await env.DB.prepare(
        `CREATE INDEX IF NOT EXISTS idx_rate_limits_updated
         ON rate_limits(updated_at DESC)`
      ).run();
    })().catch((error) => {
      rateLimitsTableReadyPromise = null;
      throw error;
    });
  }
  return rateLimitsTableReadyPromise;
}

async function consumeRateLimit(env, rateKey, windowMs, maxAttempts, ctx = null) {
  await ensureRateLimitsTable(env);
  maybeScheduleRateLimitGc(env, ctx);

  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  const safeKey = String(rateKey || "").slice(0, 180);
  const safeWindowMs = Math.max(Number(windowMs) || 60_000, 1_000);
  const safeMaxAttempts = Math.max(Number(maxAttempts) || 1, 1);

  const existing = await env.DB.prepare(
    `SELECT window_start_ms AS windowStartMs, attempts
     FROM rate_limits
     WHERE rate_key = ?`
  )
    .bind(safeKey)
    .first();

  const windowStartMs = Number(existing?.windowStartMs || 0);
  const attempts = Number(existing?.attempts || 0);
  const sameWindow = existing && nowMs - windowStartMs < safeWindowMs;

  if (!sameWindow) {
    await env.DB.prepare(
      `INSERT INTO rate_limits (rate_key, window_start_ms, attempts, updated_at)
       VALUES (?, ?, 1, ?)
       ON CONFLICT(rate_key) DO UPDATE SET
         window_start_ms = excluded.window_start_ms,
         attempts = 1,
         updated_at = excluded.updated_at`
    )
      .bind(safeKey, nowMs, nowIso)
      .run();
    return {
      allowed: true,
      attempts: 1,
      remaining: Math.max(safeMaxAttempts - 1, 0),
      retryAfterMs: 0,
    };
  }

  if (attempts >= safeMaxAttempts) {
    return {
      allowed: false,
      attempts,
      remaining: 0,
      retryAfterMs: Math.max(safeWindowMs - (nowMs - windowStartMs), 1_000),
    };
  }

  const nextAttempts = attempts + 1;
  await env.DB.prepare(
    `UPDATE rate_limits
     SET attempts = ?, updated_at = ?
     WHERE rate_key = ?`
  )
    .bind(nextAttempts, nowIso, safeKey)
    .run();

  return {
    allowed: true,
    attempts: nextAttempts,
    remaining: Math.max(safeMaxAttempts - nextAttempts, 0),
    retryAfterMs: 0,
  };
}

function maybeScheduleRateLimitGc(env, ctx = null) {
  if (Math.random() >= 0.01) {
    return;
  }
  const cleanupPromise = env.DB.prepare(
    `DELETE FROM rate_limits
     WHERE updated_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 day')`
  )
    .run()
    .catch((error) => {
      console.error("Failed to cleanup stale rate limits", error);
    });

  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(cleanupPromise);
  }
}

async function clearRateLimit(env, rateKey) {
  if (!rateKey) {
    return;
  }
  try {
    await ensureRateLimitsTable(env);
    await env.DB.prepare("DELETE FROM rate_limits WHERE rate_key = ?")
      .bind(String(rateKey).slice(0, 180))
      .run();
  } catch (error) {
    console.error("Failed to clear rate limit", error);
  }
}

async function getPostsColumns(env) {
  const db = env?.DB;
  if (!db) {
    return new Set();
  }

  let columnsPromise = postsColumnsPromiseByDb.get(db);
  if (!columnsPromise) {
    columnsPromise = db
      .prepare("PRAGMA table_info(posts)")
      .all()
      .then((result) => {
        const rows = result.results || [];
        return new Set(
          rows
            .map((item) => String(item.name || "").toLowerCase())
            .filter(Boolean)
        );
      })
      .catch((error) => {
        postsColumnsPromiseByDb.delete(db);
        throw error;
      });
    postsColumnsPromiseByDb.set(db, columnsPromise);
  }
  return columnsPromise;
}

async function hasPostsColumn(env, columnName) {
  try {
    const columns = await getPostsColumns(env);
    return columns.has(String(columnName || "").toLowerCase());
  } catch (error) {
    console.error("Failed to inspect posts columns", error);
    return false;
  }
}

async function listPostComments(env, siteId, postSlug, page = 1, pageSize = COMMENTS_PAGE_SIZE) {
  await ensureCommentsTable(env);
  const safePageSize = Math.min(Math.max(Number(pageSize) || COMMENTS_PAGE_SIZE, 1), 80);
  const safePage = Math.max(Number(page) || 1, 1);

  const totalResult = await env.DB.prepare(
    `SELECT COUNT(*) AS total
     FROM comments
     WHERE site_id = ? AND post_slug = ?`
  )
    .bind(siteId, postSlug)
    .first();

  const total = Math.max(Number(totalResult?.total || 0), 0);
  const totalPages = Math.max(1, Math.ceil(total / safePageSize));
  const boundedPage = Math.min(safePage, totalPages);
  const offset = (boundedPage - 1) * safePageSize;
  const rowsResult = await env.DB.prepare(
    `SELECT
      id,
      post_slug AS postSlug,
      author_name AS authorName,
      author_site_slug AS authorSiteSlug,
      content,
      created_at AS createdAt
    FROM comments
    WHERE site_id = ? AND post_slug = ?
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?`
  )
    .bind(siteId, postSlug, safePageSize, offset)
    .all();

  const comments = (rowsResult.results || []).map((item) => ({
    id: Number(item.id),
    postSlug: item.postSlug,
    authorName: item.authorName,
    authorSiteSlug: item.authorSiteSlug || "",
    content: item.content,
    createdAt: item.createdAt,
  }));

  return {
    comments,
    total,
    page: boundedPage,
    totalPages,
    pageSize: safePageSize,
  };
}

async function listSiteComments(env, siteId, page = 1, pageSize = COMMENTS_PAGE_SIZE) {
  await ensureCommentsTable(env);
  const safePageSize = Math.min(Math.max(Number(pageSize) || COMMENTS_PAGE_SIZE, 1), 80);
  const safePage = Math.max(Number(page) || 1, 1);

  const totalResult = await env.DB.prepare(
    "SELECT COUNT(*) AS total FROM comments WHERE site_id = ?"
  )
    .bind(siteId)
    .first();

  const total = Math.max(Number(totalResult?.total || 0), 0);
  const totalPages = Math.max(1, Math.ceil(total / safePageSize));
  const boundedPage = Math.min(safePage, totalPages);
  const offset = (boundedPage - 1) * safePageSize;
  const rowsResult = await env.DB.prepare(
    `SELECT
      id,
      post_slug AS postSlug,
      author_name AS authorName,
      author_site_slug AS authorSiteSlug,
      content,
      created_at AS createdAt
    FROM comments
    WHERE site_id = ?
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?`
  )
    .bind(siteId, safePageSize, offset)
    .all();

  const comments = (rowsResult.results || []).map((item) => ({
    id: Number(item.id),
    postSlug: item.postSlug,
    authorName: item.authorName,
    authorSiteSlug: item.authorSiteSlug || "",
    content: item.content,
    createdAt: item.createdAt,
  }));

  return {
    comments,
    total,
    page: boundedPage,
    totalPages,
    pageSize: safePageSize,
  };
}

async function createComment(env, siteId, postSlug, authorName, authorSiteSlug, content) {
  await ensureCommentsTable(env);
  const createdAt = new Date().toISOString();
  const result = await env.DB.prepare(
    `INSERT INTO comments (site_id, post_slug, author_name, author_site_slug, content, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  )
    .bind(siteId, postSlug, authorName, authorSiteSlug, content, createdAt)
    .run();

  const id = Number(result.meta?.last_row_id || 0);
  return {
    id,
    postSlug,
    authorName,
    authorSiteSlug,
    content,
    createdAt,
  };
}

async function deleteComment(env, siteId, commentId) {
  await ensureCommentsTable(env);
  const result = await env.DB.prepare(
    "DELETE FROM comments WHERE id = ? AND site_id = ?"
  )
    .bind(commentId, siteId)
    .run();
  return Number(result.meta?.changes || 0) > 0;
}

function normalizeViewResourceType(value) {
  const type = String(value || "").trim().toLowerCase();
  if (type === "home") {
    return "home";
  }
  return "post";
}

function normalizeViewResourceKey(resourceType, value) {
  if (resourceType === "home") {
    return HOME_VIEW_KEY;
  }
  const slug = String(value || "").trim().toLowerCase();
  if (!slug) {
    return "";
  }
  if (slug.length > 120) {
    return slug.slice(0, 120);
  }
  return slug;
}

function formatViewCount(value) {
  const safe = Math.max(Number(value) || 0, 0);
  return new Intl.NumberFormat("zh-Hant").format(safe);
}

async function incrementPageViewCount(env, siteId, resourceType, resourceKey) {
  await ensureViewsTable(env);
  const normalizedType = normalizeViewResourceType(resourceType);
  const normalizedKey = normalizeViewResourceKey(normalizedType, resourceKey);
  if (!normalizedKey) {
    return 0;
  }
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO page_views (site_id, resource_type, resource_key, view_count, updated_at)
     VALUES (?, ?, ?, 1, ?)
     ON CONFLICT(site_id, resource_type, resource_key)
     DO UPDATE SET
       view_count = page_views.view_count + 1,
       updated_at = excluded.updated_at`
  )
    .bind(siteId, normalizedType, normalizedKey, now)
    .run();

  const row = await env.DB.prepare(
    `SELECT view_count AS viewCount
     FROM page_views
     WHERE site_id = ? AND resource_type = ? AND resource_key = ?
     LIMIT 1`
  )
    .bind(siteId, normalizedType, normalizedKey)
    .first();

  return Math.max(Number(row?.viewCount || 0), 0);
}

async function listPageViewCounts(env, siteId, resourceType, resourceKeys = []) {
  await ensureViewsTable(env);
  const normalizedType = normalizeViewResourceType(resourceType);
  const uniqueKeys = Array.from(
    new Set(
      resourceKeys
        .map((item) => normalizeViewResourceKey(normalizedType, item))
        .filter(Boolean)
    )
  );
  if (!uniqueKeys.length) {
    return new Map();
  }
  const placeholders = uniqueKeys.map(() => "?").join(", ");
  const query = `SELECT resource_key AS resourceKey, view_count AS viewCount
    FROM page_views
    WHERE site_id = ? AND resource_type = ? AND resource_key IN (${placeholders})`;
  const result = await env.DB.prepare(query)
    .bind(siteId, normalizedType, ...uniqueKeys)
    .all();
  const viewMap = new Map();
  for (const row of result.results || []) {
    const key = String(row.resourceKey || "");
    if (!key) {
      continue;
    }
    viewMap.set(key, Math.max(Number(row.viewCount || 0), 0));
  }
  return viewMap;
}

function sanitizeActorToken(value) {
  const token = String(value || "").trim().toLowerCase();
  if (!token) {
    return "";
  }
  if (!/^[a-f0-9]{20,64}$/.test(token)) {
    return "";
  }
  return token.slice(0, 64);
}

async function deriveIpActorToken(request, env) {
  const ip = String(request.headers.get("cf-connecting-ip") || "").trim();
  const ua = String(request.headers.get("user-agent") || "").trim();
  if (!ip) {
    return randomHex(16);
  }
  const digest = await sha256Hex(`${ip}|${ua}|${getSessionSecret(env)}|reactor-ip-v1`);
  return sanitizeActorToken(digest) || randomHex(16);
}

async function resolveReactorToken(request, env) {
  const cookies = parseCookies(request.headers.get("cookie") || "");
  const existing = sanitizeActorToken(cookies[REACTOR_COOKIE]);
  const ipToken = await deriveIpActorToken(request, env);
  if (existing) {
    if (existing === ipToken) {
      return { token: ipToken, shouldSetCookie: false };
    }
    const mixed = await sha256Hex(`${existing}:${ipToken}:reactor-mix-v1`);
    return { token: sanitizeActorToken(mixed) || ipToken, shouldSetCookie: false };
  }
  return { token: ipToken, shouldSetCookie: true };
}

function buildReactorCookie(token) {
  return `${REACTOR_COOKIE}=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${REACTOR_COOKIE_TTL_SECONDS}`;
}

function sanitizeReactionKey(value) {
  const key = String(value || "").trim().toLowerCase();
  return REACTION_PRESET_MAP.has(key) ? key : "";
}

function buildReactionSnapshot(countsByKey = new Map(), selectedKeys = new Set()) {
  const normalizedSelected = new Set(
    Array.from(selectedKeys || []).map((item) => sanitizeReactionKey(item)).filter(Boolean)
  );
  const items = REACTION_PRESETS.map((preset, index) => {
    const count = Math.max(Number(countsByKey.get(preset.key) || 0), 0);
    return {
      ...preset,
      orderIndex: index,
      count,
      selected: normalizedSelected.has(preset.key),
    };
  }).sort((left, right) => {
    if (left.count !== right.count) {
      return right.count - left.count;
    }
    return left.orderIndex - right.orderIndex;
  }).map((item) => ({
    key: item.key,
    icon: item.icon,
    label: item.label,
    count: item.count,
    selected: item.selected,
  }));

  return {
    items,
    total: items.reduce((sum, item) => sum + item.count, 0),
    selectedKeys: items.filter((item) => item.selected).map((item) => item.key),
  };
}

async function listPostReactionSnapshot(env, siteId, postSlug, actorToken = "") {
  await ensureReactionsTable(env);
  const countsResult = await env.DB.prepare(
    `SELECT reaction_key AS reactionKey, COUNT(*) AS total
     FROM reactions
     WHERE site_id = ? AND post_slug = ?
     GROUP BY reaction_key`
  )
    .bind(siteId, postSlug)
    .all();
  const countsMap = new Map();
  for (const row of countsResult.results || []) {
    const key = sanitizeReactionKey(row.reactionKey);
    if (!key) {
      continue;
    }
    countsMap.set(key, Math.max(Number(row.total || 0), 0));
  }

  const normalizedActorToken = sanitizeActorToken(actorToken);
  const selected = new Set();
  if (normalizedActorToken) {
    const selectedResult = await env.DB.prepare(
      `SELECT reaction_key AS reactionKey
       FROM reactions
       WHERE site_id = ? AND post_slug = ? AND actor_token = ?`
    )
      .bind(siteId, postSlug, normalizedActorToken)
      .all();
    for (const row of selectedResult.results || []) {
      const key = sanitizeReactionKey(row.reactionKey);
      if (key) {
        selected.add(key);
      }
    }
  }
  return buildReactionSnapshot(countsMap, selected);
}

async function togglePostReaction(env, siteId, postSlug, reactionKey, actorToken) {
  await ensureReactionsTable(env);
  const normalizedReactionKey = sanitizeReactionKey(reactionKey);
  const normalizedActorToken = sanitizeActorToken(actorToken);
  if (!normalizedReactionKey || !normalizedActorToken) {
    return false;
  }

  const existing = await env.DB.prepare(
    `SELECT id
     FROM reactions
     WHERE site_id = ? AND post_slug = ? AND reaction_key = ? AND actor_token = ?
     LIMIT 1`
  )
    .bind(siteId, postSlug, normalizedReactionKey, normalizedActorToken)
    .first();

  if (existing) {
    await env.DB.prepare(
      `DELETE FROM reactions
       WHERE site_id = ? AND post_slug = ? AND reaction_key = ? AND actor_token = ?`
    )
      .bind(siteId, postSlug, normalizedReactionKey, normalizedActorToken)
      .run();
    return false;
  }

  await env.DB.prepare(
    `INSERT INTO reactions (site_id, post_slug, reaction_key, actor_token, created_at)
     VALUES (?, ?, ?, ?, ?)`
  )
    .bind(siteId, postSlug, normalizedReactionKey, normalizedActorToken, new Date().toISOString())
    .run();
  return true;
}

function sanitizeNotifyEventType(value) {
  const type = String(value || "").trim().toLowerCase();
  if (type === "comment") {
    return "comment";
  }
  if (type === "reaction") {
    return "reaction";
  }
  return "";
}

function sanitizeNotificationPath(value) {
  const rawPath = String(value || "").trim();
  if (!rawPath.startsWith("/")) {
    return "/";
  }
  const path = rawPath
    .replace(/[\u0000-\u001f\u007f"'<>`]/g, "")
    .slice(0, 320);
  return path.startsWith("/") ? path : "/";
}

function sanitizeNotificationPreview(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .trim()
    .slice(0, 320);
}

function sanitizeTelegramBotToken(value) {
  const token = String(value || "").trim();
  if (!token) {
    return "";
  }
  if (!/^\d{6,14}:[A-Za-z0-9_-]{20,140}$/.test(token)) {
    return "";
  }
  return token;
}

function sanitizeTelegramChatId(value) {
  const chatId = String(value || "").trim();
  if (!chatId) {
    return "";
  }
  if (!/^-?\d{5,20}$/.test(chatId)) {
    return "";
  }
  return chatId;
}

function normalizeNotifyBool(value, fallback = false) {
  if (value === undefined || value === null) {
    return Boolean(fallback);
  }
  return Boolean(value);
}

function defaultSiteNotifySettings() {
  return {
    enabled: false,
    notifyComments: true,
    notifyReactions: true,
    telegramChatId: "",
    hasBotToken: false,
    telegramBotToken: "",
    updatedAt: "",
  };
}

async function getSiteNotifySettings(env, siteId, options = {}) {
  await ensureNotificationTables(env);
  const includeBotToken = Boolean(options.includeBotToken);
  const row = await env.DB.prepare(
    `SELECT
      enabled,
      notify_comments AS notifyComments,
      notify_reactions AS notifyReactions,
      telegram_chat_id AS telegramChatId,
      telegram_bot_token_enc AS telegramBotTokenEnc,
      updated_at AS updatedAt
    FROM site_telegram_settings
    WHERE site_id = ?
    LIMIT 1`
  )
    .bind(siteId)
    .first();

  if (!row) {
    return defaultSiteNotifySettings();
  }

  const encryptedToken = String(row.telegramBotTokenEnc || "");
  let telegramBotToken = "";
  if (includeBotToken && encryptedToken) {
    try {
      telegramBotToken = await decryptSensitiveValue(encryptedToken, env);
    } catch (error) {
      console.error("Failed to decrypt telegram bot token", error);
      telegramBotToken = "";
    }
  }

  return {
    enabled: Number(row.enabled || 0) === 1,
    notifyComments: Number(row.notifyComments || 0) !== 0,
    notifyReactions: Number(row.notifyReactions || 0) !== 0,
    telegramChatId: sanitizeTelegramChatId(row.telegramChatId || ""),
    hasBotToken: Boolean(encryptedToken),
    telegramBotToken: sanitizeTelegramBotToken(telegramBotToken),
    updatedAt: String(row.updatedAt || ""),
  };
}

async function upsertSiteNotifySettings(env, siteId, payload) {
  await ensureNotificationTables(env);
  const existing = await env.DB.prepare(
    `SELECT telegram_bot_token_enc AS token
     FROM site_telegram_settings
     WHERE site_id = ?
     LIMIT 1`
  )
    .bind(siteId)
    .first();

  const enabled = normalizeNotifyBool(payload.enabled, false);
  const notifyComments = normalizeNotifyBool(payload.notifyComments, true);
  const notifyReactions = normalizeNotifyBool(payload.notifyReactions, true);
  const chatId = sanitizeTelegramChatId(payload.telegramChatId || "");
  const rawToken = sanitizeTelegramBotToken(payload.telegramBotToken || "");
  const clearBotToken = Boolean(payload.clearBotToken);

  let tokenEnc = String(existing?.token || "");
  if (rawToken) {
    tokenEnc = await encryptSensitiveValue(rawToken, env);
  } else if (clearBotToken) {
    tokenEnc = "";
  }

  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO site_telegram_settings (
      site_id,
      enabled,
      notify_comments,
      notify_reactions,
      telegram_chat_id,
      telegram_bot_token_enc,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(site_id)
    DO UPDATE SET
      enabled = excluded.enabled,
      notify_comments = excluded.notify_comments,
      notify_reactions = excluded.notify_reactions,
      telegram_chat_id = excluded.telegram_chat_id,
      telegram_bot_token_enc = excluded.telegram_bot_token_enc,
      updated_at = excluded.updated_at`
  )
    .bind(
      siteId,
      enabled ? 1 : 0,
      notifyComments ? 1 : 0,
      notifyReactions ? 1 : 0,
      chatId,
      tokenEnc,
      now
    )
    .run();

  return getSiteNotifySettings(env, siteId, { includeBotToken: false });
}

async function createSiteNotification(env, siteId, payload) {
  await ensureNotificationTables(env);
  const eventType = sanitizeNotifyEventType(payload.eventType);
  if (!eventType) {
    return null;
  }
  const postSlug = String(payload.postSlug || "").trim().toLowerCase().slice(0, 120);
  if (!postSlug) {
    return null;
  }

  const now = String(payload.createdAt || new Date().toISOString());
  const postTitle = sanitizeTitle(payload.postTitle || postSlug);
  const actorName = sanitizeName(payload.actorName || "");
  const actorSiteSlug = sanitizeOptionalSiteSlug(payload.actorSiteSlug || "");
  const contentPreview = sanitizeNotificationPreview(payload.contentPreview || "");
  const reactionKey = sanitizeReactionKey(payload.reactionKey || "");
  const reactionPreset = reactionKey ? REACTION_PRESET_MAP.get(reactionKey) : null;
  const reactionLabel = reactionPreset
    ? `${reactionPreset.icon} ${reactionPreset.label}`
    : sanitizeNotificationPreview(payload.reactionLabel || "");
  const targetPath = sanitizeNotificationPath(payload.targetPath || `/${encodeURIComponent(postSlug)}`);

  const result = await env.DB.prepare(
    `INSERT INTO site_notifications (
      site_id,
      event_type,
      post_slug,
      post_title,
      actor_name,
      actor_site_slug,
      content_preview,
      reaction_key,
      reaction_label,
      target_path,
      created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      siteId,
      eventType,
      postSlug,
      postTitle,
      actorName,
      actorSiteSlug,
      contentPreview,
      reactionKey,
      reactionLabel,
      targetPath,
      now
    )
    .run();

  return Number(result.meta?.last_row_id || 0);
}

async function listSiteNotifications(env, siteId, page = 1, pageSize = NOTIFICATION_PAGE_SIZE) {
  await ensureNotificationTables(env);
  const safePageSize = Math.min(Math.max(Number(pageSize) || NOTIFICATION_PAGE_SIZE, 1), 100);
  const safePage = Math.max(Number(page) || 1, 1);
  const totalResult = await env.DB.prepare(
    `SELECT COUNT(*) AS total
     FROM site_notifications
     WHERE site_id = ?`
  )
    .bind(siteId)
    .first();
  const unreadResult = await env.DB.prepare(
    `SELECT COUNT(*) AS total
     FROM site_notifications
     WHERE site_id = ? AND read_at IS NULL`
  )
    .bind(siteId)
    .first();

  const total = Math.max(Number(totalResult?.total || 0), 0);
  const unread = Math.max(Number(unreadResult?.total || 0), 0);
  const totalPages = Math.max(1, Math.ceil(total / safePageSize));
  const boundedPage = Math.min(safePage, totalPages);
  const offset = (boundedPage - 1) * safePageSize;
  const rows = await env.DB.prepare(
    `SELECT
      id,
      event_type AS eventType,
      post_slug AS postSlug,
      post_title AS postTitle,
      actor_name AS actorName,
      actor_site_slug AS actorSiteSlug,
      content_preview AS contentPreview,
      reaction_key AS reactionKey,
      reaction_label AS reactionLabel,
      target_path AS targetPath,
      created_at AS createdAt,
      read_at AS readAt
    FROM site_notifications
    WHERE site_id = ?
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?`
  )
    .bind(siteId, safePageSize, offset)
    .all();

  const notifications = (rows.results || []).map((row) => ({
    id: Number(row.id || 0),
    eventType: sanitizeNotifyEventType(row.eventType),
    postSlug: String(row.postSlug || ""),
    postTitle: String(row.postTitle || ""),
    actorName: String(row.actorName || ""),
    actorSiteSlug: String(row.actorSiteSlug || ""),
    contentPreview: String(row.contentPreview || ""),
    reactionKey: sanitizeReactionKey(row.reactionKey),
    reactionLabel: String(row.reactionLabel || ""),
    targetPath: sanitizeNotificationPath(row.targetPath || "/"),
    createdAt: String(row.createdAt || ""),
    read: Boolean(row.readAt),
  }));

  return {
    notifications,
    unread,
    total,
    page: boundedPage,
    pageSize: safePageSize,
    totalPages,
    hasPrev: boundedPage > 1,
    hasNext: boundedPage < totalPages,
  };
}

async function markSiteNotificationsRead(env, siteId, options = {}) {
  await ensureNotificationTables(env);
  const now = new Date().toISOString();
  const markAll = Boolean(options.all);
  if (markAll) {
    await env.DB.prepare(
      `UPDATE site_notifications
       SET read_at = COALESCE(read_at, ?)
       WHERE site_id = ?`
    )
      .bind(now, siteId)
      .run();
  } else {
    const ids = Array.isArray(options.ids)
      ? Array.from(
        new Set(
          options.ids
            .map((item) => Number(item))
            .filter((item) => Number.isInteger(item) && item > 0)
            .slice(0, 80)
        )
      )
      : [];
    if (!ids.length) {
      return;
    }
    const placeholders = ids.map(() => "?").join(",");
    await env.DB.prepare(
      `UPDATE site_notifications
       SET read_at = COALESCE(read_at, ?)
       WHERE site_id = ? AND id IN (${placeholders})`
    )
      .bind(now, siteId, ...ids)
      .run();
  }
}

function maybeScheduleNotificationsGc(env, siteId, ctx = null) {
  if (Math.random() >= 0.02) {
    return;
  }
  const cleanup = (async () => {
    const cutoff = await env.DB.prepare(
      `SELECT id
       FROM site_notifications
       WHERE site_id = ?
       ORDER BY created_at DESC
       LIMIT 1 OFFSET 799`
    )
      .bind(siteId)
      .first();

    const cutoffId = Number(cutoff?.id || 0);
    if (!cutoffId) {
      return;
    }

    await env.DB.prepare(
      `DELETE FROM site_notifications
       WHERE site_id = ? AND id < ?`
    )
      .bind(siteId, cutoffId)
      .run();
  })()
    .catch((error) => {
      console.error("Failed to cleanup old notifications", error);
    });
  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(cleanup);
  }
}

function buildSiteNotificationMessage(event, site, baseDomain) {
  const fullSite = `${site.slug}.${baseDomain}`;
  const postUrl = `https://${fullSite}/${encodeURIComponent(event.postSlug)}`;
  if (sanitizeNotifyEventType(event.eventType) === "reaction") {
    const reactionLine = event.reactionLabel
      ? `反應：${event.reactionLabel}`
      : "反應：新點贊";
    return [
      "🔔 新點贊通知",
      `站點：${fullSite}`,
      `文章：${event.postTitle || event.postSlug}`,
      reactionLine,
      `連結：${postUrl}`,
      `時間：${event.createdAt || new Date().toISOString()}`,
    ].join("\n");
  }
  const actorLine = event.actorSiteSlug
    ? `${event.actorName}（${event.actorSiteSlug}.${baseDomain}）`
    : (event.actorName || "匿名");
  return [
    "💬 新留言通知",
    `站點：${fullSite}`,
    `文章：${event.postTitle || event.postSlug}`,
    `留言者：${actorLine}`,
    `內容：${event.contentPreview || "(無文字)"}`,
    `連結：${postUrl}#comments`,
    `時間：${event.createdAt || new Date().toISOString()}`,
  ].join("\n");
}

async function sendTelegramViaCustomBot(botToken, chatId, text) {
  const endpoint = `https://api.telegram.org/bot${botToken}/sendMessage`;
  let response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        disable_web_page_preview: true,
      }),
    });
  } catch {
    throw new Error("Telegram custom notify request failed");
  }

  if (!response.ok) {
    throw new Error(`Telegram custom notify failed (${response.status})`);
  }
}

function queueSiteNotificationEvent(env, ctx, site, payload) {
  const task = (async () => {
    const event = {
      eventType: sanitizeNotifyEventType(payload.eventType),
      postSlug: String(payload.postSlug || "").trim().toLowerCase(),
      postTitle: sanitizeTitle(payload.postTitle || payload.postSlug || ""),
      actorName: sanitizeName(payload.actorName || ""),
      actorSiteSlug: sanitizeOptionalSiteSlug(payload.actorSiteSlug || ""),
      contentPreview: sanitizeNotificationPreview(payload.contentPreview || ""),
      reactionKey: sanitizeReactionKey(payload.reactionKey || ""),
      reactionLabel: sanitizeNotificationPreview(payload.reactionLabel || ""),
      targetPath: sanitizeNotificationPath(
        payload.targetPath || `/${encodeURIComponent(String(payload.postSlug || "").trim().toLowerCase())}`
      ),
      createdAt: String(payload.createdAt || new Date().toISOString()),
    };
    if (!event.eventType || !event.postSlug) {
      return;
    }

    await createSiteNotification(env, site.id, event);
    maybeScheduleNotificationsGc(env, site.id, ctx);

    const notifySettings = await getSiteNotifySettings(env, site.id, { includeBotToken: true });
    if (!notifySettings.enabled || !notifySettings.telegramChatId || !notifySettings.telegramBotToken) {
      return;
    }
    if (event.eventType === "comment" && !notifySettings.notifyComments) {
      return;
    }
    if (event.eventType === "reaction" && !notifySettings.notifyReactions) {
      return;
    }
    const baseDomain = String(env.BASE_DOMAIN || "bdfz.net").toLowerCase();
    const message = buildSiteNotificationMessage(event, site, baseDomain);
    await sendTelegramViaCustomBot(
      notifySettings.telegramBotToken,
      notifySettings.telegramChatId,
      message
    );
  })().catch((error) => {
    console.error("Failed to queue site notification event", error);
  });

  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(task);
  }
}

async function getPostMeta(env, siteId, postSlug, includeDrafts = false) {
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");
  const isPageSelect = hasIsPageColumn ? "is_page AS isPage" : "0 AS isPage";
  const sql = includeDrafts
    ? `SELECT
        post_slug AS postSlug,
        title,
        description,
        published,
        ${isPageSelect},
        created_at AS createdAt,
        updated_at AS updatedAt
      FROM posts
      WHERE site_id = ? AND post_slug = ?
      LIMIT 1`
    : `SELECT
        post_slug AS postSlug,
        title,
        description,
        published,
        ${isPageSelect},
        created_at AS createdAt,
        updated_at AS updatedAt
      FROM posts
      WHERE site_id = ? AND post_slug = ? AND published = 1
      LIMIT 1`;

  return env.DB.prepare(sql).bind(siteId, postSlug).first();
}

async function upsertPostMeta(
  env,
  siteId,
  postSlug,
  title,
  description,
  published,
  updatedAt,
  createdAt,
  options = {}
) {
  const created = createdAt || updatedAt;
  const excludeFromCampusFeed = options.excludeFromCampusFeed ? 1 : 0;
  const isPage = options.isPage ? 1 : 0;
  const hasExcludeColumn = await hasPostsColumn(env, "exclude_from_campus_feed");
  const hasIsPageColumn = await hasPostsColumn(env, "is_page");

  if (hasExcludeColumn && hasIsPageColumn) {
    return env.DB.prepare(
      `INSERT INTO posts (
        site_id,
        post_slug,
        title,
        description,
        published,
        is_page,
        exclude_from_campus_feed,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(site_id, post_slug)
      DO UPDATE SET
        title = excluded.title,
        description = excluded.description,
        published = excluded.published,
        is_page = excluded.is_page,
        exclude_from_campus_feed = excluded.exclude_from_campus_feed,
        updated_at = excluded.updated_at`
    )
      .bind(
        siteId,
        postSlug,
        title,
        description,
        published,
        isPage,
        excludeFromCampusFeed,
        created,
        updatedAt
      )
      .run();
  }

  if (hasExcludeColumn) {
    return env.DB.prepare(
      `INSERT INTO posts (
        site_id,
        post_slug,
        title,
        description,
        published,
        exclude_from_campus_feed,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(site_id, post_slug)
      DO UPDATE SET
        title = excluded.title,
        description = excluded.description,
        published = excluded.published,
        exclude_from_campus_feed = excluded.exclude_from_campus_feed,
        updated_at = excluded.updated_at`
    )
      .bind(
        siteId,
        postSlug,
        title,
        description,
        published,
        excludeFromCampusFeed,
        created,
        updatedAt
      )
      .run();
  }

  if (hasIsPageColumn) {
    return env.DB.prepare(
      `INSERT INTO posts (
        site_id,
        post_slug,
        title,
        description,
        published,
        is_page,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(site_id, post_slug)
      DO UPDATE SET
        title = excluded.title,
        description = excluded.description,
        published = excluded.published,
        is_page = excluded.is_page,
        updated_at = excluded.updated_at`
    )
      .bind(
        siteId,
        postSlug,
        title,
        description,
        published,
        isPage,
        created,
        updatedAt
      )
      .run();
  }

  return env.DB.prepare(
    `INSERT INTO posts (site_id, post_slug, title, description, published, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(site_id, post_slug)
     DO UPDATE SET
       title = excluded.title,
       description = excluded.description,
       published = excluded.published,
      updated_at = excluded.updated_at`
  )
    .bind(siteId, postSlug, title, description, published, created, updatedAt)
    .run();
}

async function deletePostMeta(env, siteId, postSlug) {
  const result = await env.DB.prepare(
    "DELETE FROM posts WHERE site_id = ? AND post_slug = ?"
  )
    .bind(siteId, postSlug)
    .run();
  return Number(result.meta?.changes || 0) > 0;
}

async function deleteCommentsByPost(env, siteId, postSlug) {
  await ensureCommentsTable(env);
  await env.DB.prepare(
    "DELETE FROM comments WHERE site_id = ? AND post_slug = ?"
  )
    .bind(siteId, postSlug)
    .run();
}

async function moveCommentsToPost(env, siteId, fromPostSlug, toPostSlug) {
  await ensureCommentsTable(env);
  await env.DB.prepare(
    `UPDATE comments
     SET post_slug = ?
     WHERE site_id = ? AND post_slug = ?`
  )
    .bind(toPostSlug, siteId, fromPostSlug)
    .run();
}

async function deleteReactionsByPost(env, siteId, postSlug) {
  await ensureReactionsTable(env);
  await env.DB.prepare(
    "DELETE FROM reactions WHERE site_id = ? AND post_slug = ?"
  )
    .bind(siteId, postSlug)
    .run();
}

async function moveReactionsToPost(env, siteId, fromPostSlug, toPostSlug) {
  await ensureReactionsTable(env);
  await env.DB.prepare(
    `UPDATE reactions
     SET post_slug = ?
     WHERE site_id = ? AND post_slug = ?`
  )
    .bind(toPostSlug, siteId, fromPostSlug)
    .run();
}

function getGithubConfig(env) {
  const owner = String(env.GITHUB_OWNER || "").trim();
  const repo = String(env.GITHUB_REPO || "").trim();
  const branch = String(env.GITHUB_BRANCH || "main").trim();
  const token = String(env.GITHUB_TOKEN || "").trim();

  if (!owner || !repo || !token) {
    throw new Error("Missing GitHub config: GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN");
  }

  return { owner, repo, branch, token };
}

async function githubRequest(env, path, init = {}) {
  const config = getGithubConfig(env);
  const url = `https://api.github.com/repos/${encodeURIComponent(config.owner)}/${encodeURIComponent(config.repo)}${path}`;

  const response = await fetch(url, {
    method: init.method || "GET",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${config.token}`,
      "User-Agent": "stublogs-worker",
      ...(init.headers || {}),
    },
    body: init.body,
  });

  return response;
}

async function githubReadFile(env, filePath) {
  const config = getGithubConfig(env);
  const encodedPath = encodeGitHubPath(filePath);
  const response = await githubRequest(
    env,
    `/contents/${encodedPath}?ref=${encodeURIComponent(config.branch)}`
  );

  if (response.status === 404) {
    return null;
  }

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`GitHub read failed: ${response.status} ${detail}`);
  }

  const data = await response.json();
  if (!data || Array.isArray(data) || typeof data.content !== "string") {
    return null;
  }

  return {
    sha: data.sha,
    content: fromBase64Utf8(data.content),
  };
}

async function githubWriteFile(env, filePath, content, message) {
  const config = getGithubConfig(env);
  const existing = await githubReadFile(env, filePath);
  const encodedPath = encodeGitHubPath(filePath);

  const payload = {
    message,
    branch: config.branch,
    content: toBase64Utf8(content),
  };

  if (existing && existing.sha) {
    payload.sha = existing.sha;
  }

  const response = await githubRequest(env, `/contents/${encodedPath}`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const detail = await response.text();
    if (response.status === 409) {
      const conflictError = new Error("GitHub write conflict: resource changed by another editor");
      conflictError.status = 409;
      conflictError.userMessage = "文章已被其他人修改，請重新載入後再儲存。";
      conflictError.detail = detail;
      throw conflictError;
    }
    throw new Error(`GitHub write failed: ${response.status} ${detail}`);
  }

  return response.json();
}

async function githubDeleteFile(env, filePath, message) {
  const config = getGithubConfig(env);
  const existing = await githubReadFile(env, filePath);
  if (!existing || !existing.sha) {
    return { deleted: false };
  }
  const encodedPath = encodeGitHubPath(filePath);
  const response = await githubRequest(env, `/contents/${encodedPath}`, {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      message,
      branch: config.branch,
      sha: existing.sha,
    }),
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`GitHub delete failed: ${response.status} ${detail}`);
  }

  return response.json();
}

function encodeGitHubPath(path) {
  return String(path)
    .split("/")
    .filter(Boolean)
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function getSiteConfigPath(siteSlug) {
  return `sites/${siteSlug}/site.json`;
}

function getPostFilePath(siteSlug, postSlug) {
  return `sites/${siteSlug}/posts/${postSlug}.md`;
}

function defaultSiteConfigFromSite(site) {
  return {
    slug: site.slug,
    displayName: site.displayName || site.slug,
    description: site.description || "",
    heroTitle: "",
    heroSubtitle: "",
    colorTheme: "default",
    footerNote: "",
    customCss: "",
    faviconUrl: DEFAULT_FAVICON_URL,
    headerLinks: [],
    hideCommunitySites: false,
    hideCampusFeed: false,
    commentsEnabled: true,
    createdAt: site.createdAt || new Date().toISOString(),
    updatedAt: site.updatedAt || new Date().toISOString(),
    exportVersion: SITE_CONFIG_VERSION,
  };
}

function sanitizeHexColor(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();

  if (/^#[0-9a-f]{6}$/.test(raw)) {
    return raw;
  }

  if (/^#[0-9a-f]{3}$/.test(raw)) {
    return `#${raw[1]}${raw[1]}${raw[2]}${raw[2]}${raw[3]}${raw[3]}`;
  }

  return "#7b5034";
}

function sanitizeColorTheme(value) {
  const theme = String(value || "")
    .trim()
    .toLowerCase();
  if (
    theme === "ocean" ||
    theme === "forest" ||
    theme === "violet" ||
    theme === "sunset" ||
    theme === "mint" ||
    theme === "graphite"
  ) {
    return theme;
  }
  return "default";
}

function sanitizeHeaderLinks(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .slice(0, 8)
    .map((item) => ({
      label: sanitizeName(item?.label || "").slice(0, 24),
      url: sanitizeUrl(item?.url || ""),
    }))
    .filter((item) => item.label && item.url);
}

function sanitizeUrl(value) {
  const url = String(value || "").trim();
  if (!url) {
    return "";
  }

  if (/^https?:\/\//i.test(url)) {
    return url.slice(0, 240);
  }

  return "";
}

function sanitizeFaviconUrl(value) {
  const url = String(value || "").trim();
  if (!url) {
    return "";
  }
  if (/^https?:\/\//i.test(url)) {
    return url.slice(0, 500);
  }
  return "";
}

function normalizeSiteConfig(rawConfig, site) {
  const base = site
    ? defaultSiteConfigFromSite(site)
    : {
      slug: "",
      displayName: "",
      description: "",
      heroTitle: "",
      heroSubtitle: "",
      colorTheme: "default",
      footerNote: "",
      customCss: "",
      faviconUrl: DEFAULT_FAVICON_URL,
      headerLinks: [],
      hideCommunitySites: false,
      hideCampusFeed: false,
      commentsEnabled: true,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      exportVersion: SITE_CONFIG_VERSION,
    };

  const merged = {
    ...base,
    ...(rawConfig && typeof rawConfig === "object" ? rawConfig : {}),
  };
  const normalizedFooterNote = sanitizeDescription(merged.footerNote || base.footerNote);

  return {
    slug: String(merged.slug || base.slug).toLowerCase(),
    displayName: sanitizeName(merged.displayName || base.displayName) || base.slug,
    description: sanitizeDescription(merged.description || ""),
    heroTitle: sanitizeTitle(merged.heroTitle || ""),
    heroSubtitle: sanitizeDescription(merged.heroSubtitle || ""),
    colorTheme: sanitizeColorTheme(merged.colorTheme || base.colorTheme || "default"),
    footerNote: normalizedFooterNote === LEGACY_FOOTER_NOTE ? "" : normalizedFooterNote,
    customCss: sanitizeCustomCss(merged.customCss || base.customCss || ""),
    faviconUrl: sanitizeFaviconUrl(merged.faviconUrl || base.faviconUrl || DEFAULT_FAVICON_URL),
    headerLinks: sanitizeHeaderLinks(Array.isArray(merged.headerLinks) ? merged.headerLinks : []),
    hideCommunitySites: Boolean(merged.hideCommunitySites),
    hideCampusFeed: Boolean(merged.hideCampusFeed),
    commentsEnabled: merged.commentsEnabled === undefined ? true : Boolean(merged.commentsEnabled),
    createdAt: String(merged.createdAt || base.createdAt),
    updatedAt: String(merged.updatedAt || new Date().toISOString()),
    exportVersion: SITE_CONFIG_VERSION,
  };
}

async function getSiteConfig(env, site) {
  const fallback = normalizeSiteConfig({}, site);
  const filePath = getSiteConfigPath(site.slug);

  try {
    const file = await githubReadFile(env, filePath);
    if (!file || !file.content) {
      return fallback;
    }

    const parsed = JSON.parse(file.content);
    return normalizeSiteConfig(parsed, site);
  } catch (error) {
    console.error("Failed to load site config", error);
    return fallback;
  }
}

async function notifyTelegramNewSite(env, payload) {
  const botToken = String(env.TELEGRAM_BOT_TOKEN || "").trim();
  const chatId = String(env.TELEGRAM_CHAT_ID || "").trim();

  if (!botToken || !chatId) {
    return;
  }

  const lines = [
    "🆕 新 Blog 註冊",
    `站點：${payload.slug}.bdfz.net`,
    `名稱：${payload.displayName}`,
    `時間：${payload.createdAt}`,
    `後台：${payload.siteUrl}/admin`,
  ];

  const endpoint = `https://api.telegram.org/bot${botToken}/sendMessage`;
  let response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        chat_id: chatId,
        text: lines.join("\n"),
        disable_web_page_preview: true,
      }),
    });
  } catch {
    throw new Error("Telegram notify request failed");
  }

  if (!response.ok) {
    throw new Error(`Telegram notify failed (${response.status})`);
  }
}

async function createPasswordHash(password, env) {
  const salt = randomHex(16);
  const digest = deriveScryptHex(password, salt, env, {
    n: PASSWORD_SCRYPT_N,
    r: PASSWORD_SCRYPT_R,
    p: PASSWORD_SCRYPT_P,
    keyLength: PASSWORD_SCRYPT_KEYLEN,
  });
  return `scrypt$${PASSWORD_SCRYPT_N}$${PASSWORD_SCRYPT_R}$${PASSWORD_SCRYPT_P}$${salt}$${digest}`;
}

async function verifyPassword(password, stored, env) {
  const parsedScrypt = parseScryptPasswordHash(stored);
  if (parsedScrypt) {
    const digest = deriveScryptHex(password, parsedScrypt.salt, env, {
      n: parsedScrypt.n,
      r: parsedScrypt.r,
      p: parsedScrypt.p,
      keyLength: PASSWORD_SCRYPT_KEYLEN,
    });
    return timingSafeEqual(parsedScrypt.digest, digest);
  }

  const [salt, hash] = String(stored || "").split(":");
  if (!salt || !hash) {
    return false;
  }
  const digest = await sha256Hex(`${salt}:${password}:${getSessionSecret(env)}`);
  return timingSafeEqual(hash, digest);
}

function parseScryptPasswordHash(stored) {
  const raw = String(stored || "");
  if (!raw.startsWith("scrypt$")) {
    return null;
  }
  const parts = raw.split("$");
  if (parts.length !== 6) {
    return null;
  }

  const n = Number(parts[1]);
  const r = Number(parts[2]);
  const p = Number(parts[3]);
  const salt = String(parts[4] || "");
  const digest = String(parts[5] || "");
  if (!Number.isFinite(n) || !Number.isFinite(r) || !Number.isFinite(p)) {
    return null;
  }
  if (!salt || !/^[a-f0-9]{16,64}$/i.test(salt)) {
    return null;
  }
  if (!digest || !/^[a-f0-9]{32,256}$/i.test(digest)) {
    return null;
  }

  return { n, r, p, salt, digest: digest.toLowerCase() };
}

function isLegacyPasswordHash(stored) {
  return !String(stored || "").startsWith("scrypt$");
}

function deriveScryptHex(password, salt, env, options) {
  const keyLength = Math.max(Number(options?.keyLength || PASSWORD_SCRYPT_KEYLEN), 16);
  const n = Math.max(Number(options?.n || PASSWORD_SCRYPT_N), 2);
  const r = Math.max(Number(options?.r || PASSWORD_SCRYPT_R), 1);
  const p = Math.max(Number(options?.p || PASSWORD_SCRYPT_P), 1);

  const material = `${String(password || "")}:${getSessionSecret(env)}`;
  const derived = scryptSync(material, salt, keyLength, {
    N: n,
    r,
    p,
    maxmem: 128 * 1024 * 1024,
  });
  return derived.toString("hex");
}

async function createSessionToken(slug, env) {
  const payload = {
    slug,
    exp: Date.now() + SESSION_TTL_SECONDS * 1000,
  };

  const payloadEncoded = toBase64Url(JSON.stringify(payload));
  const signature = await hmacHex(payloadEncoded, getSessionSecret(env));
  return `${payloadEncoded}.${signature}`;
}

async function verifySessionToken(token, env) {
  const raw = String(token || "");
  if (!raw.includes(".")) {
    return null;
  }

  const [payloadEncoded, signature] = raw.split(".");
  if (!payloadEncoded || !signature) {
    return null;
  }

  const expected = await hmacHex(payloadEncoded, getSessionSecret(env));
  if (!timingSafeEqual(signature, expected)) {
    return null;
  }

  let payload;
  try {
    payload = JSON.parse(fromBase64Url(payloadEncoded));
  } catch {
    return null;
  }

  if (!payload || typeof payload.slug !== "string" || typeof payload.exp !== "number") {
    return null;
  }

  if (Date.now() > payload.exp) {
    return null;
  }

  return payload;
}

async function isSiteAuthenticated(request, env, slug) {
  const cookies = parseCookies(request.headers.get("cookie") || "");
  const token = cookies[SESSION_COOKIE];
  if (!token) {
    return false;
  }

  let session;
  try {
    session = await verifySessionToken(token, env);
  } catch (error) {
    console.error("Session verification failed", error);
    return false;
  }

  if (!session) {
    return false;
  }

  return session.slug === slug;
}

function buildSessionCookie(token) {
  return `${SESSION_COOKIE}=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_TTL_SECONDS}`;
}

function buildClearSessionCookie() {
  return `${SESSION_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0`;
}

function withCookie(response, cookieValue) {
  const headers = new Headers(response.headers);
  headers.append("Set-Cookie", cookieValue);
  return new Response(response.body, {
    status: response.status,
    headers,
  });
}

function parseCookies(cookieHeader) {
  const entries = String(cookieHeader || "")
    .split(";")
    .map((value) => value.trim())
    .filter(Boolean)
    .map((pair) => {
      const [name, ...rest] = pair.split("=");
      return [name, rest.join("=")];
    });

  return Object.fromEntries(entries);
}

function getInviteCodes(env) {
  return new Set(
    String(env.INVITE_CODES || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

function getAllowedCorsOrigins(env) {
  return new Set(
    String(env.CORS_ALLOWED_ORIGINS || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

function resolveCorsOrigin(request, env) {
  const origin = String(request.headers.get("origin") || "").trim();
  if (!origin) {
    return null;
  }

  const allowedOrigins = getAllowedCorsOrigins(env);
  if (!allowedOrigins.size) {
    return null;
  }

  if (allowedOrigins.has("*")) {
    return "*";
  }

  return allowedOrigins.has(origin) ? origin : null;
}

function withCors(response, request, env) {
  const allowedOrigin = resolveCorsOrigin(request, env);
  if (!allowedOrigin) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", allowedOrigin);
  headers.set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type");
  headers.set("Vary", "Origin");

  return new Response(response.body, {
    status: response.status,
    headers,
  });
}

function buildApiPreflightResponse(request, env) {
  const allowedOrigin = resolveCorsOrigin(request, env);
  if (!allowedOrigin) {
    return new Response("Forbidden origin", { status: 403 });
  }

  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": allowedOrigin,
      "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Max-Age": "86400",
      Vary: "Origin",
    },
  });
}

function getSessionSecret(env) {
  const secret = String(env.SESSION_SECRET || "").trim();
  if (!secret) {
    throw new Error("Missing SESSION_SECRET");
  }
  return secret;
}

function randomHex(byteLength) {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

function bytesToHex(bytes) {
  return Array.from(bytes || [], (value) => Number(value).toString(16).padStart(2, "0")).join("");
}

function hexToBytes(hex) {
  const raw = String(hex || "").trim().toLowerCase();
  if (!raw || raw.length % 2 !== 0 || /[^0-9a-f]/.test(raw)) {
    return null;
  }
  const bytes = new Uint8Array(raw.length / 2);
  for (let index = 0; index < raw.length; index += 2) {
    bytes[index / 2] = Number.parseInt(raw.slice(index, index + 2), 16);
  }
  return bytes;
}

async function getSensitiveCryptoKey(env) {
  const secret = `${getSessionSecret(env)}:site-tele-notify:v1`;
  const secretDigest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(secret)
  );
  return crypto.subtle.importKey(
    "raw",
    secretDigest,
    { name: "AES-GCM" },
    false,
    ["encrypt", "decrypt"]
  );
}

async function encryptSensitiveValue(value, env) {
  const plain = String(value || "");
  if (!plain) {
    return "";
  }
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const key = await getSensitiveCryptoKey(env);
  const encrypted = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv },
    key,
    new TextEncoder().encode(plain)
  );
  return `v1$${bytesToHex(iv)}$${bytesToHex(new Uint8Array(encrypted))}`;
}

async function decryptSensitiveValue(value, env) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const parts = raw.split("$");
  if (parts.length !== 3 || parts[0] !== "v1") {
    return "";
  }
  const iv = hexToBytes(parts[1]);
  const cipherBytes = hexToBytes(parts[2]);
  if (!iv || !cipherBytes) {
    return "";
  }

  const key = await getSensitiveCryptoKey(env);
  const decrypted = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv },
    key,
    cipherBytes
  );
  return new TextDecoder().decode(decrypted);
}

async function sha256Hex(value) {
  const encoded = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  const bytes = new Uint8Array(digest);
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

async function hmacHex(value, secret) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    {
      name: "HMAC",
      hash: "SHA-256",
    },
    false,
    ["sign"]
  );

  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(value)
  );

  const bytes = new Uint8Array(signature);
  return Array.from(bytes, (item) => item.toString(16).padStart(2, "0")).join("");
}

function timingSafeEqual(left, right) {
  const a = String(left || "");
  const b = String(right || "");

  const length = Math.max(a.length, b.length);
  let mismatch = a.length ^ b.length;
  for (let i = 0; i < length; i += 1) {
    mismatch |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }

  return mismatch === 0;
}

function toBase64Utf8(value) {
  const bytes = new TextEncoder().encode(String(value || ""));
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function fromBase64Utf8(base64Value) {
  const normalized = String(base64Value || "").replace(/\n/g, "");
  const binary = atob(normalized);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

function toBase64Url(value) {
  return toBase64Utf8(value).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function fromBase64Url(value) {
  const base64 = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padding = (4 - (base64.length % 4)) % 4;
  return fromBase64Utf8(base64 + "=".repeat(padding));
}

function normalizePath(pathname) {
  const path = String(pathname || "/");
  if (path.length > 1 && path.endsWith("/")) {
    return path.slice(0, -1);
  }
  return path;
}

function parsePositiveInt(value, fallback = 1, min = 1, max = 9999) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed) || parsed < min) {
    return fallback;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
}

function parseCSV(text) {
  const lines = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const result = { headers: [], rows: [] };
  let i = 0;
  const len = lines.length;

  function parseField() {
    if (i >= len || lines[i] === "\n") return "";
    if (lines[i] === '"') {
      i++;
      let val = "";
      while (i < len) {
        if (lines[i] === '"') {
          if (i + 1 < len && lines[i + 1] === '"') {
            val += '"';
            i += 2;
          } else {
            i++;
            break;
          }
        } else {
          val += lines[i];
          i++;
        }
      }
      return val;
    }
    let val = "";
    while (i < len && lines[i] !== "," && lines[i] !== "\n") {
      val += lines[i];
      i++;
    }
    return val;
  }

  function parseRow() {
    const fields = [];
    while (i < len && lines[i] !== "\n") {
      fields.push(parseField());
      if (i < len && lines[i] === ",") i++;
    }
    if (i < len && lines[i] === "\n") i++;
    return fields;
  }

  if (len === 0) return result;

  result.headers = parseRow().map((h) => h.trim().toLowerCase().replace(/\s+/g, "_"));
  if (!result.headers.length) return result;

  while (i < len) {
    if (lines[i] === "\n") { i++; continue; }
    const fields = parseRow();
    if (fields.length === 0 || (fields.length === 1 && !fields[0])) continue;
    const obj = {};
    for (let j = 0; j < result.headers.length; j++) {
      obj[result.headers[j]] = j < fields.length ? fields[j] : "";
    }
    result.rows.push(obj);
  }

  return result;
}

const MAX_BODY_BYTES = 65536;

async function readJson(request) {
  try {
    const contentLength = Number(request.headers.get("content-length") || 0);
    if (contentLength > MAX_BODY_BYTES) {
      throw new Error("Request body too large");
    }
    const text = await request.text();
    if (text.length > MAX_BODY_BYTES) {
      throw new Error("Request body too large");
    }
    return JSON.parse(text);
  } catch (error) {
    if (error && error.message === "Request body too large") {
      throw error;
    }
    return {};
  }
}

function sanitizeName(value) {
  return String(value || "")
    .trim()
    .slice(0, 60);
}

function sanitizeTitle(value) {
  return String(value || "")
    .trim()
    .slice(0, 120);
}

function sanitizeDescription(value) {
  return String(value || "")
    .trim()
    .slice(0, 240);
}

function sanitizeCustomCss(value) {
  return String(value || "")
    .replace(/\u0000/g, "")
    .slice(0, CUSTOM_CSS_MAX_LENGTH);
}

function sanitizeCommentAuthor(value) {
  return sanitizeName(value)
    .replace(/\s+/g, " ")
    .slice(0, 40);
}

function sanitizeCommentContent(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .trim()
    .slice(0, 2000);
}

function sanitizeOptionalSiteSlug(value) {
  const slug = String(value || "").trim().toLowerCase();
  if (!slug) {
    return "";
  }
  if (slug.length < 2 || slug.length > 30) {
    return "";
  }
  if (!/^[a-z0-9-]+$/.test(slug)) {
    return "";
  }
  if (slug.startsWith("-") || slug.endsWith("-") || slug.includes("--")) {
    return "";
  }
  return slug;
}

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...extraHeaders,
    },
  });
}

function text(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
    },
  });
}

function xml(body, status = 200, extraHeaders = {}) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      ...extraHeaders,
    },
  });
}

function html(body, status = 200, extraHeaders = {}) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' https:; font-src https: data:; connect-src 'self'; img-src 'self' data: https:; frame-src https://www.youtube.com https://www.youtube-nocookie.com https://player.bilibili.com https://open.spotify.com https://twitframe.com https://www.instagram.com; media-src https: data:; frame-ancestors 'none'",
      "X-Content-Type-Options": "nosniff",
      "X-Frame-Options": "DENY",
      "Referrer-Policy": "strict-origin-when-cross-origin",
      ...extraHeaders,
    },
  });
}

function escapeXml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function notFound(message = "Not found") {
  return html(renderSimpleMessage("404", message), 404);
}

function renderRootPage(baseDomain) {
  return renderLayout(
    "Stublogs",
    `
    <section class="panel">
      <p class="eyebrow">bdfz.net student blogs</p>
      <h1>所謂語文，無非你寫。</h1>
      <p class="muted">每位學生可選擇自己的 <code>xxx</code>，站點會是 <code>https://xxx.${escapeHtml(baseDomain)}</code>。</p>

      <form id="register-form" class="stack" autocomplete="off">
        <label>子域名 slug</label>
        <input id="slug" name="slug" placeholder="alice" minlength="2" maxlength="30" required />

        <label>顯示名稱</label>
        <input id="displayName" name="displayName" placeholder="Alice" maxlength="60" required />

        <label>管理密碼</label>
        <input id="adminPassword" name="adminPassword" type="password" minlength="8" required />

        <label>註冊邀請碼</label>
        <input id="inviteCode" name="inviteCode" placeholder="請輸入邀請碼" required />

        <label>站點簡介（可選）</label>
        <input id="description" name="description" maxlength="240" placeholder="這裡寫你的簡介" />

        <button type="submit">建立 Blog</button>
      </form>

      <p id="status" class="muted"></p>
    </section>

    <script>
      const statusEl = document.getElementById("status");
      const form = document.getElementById("register-form");
      const slugInput = document.getElementById("slug");
      const nameInput = document.getElementById("displayName");

      let timer = null;

      function setStatus(message, isError = false) {
        statusEl.textContent = message;
        statusEl.style.color = isError ? "var(--danger)" : "var(--muted)";
      }

      async function checkSlug() {
        const slug = slugInput.value.trim().toLowerCase();
        if (!slug) {
          setStatus("");
          return;
        }

        try {
          const response = await fetch('/api/check-slug?slug=' + encodeURIComponent(slug));
          const payload = await response.json();
          if (payload.available) {
            setStatus('可用：' + slug + '.${escapeHtml(baseDomain)}');
          } else {
            setStatus('不可用（' + (payload.reason || 'unknown') + '）', true);
          }
        } catch {
          setStatus('無法檢查 slug，請稍後再試', true);
        }
      }

      slugInput.addEventListener('input', () => {
        const value = slugInput.value
          .toLowerCase()
          .replace(/[^a-z0-9-]/g, '')
          .replace(/--+/g, '-')
          .replace(/^-|-$/g, '')
          .slice(0, 30);
        slugInput.value = value;

        if (!nameInput.value.trim()) {
          nameInput.value = value;
        }

        clearTimeout(timer);
        timer = setTimeout(checkSlug, 220);
      });

      form.addEventListener('submit', async (event) => {
        event.preventDefault();

        const payload = {
          slug: slugInput.value.trim().toLowerCase(),
          displayName: nameInput.value.trim(),
          adminPassword: document.getElementById('adminPassword').value,
          inviteCode: document.getElementById('inviteCode').value.trim(),
          description: document.getElementById('description').value.trim(),
        };

        setStatus('正在建立...');

        try {
          const response = await fetch('/api/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const result = await response.json();

          if (!response.ok) {
            setStatus(result.error || '建立失敗', true);
            return;
          }

          setStatus('建立成功，跳轉中...');
          window.location.href = result.siteUrl + '/admin';
        } catch {
          setStatus('建立失敗，請稍後重試', true);
        }
      });
    </script>
  `
  );
}

function renderClaimPage(slug, baseDomain) {
  return renderLayout(
    `${slug}.${baseDomain}`,
    `
    <section class="panel">
      <p class="eyebrow">claim this subdomain</p>
      <h1>建立 <code>${escapeHtml(slug)}.${escapeHtml(baseDomain)}</code></h1>

      <form id="claim-form" class="stack" autocomplete="off">
        <label>子域名</label>
        <input id="slug" value="${escapeHtml(slug)}" readonly />

        <label>顯示名稱</label>
        <input id="displayName" name="displayName" maxlength="60" placeholder="${escapeHtml(slug)}" required />

        <label>管理密碼</label>
        <input id="adminPassword" type="password" minlength="8" required />

        <label>註冊邀請碼</label>
        <input id="inviteCode" placeholder="請輸入邀請碼" required />

        <label>站點簡介（可選）</label>
        <input id="description" maxlength="240" placeholder="這裡寫你的簡介" />

        <button type="submit">建立並進入後台</button>
      </form>

      <p id="status" class="muted"></p>
    </section>

    <script>
      const form = document.getElementById('claim-form');
      const statusEl = document.getElementById('status');

      function setStatus(message, isError = false) {
        statusEl.textContent = message;
        statusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
      }

      form.addEventListener('submit', async (event) => {
        event.preventDefault();

        const payload = {
          slug: ${JSON.stringify(slug)},
          displayName: document.getElementById('displayName').value.trim() || ${JSON.stringify(slug)},
          adminPassword: document.getElementById('adminPassword').value,
          inviteCode: document.getElementById('inviteCode').value.trim(),
          description: document.getElementById('description').value.trim(),
        };

        setStatus('正在建立...');

        try {
          const response = await fetch('/api/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });

          const result = await response.json();
          if (!response.ok) {
            setStatus(result.error || '建立失敗', true);
            return;
          }

          window.location.href = '/admin';
        } catch {
          setStatus('建立失敗，請稍後再試', true);
        }
      });
    </script>
  `
  );
}

function renderRootAdminHelp(baseDomain) {
  return renderLayout(
    "Admin",
    `
    <section class="panel">
      <p class="eyebrow">admin gateway</p>
      <h1>請從你的子域名登入</h1>
      <p class="muted">管理地址格式：<code>https://xxx.${escapeHtml(baseDomain)}/admin</code></p>
    </section>
  `, 'default'
  );
}

const SITE_PAGE_NAV_PRIORITY = Object.freeze({
  home: 0,
  now: 1,
  projects: 2,
});

function sortSitePagesForNav(sitePages) {
  const pages = Array.isArray(sitePages) ? [...sitePages] : [];
  pages.sort((left, right) => {
    const leftSlug = String(left?.postSlug || "").toLowerCase();
    const rightSlug = String(right?.postSlug || "").toLowerCase();
    const leftPriority = Object.prototype.hasOwnProperty.call(
      SITE_PAGE_NAV_PRIORITY,
      leftSlug
    )
      ? SITE_PAGE_NAV_PRIORITY[leftSlug]
      : 99;
    const rightPriority = Object.prototype.hasOwnProperty.call(
      SITE_PAGE_NAV_PRIORITY,
      rightSlug
    )
      ? SITE_PAGE_NAV_PRIORITY[rightSlug]
      : 99;
    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }
    const leftUpdated = Date.parse(left?.updatedAt || left?.createdAt || "") || 0;
    const rightUpdated = Date.parse(right?.updatedAt || right?.createdAt || "") || 0;
    if (leftUpdated !== rightUpdated) {
      return rightUpdated - leftUpdated;
    }
    return leftSlug.localeCompare(rightSlug);
  });
  return pages;
}

function renderSiteModeNav(sitePages, activeSlug = "blog") {
  const sortedPages = sortSitePagesForNav(sitePages);
  if (!sortedPages.length) {
    return "";
  }

  const normalizedActiveSlug = String(activeSlug || "blog").toLowerCase();
  const pageLinks = sortedPages
    .map((item) => {
      const slug = String(item.postSlug || "").toLowerCase();
      const activeClass = slug === normalizedActiveSlug ? "active" : "";
      return `<a class="${activeClass}" href="/${encodeURIComponent(item.postSlug)}">${escapeHtml(
        item.title
      )}</a>`;
    })
    .join("");
  const blogActiveClass = normalizedActiveSlug === "blog" ? "active" : "";

  return `<nav class="site-nav mode-nav" aria-label="站點導覽">${pageLinks}<a class="${blogActiveClass}" href="/">Blog</a></nav>`;
}

function renderSiteHomePage(
  site,
  siteConfig,
  posts,
  sitePages,
  communitySites,
  campusFeed,
  baseDomain,
  postsPage = null,
  homeViewCount = 0
) {
  const heading = siteConfig.heroTitle || site.displayName;
  const subtitle = siteConfig.heroSubtitle || site.description || "";
  const siteUrl = `https://${site.slug}.${baseDomain}/`;

  const navLinks = (siteConfig.headerLinks || []).length
    ? `<nav class="site-nav">${siteConfig.headerLinks
      .map(
        (item) =>
          `<a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(
            item.label
          )}</a>`
      )
      .join("")}</nav>`
    : "";

  const modeNav = renderSiteModeNav(sitePages, "blog");

  const list = posts.length
    ? posts
      .map(
        (post) => `
          <li class="post-item">
            <a href="/${encodeURIComponent(post.postSlug)}" class="post-link">${escapeHtml(post.title)}</a>
            <p class="muted">${escapeHtml(post.description || "")}</p>
            <small>${escapeHtml(formatDate(post.updatedAt))} · 訪問 ${escapeHtml(formatViewCount(post.viewCount))}</small>
          </li>
        `
      )
      .join("\n")
    : `<li class="post-item muted">還沒有已發佈文章。</li>`;

  const peerSites = communitySites.length
    ? communitySites
      .map(
        (peer) =>
          `<li><a href="${escapeHtml(peer.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(
            peer.displayName
          )}</a><span class="muted"> · ${escapeHtml(peer.slug)}.bdfz.net</span></li>`
      )
      .join("")
    : `<li class="muted">暫時沒有其他同學站點。</li>`;

  const feedItems = campusFeed.length
    ? campusFeed
      .map(
        (entry) =>
          `<li><a href="${escapeHtml(entry.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(
            entry.title
          )}</a><span class="muted"> · ${escapeHtml(entry.siteName)}</span></li>`
      )
      .join("")
    : `<li class="muted">全校文章流暫時為空。</li>`;

  const pagination = postsPage && postsPage.totalPages > 1
    ? `<nav class="pager" aria-label="文章分頁">
        <a class="${postsPage.hasPrev ? "" : "disabled"}" href="${postsPage.hasPrev ? (postsPage.page - 1 <= 1 ? "/" : `/?page=${postsPage.page - 1}`) : "#"}">上一頁</a>
        <span class="muted">第 ${postsPage.page} / ${postsPage.totalPages} 頁</span>
        <a class="${postsPage.hasNext ? "" : "disabled"}" href="${postsPage.hasNext ? `/?page=${postsPage.page + 1}` : "#"}">下一頁</a>
      </nav>`
    : "";

  return renderLayout(
    site.displayName,
    `
    <section class="panel wide site-home-shell">
      <header class="site-header">
        <div>
          <p class="eyebrow">${escapeHtml(site.slug)}.${escapeHtml(baseDomain)}</p>
          <h1>${escapeHtml(heading)}</h1>
          <p class="muted">${escapeHtml(subtitle)}</p>
          <p class="muted">首頁訪問：<span id="home-view-count">${escapeHtml(formatViewCount(homeViewCount))}</span></p>
        </div>
        <div class="row-actions">
          <a class="link-button" href="/admin">Admin</a>
        </div>
      </header>
      ${renderThemeControlDock("front")}
      ${navLinks}
      ${modeNav}

      <div class="community-grid">
        <section>
          <h2>文章</h2>
          <ul class="post-list">
            ${list}
          </ul>
          ${pagination}
        </section>
        ${(!siteConfig.hideCommunitySites || !siteConfig.hideCampusFeed) ? `
        <aside class="community-panel">
          ${!siteConfig.hideCommunitySites ? `<h3>同學新站</h3><ul class="mini-list">${peerSites}</ul>` : ''}
          ${!siteConfig.hideCampusFeed ? `<h3>全校最新文章</h3><ul class="mini-list">${feedItems}</ul>` : ''}
        </aside>
        ` : ''}
      </div>

      ${(siteConfig.footerNote)
      ? `<footer class="site-footer muted">${escapeHtml(siteConfig.footerNote)}</footer>`
      : ''}
    </section>
    <script>
      (function () {
        const viewEl = document.getElementById('home-view-count');
        const dedupeKey = 'stublogs-view:' + location.host + ':home';
        try {
          if (sessionStorage.getItem(dedupeKey)) {
            return;
          }
          sessionStorage.setItem(dedupeKey, '1');
        } catch {
          // ignore
        }
        fetch('/api/view', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ resourceType: 'home', resourceKey: '${HOME_VIEW_KEY}' }),
        })
          .then((response) => (response.ok ? response.json() : null))
          .then((payload) => {
            if (!payload || typeof payload.count !== 'number' || !viewEl) {
              return;
            }
            const next = Math.max(Number(payload.count || 0), 0);
            viewEl.textContent = new Intl.NumberFormat('zh-Hant').format(next);
          })
          .catch(() => {
            // ignore view beacon failures
          });
      })();
    </script>
    `,
    siteConfig.colorTheme || 'default',
    siteConfig.customCss || "",
    siteConfig.faviconUrl || DEFAULT_FAVICON_URL,
    {
      title: site.displayName,
      description: subtitle || site.description || `${site.displayName} 的部落格`,
      type: "website",
      url: siteUrl,
    }
  );
}

function renderSiteRssXml(site, siteConfig, posts, baseDomain) {
  const siteUrl = `https://${site.slug}.${baseDomain}`;
  const safePosts = Array.isArray(posts) ? posts.slice(0, 80) : [];
  const now = new Date().toUTCString();

  const items = safePosts
    .map((post) => {
      const postUrl = `${siteUrl}/${encodeURIComponent(post.postSlug)}`;
      const publishedAt = new Date(post.updatedAt || post.createdAt || Date.now()).toUTCString();
      const description = post.description || "";
      return [
        "<item>",
        `<title>${escapeXml(post.title || post.postSlug)}</title>`,
        `<link>${escapeXml(postUrl)}</link>`,
        `<guid>${escapeXml(postUrl)}</guid>`,
        `<pubDate>${escapeXml(publishedAt)}</pubDate>`,
        `<description>${escapeXml(description)}</description>`,
        "</item>",
      ].join("");
    })
    .join("");

  const channelTitle = siteConfig.heroTitle || site.displayName;
  const channelDescription = siteConfig.heroSubtitle || site.description || `${site.displayName} 的最新文章`;
  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>${escapeXml(channelTitle)}</title>
    <link>${escapeXml(siteUrl)}</link>
    <description>${escapeXml(channelDescription)}</description>
    <language>zh-Hant</language>
    <lastBuildDate>${escapeXml(now)}</lastBuildDate>
    ${items}
  </channel>
</rss>`;
}

function renderSiteSitemapXml(site, posts, pages, baseDomain) {
  const siteUrl = `https://${site.slug}.${baseDomain}`;
  const urls = [
    { loc: `${siteUrl}/`, lastmod: site.updatedAt || site.createdAt || new Date().toISOString() },
  ];

  for (const post of Array.isArray(posts) ? posts : []) {
    urls.push({
      loc: `${siteUrl}/${encodeURIComponent(post.postSlug)}`,
      lastmod: post.updatedAt || post.createdAt || new Date().toISOString(),
    });
  }
  for (const page of Array.isArray(pages) ? pages : []) {
    urls.push({
      loc: `${siteUrl}/${encodeURIComponent(page.postSlug)}`,
      lastmod: page.updatedAt || page.createdAt || new Date().toISOString(),
    });
  }

  const body = urls
    .map((item) => {
      const lastmod = new Date(item.lastmod).toISOString();
      return `<url><loc>${escapeXml(item.loc)}</loc><lastmod>${escapeXml(lastmod)}</lastmod></url>`;
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  ${body}
</urlset>`;
}

export function renderPostPage(
  site,
  siteConfig,
  post,
  articleHtml,
  communitySites,
  sitePages,
  baseDomain,
  options = {}
) {
  const previewMode = Boolean(options.previewMode);
  const commentsEnabled = options.commentsEnabled !== false;
  const comments = Array.isArray(options.comments) ? options.comments : [];
  const commentsPage = Math.max(Number(options.commentsPage) || 1, 1);
  const commentsTotalPages = Math.max(Number(options.commentsTotalPages) || 1, 1);
  const commentBasePath = String(
    options.commentBasePath || `/${encodeURIComponent(post.postSlug)}`
  );
  const commentsTotal = Number(options.commentsTotal || comments.length || 0);
  const postViewCount = Math.max(Number(options.postViewCount || 0), 0);
  const reactionsEnabled = options.reactionsEnabled !== false && !previewMode;
  const reactionSnapshot = buildReactionSnapshot(
    new Map(
      Array.isArray(options.reactionSnapshot?.items)
        ? options.reactionSnapshot.items.map((item) => [
          sanitizeReactionKey(item.key),
          Math.max(Number(item.count || 0), 0),
        ]).filter((entry) => Boolean(entry[0]))
        : []
    ),
    new Set(Array.isArray(options.reactionSnapshot?.selectedKeys) ? options.reactionSnapshot.selectedKeys : [])
  );
  const showCommunityPanel = !siteConfig.hideCommunitySites;
  const canonicalPostUrl = `https://${site.slug}.${baseDomain}/${encodeURIComponent(post.postSlug)}`;
  const modeNav = renderSiteModeNav(
    sitePages,
    Number(post.isPage) === 1 ? post.postSlug : "blog"
  );

  const peerSites = communitySites.length
    ? communitySites
      .map(
        (peer) =>
          `<li><a href="${escapeHtml(peer.url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(
            peer.displayName
          )}</a></li>`
      )
      .join("")
    : `<li class="muted">暫時沒有其他同學站點。</li>`;

  const commentsList = comments.length
    ? comments
      .map((item) => {
        const author = item.authorSiteSlug
          ? `<a href="https://${escapeHtml(item.authorSiteSlug)}.${escapeHtml(baseDomain)}" target="_blank" rel="noreferrer noopener">${escapeHtml(item.authorName)}</a>`
          : escapeHtml(item.authorName);
        return `<li class="comment-item">
          <p class="comment-meta">${author} · ${escapeHtml(formatDate(item.createdAt))}</p>
          <p class="comment-content">${escapeHtml(item.content).replace(/\n/g, "<br />")}</p>
        </li>`;
      })
      .join("")
    : `<li class="comment-item muted">目前還沒有留言。</li>`;

  const commentPager = commentsTotalPages > 1
    ? `<nav class="pager comments-pager" aria-label="留言分頁">
        <a class="${commentsPage > 1 ? "" : "disabled"}" href="${commentsPage > 1 ? `${commentBasePath}?cpage=${commentsPage - 1}#comments` : "#"}">上一頁</a>
        <span class="muted">留言 ${commentsPage} / ${commentsTotalPages}</span>
        <a class="${commentsPage < commentsTotalPages ? "" : "disabled"}" href="${commentsPage < commentsTotalPages ? `${commentBasePath}?cpage=${commentsPage + 1}#comments` : "#"}">下一頁</a>
      </nav>`
    : "";

  const reactionsPanel = reactionsEnabled
    ? `<section id="reactions" class="panel wide reaction-panel" data-post-slug="${escapeHtml(post.postSlug)}">
        <div class="reaction-head">
          <h2>點贊反應</h2>
          <p class="muted">總反應：<span id="reaction-total">${reactionSnapshot.total}</span></p>
        </div>
        <div class="reaction-grid">
          ${reactionSnapshot.items
        .map((item) => `
            <button
              type="button"
              class="reaction-btn ${item.selected ? "active" : ""}"
              data-reaction-key="${escapeHtml(item.key)}"
              aria-pressed="${item.selected ? "true" : "false"}"
              title="${escapeHtml(item.label)}"
            >
              <span class="reaction-icon">${escapeHtml(item.icon)}</span>
              <span class="reaction-label">${escapeHtml(item.label)}</span>
              <span class="reaction-count" data-role="count">${item.count}</span>
            </button>
          `)
        .join("")}
        </div>
        <p id="reaction-status" class="muted"></p>
      </section>`
    : "";

  // Estimate read time (~400 chars/min for Chinese)
  const charCount = articleHtml.replace(/<[^>]+>/g, "").length;
  const readMinutes = Math.max(1, Math.round(charCount / 400));

  return renderLayout(
    `${post.title} - ${site.displayName}`,
    String.raw`
    <div class="reading-progress" id="reading-progress"></div>
    <section class="panel wide article-wrap">
      <article class="article">
        <p class="eyebrow"><a href="/">← ${escapeHtml(site.displayName)}</a> · ${escapeHtml(
      site.slug
    )}.${escapeHtml(baseDomain)} ${Number(post.isPage) === 1 ? '<span class="preview-badge">Page</span>' : ''} ${previewMode ? '<span class="preview-badge">Preview</span>' : ''}</p>
        <h1>${escapeHtml(post.title)}</h1>
        <p class="muted">${escapeHtml(formatDate(post.updatedAt))} <span class="read-time">· ${readMinutes} min read</span> <span class="read-time">· 訪問 <span id="post-view-count">${escapeHtml(formatViewCount(postViewCount))}</span></span></p>
        ${renderThemeControlDock("front")}
        ${modeNav}
        <div class="article-body">${articleHtml}</div>
      </article>
      ${showCommunityPanel ? `
      <aside class="article-side">
        <h3>同學站點</h3>
        <ul class="mini-list">${peerSites}</ul>
      </aside>
      ` : ''}
    </section>
    ${reactionsPanel}
    ${commentsEnabled ? `
    <section id="comments" class="panel wide comment-panel">
      <h2>留言 (${commentsTotal})</h2>
      <ul class="comment-list">${commentsList}</ul>
      ${commentPager}
      <form id="comment-form" class="stack" autocomplete="off">
        <label>名稱</label>
        <input id="comment-author" maxlength="40" required placeholder="你的名字" />
        <label>你的站點 slug（可選）</label>
        <input id="comment-site" maxlength="30" placeholder="alice" />
        <label>留言內容</label>
        <textarea id="comment-content" class="small-textarea" maxlength="2000" required placeholder="寫下你的留言"></textarea>
        <button id="comment-submit" type="submit">送出留言</button>
      </form>
      <p id="comment-status" class="muted"></p>
    </section>
    ` : ""}
    <button class="back-top" id="back-top" aria-label="Back to top">↑</button>
    <script>
      (function() {
        const progress = document.getElementById('reading-progress');
        const backTop = document.getElementById('back-top');
        const postViewEl = document.getElementById('post-view-count');
        const canTrackView = ${previewMode ? "false" : "true"};
        if (canTrackView) {
          const dedupeKey = 'stublogs-view:' + location.host + ':post:' + ${JSON.stringify(post.postSlug)};
          let shouldSend = true;
          try {
            if (sessionStorage.getItem(dedupeKey)) {
              shouldSend = false;
            } else {
              sessionStorage.setItem(dedupeKey, '1');
            }
          } catch {
            // ignore
          }
          if (shouldSend) {
            fetch('/api/view', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ resourceType: 'post', resourceKey: ${JSON.stringify(post.postSlug)} }),
            })
              .then((response) => (response.ok ? response.json() : null))
              .then((payload) => {
                if (!payload || typeof payload.count !== 'number' || !postViewEl) {
                  return;
                }
                const next = Math.max(Number(payload.count || 0), 0);
                postViewEl.textContent = new Intl.NumberFormat('zh-Hant').format(next);
              })
              .catch(() => {
                // ignore view beacon failures
              });
          }
        }
        function onScroll() {
          const scrollTop = window.scrollY;
          const docHeight = document.documentElement.scrollHeight - window.innerHeight;
          if (docHeight > 0 && progress) {
            progress.style.width = Math.min(100, (scrollTop / docHeight) * 100) + '%';
          }
          if (backTop) {
            backTop.classList.toggle('visible', scrollTop > 400);
          }
        }
        window.addEventListener('scroll', onScroll, { passive: true });
        if (backTop) {
          backTop.addEventListener('click', function() {
            window.scrollTo({ top: 0, behavior: 'smooth' });
          });
        }

        const reactionPanel = document.getElementById('reactions');
        if (reactionPanel) {
          const reactionStatusEl = document.getElementById('reaction-status');
          const reactionTotalEl = document.getElementById('reaction-total');
          const reactionButtons = Array.from(
            reactionPanel.querySelectorAll('.reaction-btn[data-reaction-key]')
          );
          let isSubmittingReaction = false;

          function setReactionStatus(message, isError) {
            if (!reactionStatusEl) return;
            reactionStatusEl.textContent = message;
            reactionStatusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
          }

          function applyReactionSnapshot(snapshot) {
            const selected = new Set(Array.isArray(snapshot.selectedKeys) ? snapshot.selectedKeys : []);
            const counts = {};
            const items = Array.isArray(snapshot.reactions) ? snapshot.reactions : [];
            for (const item of items) {
              if (!item || !item.key) continue;
              counts[item.key] = Math.max(Number(item.count || 0), 0);
            }
            let total = Math.max(Number(snapshot.total || 0), 0);
            if (!Number.isFinite(total) || total < 0) {
              total = Object.values(counts).reduce((sum, value) => sum + value, 0);
            }

            reactionButtons.forEach((button) => {
              const key = String(button.getAttribute('data-reaction-key') || '');
              const countEl = button.querySelector('[data-role="count"]');
              if (countEl) {
                countEl.textContent = String(Math.max(Number(counts[key] || 0), 0));
              }
              const active = selected.has(key);
              button.classList.toggle('active', active);
              button.setAttribute('aria-pressed', active ? 'true' : 'false');
            });

            if (reactionTotalEl) {
              reactionTotalEl.textContent = String(total);
            }
          }

          reactionPanel.addEventListener('click', async (event) => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
              return;
            }
            const button = target.closest('.reaction-btn[data-reaction-key]');
            if (!button || isSubmittingReaction) {
              return;
            }
            const reactionKey = String(button.getAttribute('data-reaction-key') || '');
            if (!reactionKey) {
              return;
            }

            isSubmittingReaction = true;
            reactionButtons.forEach((item) => {
              item.disabled = true;
            });
            setReactionStatus('更新反應中...', false);
            try {
              const response = await fetch('/api/reactions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  postSlug: ${JSON.stringify(post.postSlug)},
                  reactionKey,
                }),
              });
              const data = await response.json();
              if (!response.ok) {
                setReactionStatus(data.error || '更新反應失敗', true);
                return;
              }
              applyReactionSnapshot(data);
              setReactionStatus('反應已更新', false);
            } catch (error) {
              setReactionStatus(error.message || '更新反應失敗', true);
            } finally {
              isSubmittingReaction = false;
              reactionButtons.forEach((item) => {
                item.disabled = false;
              });
            }
          });
        }

        const form = document.getElementById('comment-form');
        const statusEl = document.getElementById('comment-status');
        if (form && statusEl) {
          const authorInput = document.getElementById('comment-author');
          const siteInput = document.getElementById('comment-site');
          const contentInput = document.getElementById('comment-content');
          const submitBtn = document.getElementById('comment-submit');
          const commentsSection = document.getElementById('comments');
          const commentsListEl = commentsSection ? commentsSection.querySelector('.comment-list') : null;
          const commentsTitleEl = commentsSection ? commentsSection.querySelector('h2') : null;
          const storageKey = 'stublogs-comment-profile:' + location.host;
          let isSubmittingComment = false;

          try {
            const profile = JSON.parse(localStorage.getItem(storageKey) || '{}');
            if (profile.authorName) authorInput.value = profile.authorName;
            if (profile.authorSiteSlug) siteInput.value = profile.authorSiteSlug;
          } catch {}

          function escapeHtml(value) {
            return String(value || '')
              .replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;')
              .replace(/'/g, '&#39;');
          }

          function addCommentToList(comment) {
            if (!commentsListEl || !comment) return;

            const placeholder = commentsListEl.querySelector('.comment-item.muted');
            if (placeholder) {
              placeholder.remove();
            }

            const author = comment.authorSiteSlug
              ? comment.authorName + ' · ' + comment.authorSiteSlug + '.bdfz.net'
              : comment.authorName;
            const createdAt = comment.createdAt
              ? new Date(comment.createdAt).toLocaleString('zh-Hant')
              : new Date().toLocaleString('zh-Hant');
            const safeContent = escapeHtml(comment.content || '').split('\n').join('<br />');

            const item = document.createElement('li');
            item.className = 'comment-item';
            item.innerHTML =
              '<p class="comment-meta">' + escapeHtml(author) + ' · ' + escapeHtml(createdAt) + '</p>' +
              '<p class="comment-content">' + safeContent + '</p>';

            commentsListEl.prepend(item);
          }

          function bumpCommentCount() {
            if (!commentsTitleEl) return;
            const text = commentsTitleEl.textContent || '';
            const match = text.match(/\((\d+)\)/);
            const current = match ? Number(match[1]) : 0;
            commentsTitleEl.textContent = '留言 (' + (current + 1) + ')';
          }

          function setCommentStatus(message, isError) {
            statusEl.textContent = message;
            statusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
          }

          form.addEventListener('submit', async (event) => {
            event.preventDefault();
            if (isSubmittingComment) {
              return;
            }
            const payload = {
              postSlug: ${JSON.stringify(post.postSlug)},
              authorName: authorInput.value.trim(),
              authorSiteSlug: siteInput.value.trim().toLowerCase(),
              content: contentInput.value.trim(),
            };
            if (!payload.authorName || !payload.content) {
              setCommentStatus('請填寫名稱與留言內容', true);
              return;
            }

            isSubmittingComment = true;
            submitBtn.disabled = true;
            setCommentStatus('送出中...', false);
            try {
              const response = await fetch('/api/comments', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
              });
              const data = await response.json();
              if (!response.ok) {
                setCommentStatus(data.error || '留言送出失敗', true);
                return;
              }
              try {
                localStorage.setItem(storageKey, JSON.stringify({
                  authorName: payload.authorName,
                  authorSiteSlug: payload.authorSiteSlug,
                }));
              } catch {}

              const createdComment = data && data.comment
                ? data.comment
                : {
                  authorName: payload.authorName,
                  authorSiteSlug: payload.authorSiteSlug,
                  content: payload.content,
                  createdAt: new Date().toISOString(),
                };
              addCommentToList(createdComment);
              bumpCommentCount();
              contentInput.value = '';
              contentInput.focus();
              setCommentStatus('留言成功，已更新列表。', false);
            } catch (error) {
              setCommentStatus(error.message || '留言送出失敗', true);
            } finally {
              isSubmittingComment = false;
              submitBtn.disabled = false;
            }
          });
        }
      })();
    </script>
    `,
    siteConfig.colorTheme || "default",
    siteConfig.customCss || "",
    siteConfig.faviconUrl || DEFAULT_FAVICON_URL,
    {
      title: post.title,
      description: post.description || site.description || `${site.displayName} 的文章`,
      type: "article",
      url: canonicalPostUrl,
    }
  );
}

export function renderAdminPage(site, siteConfig, authed, baseDomain) {
  if (!authed) {
    return renderLayout(
      `${site.displayName} Admin`,
      `
      <section class="panel">
        <p class="eyebrow">admin</p>
        <h1>${escapeHtml(site.displayName)}</h1>

        <form id="login-form" class="stack" autocomplete="off">
          <label>管理密碼</label>
          <input id="password" type="password" minlength="8" required />
          <button type="submit">登入</button>
        </form>

        <p id="status" class="muted"></p>
      </section>

      <script>
        const form = document.getElementById('login-form');
        const statusEl = document.getElementById('status');

        function setStatus(message, isError = false) {
          statusEl.textContent = message;
          statusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
        }

        form.addEventListener('submit', async (event) => {
          event.preventDefault();
          setStatus('登入中...');

          try {
            const response = await fetch('/api/login', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                slug: ${JSON.stringify(site.slug)},
                password: document.getElementById('password').value,
              }),
            });

            const result = await response.json();
            if (!response.ok) {
              setStatus(result.error || '登入失敗', true);
              return;
            }

            location.reload();
          } catch {
            setStatus('登入失敗，請稍後再試', true);
          }
        });
      </script>
    `,
      siteConfig.colorTheme || 'default',
      "",
      siteConfig.faviconUrl || DEFAULT_FAVICON_URL
    );
  }

  return renderLayout(
    `${site.displayName} Admin`,
    String.raw`
    <section class="panel wide admin-shell">
      <header class="site-header">
        <div>
          <p class="eyebrow">editor</p>
          <h1>${escapeHtml(siteConfig.heroTitle || site.displayName)}</h1>
          <p class="muted">${escapeHtml(site.slug)}.${escapeHtml(baseDomain)}</p>
        </div>
        <div class="row-actions">
          <a class="link-button" href="/" target="_blank" rel="noreferrer noopener">Frontend</a>
          <button id="new-post" class="link-button" type="button">New</button>
          <button id="logout" class="link-button" type="button">Logout</button>
          <a class="link-button" href="/api/export">Export</a>
          <a class="link-button" href="https://blog.bdfz.net/" target="_blank" rel="noreferrer noopener">Project</a>
        </div>
      </header>

      <nav class="admin-tabs">
        <button id="tab-posts" class="admin-tab active" type="button">✏️ Posts</button>
        <button id="tab-notifications" class="admin-tab" type="button">🔔 通知 <span id="notify-tab-badge" class="tab-badge">0</span></button>
        <button id="tab-settings" class="admin-tab" type="button">⚙️ Settings</button>
      </nav>

      <!-- ═══ POSTS TAB ═══ -->
      <div id="panel-posts" class="admin-panel">
        <div class="admin-grid">
          <aside class="admin-list">
            <p class="muted">My Posts</p>
            <input id="post-filter" placeholder="搜尋標題或 slug..." />
            <p id="post-count" class="muted">0 篇</p>
            <ul id="post-list"></ul>
          </aside>
          <section class="admin-editor">
            <label>Title</label>
            <input id="title" maxlength="120" />
            <label>Post slug</label>
            <input id="postSlug" maxlength="80" />
            <label>Description</label>
            <input id="description" maxlength="240" />
            <label class="inline-check">
              <input id="published" type="checkbox" />
              Published
            </label>
            <label class="inline-check">
              <input id="isPage" type="checkbox" />
              Page（顯示於頁面導航，不進文章流）
            </label>
            <label>Content</label>
            <div class="md-toolbar">
              <button type="button" data-md="h1" title="Heading 1">H1</button>
              <button type="button" data-md="bold" title="Bold">B</button>
              <button type="button" data-md="italic" title="Italic">I</button>
              <button type="button" data-md="code" title="Code">&#96;</button>
              <button type="button" data-md="heading" title="Heading">H2</button>
              <button type="button" data-md="h3" title="Heading 3">H3</button>
              <button type="button" data-md="link" title="Link">🔗</button>
              <button type="button" data-md="image" title="Image">🖼</button>
              <button type="button" data-md="youtube" title="YouTube">▶︎YT</button>
              <button type="button" data-md="bilibili" title="Bilibili">▶︎B站</button>
              <button type="button" data-md="spotify" title="Spotify">♫SP</button>
              <button type="button" data-md="x" title="X / Twitter">𝕏</button>
              <button type="button" data-md="instagram" title="Instagram">◎INS</button>
              <button type="button" data-md="list" title="List">•</button>
              <button type="button" data-md="task" title="Task list">☑︎</button>
              <button type="button" data-md="ordered" title="Ordered list">1.</button>
              <button type="button" data-md="quote" title="Quote">❝</button>
              <button type="button" data-md="table" title="Table">▦</button>
              <button type="button" data-md="math-inline" title="Inline KaTeX">∑</button>
              <button type="button" data-md="math-block" title="Block KaTeX">$$</button>
              <button type="button" data-md="br" title="Line break">↵</button>
              <button type="button" data-md="codeblock" title="Code block">{ }</button>
              <button type="button" data-md="hr" title="Divider">—</button>
              <button type="button" id="fullscreen-toggle" class="fullscreen-btn">⛶ 全屏</button>
            </div>
            <div class="template-row">
              <select id="content-template" aria-label="內容範本">
                <option value="">選擇內容範本（Bear 風格）</option>
                <option value="daily-note">每日筆記</option>
                <option value="reading-note">閱讀筆記</option>
                <option value="essay-outline">文章大綱</option>
                <option value="project-log">專案日誌</option>
                <option value="math-note">數學筆記（KaTeX）</option>
                <option value="about-page">關於頁面</option>
                <option value="links-page">連結頁面</option>
              </select>
              <button id="apply-template" type="button" class="link-button">插入範本</button>
            </div>
            <textarea id="content" placeholder="# Start writing..."></textarea>
            <div class="row-actions">
              <button id="save" type="button">發佈 / 更新</button>
              <a id="preview" class="link-button" href="#" target="_blank" rel="noreferrer noopener">預覽</a>
              <button id="delete-post" type="button" class="link-button danger-ghost">刪除文章</button>
            </div>
            <p id="editor-status" class="muted"></p>
            <section class="comment-admin-panel">
              <h3>留言管理（目前文章）</h3>
              <ul id="comment-admin-list" class="comment-list compact"></ul>
              <p id="comment-admin-status" class="muted"></p>
            </section>
          </section>
        </div>
      </div>

      <!-- ═══ SETTINGS TAB ═══ -->
      <div id="panel-settings" class="admin-panel" style="display:none">
        <div class="settings-grid">
          <section class="settings-form">
            <h2>站點設定</h2>
            <label>顯示名稱</label>
            <input id="siteDisplayName" maxlength="60" />
            <label>站點簡介</label>
            <input id="siteDescription" maxlength="240" />
            <label>首頁標題</label>
            <input id="siteHeroTitle" maxlength="120" />
            <label>首頁副標</label>
            <input id="siteHeroSubtitle" maxlength="240" />
            <label>主題色系</label>
            <select id="siteColorTheme" style="font:inherit;border-radius:10px;border:1px solid var(--line);background:rgba(255,255,255,.65);padding:.65rem .78rem;color:var(--ink);font-family:var(--font-mono);font-size:.92rem;">
              <option value="default">預設 / 棕色 (Brown)</option>
              <option value="ocean">大海 / 湖藍 (Ocean)</option>
              <option value="forest">森林 / 墨綠 (Forest)</option>
              <option value="violet">紫羅蘭 / 淡紫 (Violet)</option>
              <option value="sunset">晚霞 / 赭紅 (Sunset)</option>
              <option value="mint">薄荷 / 青綠 (Mint)</option>
              <option value="graphite">石墨 / 藍灰 (Graphite)</option>
            </select>
            <label>站點 Favicon URL</label>
            <input id="siteFaviconUrl" maxlength="500" placeholder="https://example.com/favicon.webp" />
            <label>頁尾文字</label>
            <input id="siteFooterNote" maxlength="240" />
            <label>自訂 CSS（前台）</label>
            <textarea id="siteCustomCss" class="small-textarea" maxlength="64000" placeholder="@import url('https://fonts.googleapis.com/css2?family=Noto+Serif+TC:wght@400;700&display=swap');"></textarea>
            <p class="muted">僅作用於你的前台頁面（首頁與文章頁），支援 @import 與字體 CDN。</p>
            <label>外部連結（每行：標題|https://url）</label>
            <textarea id="siteHeaderLinks" class="small-textarea" placeholder="作品集|https://example.com"></textarea>
            <label class="inline-check">
              <input id="siteHideCommunitySites" type="checkbox" />
              隱藏「同學新站」板塊
            </label>
            <label class="inline-check">
              <input id="siteHideCampusFeed" type="checkbox" />
              隱藏「全校最新文章」板塊
            </label>
            <label class="inline-check">
              <input id="siteCommentsEnabled" type="checkbox" />
              啟用文章留言
            </label>
            <h3>通知設定</h3>
            <label class="inline-check">
              <input id="notifyEnabled" type="checkbox" />
              啟用 Telegram 即時通知
            </label>
            <label class="inline-check">
              <input id="notifyComments" type="checkbox" />
              留言通知
            </label>
            <label class="inline-check">
              <input id="notifyReactions" type="checkbox" />
              點贊通知
            </label>
            <label>Telegram Bot Token（選填）</label>
            <input id="notifyTelegramBotToken" type="password" maxlength="200" placeholder="123456789:AA..." />
            <label>Telegram Chat ID（選填）</label>
            <input id="notifyTelegramChatId" maxlength="24" placeholder="5016203472 或 -100xxxxxxxxxx" />
            <label class="inline-check">
              <input id="notifyClearBotToken" type="checkbox" />
              清除已儲存 Bot Token
            </label>
            <p id="notify-settings-hint" class="muted">Bot Token 僅保存在平台資料庫，不會寫入 GitHub 倉庫。</p>
            <button id="save-settings" type="button">儲存站點設定</button>
            <p id="settings-status" class="muted"></p>
            <h3>修改密碼</h3>
            <label>目前密碼</label>
            <input id="currentPassword" type="password" minlength="8" />
            <label>新密碼</label>
            <input id="newPassword" type="password" minlength="8" />
            <label>確認新密碼</label>
            <input id="confirmNewPassword" type="password" minlength="8" />
            <button id="change-password" type="button">更新密碼</button>
            <p id="password-status" class="muted"></p>
          </section>
          <aside class="settings-aside">
            <h3>匯入</h3>
            <p class="muted">從 BearBlog 匯入 CSV</p>
            <input id="import-file" type="file" accept=".csv" />
            <button id="import-btn" type="button">匯入</button>
            <p id="import-status" class="muted"></p>
          </aside>
        </div>
      </div>

      <!-- ═══ NOTIFICATIONS TAB ═══ -->
      <div id="panel-notifications" class="admin-panel" style="display:none">
        <section class="notification-panel">
          <div class="row-actions">
            <button id="refresh-notifications" type="button">重新整理</button>
            <button id="mark-all-notifications-read" type="button" class="link-button">全部標為已讀</button>
          </div>
          <ul id="notification-list" class="notification-list"></ul>
          <p id="notification-status" class="muted"></p>
        </section>
      </div>
    </section>

    <script>
      const initialConfig = ${toScriptJson(siteConfig)};
      const state = {
        currentSlug: '',
        posts: [],
        comments: [],
        notifications: [],
        postFilter: '',
        keepEditorSelection: false,
        siteConfig: initialConfig,
        notifySettings: {
          enabled: false,
          notifyComments: true,
          notifyReactions: true,
          telegramChatId: '',
          hasBotToken: false,
        },
        notificationUnread: 0,
      };

      const postList = document.getElementById('post-list');
      const postFilterInput = document.getElementById('post-filter');
      const postCountEl = document.getElementById('post-count');
      const siteDisplayNameInput = document.getElementById('siteDisplayName');
      const siteDescriptionInput = document.getElementById('siteDescription');
      const siteHeroTitleInput = document.getElementById('siteHeroTitle');
      const siteHeroSubtitleInput = document.getElementById('siteHeroSubtitle');
      const siteColorThemeInput = document.getElementById('siteColorTheme');
      const siteFaviconUrlInput = document.getElementById('siteFaviconUrl');
      const siteFooterNoteInput = document.getElementById('siteFooterNote');
      const siteCustomCssInput = document.getElementById('siteCustomCss');
      const siteHeaderLinksInput = document.getElementById('siteHeaderLinks');
      const siteHideCommunitySitesInput = document.getElementById('siteHideCommunitySites');
      const siteHideCampusFeedInput = document.getElementById('siteHideCampusFeed');
      const siteCommentsEnabledInput = document.getElementById('siteCommentsEnabled');
      const notifyEnabledInput = document.getElementById('notifyEnabled');
      const notifyCommentsInput = document.getElementById('notifyComments');
      const notifyReactionsInput = document.getElementById('notifyReactions');
      const notifyTelegramBotTokenInput = document.getElementById('notifyTelegramBotToken');
      const notifyTelegramChatIdInput = document.getElementById('notifyTelegramChatId');
      const notifyClearBotTokenInput = document.getElementById('notifyClearBotToken');
      const notifySettingsHintEl = document.getElementById('notify-settings-hint');
      const currentPasswordInput = document.getElementById('currentPassword');
      const newPasswordInput = document.getElementById('newPassword');
      const confirmNewPasswordInput = document.getElementById('confirmNewPassword');
      const passwordStatusEl = document.getElementById('password-status');
      const notificationListEl = document.getElementById('notification-list');
      const notificationStatusEl = document.getElementById('notification-status');
      const notificationBadgeEl = document.getElementById('notify-tab-badge');
      const markAllNotificationsReadBtn = document.getElementById('mark-all-notifications-read');
      const refreshNotificationsBtn = document.getElementById('refresh-notifications');
      const titleInput = document.getElementById('title');
      const postSlugInput = document.getElementById('postSlug');
      const descriptionInput = document.getElementById('description');
      const publishedInput = document.getElementById('published');
      const isPageInput = document.getElementById('isPage');
      const contentInput = document.getElementById('content');
      const statusEl = document.getElementById('editor-status');
      const settingsStatusEl = document.getElementById('settings-status');
      const commentAdminListEl = document.getElementById('comment-admin-list');
      const commentAdminStatusEl = document.getElementById('comment-admin-status');
      const previewLink = document.getElementById('preview');
      const deletePostBtn = document.getElementById('delete-post');
      const applyTemplateBtn = document.getElementById('apply-template');
      const contentTemplateSelect = document.getElementById('content-template');
      let savingPost = false;
      let savingSettings = false;
      let importingPosts = false;
      let loadPostToken = 0;
      let baselineState = '';

      function setStatus(message, isError = false) {
        statusEl.textContent = message;
        statusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
      }

      function setSettingsStatus(message, isError = false) {
        if (!settingsStatusEl) {
          return;
        }
        settingsStatusEl.textContent = message;
        settingsStatusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
      }

      function setPasswordStatus(message, isError = false) {
        if (!passwordStatusEl) {
          return;
        }
        passwordStatusEl.textContent = message;
        passwordStatusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
      }

      function setNotificationStatus(message, isError = false) {
        if (!notificationStatusEl) {
          return;
        }
        notificationStatusEl.textContent = message;
        notificationStatusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
      }

      function updateNotificationBadge() {
        if (!notificationBadgeEl) {
          return;
        }
        const unread = Math.max(Number(state.notificationUnread || 0), 0);
        notificationBadgeEl.textContent = String(unread);
        notificationBadgeEl.classList.toggle('has-unread', unread > 0);
      }

      function syncNotifyInputsState() {
        if (!notifyEnabledInput) {
          return;
        }
        const disabled = !notifyEnabledInput.checked;
        if (notifyCommentsInput) notifyCommentsInput.disabled = disabled;
        if (notifyReactionsInput) notifyReactionsInput.disabled = disabled;
        if (notifyTelegramBotTokenInput) notifyTelegramBotTokenInput.disabled = disabled;
        if (notifyTelegramChatIdInput) notifyTelegramChatIdInput.disabled = disabled;
        if (notifyClearBotTokenInput) notifyClearBotTokenInput.disabled = disabled;
      }

      function applyNotifySettingsToForm(settings) {
        const safe = settings || {};
        state.notifySettings = {
          enabled: !!safe.enabled,
          notifyComments: safe.notifyComments !== false,
          notifyReactions: safe.notifyReactions !== false,
          telegramChatId: safe.telegramChatId || '',
          hasBotToken: !!safe.hasBotToken,
        };
        if (notifyEnabledInput) notifyEnabledInput.checked = state.notifySettings.enabled;
        if (notifyCommentsInput) notifyCommentsInput.checked = state.notifySettings.notifyComments;
        if (notifyReactionsInput) notifyReactionsInput.checked = state.notifySettings.notifyReactions;
        if (notifyTelegramChatIdInput) notifyTelegramChatIdInput.value = state.notifySettings.telegramChatId || '';
        if (notifyTelegramBotTokenInput) notifyTelegramBotTokenInput.value = '';
        if (notifyClearBotTokenInput) notifyClearBotTokenInput.checked = false;
        if (notifySettingsHintEl) {
          notifySettingsHintEl.textContent = state.notifySettings.hasBotToken
            ? '已儲存 Telegram Bot Token（留空表示保持不變）。'
            : '尚未儲存 Telegram Bot Token（可選填）。';
        }
        syncNotifyInputsState();
      }

      function renderNotificationList() {
        if (!notificationListEl) {
          return;
        }
        if (!state.notifications.length) {
          notificationListEl.innerHTML = '<li class="muted">目前沒有通知。</li>';
          return;
        }
        notificationListEl.innerHTML = state.notifications
          .map((item) => {
            const safeTitle = escapeText(item.postTitle || item.postSlug || 'untitled');
            const safePath = String(item.targetPath || '/');
            const safeHref = escapeText(safePath);
            const safeContent = escapeText(item.contentPreview || '').replace(/\n/g, '<br />');
            const safeTime = item.createdAt ? escapeText(new Date(item.createdAt).toLocaleString('zh-Hant')) : '';
            const safeActor = item.actorName ? (' · ' + escapeText(item.actorName)) : '';
            const kind = item.eventType === 'reaction'
              ? '點贊'
              : (item.eventType === 'comment' ? '留言' : '通知');
            const reactionLine = item.eventType === 'reaction' && item.reactionLabel
              ? ('<p class="muted">' + escapeText(item.reactionLabel) + '</p>')
              : '';
            const unreadClass = item.read ? '' : ' unread';
            return '<li class="notification-item' + unreadClass + '" data-id="' + item.id + '">' +
              '<div class="notification-head">' +
                '<span class="notification-kind">' + kind + safeActor + '</span>' +
                '<time>' + safeTime + '</time>' +
              '</div>' +
              '<p><a href="' + safeHref + '" target="_blank" rel="noreferrer noopener">' + safeTitle + '</a></p>' +
              (safeContent ? ('<p class="notification-content">' + safeContent + '</p>') : '') +
              reactionLine +
              '<div class="row-actions">' +
                '<button type="button" class="link-button small ghost notification-read-btn" data-id="' + item.id + '">標為已讀</button>' +
              '</div>' +
            '</li>';
          })
          .join('');
      }

      async function refreshNotifySettings() {
        const payload = await fetchJson('/api/admin/notify-settings');
        applyNotifySettingsToForm(payload.settings || {});
      }

      async function refreshNotifications(options = {}) {
        const silent = !!options.silent;
        if (!silent) {
          setNotificationStatus('載入通知中...');
        }
        const payload = await fetchJson('/api/admin/notifications?page=1');
        state.notifications = Array.isArray(payload.notifications) ? payload.notifications : [];
        state.notificationUnread = Math.max(Number(payload.unread || 0), 0);
        renderNotificationList();
        updateNotificationBadge();
        if (!silent) {
          setNotificationStatus('通知已更新');
        }
      }

      async function markNotificationsRead(options = {}) {
        const all = !!options.all;
        const ids = Array.isArray(options.ids) ? options.ids : [];
        const payload = await fetchJson('/api/admin/notifications/read', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ all, ids }),
        });
        state.notificationUnread = Math.max(Number(payload.unread || 0), 0);
        updateNotificationBadge();
      }

      function toSlug(value, withFallback = false) {
        const raw = String(value || '').toLowerCase().trim();
        const slug = raw
          .replace(/[^a-z0-9\s-]/g, '')
          .replace(/\s+/g, '-')
          .replace(/-+/g, '-')
          .replace(/^-|-$/g, '')
          .slice(0, 80);
        if (slug) {
          return slug;
        }
        if (!raw || !withFallback) {
          return '';
        }
        return 'post-' + stableClientHash(raw);
      }

      function stableClientHash(input) {
        let hash = 0x811c9dc5;
        for (let index = 0; index < input.length; index += 1) {
          hash ^= input.charCodeAt(index);
          hash = Math.imul(hash, 0x01000193);
        }
        return (hash >>> 0).toString(36).padStart(6, '0').slice(0, 8);
      }

      function normalizeHexColor(value) {
        const raw = String(value || '').trim().toLowerCase();
      if (/^#[0-9a-f]{6}$/.test(raw)) {
          return raw;
        }
      if (/^#[0-9a-f]{3}$/.test(raw)) {
          return '#' + raw[1] + raw[1] + raw[2] + raw[2] + raw[3] + raw[3];
        }
      return '#7b5034';
      }

      function escapeText(value) {
        return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parseHeaderLinks(raw) {
  return String(raw || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 8)
    .map((line) => {
      const [label, url] = line.split('|').map((item) => item.trim());
      if (!label || !url || !/^https?:\/\//i.test(url)) {
        return null;
      }
      return { label: label.slice(0, 24), url: url.slice(0, 240) };
    })
    .filter(Boolean);
}

function renderHeaderLinksValue(links) {
  return (links || [])
    .map((item) => item.label + '|' + item.url)
    .join('\n');
}

function applySettingsToForm(config) {
  const safe = config || {};
  siteDisplayNameInput.value = safe.displayName || '';
  siteDescriptionInput.value = safe.description || '';
  siteHeroTitleInput.value = safe.heroTitle || '';
  siteHeroSubtitleInput.value = safe.heroSubtitle || '';
  if (siteColorThemeInput) siteColorThemeInput.value = safe.colorTheme || 'default';
  if (siteFaviconUrlInput) siteFaviconUrlInput.value = safe.faviconUrl || '';
  siteFooterNoteInput.value = safe.footerNote || '';
  if (siteCustomCssInput) siteCustomCssInput.value = safe.customCss || '';
  siteHeaderLinksInput.value = renderHeaderLinksValue(safe.headerLinks || []);
  if (siteHideCommunitySitesInput) siteHideCommunitySitesInput.checked = !!safe.hideCommunitySites;
  if (siteHideCampusFeedInput) siteHideCampusFeedInput.checked = !!safe.hideCampusFeed;
  if (siteCommentsEnabledInput) siteCommentsEnabledInput.checked = safe.commentsEnabled !== false;
}

function draftKey(slug) {
  const id = slug || 'new';
  return 'stublogs-draft:' + location.host + ':' + id;
}

function getEditorSnapshot() {
  return JSON.stringify({
    title: titleInput.value,
    postSlug: postSlugInput.value,
    description: descriptionInput.value,
    content: contentInput.value,
    published: publishedInput.checked,
    isPage: isPageInput ? isPageInput.checked : false,
  });
}

function markBaseline() {
  baselineState = getEditorSnapshot();
}

function hasUnsavedChanges() {
  return baselineState && baselineState !== getEditorSnapshot();
}

function saveDraft() {
  const key = draftKey(state.currentSlug);
  const payload = {
    title: titleInput.value,
    postSlug: postSlugInput.value,
    description: descriptionInput.value,
    content: contentInput.value,
    published: publishedInput.checked,
    isPage: isPageInput ? isPageInput.checked : false,
    savedAt: Date.now(),
  };

  try {
    localStorage.setItem(key, JSON.stringify(payload));
  } catch {
    // ignore localStorage quota errors
  }
}

function tryRestoreDraft(slug) {
  const key = draftKey(slug);
  try {
    const raw = localStorage.getItem(key);
    if (!raw) {
      return false;
    }
    const draft = JSON.parse(raw);
    if (!draft || !draft.content) {
      return false;
    }

    if (!contentInput.value.trim()) {
      titleInput.value = draft.title || titleInput.value;
      postSlugInput.value = draft.postSlug || postSlugInput.value;
      descriptionInput.value = draft.description || descriptionInput.value;
      contentInput.value = draft.content || contentInput.value;
      publishedInput.checked = Boolean(draft.published);
      if (isPageInput) isPageInput.checked = Boolean(draft.isPage);
      if (typeof updateSaveBtn === 'function') updateSaveBtn();
      syncPreview();
      return true;
    }
    return false;
  } catch {
    // ignore malformed drafts
    return false;
  }
}

function syncPreview() {
  const slug = postSlugInput.value.trim().toLowerCase();
  if (previewLink) {
    const exists = slug && (state.currentSlug === slug || state.posts.some((post) => post.postSlug === slug));
    previewLink.href = exists ? '/preview/' + encodeURIComponent(slug) : '#';
    previewLink.setAttribute('aria-disabled', exists ? 'false' : 'true');
  }
}

function resetEditor(options = {}) {
  const { restoreDraft = true, keepEditorSelection = false } = options;
  state.currentSlug = '';
  state.keepEditorSelection = keepEditorSelection;
  titleInput.value = '';
  postSlugInput.value = '';
  descriptionInput.value = '';
  publishedInput.checked = true;
  if (isPageInput) isPageInput.checked = false;
  contentInput.value = '';
  if (typeof updateSaveBtn === 'function') updateSaveBtn();
  syncPreview();
  state.comments = [];
  renderCommentAdminList();
  setCommentAdminStatus('');
  if (deletePostBtn) deletePostBtn.disabled = true;
  let restored = false;
  if (restoreDraft) {
    restored = tryRestoreDraft('');
  }
  setStatus(restored ? '已恢復未發佈草稿' : 'New post');
  markBaseline();
}

function renderPostList() {
  if (!state.posts.length) {
    postList.innerHTML = '<li class="muted">No posts yet</li>';
    if (postCountEl) postCountEl.textContent = '0 篇';
    return;
  }

  const keyword = String(state.postFilter || '').trim().toLowerCase();
  const filtered = keyword
    ? state.posts.filter((post) => {
      const text = (post.title + ' ' + post.postSlug).toLowerCase();
      return text.includes(keyword);
    })
    : state.posts;

  if (postCountEl) {
    postCountEl.textContent = keyword
      ? ('共 ' + state.posts.length + ' 篇，顯示 ' + filtered.length + ' 篇')
      : (state.posts.length + ' 篇');
  }

  if (!filtered.length) {
    postList.innerHTML = '<li class="muted">沒有符合搜尋的文章</li>';
    return;
  }

  postList.innerHTML = filtered
    .map((post) => {
      const activeClass = post.postSlug === state.currentSlug ? 'active' : '';
      const visibilityLabel = Number(post.published) === 1 ? 'Published' : 'Draft';
      const typeLabel = Number(post.isPage) === 1 ? 'Page' : 'Post';
      const stateLabel = typeLabel + ' · ' + visibilityLabel;
      return '<li><button class="post-item-btn ' + activeClass + '" data-slug="' +
        post.postSlug + '">' +
        escapeText(post.title) +
        ' <small>(' + stateLabel + ')</small></button></li>';
    })
    .join('');

  Array.from(document.querySelectorAll('.post-item-btn')).forEach((button) => {
    button.addEventListener('click', () => {
      const targetSlug = button.getAttribute('data-slug');
      if (!targetSlug || targetSlug === state.currentSlug) {
        return;
      }
      if (hasUnsavedChanges() && !confirm('目前有未儲存內容，確定切換文章？')) {
        return;
      }
      loadPost(targetSlug);
    });
  });
}

function setCommentAdminStatus(message, isError = false) {
  if (!commentAdminStatusEl) {
    return;
  }
  commentAdminStatusEl.textContent = message;
  commentAdminStatusEl.style.color = isError ? 'var(--danger)' : 'var(--muted)';
}

function renderCommentAdminList() {
  if (!commentAdminListEl) {
    return;
  }
  if (!state.currentSlug) {
    commentAdminListEl.innerHTML = '<li class="muted">請先選擇文章。</li>';
    return;
  }
  if (!state.comments.length) {
    commentAdminListEl.innerHTML = '<li class="muted">此文章目前沒有留言。</li>';
    return;
  }

  commentAdminListEl.innerHTML = state.comments
    .map((comment) => {
      const authorSite = comment.authorSiteSlug
        ? '<small class="muted"> · ' + escapeText(comment.authorSiteSlug) + '.bdfz.net</small>'
        : '';
      const createdAt = comment.createdAt
        ? new Date(comment.createdAt).toLocaleString()
        : '';
      return '<li class="comment-item" data-comment-id="' + comment.id + '">' +
        '<p class="comment-meta">' + escapeText(comment.authorName) + authorSite + ' · ' + escapeText(createdAt) + '</p>' +
        '<p class="comment-content">' + escapeText(comment.content || '').replace(/\n/g, '<br />') + '</p>' +
        '<button type="button" class="comment-delete-btn" data-comment-id="' + comment.id + '">刪除留言</button>' +
      '</li>';
    })
    .join('');
}

async function refreshCommentsForCurrentPost() {
  if (!state.currentSlug) {
    state.comments = [];
    renderCommentAdminList();
    return;
  }
  setCommentAdminStatus('載入留言中...');
  try {
    const payload = await fetchJson('/api/admin/comments?postSlug=' + encodeURIComponent(state.currentSlug));
    state.comments = payload.comments || [];
    renderCommentAdminList();
    setCommentAdminStatus('留言載入完成');
  } catch (error) {
    setCommentAdminStatus(error.message || '留言載入失敗', true);
  }
}

async function fetchJson(path, options) {
  const response = await fetch(path, options);
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    if (response.status === 401) {
      throw new Error('登入已過期，請重新登入');
    }
    throw new Error((payload && payload.error) || ('Request failed (' + response.status + ')'));
  }
  if (!payload || typeof payload !== 'object') {
    throw new Error('Invalid server response');
  }
  return payload;
}

async function refreshPosts() {
  const payload = await fetchJson('/api/list-posts?includeDrafts=1');
  state.posts = payload.posts || [];
  if (state.currentSlug && !state.posts.some((post) => post.postSlug === state.currentSlug)) {
    state.currentSlug = '';
    if (deletePostBtn) deletePostBtn.disabled = true;
  }
  renderPostList();
  if (!state.currentSlug && state.posts.length && !state.keepEditorSelection) {
    loadPost(state.posts[0].postSlug).catch((error) => {
      setStatus(error.message || 'Failed to load first post', true);
    });
  } else if (!state.posts.length && deletePostBtn) {
    deletePostBtn.disabled = true;
  }
}

async function refreshSettings() {
  const payload = await fetchJson('/api/site-settings');
  state.siteConfig = payload.config || state.siteConfig;
  applySettingsToForm(state.siteConfig);
}

async function loadPost(slug) {
  if (!slug) {
    return;
  }

  const token = ++loadPostToken;
  try {
    const payload = await fetchJson('/api/posts/' + encodeURIComponent(slug));
    if (token !== loadPostToken) {
      return;
    }
    const post = payload.post;
    state.keepEditorSelection = false;
    state.currentSlug = post.postSlug;
    titleInput.value = post.title || '';
    postSlugInput.value = post.postSlug || '';
    descriptionInput.value = post.description || '';
    publishedInput.checked = Number(post.published) === 1;
    if (isPageInput) isPageInput.checked = Number(post.isPage) === 1;
    contentInput.value = post.content || '';
    if (deletePostBtn) deletePostBtn.disabled = false;
    if (typeof updateSaveBtn === 'function') updateSaveBtn();
    renderPostList();
    tryRestoreDraft(post.postSlug);
    markBaseline();
    await refreshCommentsForCurrentPost();
    setStatus('Loaded ' + post.postSlug);
  } catch (error) {
    if (deletePostBtn) deletePostBtn.disabled = true;
    setStatus(error.message || 'Failed to load post', true);
  }
}

async function savePost() {
  if (savingPost) {
    return;
  }
  const title = titleInput.value.trim();
  const postSlug = (postSlugInput.value.trim() || toSlug(title, true)).toLowerCase();

  if (!title) {
    setStatus('Title is required', true);
    return;
  }

  if (!postSlug) {
    setStatus('Post slug is required', true);
    return;
  }

  setStatus('Saving...');
  savingPost = true;
  if (saveBtn) {
    saveBtn.disabled = true;
    saveBtn.textContent = '儲存中...';
  }

  try {
    const payload = await fetchJson('/api/posts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        postSlug,
        previousSlug: state.currentSlug || null,
        description: descriptionInput.value.trim(),
        content: contentInput.value,
        published: publishedInput.checked,
        isPage: isPageInput ? isPageInput.checked : false,
      }),
    });

    state.currentSlug = payload.post.postSlug;
    postSlugInput.value = payload.post.postSlug;
    syncPreview();
    await refreshPosts();
    await refreshCommentsForCurrentPost();
    if (publishedInput.checked) {
      if (isPageInput && isPageInput.checked) {
        setStatus('頁面已發佈：' + new Date().toLocaleTimeString());
      } else {
        setStatus('文章已發佈：' + new Date().toLocaleTimeString());
      }
    } else {
      if (isPageInput && isPageInput.checked) {
        setStatus('頁面草稿已儲存（前台不顯示）');
      } else {
        setStatus('文章草稿已儲存（前台不顯示）');
      }
    }
    markBaseline();
    saveDraft();
  } catch (error) {
    setStatus(error.message || 'Save failed', true);
  } finally {
    savingPost = false;
    if (saveBtn) {
      saveBtn.disabled = false;
    }
    updateSaveBtn();
  }
}

async function saveSiteSettings() {
  if (savingSettings) {
    return;
  }
  const notifyTokenValue = notifyTelegramBotTokenInput ? notifyTelegramBotTokenInput.value.trim() : '';
  const notifyChatIdValue = notifyTelegramChatIdInput ? notifyTelegramChatIdInput.value.trim() : '';
  const clearNotifyToken = notifyClearBotTokenInput ? notifyClearBotTokenInput.checked : false;
  const notifyEnabled = notifyEnabledInput ? notifyEnabledInput.checked : false;
  if (notifyTokenValue && !/^\d{6,14}:[A-Za-z0-9_-]{20,140}$/.test(notifyTokenValue)) {
    setSettingsStatus('Telegram Bot Token 格式不正確', true);
    return;
  }
  if (notifyChatIdValue && !/^-?\d{5,20}$/.test(notifyChatIdValue)) {
    setSettingsStatus('Telegram Chat ID 格式不正確', true);
    return;
  }
  const hasTokenAfterSave = Boolean(notifyTokenValue || (state.notifySettings.hasBotToken && !clearNotifyToken));
  if (
    notifyEnabled &&
    (!notifyChatIdValue || !hasTokenAfterSave)
  ) {
    setSettingsStatus('啟用通知時，請填寫 Telegram Chat ID 與 Bot Token', true);
    return;
  }

  savingSettings = true;
  setSettingsStatus('儲存設定中...');
  if (saveSettingsBtn) {
    saveSettingsBtn.disabled = true;
    saveSettingsBtn.textContent = '儲存中...';
  }
  try {
    const payload = await fetchJson('/api/site-settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        displayName: siteDisplayNameInput.value.trim(),
        description: siteDescriptionInput.value.trim(),
        heroTitle: siteHeroTitleInput.value.trim(),
        heroSubtitle: siteHeroSubtitleInput.value.trim(),
        colorTheme: siteColorThemeInput.value,
        faviconUrl: siteFaviconUrlInput ? siteFaviconUrlInput.value.trim() : '',
        footerNote: siteFooterNoteInput.value.trim(),
        customCss: siteCustomCssInput ? siteCustomCssInput.value : '',
        headerLinks: parseHeaderLinks(siteHeaderLinksInput.value),
        hideCommunitySites: siteHideCommunitySitesInput.checked,
        hideCampusFeed: siteHideCampusFeedInput.checked,
        commentsEnabled: siteCommentsEnabledInput.checked,
      }),
    });
    const notifyPayload = await fetchJson('/api/admin/notify-settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: notifyEnabledInput ? notifyEnabledInput.checked : false,
        notifyComments: notifyCommentsInput ? notifyCommentsInput.checked : true,
        notifyReactions: notifyReactionsInput ? notifyReactionsInput.checked : true,
        telegramChatId: notifyChatIdValue,
        telegramBotToken: notifyTokenValue,
        clearBotToken: clearNotifyToken,
      }),
    });

    state.siteConfig = payload.config || state.siteConfig;
    applySettingsToForm(state.siteConfig);
    applyNotifySettingsToForm(notifyPayload.settings || {});
    if (typeof window.__applyThemeDockTheme === 'function') {
      window.__applyThemeDockTheme(state.siteConfig.colorTheme || 'default');
    } else {
      document.body.className = "theme-" + (state.siteConfig.colorTheme || "default");
    }
    setSettingsStatus('站點與通知設定已儲存');
  } catch (error) {
    setSettingsStatus(error.message || '儲存站點設定失敗', true);
  } finally {
    savingSettings = false;
    if (saveSettingsBtn) {
      saveSettingsBtn.disabled = false;
      saveSettingsBtn.textContent = '儲存站點設定';
    }
  }
}

async function changePassword() {
  if (!currentPasswordInput || !newPasswordInput || !confirmNewPasswordInput) {
    return;
  }
  const currentPassword = currentPasswordInput.value;
  const newPassword = newPasswordInput.value;
  const confirmPassword = confirmNewPasswordInput.value;

  if (!currentPassword || !newPassword || !confirmPassword) {
    setPasswordStatus('請完整填寫密碼欄位', true);
    return;
  }
  if (newPassword.length < 8) {
    setPasswordStatus('新密碼至少 8 位', true);
    return;
  }
  if (newPassword !== confirmPassword) {
    setPasswordStatus('兩次新密碼不一致', true);
    return;
  }

  setPasswordStatus('更新密碼中...');
  const changePasswordBtn = document.getElementById('change-password');
  if (changePasswordBtn) {
    changePasswordBtn.disabled = true;
  }

  try {
    await fetchJson('/api/change-password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        currentPassword,
        newPassword,
      }),
    });
    currentPasswordInput.value = '';
    newPasswordInput.value = '';
    confirmNewPasswordInput.value = '';
    setPasswordStatus('密碼更新完成');
  } catch (error) {
    setPasswordStatus(error.message || '更新密碼失敗', true);
  } finally {
    if (changePasswordBtn) {
      changePasswordBtn.disabled = false;
    }
  }
}

document.getElementById('new-post').addEventListener('click', () => {
  if (hasUnsavedChanges() && !confirm('目前有未儲存內容，確定建立新文章？')) {
    return;
  }
  resetEditor({ restoreDraft: false, keepEditorSelection: true });
});
document.getElementById('save').addEventListener('click', savePost);
document.getElementById('save-settings').addEventListener('click', saveSiteSettings);
const changePasswordBtn = document.getElementById('change-password');
if (changePasswordBtn) {
  changePasswordBtn.addEventListener('click', changePassword);
}
document.getElementById('logout').addEventListener('click', async () => {
  await fetch('/api/logout', { method: 'POST' });
  location.reload();
});

if (commentAdminListEl) {
  commentAdminListEl.addEventListener('click', async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const commentId = target.getAttribute('data-comment-id');
    if (!commentId || !target.classList.contains('comment-delete-btn')) {
      return;
    }
    if (!confirm('確認刪除此留言？')) {
      return;
    }
    setCommentAdminStatus('刪除留言中...');
    try {
      await fetchJson('/api/comments/' + encodeURIComponent(commentId), { method: 'DELETE' });
      await refreshCommentsForCurrentPost();
      setCommentAdminStatus('留言已刪除');
    } catch (error) {
      setCommentAdminStatus(error.message || '刪除留言失敗', true);
    }
  });
}

if (deletePostBtn) {
  deletePostBtn.disabled = true;
  deletePostBtn.addEventListener('click', async () => {
    if (!state.currentSlug) {
      setStatus('請先選擇文章', true);
      return;
    }
    const slugToDelete = state.currentSlug;
    const ok = confirm('確認刪除文章 ' + slugToDelete + '？此操作不可復原。');
    if (!ok) {
      return;
    }

    deletePostBtn.disabled = true;
    setStatus('刪除中...');
    try {
      await fetchJson('/api/posts/' + encodeURIComponent(slugToDelete), { method: 'DELETE' });
      const idx = state.posts.findIndex((post) => post.postSlug === slugToDelete);
      state.posts = state.posts.filter((post) => post.postSlug !== slugToDelete);
      renderPostList();
      setStatus('已刪除：' + slugToDelete);
      state.currentSlug = '';
      state.comments = [];
      renderCommentAdminList();

      if (state.posts.length) {
        const next = state.posts[Math.max(0, idx - 1)] || state.posts[0];
        if (next && next.postSlug) {
          await loadPost(next.postSlug);
        }
      } else {
        resetEditor();
      }
    } catch (error) {
      setStatus(error.message || '刪除失敗', true);
      deletePostBtn.disabled = false;
    }
  });
}

  titleInput.addEventListener('blur', () => {
  if (!postSlugInput.value.trim()) {
    postSlugInput.value = toSlug(titleInput.value, true);
  }
  syncPreview();
});

postSlugInput.addEventListener('input', () => {
  postSlugInput.value = toSlug(postSlugInput.value);
  syncPreview();
});

if (previewLink) {
  previewLink.addEventListener('click', (event) => {
    if (previewLink.getAttribute('aria-disabled') === 'true') {
      event.preventDefault();
      setStatus('請先儲存文章後再預覽', true);
    }
  });
}

if (postFilterInput) {
  postFilterInput.addEventListener('input', () => {
    state.postFilter = postFilterInput.value;
    renderPostList();
  });
}

contentInput.addEventListener('input', saveDraft);
titleInput.addEventListener('input', saveDraft);
descriptionInput.addEventListener('input', saveDraft);

const saveBtn = document.getElementById('save');
const saveSettingsBtn = document.getElementById('save-settings');
function updateSaveBtn() {
  if (!saveBtn) {
    return;
  }
  const isPage = isPageInput ? isPageInput.checked : false;
  if (publishedInput.checked) {
    saveBtn.textContent = isPage ? '發佈頁面 / 更新 (⌘S)' : '發佈文章 / 更新 (⌘S)';
  } else {
    saveBtn.textContent = isPage ? '儲存頁面草稿 (⌘S)' : '儲存文章草稿 (⌘S)';
  }
}
publishedInput.addEventListener('change', () => {
  saveDraft();
  updateSaveBtn();
});
if (isPageInput) {
  isPageInput.addEventListener('change', () => {
    saveDraft();
    updateSaveBtn();
  });
}
if (notifyEnabledInput) {
  notifyEnabledInput.addEventListener('change', () => {
    syncNotifyInputsState();
  });
}
updateSaveBtn();

document.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
    event.preventDefault();
    if (event.shiftKey) {
      saveSiteSettings();
    } else {
      savePost();
    }
  }
});

window.addEventListener('beforeunload', (event) => {
  if (!hasUnsavedChanges()) {
    return;
  }
  event.preventDefault();
  event.returnValue = '';
});

applySettingsToForm(initialConfig);
syncNotifyInputsState();
resetEditor({ restoreDraft: true, keepEditorSelection: false });
refreshSettings().catch((error) => {
  setSettingsStatus(error.message || 'Failed to load site settings', true);
});
refreshNotifySettings().catch((error) => {
  setSettingsStatus(error.message || 'Failed to load notification settings', true);
});
refreshPosts().catch((error) => {
  setStatus(error.message || 'Failed to load posts', true);
});
refreshNotifications({ silent: true }).catch((error) => {
  setNotificationStatus(error.message || '通知載入失敗', true);
});
setInterval(() => {
  refreshNotifications({ silent: true }).catch(() => {
    // Ignore transient polling failures.
  });
}, 30000);

// ── Tab switching ──
const tabPosts = document.getElementById('tab-posts');
const tabNotifications = document.getElementById('tab-notifications');
const tabSettings = document.getElementById('tab-settings');
const panelPosts = document.getElementById('panel-posts');
const panelNotifications = document.getElementById('panel-notifications');
const panelSettings = document.getElementById('panel-settings');
if (tabPosts && tabNotifications && tabSettings) {
  tabPosts.addEventListener('click', () => {
    tabPosts.classList.add('active');
    tabNotifications.classList.remove('active');
    tabSettings.classList.remove('active');
    panelPosts.style.display = '';
    panelNotifications.style.display = 'none';
    panelSettings.style.display = 'none';
  });
  tabNotifications.addEventListener('click', () => {
    tabNotifications.classList.add('active');
    tabPosts.classList.remove('active');
    tabSettings.classList.remove('active');
    panelNotifications.style.display = '';
    panelPosts.style.display = 'none';
    panelSettings.style.display = 'none';
    refreshNotifications().catch((error) => {
      setNotificationStatus(error.message || '通知載入失敗', true);
    });
  });
  tabSettings.addEventListener('click', () => {
    tabSettings.classList.add('active');
    tabNotifications.classList.remove('active');
    tabPosts.classList.remove('active');
    panelNotifications.style.display = 'none';
    panelSettings.style.display = '';
    panelPosts.style.display = 'none';
  });
}

if (markAllNotificationsReadBtn) {
  markAllNotificationsReadBtn.addEventListener('click', async () => {
    setNotificationStatus('更新已讀狀態中...');
    try {
      await markNotificationsRead({ all: true });
      await refreshNotifications({ silent: true });
      setNotificationStatus('已全部標為已讀');
    } catch (error) {
      setNotificationStatus(error.message || '更新通知失敗', true);
    }
  });
}
if (refreshNotificationsBtn) {
  refreshNotificationsBtn.addEventListener('click', () => {
    refreshNotifications().catch((error) => {
      setNotificationStatus(error.message || '通知載入失敗', true);
    });
  });
}
if (notificationListEl) {
  notificationListEl.addEventListener('click', async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const readButton = target.closest('.notification-read-btn[data-id]');
    if (!readButton) {
      return;
    }
    const id = Number(readButton.getAttribute('data-id'));
    if (!Number.isInteger(id) || id <= 0) {
      return;
    }
    try {
      await markNotificationsRead({ ids: [id] });
      await refreshNotifications({ silent: true });
      setNotificationStatus('通知已標為已讀');
    } catch (error) {
      setNotificationStatus(error.message || '操作失敗', true);
    }
  });
}

// ── Markdown toolbar ──
function insertMd(type) {
  const ta = contentInput;
  const start = ta.selectionStart;
  const end = ta.selectionEnd;
  const sel = ta.value.substring(start, end);
  const tick = String.fromCharCode(96);
  const fence = tick + tick + tick;
  const lines = (sel || '').split('\n');
  let replacement = '';

  const wrapSelection = (before, after, fallback = '') =>
    before + (sel || fallback) + after;
  const mapLines = (transform, fallback = '') =>
    (sel || fallback)
      .split('\n')
      .map((line, index) => transform(line, index))
      .join('\n');

  switch (type) {
    case 'h1':
      replacement = wrapSelection('# ', '', '標題');
      break;
    case 'heading':
      replacement = wrapSelection('## ', '', '標題');
      break;
    case 'h3':
      replacement = wrapSelection('### ', '', '標題');
      break;
    case 'bold':
      replacement = wrapSelection('**', '**', '文字');
      break;
    case 'italic':
      replacement = wrapSelection('*', '*', '文字');
      break;
    case 'code':
      replacement = sel.includes('\n')
        ? '\n' + fence + '\n' + (sel || 'code') + '\n' + fence + '\n'
        : tick + (sel || 'code') + tick;
      break;
    case 'codeblock':
      replacement = '\n' + fence + '\n' + (sel || 'code') + '\n' + fence + '\n';
      break;
    case 'link':
      replacement = '[' + (sel || '連結文字') + '](https://)';
      break;
    case 'image':
      replacement = '![' + (sel || '圖片描述') + '](https://)';
      break;
    case 'youtube':
      replacement = (sel || 'https://www.youtube.com/watch?v=dQw4w9WgXcQ');
      break;
    case 'bilibili':
      replacement = (sel || 'https://www.bilibili.com/video/BV1xx411c7mD/');
      break;
    case 'spotify':
      replacement = (sel || 'https://open.spotify.com/track/11dFghVXANMlKmJXsNCbNl');
      break;
    case 'x':
      replacement = (sel || 'https://x.com/jack/status/20');
      break;
    case 'instagram':
      replacement = (sel || 'https://www.instagram.com/p/CxXn-example/');
      break;
    case 'list':
      replacement = mapLines((line) => '- ' + (line || '列表項'), '列表項');
      break;
    case 'task':
      replacement = mapLines((line) => '- [ ] ' + (line || '待辦事項'), '待辦事項');
      break;
    case 'ordered':
      replacement = mapLines((line, index) => (index + 1) + '. ' + (line || '列表項'), '列表項');
      break;
    case 'quote':
      replacement = mapLines((line) => '> ' + (line || '引用文字'), '引用文字');
      break;
    case 'table':
      replacement = '\n| 欄位1 | 欄位2 |\n| --- | --- |\n| 內容1 | 內容2 |\n';
      break;
    case 'math-inline':
      replacement = '$' + (sel || 'x^2+y^2=z^2') + '$';
      break;
    case 'math-block':
      replacement = '\n$$\n' + (sel || '\\int_0^1 x^2 \\, dx') + '\n$$\n';
      break;
    case 'br':
      replacement = sel
        ? lines.join('  \n')
        : '第一行  \n第二行';
      break;
    case 'hr':
      replacement = '\n---\n';
      break;
    default:
      replacement = sel;
      break;
  }

  ta.setRangeText(replacement, start, end, 'end');
  ta.focus();
  saveDraft();
}

const TEMPLATE_LIBRARY = {
  'daily-note': '# 每日筆記\n\n## 今天做了什麼\n- \n\n## 今天學到什麼\n- \n\n## 明天要做什麼\n- [ ] \n',
  'reading-note': '# 閱讀筆記：書名 / 文章名\n\n## 核心觀點\n- \n\n## 精彩段落\n> \n\n## 我的想法\n- \n\n## 可行動項\n- [ ] \n',
  'essay-outline': '# 文章標題\n\n## 問題背景\n\n## 核心觀點\n\n## 例子與論證\n\n## 反方與回應\n\n## 結論\n',
  'project-log': '# 專案日誌：專案名\n\n## 本次目標\n- \n\n## 進展\n- \n\n## 問題\n- \n\n## 下一步\n- [ ] \n',
  'math-note': '# 數學筆記\n\n行內公式示例：$a^2+b^2=c^2$\n\n區塊公式示例：\n$$\n\\int_0^1 x^2 \\, dx = \\frac{1}{3}\n$$\n\n## 推導\n1. \n2. \n3. \n',
  'about-page': '# 關於我\n\n你好，我是 ____。\n\n## 我在做什麼\n- \n\n## 我關心的主題\n- \n\n## 聯絡我\n- Email：\n- 網站：\n',
  'links-page': '# 連結\n\n## 作品\n- [作品一](https://)\n- [作品二](https://)\n\n## 推薦閱讀\n- [文章一](https://)\n- [文章二](https://)\n\n## 常用工具\n- [工具一](https://)\n',
};

function applyTemplateToEditor() {
  if (!contentTemplateSelect) {
    return;
  }
  const key = contentTemplateSelect.value;
  if (!key || !TEMPLATE_LIBRARY[key]) {
    return;
  }
  const template = TEMPLATE_LIBRARY[key];
  const current = contentInput.value;
  if (!current.trim()) {
    contentInput.value = template;
  } else {
    contentInput.value = current.replace(/\s+$/, "") + "\n\n" + template;
  }
  if (isPageInput && (key === 'about-page' || key === 'links-page')) {
    isPageInput.checked = true;
    updateSaveBtn();
  }
  contentInput.focus();
  saveDraft();
}

document.querySelectorAll('.md-toolbar button[data-md]').forEach((btn) => {
  btn.addEventListener('click', (e) => {
    e.preventDefault();
    insertMd(btn.getAttribute('data-md'));
  });
});

if (applyTemplateBtn) {
  applyTemplateBtn.addEventListener('click', (event) => {
    event.preventDefault();
    applyTemplateToEditor();
  });
}

// ── Fullscreen editor ──
const fsToggle = document.getElementById('fullscreen-toggle');
if (fsToggle) {
  fsToggle.addEventListener('click', () => {
    const overlay = document.createElement('div');
    overlay.className = 'editor-fullscreen';
    const fsTextarea = document.createElement('textarea');
    fsTextarea.value = contentInput.value;
    fsTextarea.placeholder = '# Start writing...';
    const actions = document.createElement('div');
    actions.className = 'fs-actions';
    const saveBtn = document.createElement('button');
    saveBtn.textContent = 'Save & Close';
    saveBtn.addEventListener('click', () => {
      contentInput.value = fsTextarea.value;
      saveDraft();
      overlay.remove();
    });
    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.style.background = 'transparent';
    cancelBtn.style.borderColor = 'var(--line)';
    cancelBtn.style.color = 'var(--muted)';
    cancelBtn.addEventListener('click', () => overlay.remove());
    actions.appendChild(saveBtn);
    actions.appendChild(cancelBtn);
    overlay.appendChild(fsTextarea);
    overlay.appendChild(actions);
    document.body.appendChild(overlay);
    fsTextarea.focus();
  });
}
// ── BearBlog import ──
const importBtn = document.getElementById('import-btn');
const importFile = document.getElementById('import-file');
const importStatus = document.getElementById('import-status');
if (importBtn && importFile) {
  importBtn.addEventListener('click', async () => {
    if (importingPosts) {
      return;
    }
    const file = importFile.files[0];
    if (!file) {
      importStatus.textContent = 'Please select a CSV file';
      return;
    }
    importingPosts = true;
    importStatus.textContent = 'Importing...';
    importBtn.disabled = true;
    try {
      const fd = new FormData();
      fd.append('file', file);
      const res = await fetch('/api/import', { method: 'POST', body: fd });
      let data = null;
      try {
        data = await res.json();
      } catch {
        data = null;
      }
      if (!res.ok) {
        importStatus.textContent = (data && data.error) || 'Import failed';
        importStatus.style.color = 'var(--accent)';
        return;
      }
      importStatus.textContent = 'Imported ' + data.imported + ', skipped ' + data.skipped + ', errors ' + data.errors;
      importStatus.style.color = '';
      await refreshPosts();
    } catch (e) {
      importStatus.textContent = e.message || 'Import failed';
    } finally {
      importingPosts = false;
      importBtn.disabled = false;
    }
  });
}
    </script>
  `,
    siteConfig.colorTheme || 'default',
    "",
    siteConfig.faviconUrl || DEFAULT_FAVICON_URL
  );
}

function renderSimpleMessage(code, message) {
  return renderLayout(
    `${code}`,
    `
    <section class="panel">
      <p class="eyebrow">${escapeHtml(code)}</p>
      <h1>${escapeHtml(message)}</h1>
      <p class="muted"><a href="/">Back</a></p>
    </section>
  `
  );
}

function renderOgMetaTags(meta, fallbackTitle) {
  const source = meta && typeof meta === "object" ? meta : {};
  const safeTitle = sanitizeTitle(source.title || fallbackTitle || "").trim();
  const safeDescription = sanitizeDescription(source.description || "").trim();
  const safeType = String(source.type || "").toLowerCase() === "article" ? "article" : "website";
  const safeUrl = sanitizeUrl(source.url || "");
  const safeImage = sanitizeUrl(source.image || "");

  const tags = [
    `<meta property="og:title" content="${escapeHtml(safeTitle)}" />`,
    `<meta property="og:type" content="${escapeHtml(safeType)}" />`,
    `<meta name="twitter:card" content="summary" />`,
    `<meta name="twitter:title" content="${escapeHtml(safeTitle)}" />`,
  ];

  if (safeDescription) {
    tags.push(`<meta name="description" content="${escapeHtml(safeDescription)}" />`);
    tags.push(`<meta property="og:description" content="${escapeHtml(safeDescription)}" />`);
    tags.push(`<meta name="twitter:description" content="${escapeHtml(safeDescription)}" />`);
  }
  if (safeUrl) {
    tags.push(`<meta property="og:url" content="${escapeHtml(safeUrl)}" />`);
  }
  if (safeImage) {
    tags.push(`<meta property="og:image" content="${escapeHtml(safeImage)}" />`);
    tags.push(`<meta name="twitter:image" content="${escapeHtml(safeImage)}" />`);
  }
  return tags.join("\n    ");
}

function renderThemeControlDock(mode = "front") {
  return `
  <section id="theme-dock" class="theme-dock" data-mode="${escapeHtml(mode)}">
    <label for="theme-dock-select">頁面色系</label>
    <select id="theme-dock-select" aria-label="頁面色系">
      <option value="default">Default</option>
      <option value="ocean">Ocean</option>
      <option value="forest">Forest</option>
      <option value="violet">Violet</option>
      <option value="sunset">Sunset</option>
      <option value="mint">Mint</option>
      <option value="graphite">Graphite</option>
    </select>
    <label for="contrast-dock-select">文字對比</label>
    <select id="contrast-dock-select" aria-label="文字對比">
      <option value="normal">標準</option>
      <option value="soft">柔和</option>
      <option value="strong">高對比</option>
    </select>
  </section>
  `;
}

function renderLayout(
  title,
  body,
  colorTheme = 'default',
  customCss = "",
  faviconUrl = DEFAULT_FAVICON_URL,
  ogMeta = {}
) {
  const safeCustomCss = customCss
    ? `\n/* user custom css */\n${escapeStyleTagContent(customCss)}\n`
    : "";
  const normalizedFaviconUrl = sanitizeFaviconUrl(faviconUrl || DEFAULT_FAVICON_URL);
  const faviconMime = inferFaviconMimeType(normalizedFaviconUrl);
  const ogMetaTags = renderOgMetaTags(ogMeta, title);
  const canonicalUrl = sanitizeUrl((ogMeta && ogMeta.url) || "");
  const canonicalLink = canonicalUrl
    ? `<link rel="canonical" href="${escapeHtml(canonicalUrl)}" />`
    : "";
  return `<!doctype html>
<html lang="zh-Hant">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Fira+Code:wght@400;500;600&display=swap" rel="stylesheet" />
    <link rel="icon" href="${escapeHtml(normalizedFaviconUrl)}" type="${escapeHtml(faviconMime)}" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.css" />
    <title>${escapeHtml(title)}</title>
    ${canonicalLink}
    ${ogMetaTags}
    <style>
:root {
  --bg-1: #f6f0e8;
  --bg-2: #eadfce;
  --ink: #2b261f;
  --ink-2: #3d362d;
  --muted: #666052;
  --panel: rgba(255,252,247,0.92);
  --line: rgba(57,47,38,0.15);
  --accent: #7b5034;
  --danger: #b24329;
  --accent-glow: rgba(123,80,52,0.15);
  --code-bg: rgba(90,82,69,0.1);
  --font-mono: 'Fira Code','JetBrains Mono',Menlo,Consolas,monospace;
  --font-sans: 'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
}
.theme-ocean{--bg-1:#e9f3fb;--bg-2:#d9e9f8;--ink:#173042;--ink-2:#274459;--muted:#59768f;--panel:rgba(247,252,255,.92);--line:rgba(34,74,112,.18);--accent:#0a6fab;--accent-glow:rgba(10,111,171,.16);--code-bg:rgba(10,111,171,.1)}
.theme-forest{--bg-1:#ebf4ef;--bg-2:#dcece3;--ink:#1f3329;--ink-2:#2d483b;--muted:#62786b;--panel:rgba(246,252,248,.92);--line:rgba(45,88,67,.18);--accent:#2f6c4b;--accent-glow:rgba(47,108,75,.16);--code-bg:rgba(47,108,75,.1)}
.theme-violet{--bg-1:#f3effa;--bg-2:#e8def7;--ink:#2f2440;--ink-2:#3f3056;--muted:#6b5f83;--panel:rgba(252,248,255,.92);--line:rgba(77,56,117,.16);--accent:#7248b5;--accent-glow:rgba(114,72,181,.16);--code-bg:rgba(114,72,181,.1)}
.theme-sunset{--bg-1:#f9eee7;--bg-2:#f2dfd3;--ink:#3b2418;--ink-2:#563627;--muted:#866450;--panel:rgba(255,249,245,.92);--line:rgba(128,74,49,.18);--accent:#b85b31;--accent-glow:rgba(184,91,49,.16);--code-bg:rgba(184,91,49,.1)}
.theme-mint{--bg-1:#e8f6f2;--bg-2:#d6eee7;--ink:#17352f;--ink-2:#245048;--muted:#5d7f77;--panel:rgba(245,253,250,.92);--line:rgba(37,100,88,.18);--accent:#22826f;--accent-glow:rgba(34,130,111,.16);--code-bg:rgba(34,130,111,.1)}
.theme-graphite{--bg-1:#edf0f5;--bg-2:#dfe5ee;--ink:#202936;--ink-2:#313d4d;--muted:#677180;--panel:rgba(248,251,255,.92);--line:rgba(61,79,103,.18);--accent:#4f6688;--accent-glow:rgba(79,102,136,.18);--code-bg:rgba(79,102,136,.1)}
.contrast-soft{--ink:#4a473f;--ink-2:#5b554b;--muted:#7b7467}
.contrast-strong{--ink:#1c1812;--ink-2:#2a241c;--muted:#4d473d}
@media (prefers-color-scheme:dark) {
  :root {
    --bg-1: #0f1318;
    --bg-2: #141a22;
    --ink: #c8cfd8;
    --ink-2: #a0a8b4;
    --muted: #6b7580;
    --panel: rgba(20,26,34,0.92);
    --line: rgba(100,180,255,0.08);
    --accent: #5ca0d0;
    --danger: #ff8c73;
    --accent-glow: rgba(92,160,208,0.12);
    --code-bg: rgba(255,255,255,0.06);
  }
  .theme-ocean{--bg-1:#0b1520;--bg-2:#122131;--ink:#c8ddf0;--ink-2:#acc8df;--muted:#7e97ad;--panel:rgba(15,29,44,.92);--line:rgba(102,162,224,.15);--accent:#5aaef6;--accent-glow:rgba(90,174,246,.18);--code-bg:rgba(90,174,246,.13)}
  .theme-forest{--bg-1:#0f1815;--bg-2:#14241d;--ink:#cae3d8;--ink-2:#aed4c5;--muted:#83a698;--panel:rgba(18,33,28,.92);--line:rgba(101,166,133,.14);--accent:#6ab58f;--accent-glow:rgba(106,181,143,.18);--code-bg:rgba(106,181,143,.13)}
  .theme-violet{--bg-1:#15111c;--bg-2:#21182d;--ink:#ded2f0;--ink-2:#c3b4de;--muted:#9f8fb9;--panel:rgba(32,23,44,.92);--line:rgba(152,121,215,.14);--accent:#a782e0;--accent-glow:rgba(167,130,224,.2);--code-bg:rgba(167,130,224,.14)}
  .theme-sunset{--bg-1:#1d120f;--bg-2:#291915;--ink:#f0d2c6;--ink-2:#dfbaa9;--muted:#b08a79;--panel:rgba(43,26,21,.92);--line:rgba(199,128,96,.14);--accent:#ed946f;--accent-glow:rgba(237,148,111,.2);--code-bg:rgba(237,148,111,.14)}
  .theme-mint{--bg-1:#0d1917;--bg-2:#112824;--ink:#c7ebe3;--ink-2:#abd8cd;--muted:#80a89e;--panel:rgba(17,35,31,.92);--line:rgba(106,190,171,.14);--accent:#74cdb7;--accent-glow:rgba(116,205,183,.2);--code-bg:rgba(116,205,183,.14)}
  .theme-graphite{--bg-1:#11151b;--bg-2:#181f29;--ink:#d3dbe8;--ink-2:#b4c0d0;--muted:#8592a3;--panel:rgba(24,33,43,.92);--line:rgba(131,154,188,.14);--accent:#95aaca;--accent-glow:rgba(149,170,202,.2);--code-bg:rgba(149,170,202,.14)}
  .contrast-soft{--ink:#b6c0ce;--ink-2:#9ca7b7;--muted:#7f8998}
  .contrast-strong{--ink:#ecf2fb;--ink-2:#d2dced;--muted:#a8b3c2}
}
*{box-sizing:border-box;margin:0;padding:0}
::selection{background:var(--accent-glow)}
body{color:var(--ink);font-family:var(--font-sans);background:linear-gradient(160deg,var(--bg-1),var(--bg-2));min-height:100vh;line-height:1.6;-webkit-font-smoothing:antialiased}
.reading-progress{position:fixed;top:0;left:0;height:3px;background:var(--accent);width:0%;z-index:9999;transition:width .1s linear}
main{width:min(980px,92vw);margin:2rem auto}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;box-shadow:0 12px 40px rgba(0,0,0,.06);padding:clamp(1.2rem,3vw,2rem);backdrop-filter:blur(8px)}
.wide{width:100%}
.eyebrow{font-family:var(--font-mono);letter-spacing:.08em;text-transform:uppercase;color:var(--muted);font-size:.72rem;margin:0}
.eyebrow a{color:var(--muted);text-decoration:none}
.eyebrow a:hover{color:var(--accent)}
h1,h2,h3,h4{margin:.35rem 0 .7rem;line-height:1.25;color:var(--ink)}
h1{font-weight:700}
.muted{color:var(--muted)}
.stack{display:grid;gap:.6rem;margin-top:1rem}
label{font-family:var(--font-mono);font-size:.85rem;color:var(--muted)}
input,textarea,button,.link-button{font:inherit;border-radius:10px}
input,textarea{border:1px solid var(--line);background:rgba(255,255,255,.65);padding:.65rem .78rem;color:var(--ink);font-family:var(--font-mono);font-size:.92rem;transition:border-color .2s,box-shadow .2s}
@media(prefers-color-scheme:dark){input,textarea{background:rgba(255,255,255,.05)}}
input:focus,textarea:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-glow)}
textarea{min-height:360px;resize:vertical;line-height:1.65}
.small-textarea{min-height:110px}
button,.link-button{display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--accent);cursor:pointer;padding:.62rem .95rem;background:var(--accent);color:#f7eee2;text-decoration:none;font-weight:500;transition:all .2s;min-height:44px}
button:hover,.link-button:hover{filter:brightness(1.08);transform:translateY(-1px)}
button:active,.link-button:active{transform:translateY(0)}
.link-button[aria-disabled="true"]{opacity:.45;pointer-events:auto;filter:none}
.site-header{display:flex;align-items:start;justify-content:space-between;gap:1rem;margin-bottom:1rem}
.post-list{display:grid;gap:.9rem;margin:1rem 0 0;padding:0;list-style:none}
.post-item{border-bottom:1px dashed var(--line);padding-bottom:.7rem;transition:transform .15s}
.post-item:hover{transform:translateX(4px)}
.post-link{text-decoration:none;color:var(--ink);font-size:1.15rem;font-weight:600;transition:color .2s}
.post-link:hover{color:var(--accent)}
.site-nav{display:flex;flex-wrap:wrap;gap:.45rem;margin:.6rem 0 1rem}
.site-nav a{border:1px solid var(--line);border-radius:999px;padding:.35rem .65rem;text-decoration:none;color:var(--ink);font-size:.88rem;transition:all .2s}
.site-nav a:hover{border-color:var(--accent);color:var(--accent)}
.mode-nav{margin-top:.25rem}
.mode-nav a.active{background:var(--accent);border-color:var(--accent);color:#fff}
.mode-nav a.active:hover{color:#fff;filter:brightness(1.04)}
.community-grid{display:grid;grid-template-columns:1fr 300px;gap:1.2rem}
.community-panel{border-left:1px solid var(--line);padding-left:1rem}
.mini-list{list-style:none;padding:0;margin:0 0 1rem;display:grid;gap:.35rem}
.mini-list li{line-height:1.5;font-size:.92rem}
.mini-list a{text-decoration:none;transition:color .2s}
.mini-list a:hover{color:var(--accent)}
.site-footer{margin-top:1.2rem;border-top:1px dashed var(--line);padding-top:.75rem;font-family:var(--font-mono);font-size:.82rem}
.pager{display:flex;align-items:center;gap:.8rem;margin-top:1rem;flex-wrap:wrap}
.pager a{text-decoration:none;border:1px solid var(--line);padding:.3rem .7rem;border-radius:999px;color:var(--ink);transition:all .2s}
.pager a:hover{border-color:var(--accent);color:var(--accent)}
.pager a.disabled{opacity:.4;pointer-events:none}
.preview-badge{display:inline-block;margin-left:.4rem;padding:.1rem .4rem;border-radius:999px;border:1px solid var(--line);font-size:.7rem;letter-spacing:.04em}
.theme-dock{display:flex;flex-wrap:wrap;align-items:center;gap:.45rem;margin:.8rem 0;padding:.55rem;border:1px solid var(--line);border-radius:12px;background:var(--code-bg)}
.theme-dock label{font-size:.74rem;margin-right:.15rem}
.theme-dock select{min-width:124px;max-width:100%;padding:.35rem .45rem;border:1px solid var(--line);border-radius:8px;background:var(--panel);color:var(--ink);font-family:var(--font-mono);font-size:.8rem}
/* article */
.article-body{line-height:1.78;font-size:1.05rem}
.article-body h2,.article-body h3,.article-body h4{margin-top:1.6rem}
.article-body p{margin:.8rem 0}
.article-body hr{border:none;border-top:1px dashed var(--line);margin:1.2rem 0}
.article-body blockquote{border-left:3px solid var(--accent);padding:.5rem 0 .5rem 1rem;margin:1rem 0;color:var(--muted);background:var(--accent-glow);border-radius:0 8px 8px 0}
.article-body ul,.article-body ol{padding-left:1.4rem;margin:.6rem 0}
.article-body .task-label{display:inline-flex;align-items:flex-start;gap:.45rem}
.article-body .task-label input[type="checkbox"]{margin-top:.25rem;accent-color:var(--accent)}
.article-body del{opacity:.75}
.article-body img{max-width:100%;border-radius:8px;margin:.8rem 0}
.article-body .embed-block{margin:1rem 0;border:1px solid var(--line);border-radius:12px;overflow:hidden;background:var(--code-bg)}
.article-body .embed-block iframe{display:block;width:100%;border:0;min-height:320px;background:rgba(0,0,0,.08)}
.article-body .embed-block.embed-spotify iframe{min-height:232px}
.article-body .embed-block.embed-instagram iframe{min-height:560px}
.article-body .embed-block.embed-x iframe{min-height:480px}
.article-body .embed-caption{padding:.45rem .65rem;border-top:1px solid var(--line);font-family:var(--font-mono);font-size:.75rem;color:var(--muted)}
.article-wrap{display:grid;grid-template-columns:minmax(0,1fr) 240px;gap:1.2rem}
.article-side{border-left:1px solid var(--line);padding-left:.9rem}
.article-body pre{background:rgba(30,28,24,.96);color:#e8e4dc;padding:1rem;border-radius:10px;overflow-x:auto;font-size:.88rem;line-height:1.5;margin:.8rem 0}
@media(prefers-color-scheme:dark){.article-body pre{background:rgba(255,255,255,.05);border:1px solid var(--line)}}
.article-body code{background:var(--code-bg);padding:.12rem .35rem;border-radius:4px;font-size:.88em;font-family:var(--font-mono)}
.article-body pre code{background:none;padding:0;font-size:inherit}
.article-body .katex-display{overflow-x:auto;overflow-y:hidden;padding:.2rem 0}
.article-body .katex{max-width:100%}
.article-body .katex-display > .katex{max-width:100%}
.read-time{font-family:var(--font-mono);font-size:.78rem;color:var(--muted);margin-left:.5rem}
.reaction-panel{margin-top:1rem;display:grid;gap:.7rem}
.reaction-head{display:flex;align-items:center;justify-content:space-between;gap:.8rem;flex-wrap:wrap}
.reaction-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:.5rem}
.reaction-btn{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:.45rem;text-align:left;padding:.5rem .6rem;min-height:42px;background:rgba(255,255,255,.55);color:var(--ink);border:1px solid var(--line)}
.reaction-btn:hover{border-color:var(--accent);transform:none}
.reaction-btn.active{border-color:var(--accent);background:var(--accent-glow)}
.reaction-icon{font-size:1.15rem;line-height:1}
.reaction-label{font-family:var(--font-mono);font-size:.76rem;color:var(--ink-2)}
.reaction-count{font-family:var(--font-mono);font-size:.78rem;padding:.12rem .42rem;border-radius:999px;border:1px solid var(--line);background:var(--panel)}
.comment-panel{margin-top:1rem}
.comment-list{list-style:none;margin:1rem 0 0;padding:0;display:grid;gap:.75rem}
.comment-list.compact{gap:.5rem}
.comment-item{border:1px solid var(--line);background:rgba(255,255,255,.48);border-radius:10px;padding:.65rem .75rem}
@media(prefers-color-scheme:dark){.comment-item{background:rgba(255,255,255,.03)}}
.comment-meta{font-family:var(--font-mono);font-size:.76rem;color:var(--muted);margin-bottom:.28rem}
.comment-content{white-space:normal;overflow-wrap:anywhere}
.comment-delete-btn{margin-top:.45rem;background:transparent;color:var(--muted);border:1px solid var(--line);padding:.36rem .62rem;min-height:34px}
.comment-delete-btn:hover{color:var(--accent);border-color:var(--accent);transform:none}
.comments-pager{margin-top:.35rem}
/* back to top */
.back-top{position:fixed;bottom:1.5rem;right:1.5rem;width:42px;height:42px;border-radius:50%;background:var(--accent);color:#fff;border:none;font-size:1.1rem;cursor:pointer;opacity:0;transform:translateY(10px);transition:all .25s;z-index:100;display:flex;align-items:center;justify-content:center}
.back-top.visible{opacity:1;transform:translateY(0)}
/* admin */
.admin-shell{display:grid;gap:1rem}
.admin-tabs{display:flex;gap:.4rem;border-bottom:2px solid var(--line);padding-bottom:0}
.admin-tab{background:transparent;color:var(--muted);border:none;border-bottom:2px solid transparent;border-radius:0;padding:.6rem 1.2rem;font-family:var(--font-mono);font-size:.85rem;margin-bottom:-2px;transition:all .2s}
.admin-tab:hover{color:var(--ink);transform:none}
.admin-tab.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:600}
.tab-badge{display:inline-flex;align-items:center;justify-content:center;min-width:1.25rem;padding:0 .35rem;height:1.25rem;border-radius:999px;border:1px solid var(--line);font-size:.72rem;margin-left:.35rem;color:var(--muted);background:transparent}
.tab-badge.has-unread{background:var(--accent);border-color:var(--accent);color:#fff}
.admin-panel{margin-top:1rem}
.admin-grid{display:grid;grid-template-columns:220px minmax(0,1fr);gap:1.5rem}
.admin-list{border-right:1px solid var(--line);padding-right:1rem;display:grid;gap:.5rem;align-content:start;min-width:0}
.admin-list ul{list-style:none;margin:0;padding:0;display:grid;gap:.4rem;max-height:min(72vh,780px);overflow:auto;padding-right:.25rem}
.danger-ghost{background:transparent;color:var(--muted);border-color:var(--line)}
.danger-ghost:hover{color:var(--danger);border-color:var(--danger);transform:none}
.settings-grid{display:grid;grid-template-columns:1fr 280px;gap:2rem}
.settings-grid > *{min-width:0}
.settings-form{display:grid;gap:.5rem;align-content:start}
.settings-aside{border-left:1px solid var(--line);padding-left:1.5rem;display:grid;gap:.5rem;align-content:start}
.settings-aside input[type="file"]{max-width:100%;width:100%;min-width:0}
.settings-aside #import-btn{width:100%}
.post-item-btn{width:100%;text-align:left;background:rgba(255,255,255,.55);color:var(--ink);border:1px solid var(--line);font-size:.88rem;transition:all .15s}
@media(prefers-color-scheme:dark){.post-item-btn{background:rgba(255,255,255,.04)}}
.post-item-btn:hover{border-color:var(--accent)}
.post-item-btn.active{border-color:var(--accent);background:var(--accent-glow)}
.admin-editor{display:grid;gap:.5rem}
/* md toolbar */
.md-toolbar{display:flex;flex-wrap:wrap;gap:.3rem;padding:.4rem;background:var(--code-bg);border:1px solid var(--line);border-radius:8px}
.md-toolbar button{min-height:36px;min-width:36px;padding:.3rem .5rem;font-family:var(--font-mono);font-size:.78rem;background:transparent;color:var(--muted);border:1px solid transparent}
.md-toolbar button:hover{background:var(--accent-glow);border-color:var(--line);color:var(--ink);transform:none}
.fullscreen-btn{background:transparent!important;color:var(--muted)!important;border:1px solid var(--line)!important;font-family:var(--font-mono)!important;font-size:.78rem!important;min-height:36px}
.template-row{display:flex;flex-wrap:wrap;gap:.45rem;align-items:center}
.template-row select{flex:1;min-width:220px;border:1px solid var(--line);background:rgba(255,255,255,.65);padding:.58rem .7rem;border-radius:8px;color:var(--ink);font-family:var(--font-mono);font-size:.86rem}
@media(prefers-color-scheme:dark){.template-row select{background:rgba(255,255,255,.05)}}
.row-actions{display:flex;flex-wrap:wrap;gap:.5rem}
.inline-check{display:inline-flex;align-items:center;gap:.5rem;margin:.4rem 0}
.inline-check input[type="checkbox"]{width:18px;height:18px;accent-color:var(--accent)}
.comment-admin-panel{margin-top:.8rem;display:grid;gap:.5rem}
.notification-panel{display:grid;gap:.75rem}
.notification-list{list-style:none;margin:0;padding:0;display:grid;gap:.65rem}
.notification-item{border:1px solid var(--line);border-radius:10px;padding:.7rem .8rem;background:rgba(255,255,255,.45);display:grid;gap:.35rem}
@media(prefers-color-scheme:dark){.notification-item{background:rgba(255,255,255,.03)}}
.notification-item.unread{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent-glow) inset}
.notification-head{display:flex;align-items:center;justify-content:space-between;gap:.6rem}
.notification-kind{font-family:var(--font-mono);font-size:.78rem;color:var(--ink-2)}
.notification-head time{font-family:var(--font-mono);font-size:.74rem;color:var(--muted)}
.notification-content{color:var(--muted);font-size:.9rem}
.link-button.small,.notification-read-btn{min-height:34px;padding:.35rem .6rem;font-size:.8rem}
.link-button.ghost,.notification-read-btn{background:transparent;color:var(--muted);border-color:var(--line)}
.link-button.ghost:hover,.notification-read-btn:hover{color:var(--accent);border-color:var(--accent)}
a{color:var(--accent)}
code{font-family:var(--font-mono)}
/* fullscreen overlay */
.editor-fullscreen{position:fixed;inset:0;z-index:9000;background:var(--bg-1);display:flex;flex-direction:column;padding:.8rem;padding-top:env(safe-area-inset-top,.8rem);padding-bottom:env(safe-area-inset-bottom,.8rem)}
.editor-fullscreen textarea{flex:1;border-radius:8px;font-size:16px;resize:none}
.editor-fullscreen .fs-actions{display:flex;gap:.5rem;padding-top:.5rem}
.editor-fullscreen .fs-actions button{flex:1}
/* responsive */
@media(max-width:860px){
  main{margin-top:1rem;width:95vw}
  .admin-grid{grid-template-columns:1fr}
  .admin-list{border-right:0;border-bottom:1px solid var(--line);padding-right:0;padding-bottom:.9rem}
  .admin-list ul{max-height:none}
  .settings-grid{grid-template-columns:1fr}
  .settings-aside{border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:1rem}
  .site-header{flex-direction:column;align-items:stretch}
  .site-header .row-actions{width:100%}
  .site-header .row-actions .link-button,
  .site-header .row-actions button{flex:1}
  .community-grid{grid-template-columns:1fr}
  .community-panel{border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:.9rem}
  .article-wrap{grid-template-columns:1fr}
  .article-side{border-left:0;border-top:1px solid var(--line);padding-left:0;padding-top:.8rem}
  .article-body .embed-block iframe{min-height:240px}
  .article-body .embed-block.embed-instagram iframe{min-height:420px}
  .article-body .embed-block.embed-x iframe{min-height:360px}
  .theme-dock{display:grid;grid-template-columns:1fr;gap:.4rem}
  .theme-dock select{width:100%}
  input,textarea,button,.link-button{font-size:16px;min-height:44px}
  textarea{min-height:60vh}
  .admin-editor .row-actions{position:sticky;bottom:0;background:var(--panel);padding:.5rem;border:1px solid var(--line);border-radius:10px;z-index:10}
  .back-top{bottom:4.5rem}
}
@media(max-width:480px){
  .panel{padding:.9rem;border-radius:12px}
  h1{font-size:1.3rem}
  .admin-tabs{overflow:auto;scrollbar-width:thin}
  .post-item-btn{font-size:.82rem}
}
${safeCustomCss}
    </style>
  </head>
  <body class="theme-${escapeHtml(colorTheme)}" data-default-theme="${escapeHtml(colorTheme)}">
    <main>
      ${body}
    </main>
    <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.js"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/contrib/auto-render.min.js"></script>
    <script>
      (function () {
        const availableThemes = ['default', 'ocean', 'forest', 'violet', 'sunset', 'mint', 'graphite'];
        const availableContrasts = ['normal', 'soft', 'strong'];
        const body = document.body;
        const dock = document.getElementById('theme-dock');
        const themeSelect = document.getElementById('theme-dock-select');
        const contrastSelect = document.getElementById('contrast-dock-select');
        const storageThemeKey = 'stublogs-theme:' + location.host;
        const storageContrastKey = 'stublogs-contrast:' + location.host;
        function safeGet(key) {
          try {
            return localStorage.getItem(key);
          } catch {
            return null;
          }
        }
        function safeSet(key, value) {
          try {
            localStorage.setItem(key, value);
          } catch {
            // ignore
          }
        }

        function applyTheme(theme, persist) {
          const nextTheme = availableThemes.includes(theme) ? theme : 'default';
          availableThemes.forEach((item) => body.classList.remove('theme-' + item));
          body.classList.add('theme-' + nextTheme);
          if (themeSelect) {
            themeSelect.value = nextTheme;
          }
          if (persist) {
            safeSet(storageThemeKey, nextTheme);
          }
          return nextTheme;
        }

        function applyContrast(level, persist) {
          const next = availableContrasts.includes(level) ? level : 'normal';
          body.classList.remove('contrast-soft', 'contrast-strong');
          if (next === 'soft') {
            body.classList.add('contrast-soft');
          } else if (next === 'strong') {
            body.classList.add('contrast-strong');
          }
          if (contrastSelect) {
            contrastSelect.value = next;
          }
          if (persist) {
            safeSet(storageContrastKey, next);
          }
          return next;
        }

        window.__applyThemeDockTheme = function(theme) {
          applyTheme(theme, true);
        };

        const defaultTheme = body.getAttribute('data-default-theme') || 'default';
        const canCustomizeTheme = Boolean(dock && themeSelect && contrastSelect);
        const storedTheme = canCustomizeTheme ? (safeGet(storageThemeKey) || defaultTheme) : defaultTheme;
        const storedContrast = canCustomizeTheme ? (safeGet(storageContrastKey) || 'normal') : 'normal';
        applyTheme(storedTheme, false);
        applyContrast(storedContrast, false);

        if (themeSelect) {
          themeSelect.addEventListener('change', () => {
            applyTheme(themeSelect.value, true);
          });
        }
        if (contrastSelect) {
          contrastSelect.addEventListener('change', () => {
            applyContrast(contrastSelect.value, true);
          });
        }

        function applyMath(retries) {
          if (typeof window.renderMathInElement !== 'function') {
            if (retries > 0) {
              setTimeout(function() {
                applyMath(retries - 1);
              }, 120);
            }
            return;
          }
          window.renderMathInElement(document.body, {
            delimiters: [
              { left: '$$', right: '$$', display: true },
              { left: '\\\\[', right: '\\\\]', display: true },
              { left: '$', right: '$', display: false },
              { left: '\\\\(', right: '\\\\)', display: false }
            ],
            ignoredTags: ['script', 'noscript', 'style', 'textarea', 'pre', 'code'],
            throwOnError: false,
            strict: 'ignore'
          });
        }

        applyMath(40);
      })();
    </script>
  </body>
</html>`;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeStyleTagContent(value) {
  return String(value || "")
    .replace(/\u0000/g, "")
    .replace(/<\/style/gi, "<\\/style");
}

function inferFaviconMimeType(url) {
  const normalized = String(url || "").toLowerCase();
  if (normalized.endsWith(".png")) {
    return "image/png";
  }
  if (normalized.endsWith(".svg")) {
    return "image/svg+xml";
  }
  if (normalized.endsWith(".ico")) {
    return "image/x-icon";
  }
  if (normalized.endsWith(".webp")) {
    return "image/webp";
  }
  return "image/jpeg";
}

function toScriptJson(value) {
  return JSON.stringify(value)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026");
}

function formatDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleString("zh-Hant", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function buildWelcomePost(slug, displayName, baseDomain) {
  return `# Welcome to ${displayName} \n\n你已成功建立站點：\`${slug}.${baseDomain}\`。\n\n- 前台首頁：https://${slug}.${baseDomain}\n- 後台編輯：https://${slug}.${baseDomain}/admin\n\n現在你可以直接在後台開始寫作，體驗會偏向 Bear 的簡潔流。\n`;
}

export function renderMarkdown(source) {
  const lines = String(source || "").replace(/\r\n/g, "\n").split("\n");

  const blocks = [];
  let paragraph = [];
  let bulletItems = [];
  let orderedItems = [];
  let quoteLines = [];
  let codeBlock = null;
  let mathBlock = null;

  const flushParagraph = () => {
    if (!paragraph.length) {
      return;
    }
    const text = paragraph.join("\n");
    blocks.push(`<p>${renderInline(text)}</p>`);
    paragraph = [];
  };

  const flushBulletList = () => {
    if (!bulletItems.length) {
      return;
    }
    blocks.push(renderNestedListHtml(bulletItems, "ul", (item) => {
      if (item.task) {
        const checkedAttr = item.checked ? " checked" : "";
        return `<label class="task-label"><input type="checkbox"${checkedAttr} disabled /><span>${renderInline(item.text)}</span></label>`;
      }
      return renderInline(item.text);
    }));
    bulletItems = [];
  };

  const flushOrderedList = () => {
    if (!orderedItems.length) {
      return;
    }
    blocks.push(renderNestedListHtml(orderedItems, "ol", (item) => renderInline(item.text)));
    orderedItems = [];
  };

  const flushQuote = () => {
    if (!quoteLines.length) {
      return;
    }
    const quoteBody = quoteLines.map((line) => renderInline(line)).join("<br />");
    blocks.push(`<blockquote>${quoteBody}</blockquote>`);
    quoteLines = [];
  };

  const flushCode = () => {
    if (!codeBlock) {
      return;
    }
    blocks.push(`<pre><code>${escapeHtml(codeBlock.join("\n"))}</code></pre>`);
    codeBlock = null;
  };

  const flushMathBlock = () => {
    if (!mathBlock) {
      return;
    }
    const formula = mathBlock.join("\n").trim();
    if (formula) {
      blocks.push(`<div class="math-block">\\[${escapeHtml(formula)}\\]</div>`);
    }
    mathBlock = null;
  };

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed.startsWith("```")) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();
      flushMathBlock();

      if (codeBlock) {
        flushCode();
      } else {
        codeBlock = [];
      }

      continue;
    }

    if (codeBlock) {
      codeBlock.push(line);
      continue;
    }

    if (trimmed === "$$") {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();

      if (mathBlock) {
        flushMathBlock();
      } else {
        mathBlock = [];
      }
      continue;
    }

    if (mathBlock) {
      mathBlock.push(line);
      continue;
    }

    if (!trimmed) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();
      continue;
    }

    const singleLineMath = line.match(/^\s*\$\$(.+)\$\$\s*$/);
    if (singleLineMath) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();
      blocks.push(`<div class="math-block">\\[${escapeHtml(singleLineMath[1].trim())}\\]</div>`);
      continue;
    }

    const embedUrl = extractStandaloneEmbedUrl(line);
    if (embedUrl) {
      const embedHtml = renderEmbedBlockFromUrl(embedUrl);
      if (embedHtml) {
        flushParagraph();
        flushBulletList();
        flushOrderedList();
        flushQuote();
        blocks.push(embedHtml);
        continue;
      }
    }

    const heading = line.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();
      const level = heading[1].length;
      blocks.push(`<h${level}>${renderInline(heading[2])}</h${level}>`);
      continue;
    }

    const listItem = line.match(/^(\s*)[-*]\s+(.+)$/);
    if (listItem) {
      flushParagraph();
      flushOrderedList();
      flushQuote();
      const listText = listItem[2];
      const taskMatch = listText.match(/^\[([ xX])\]\s+(.*)$/);
      bulletItems.push({
        level: listIndentLevel(listItem[1]),
        text: taskMatch ? taskMatch[2] : listText,
        task: Boolean(taskMatch),
        checked: Boolean(taskMatch && taskMatch[1].toLowerCase() === "x"),
      });
      continue;
    }

    const orderedItem = line.match(/^(\s*)\d+\.\s+(.+)$/);
    if (orderedItem) {
      flushParagraph();
      flushBulletList();
      flushQuote();
      orderedItems.push({
        level: listIndentLevel(orderedItem[1]),
        text: orderedItem[2],
      });
      continue;
    }

    if (/^\s*---\s*$/.test(line)) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      flushQuote();
      blocks.push("<hr />");
      continue;
    }

    const quote = line.match(/^>\s?(.*)$/);
    if (quote) {
      flushParagraph();
      flushBulletList();
      flushOrderedList();
      quoteLines.push(quote[1]);
      continue;
    }
    flushQuote();

    paragraph.push(line);
  }

  flushParagraph();
  flushBulletList();
  flushOrderedList();
  flushQuote();
  flushCode();
  flushMathBlock();

  return blocks.join("\n");
}

function listIndentLevel(rawIndent) {
  const spaces = String(rawIndent || "")
    .replace(/\t/g, "  ")
    .length;
  return Math.max(Math.floor(spaces / 2), 0);
}

function renderNestedListHtml(items, listTag, renderItemContent) {
  const normalizedItems = items.map((item) => ({
    ...item,
    level: Math.max(Number(item.level) || 0, 0),
    children: [],
  }));
  const roots = [];
  const stack = [{ level: -1, children: roots }];

  for (const item of normalizedItems) {
    while (stack.length > 1 && stack[stack.length - 1].level >= item.level) {
      stack.pop();
    }
    stack[stack.length - 1].children.push(item);
    stack.push(item);
  }

  const renderNodes = (nodes) => {
    if (!nodes.length) {
      return "";
    }
    return `<${listTag}>${nodes
      .map((node) => {
        const childHtml = renderNodes(node.children);
        return `<li>${renderItemContent(node)}${childHtml}</li>`;
      })
      .join("")}</${listTag}>`;
  };

  return renderNodes(roots);
}

function extractStandaloneEmbedUrl(line) {
  const trimmed = String(line || "").trim();
  if (!trimmed) {
    return "";
  }
  if (/^https?:\/\/\S+$/i.test(trimmed)) {
    return trimmed;
  }
  const angle = trimmed.match(/^<\s*(https?:\/\/[^>\s]+)\s*>$/i);
  if (angle) {
    return angle[1];
  }
  const markdownLink = trimmed.match(/^\[[^\]]+\]\((https?:\/\/[^)\s]+)\)$/i);
  if (markdownLink) {
    return markdownLink[1];
  }
  return "";
}

function parseDurationToSeconds(raw) {
  const text = String(raw || "").trim().toLowerCase();
  if (!text) {
    return 0;
  }
  if (/^\d+$/.test(text)) {
    return Math.max(Number(text), 0);
  }
  const match = text.match(/^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?$/);
  if (!match) {
    return 0;
  }
  const hours = Number(match[1] || 0);
  const minutes = Number(match[2] || 0);
  const seconds = Number(match[3] || 0);
  return Math.max(hours * 3600 + minutes * 60 + seconds, 0);
}

function parseYoutubeEmbed(url) {
  const host = url.hostname.toLowerCase();
  let videoId = "";
  if (host === "youtu.be") {
    videoId = url.pathname.split("/").filter(Boolean)[0] || "";
  } else if (
    host === "youtube.com" ||
    host === "www.youtube.com" ||
    host === "m.youtube.com" ||
    host === "youtube-nocookie.com" ||
    host === "www.youtube-nocookie.com"
  ) {
    if (url.pathname === "/watch") {
      videoId = url.searchParams.get("v") || "";
    } else {
      const parts = url.pathname.split("/").filter(Boolean);
      if (parts[0] === "embed" || parts[0] === "shorts" || parts[0] === "live") {
        videoId = parts[1] || "";
      }
    }
  }

  if (!/^[a-zA-Z0-9_-]{6,20}$/.test(videoId)) {
    return null;
  }
  const startSeconds = Math.max(
    Number(url.searchParams.get("start") || 0) || 0,
    parseDurationToSeconds(url.searchParams.get("t"))
  );
  const startQuery = startSeconds > 0 ? `?start=${startSeconds}` : "";
  return {
    className: "embed-youtube",
    title: "YouTube",
    src: `https://www.youtube.com/embed/${videoId}${startQuery}`,
    allow:
      "accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share",
    allowFullscreen: true,
    caption: "YouTube 嵌入",
  };
}

function parseBilibiliEmbed(url) {
  const host = url.hostname.toLowerCase();
  if (
    !host.endsWith("bilibili.com") &&
    host !== "b23.tv"
  ) {
    return null;
  }
  const path = url.pathname;
  let bvid = "";
  let aid = "";
  const bvMatch = path.match(/\/(BV[0-9A-Za-z]{10,})/);
  if (bvMatch) {
    bvid = bvMatch[1];
  }
  const avMatch = path.match(/\/av(\d+)/i);
  if (avMatch) {
    aid = avMatch[1];
  }
  if (!bvid && !aid) {
    return null;
  }
  const pageValue = Number(url.searchParams.get("p") || 1);
  const page = Number.isFinite(pageValue) && pageValue > 0 ? Math.floor(pageValue) : 1;
  const query = bvid
    ? `bvid=${encodeURIComponent(bvid)}&page=${page}`
    : `aid=${encodeURIComponent(aid)}&page=${page}`;
  return {
    className: "embed-bilibili",
    title: "Bilibili",
    src: `https://player.bilibili.com/player.html?${query}`,
    allow: "autoplay; fullscreen; picture-in-picture",
    allowFullscreen: true,
    caption: "Bilibili 嵌入",
  };
}

function parseSpotifyEmbed(url) {
  const host = url.hostname.toLowerCase();
  if (host !== "open.spotify.com") {
    return null;
  }
  const parts = url.pathname.split("/").filter(Boolean);
  const type = parts[0] || "";
  const id = parts[1] || "";
  const supportedTypes = new Set(["track", "episode", "album", "playlist", "show", "artist"]);
  if (!supportedTypes.has(type) || !id) {
    return null;
  }
  return {
    className: "embed-spotify",
    title: "Spotify",
    src: `https://open.spotify.com/embed/${encodeURIComponent(type)}/${encodeURIComponent(id)}`,
    allow: "autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture",
    allowFullscreen: true,
    caption: "Spotify 嵌入",
  };
}

function parseXEmbed(url) {
  const host = url.hostname.toLowerCase();
  if (
    host !== "x.com" &&
    host !== "www.x.com" &&
    host !== "twitter.com" &&
    host !== "www.twitter.com" &&
    host !== "mobile.twitter.com"
  ) {
    return null;
  }
  const parts = url.pathname.split("/").filter(Boolean);
  if (parts.length < 3 || parts[1].toLowerCase() !== "status") {
    return null;
  }
  const username = parts[0];
  const statusId = parts[2];
  if (!/^\d{1,30}$/.test(statusId)) {
    return null;
  }
  const canonical = `https://x.com/${encodeURIComponent(username)}/status/${statusId}`;
  return {
    className: "embed-x",
    title: "X",
    src: `https://twitframe.com/show?url=${encodeURIComponent(canonical)}`,
    allow: "autoplay; clipboard-write; encrypted-media; picture-in-picture",
    allowFullscreen: false,
    caption: "X 貼文嵌入",
  };
}

function parseInstagramEmbed(url) {
  const host = url.hostname.toLowerCase();
  if (host !== "instagram.com" && host !== "www.instagram.com") {
    return null;
  }
  const parts = url.pathname.split("/").filter(Boolean);
  if (parts.length < 2) {
    return null;
  }
  const type = parts[0].toLowerCase();
  const code = parts[1];
  if (!["p", "reel", "tv"].includes(type) || !code) {
    return null;
  }
  return {
    className: "embed-instagram",
    title: "Instagram",
    src: `https://www.instagram.com/${type}/${encodeURIComponent(code)}/embed`,
    allow: "autoplay; encrypted-media; picture-in-picture; web-share",
    allowFullscreen: false,
    caption: "Instagram 嵌入",
  };
}

function renderEmbedBlockFromUrl(rawUrl) {
  const input = String(rawUrl || "").trim();
  if (!input) {
    return "";
  }
  let parsedUrl;
  try {
    parsedUrl = new URL(input);
  } catch {
    return "";
  }
  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    return "";
  }

  const parsers = [
    parseYoutubeEmbed,
    parseBilibiliEmbed,
    parseSpotifyEmbed,
    parseXEmbed,
    parseInstagramEmbed,
  ];
  let embed = null;
  for (const parser of parsers) {
    embed = parser(parsedUrl);
    if (embed) {
      break;
    }
  }
  if (!embed) {
    return "";
  }

  const safeSrc = escapeHtml(embed.src || input);
  const safeTitle = escapeHtml(embed.title || "Embedded media");
  const safeClassName = escapeHtml(embed.className || "embed-generic");
  const safeCaption = escapeHtml(embed.caption || input);
  const allow = embed.allow ? ` allow="${escapeHtml(embed.allow)}"` : "";
  const allowFullscreen = embed.allowFullscreen ? " allowfullscreen" : "";

  return `<figure class="embed-block ${safeClassName}">
    <iframe src="${safeSrc}" title="${safeTitle}" loading="lazy" referrerpolicy="strict-origin-when-cross-origin"${allow}${allowFullscreen}></iframe>
    <figcaption class="embed-caption">${safeCaption}</figcaption>
  </figure>`;
}

function renderInline(value) {
  let text = escapeHtml(value);

  const mathTokens = [];
  const tokenPrefix = "@@MATH_TOKEN_";

  const pushMathToken = (segment) => {
    const key = `${tokenPrefix}${mathTokens.length}@@`;
    mathTokens.push(segment);
    return key;
  };

  const isEscaped = (input, index) => {
    let slashCount = 0;
    for (let cursor = index - 1; cursor >= 0 && input[cursor] === "\\"; cursor -= 1) {
      slashCount += 1;
    }
    return slashCount % 2 === 1;
  };

  // Protect math delimiters from inline markdown transforms.
  text = text.replace(/\\\[[\s\S]+?\\\]/g, (segment) => pushMathToken(segment));
  text = text.replace(/\\\([\s\S]+?\\\)/g, (segment) => pushMathToken(segment));
  text = text.replace(/\$\$[\s\S]+?\$\$/g, (segment) => pushMathToken(segment));

  let scanned = "";
  for (let index = 0; index < text.length; index += 1) {
    if (text[index] !== "$" || isEscaped(text, index) || text[index + 1] === "$") {
      scanned += text[index];
      continue;
    }

    let end = index + 1;
    while (end < text.length) {
      if (text[end] === "$" && !isEscaped(text, end)) {
        break;
      }
      end += 1;
    }

    if (end >= text.length || end === index + 1) {
      scanned += text[index];
      continue;
    }

    scanned += pushMathToken(text.slice(index, end + 1));
    index = end;
  }
  text = scanned;

  text = text.replace(/`([^`]+)`/g, "<code>$1</code>");
  text = text.replace(/~~([^~]+)~~/g, "<del>$1</del>");
  text = text.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  text = text.replace(/\*([^*]+)\*/g, "<em>$1</em>");

  text = text.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (match, altText, url) => {
    const rawUrl = String(url || "").trim();
    if (!/^https?:\/\//i.test(rawUrl)) {
      return `![${altText}](${escapeHtml(rawUrl)})`;
    }
    const safeUrl = escapeHtml(rawUrl);
    const safeAlt = escapeHtml(String(altText || "").trim());
    return `<img src="${safeUrl}" alt="${safeAlt}" loading="lazy" referrerpolicy="no-referrer" />`;
  });

  text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (match, label, url) => {
    const rawUrl = String(url || "").trim();
    if (!/^https?:\/\//i.test(rawUrl)) {
      return `${label} (${escapeHtml(rawUrl)})`;
    }

    const safeUrl = escapeHtml(rawUrl);
    return `<a href="${safeUrl}" target="_blank" rel="noreferrer noopener">${label}</a>`;
  });

  for (let index = 0; index < mathTokens.length; index += 1) {
    const key = `${tokenPrefix}${index}@@`;
    text = text.split(key).join(mathTokens[index]);
  }

  text = text.replace(/\n/g, "<br />");

  return text;
}
