import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";

import { renderPostPage } from "../src/index.js";

function buildSite() {
  return {
    id: 1,
    slug: "demo",
    displayName: "Demo",
    description: "demo site",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function buildSiteConfig() {
  return {
    slug: "demo",
    displayName: "Demo",
    description: "demo site",
    heroTitle: "",
    heroSubtitle: "",
    colorTheme: "default",
    footerNote: "",
    headerLinks: [],
    hideCommunitySites: false,
    hideCampusFeed: false,
    commentsEnabled: true,
    customCss: "",
    faviconUrl: "",
  };
}

test("post page inline script parses without syntax error", () => {
  const html = renderPostPage(
    buildSite(),
    buildSiteConfig(),
    {
      postSlug: "hello-world",
      title: "Hello World",
      description: "",
      published: 1,
      isPage: 0,
      updatedAt: new Date().toISOString(),
    },
    "<p>content</p>",
    [],
    [
      { postSlug: "projects", title: "Projects", updatedAt: "2026-02-20T00:00:00.000Z" },
      { postSlug: "home", title: "Home", updatedAt: "2026-02-21T00:00:00.000Z" },
    ],
    "bdfz.net",
    {
      commentsEnabled: true,
      comments: [],
      commentsTotal: 0,
      commentsPage: 1,
      commentsTotalPages: 1,
      commentBasePath: "/hello-world",
    }
  );

  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((match) => match[1]);
  const postInlineScript = scripts.find((source) => source.includes("comment-form"));
  assert.ok(postInlineScript, "post page comment script should exist");
  assert.doesNotThrow(() => {
    new vm.Script(postInlineScript);
  });
});

test("post page mode nav follows Home/Now/Projects/Blog order", () => {
  const html = renderPostPage(
    buildSite(),
    buildSiteConfig(),
    {
      postSlug: "now",
      title: "Now",
      description: "",
      published: 1,
      isPage: 1,
      updatedAt: new Date().toISOString(),
    },
    "<p>now</p>",
    [],
    [
      { postSlug: "projects", title: "Projects", updatedAt: "2026-02-19T00:00:00.000Z" },
      { postSlug: "misc", title: "Misc", updatedAt: "2026-02-22T00:00:00.000Z" },
      { postSlug: "home", title: "Home", updatedAt: "2026-02-18T00:00:00.000Z" },
      { postSlug: "now", title: "Now", updatedAt: "2026-02-17T00:00:00.000Z" },
    ],
    "bdfz.net",
    {
      commentsEnabled: true,
      comments: [],
      commentsTotal: 0,
      commentsPage: 1,
      commentsTotalPages: 1,
      commentBasePath: "/now",
    }
  );

  assert.match(html, /class="site-nav mode-nav"/);
  assert.match(html, /<a class="active" href="\/now">Now<\/a>/);

  const homePos = html.indexOf('href="/home"');
  const nowPos = html.indexOf('href="/now"');
  const projectsPos = html.indexOf('href="/projects"');
  const blogPos = html.indexOf('href="/">Blog</a>');
  assert.ok(homePos > -1 && nowPos > -1 && projectsPos > -1 && blogPos > -1);
  assert.ok(homePos < nowPos);
  assert.ok(nowPos < projectsPos);
  assert.ok(projectsPos < blogPos);
});

test("post page renders reactions and view counter", () => {
  const html = renderPostPage(
    buildSite(),
    buildSiteConfig(),
    {
      postSlug: "hello-world",
      title: "Hello World",
      description: "",
      published: 1,
      isPage: 0,
      updatedAt: new Date().toISOString(),
    },
    "<p>content</p>",
    [],
    [{ postSlug: "home", title: "Home", updatedAt: "2026-02-21T00:00:00.000Z" }],
    "bdfz.net",
    {
      commentsEnabled: true,
      comments: [],
      commentsTotal: 0,
      commentsPage: 1,
      commentsTotalPages: 1,
      commentBasePath: "/hello-world",
      postViewCount: 1234,
      reactionSnapshot: {
        items: [{ key: "lion", count: 2 }],
        selectedKeys: ["lion"],
      },
    }
  );

  assert.match(html, /訪問 1,234/);
  assert.match(html, /id="reactions"/);
  assert.match(html, /data-reaction-key="lion"/);
  assert.match(html, /class="reaction-btn active"/);
  assert.match(html, /\/api\/reactions/);
});

test("post page reactions are sorted by count desc", () => {
  const html = renderPostPage(
    buildSite(),
    buildSiteConfig(),
    {
      postSlug: "hello-world",
      title: "Hello World",
      description: "",
      published: 1,
      isPage: 0,
      updatedAt: new Date().toISOString(),
    },
    "<p>content</p>",
    [],
    [{ postSlug: "home", title: "Home", updatedAt: "2026-02-21T00:00:00.000Z" }],
    "bdfz.net",
    {
      commentsEnabled: true,
      comments: [],
      commentsTotal: 0,
      commentsPage: 1,
      commentsTotalPages: 1,
      commentBasePath: "/hello-world",
      reactionSnapshot: {
        items: [
          { key: "lion", count: 1 },
          { key: "rocket", count: 5 },
          { key: "dragon", count: 3 },
        ],
        selectedKeys: ["rocket"],
      },
    }
  );

  const rocketPos = html.indexOf('data-reaction-key="rocket"');
  const dragonPos = html.indexOf('data-reaction-key="dragon"');
  const lionPos = html.indexOf('data-reaction-key="lion"');
  assert.ok(rocketPos > -1 && dragonPos > -1 && lionPos > -1);
  assert.ok(rocketPos < dragonPos);
  assert.ok(dragonPos < lionPos);
});

test("post page keeps custom css @import content", () => {
  const config = buildSiteConfig();
  config.customCss = "@import url('https://fonts.googleapis.com/css2?family=Noto+Serif+TC:wght@400;700&display=swap');";
  const html = renderPostPage(
    buildSite(),
    config,
    {
      postSlug: "hello-world",
      title: "Hello World",
      description: "",
      published: 1,
      isPage: 0,
      updatedAt: new Date().toISOString(),
    },
    "<p>content</p>",
    [],
    [{ postSlug: "home", title: "Home", updatedAt: "2026-02-21T00:00:00.000Z" }],
    "bdfz.net",
    {
      commentsEnabled: true,
      comments: [],
      commentsTotal: 0,
      commentsPage: 1,
      commentsTotalPages: 1,
      commentBasePath: "/hello-world",
    }
  );

  assert.match(html, /@import url\('https:\/\/fonts\.googleapis\.com/);
});
