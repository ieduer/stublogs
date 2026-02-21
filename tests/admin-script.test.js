import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";

import { renderAdminPage } from "../src/index.js";

test("admin inline script parses without syntax error", () => {
  const html = renderAdminPage(
    {
      id: 1,
      slug: "suen",
      displayName: "suen",
      description: "",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    },
    {
      slug: "suen",
      displayName: "suen",
      description: "",
      heroTitle: "",
      heroSubtitle: "",
      colorTheme: "default",
      footerNote: "",
      headerLinks: [],
      hideCommunitySites: false,
      hideCampusFeed: false,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      exportVersion: 2,
    },
    true,
    "bdfz.net"
  );

  const match = html.match(/<script>([\s\S]*?)<\/script>/);
  assert.ok(match, "admin page script should exist");

  const scriptSource = match[1];
  assert.doesNotThrow(() => {
    new vm.Script(scriptSource);
  });
});

