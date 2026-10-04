const { test, expect } = require("@playwright/test");
const AxeBuilder = require("@axe-core/playwright").default;
const fs = require("node:fs/promises");

async function login(page, user = "browser-admin") {
  await page.goto("/");
  await page.getByLabel("Username", { exact: true }).fill(user);
  await page
    .getByLabel("Password", { exact: true })
    .fill("Browser-test-password-1004");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(
    page.getByRole("dialog", { name: "Connect to your workspace" }),
  ).not.toBeVisible();
  await expect(page.getByLabel("Active project")).toBeVisible();
}

async function navigate(page, name) {
  await page
    .getByRole("navigation")
    .getByRole("button", { name, exact: true })
    .click();
  await expect(page.locator("#title")).toHaveText(
    name === "Account & projects" ? "Account" : name,
  );
}

async function accessible(page, info, name) {
  const report = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  const reportPath = info.outputPath(`${name}-accessibility.json`);
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
  await info.attach(`${name}-accessibility`, {
    path: reportPath,
    contentType: "application/json",
  });
  expect(
    report.violations,
    `${name}: ${JSON.stringify(report.violations)}`,
  ).toEqual([]);
}

test("native sign-in, project selection, refresh and logout", async ({
  page,
}) => {
  await login(page);
  await page
    .getByLabel("Active project")
    .selectOption({ label: "Private project" });
  await page.reload();
  await expect(page.getByLabel("Active project")).toHaveValue(/.+/);
  await expect(
    page.getByLabel("Active project").locator("option:checked"),
  ).toHaveText("Private project");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(page.getByLabel("Username", { exact: true })).toBeVisible();
});

test("navigation and WCAG automated checks on every workspace page", async ({
  page,
}, info) => {
  await page.goto("/");
  await expect(page.getByLabel("Username", { exact: true })).toBeVisible();
  await accessible(page, info, "login");
  await login(page);
  for (const name of [
    "Overview",
    "Campaigns",
    "Experiments",
    "Schedules",
    "Webhooks",
    "Jobs",
    "Compute groups",
    "Interactive sessions",
    "Datasets",
    "Workers",
    "Account & projects",
  ]) {
    await navigate(page, name);
    await expect(page.locator("#content")).not.toBeEmpty();
    await accessible(page, info, name);
  }
});

test("lost job submission response reuses its key and creates one job", async ({
  page,
}, info) => {
  await login(page);
  await navigate(page, "Jobs");
  await page.getByRole("button", { name: "Submit job", exact: true }).click();
  const dialog = page.getByRole("dialog", {
    name: "Submit a job",
    exact: true,
  });
  const specification = dialog.getByLabel("Job specification", { exact: true });
  const job = JSON.parse(await specification.inputValue());
  job.name = `Browser reply-loss workload ${info.project.name}`;
  await specification.fill(JSON.stringify(job));
  const keys = [];
  await page.route("**/jobs", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    keys.push(route.request().headers()["idempotency-key"]);
    if (keys.length === 1) {
      const response = await route.fetch();
      expect(response.ok()).toBeTruthy();
      return route.abort("failed");
    }
    return route.continue();
  });
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(dialog).not.toBeVisible();
  await expect(
    page.getByRole("cell", {
      name: job.name,
      exact: true,
    }),
  ).toHaveCount(1);
  expect(keys.length).toBe(2);
  expect(keys[0]).toBeTruthy();
  expect(keys[1]).toBe(keys[0]);
});

test("queued notebook lifecycle, editor and standard export", async ({
  page,
}, info) => {
  await login(page);
  await navigate(page, "Interactive sessions");
  await page
    .getByRole("button", { name: "Start Python notebook", exact: true })
    .click();
  const dialog = page.getByRole("dialog");
  const specification = dialog.getByLabel(
    "Runtime resources, image and inputs",
    { exact: true },
  );
  const job = JSON.parse(await specification.inputValue());
  job.name = `Browser notebook lifecycle ${info.project.name}`;
  await specification.fill(JSON.stringify(job));
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(dialog).not.toBeVisible();
  const row = page.getByRole("row").filter({ hasText: job.name });
  await row.getByRole("button", { name: "Open session", exact: true }).click();
  await expect(page.getByLabel("Python code", { exact: true })).toBeVisible();
  await accessible(page, info, "notebook");
  const download = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "Download notebook", exact: true })
    .click();
  const file = await download;
  expect(file.suggestedFilename()).toMatch(/\.ipynb$/);
  const stream = await file.createReadStream();
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  expect(JSON.parse(Buffer.concat(chunks).toString()).nbformat).toBe(4);
  await page.getByRole("button", { name: "Stop session", exact: true }).click();
  await expect(
    page.getByLabel("Python code", { exact: true }),
  ).not.toBeVisible();
  await expect(page.locator("#content")).toContainText("CANCELLED");
});

test("gang form admits a complete queued group and cancels every rank", async ({
  page,
}, info) => {
  await login(page);
  await navigate(page, "Compute groups");
  await page.getByRole("button", { name: "Create group", exact: true }).click();
  const dialog = page.getByRole("dialog");
  const name = `Browser computation group ${info.project.name}`;
  await dialog.getByLabel("Name", { exact: true }).fill(name);
  await accessible(page, info, "group-form");
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(dialog).not.toBeVisible();
  await page
    .getByRole("row")
    .filter({ hasText: name })
    .getByRole("button", { name: "Inspect group" })
    .click();
  await expect(page.getByRole("table").getByRole("row")).toHaveCount(3);
  await page
    .getByRole("button", { name: "Cancel entire group", exact: true })
    .click();
  await expect(page.locator("#content")).toContainText("CANCELLED");
});

test("viewer has only its project and read-only controls", async ({ page }) => {
  await login(page, "browser-viewer");
  await expect(page.getByLabel("Active project").locator("option")).toHaveCount(
    1,
  );
  await expect(page.getByLabel("Active project")).toContainText(
    "Browser research",
  );
  await navigate(page, "Jobs");
  await expect(
    page.getByRole("button", { name: "Submit job", exact: true }),
  ).toHaveCount(0);
  await navigate(page, "Interactive sessions");
  await expect(
    page.getByRole("button", { name: "Start Python notebook", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Start HTTP service", exact: true }),
  ).toHaveCount(0);
});

test("mobile tables stay bounded and keyboard focusable", async ({
  page,
}, info) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page);
  await navigate(page, "Jobs");
  await expect(
    page.getByRole("region", { name: "Job queue table", exact: true }),
  ).toHaveAttribute("tabindex", "0");
  const region = page.getByRole("region", {
    name: "Job queue table",
    exact: true,
  });
  await region.focus();
  await expect(region).toBeFocused();
  expect(
    await page.evaluate(
      () =>
        document.documentElement.scrollWidth <=
        document.documentElement.clientWidth,
    ),
  ).toBe(true);
  await accessible(page, info, "mobile-jobs");
  await page.screenshot({
    path: info.outputPath("mobile-jobs.png"),
    fullPage: true,
  });
});
