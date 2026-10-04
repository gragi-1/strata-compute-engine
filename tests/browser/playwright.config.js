const { defineConfig } = require("@playwright/test");
const path = require("node:path");
const root = path.resolve(__dirname, "../..");
const port = 58005;
const python =
  process.env.STRATA_BROWSER_PYTHON ||
  path.join(
    root,
    process.platform === "win32"
      ? ".venv/Scripts/python.exe"
      : ".venv/bin/python",
  );

module.exports = defineConfig({
  testDir: ".",
  testMatch: "*.spec.js",
  workers: 1,
  retries: 0,
  timeout: 30000,
  projects: ["chromium", "firefox", "webkit"].map((browserName) => ({
    name: browserName,
    use: { browserName },
  })),
  reporter: [
    ["list"],
    ["junit", { outputFile: "../../build/browser/junit.xml" }],
  ],
  outputDir: "../../build/browser/results",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: `"${python}" -m scripts.browser_fixture --port ${port}`,
    cwd: root,
    url: `http://127.0.0.1:${port}/health`,
    reuseExistingServer: false,
    timeout: 30000,
  },
});
