// Browser tests for the oxn dashboard. Drives the installed Google Chrome
// (channel "chrome"); in CI, `npx playwright install chrome` provides it.
// The fixture server (examples/e2e_dashboard.rs) needs a Redis:
// TEST_REDIS_URL, default redis://127.0.0.1:6390 (scripts/test-redis.sh).
const { defineConfig } = require('@playwright/test');

const PORT = process.env.E2E_PORT || '8199';

module.exports = defineConfig({
  testDir: './tests',
  timeout: 30000,
  expect: { timeout: 5000 },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'report' }]],
  outputDir: 'results',
  use: {
    baseURL: 'http://127.0.0.1:' + PORT,
    channel: 'chrome',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'phone', use: { viewport: { width: 375, height: 812 }, isMobile: true, hasTouch: true } },
    { name: 'tablet', use: { viewport: { width: 768, height: 1024 }, hasTouch: true } },
    { name: 'desktop', use: { viewport: { width: 1280, height: 800 } } },
  ],
  webServer: {
    command: 'cargo run -q --example e2e_dashboard --features dashboard-axum,flow,scheduler',
    cwd: '../..',
    url: 'http://127.0.0.1:' + PORT + '/admin/v1/health',
    env: { PORT, TEST_REDIS_URL: process.env.TEST_REDIS_URL || 'redis://127.0.0.1:6390' },
    reuseExistingServer: !process.env.CI,
    timeout: 600000,
    stdout: 'pipe',
  },
});
