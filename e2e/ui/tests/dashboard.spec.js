// The oxn dashboard in a real browser, at phone / tablet / desktop widths
// (see playwright.config.js). Each test seeds its own queue through the
// fixture server, so tests run in parallel without sharing state.
const fs = require('fs');
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

let seq = 0;
function qname(info) {
  return 'ui-' + info.project.name + '-' + info.workerIndex + '-' + Date.now().toString(36) + (seq++);
}
async function seed(request, name) {
  const r = await request.post('/fixture/seed/' + name);
  expect(r.ok(), await r.text()).toBeTruthy();
  return r.json();
}
// Any script the payloads manage to run sets window.__xss or opens a dialog.
async function guard(page) {
  await page.addInitScript(() => { window.__xss = 0; window.alert = () => { window.__xss = 99; }; });
  page.on('dialog', (d) => { throw new Error('a dialog opened: ' + d.message()); });
}
async function open(page, path, params) {
  await guard(page);
  await page.goto(path + '#' + new URLSearchParams(params).toString());
  await expect(page.locator('main h1')).toBeVisible();
}
const rows = (page) => page.locator('table.jobs tbody tr:not(.skel)');
const chip = (page, label) => page.getByRole('tab', { name: new RegExp('^' + label + ' [0-9]') });
const drawer = (page) => page.locator('aside.drawer');
const toast = (page, text) => page.locator('.toast', { hasText: text });
// Queue actions sit behind an "Actions" toggle on phones.
async function qaction(page, name) {
  const more = page.locator('.qhead .more');
  if (await more.isVisible() && (await more.getAttribute('aria-expanded')) !== 'true') await more.click();
  await page.locator('#qactions').getByRole('button', { name }).click();
}
async function noXss(page) {
  expect(await page.evaluate(() => window.__xss)).toBe(0);
  expect(await page.locator('img[src="x"], svg[onload], script:not([src])').count()).toBeLessThanOrEqual(1); // the page's own script
}
async function noHorizontalScroll(page) {
  const [sw, w] = await page.evaluate(() => [document.documentElement.scrollWidth, window.innerWidth]);
  expect(sw, 'page wider than the viewport').toBeLessThanOrEqual(w);
}
async function shot(page, info, what) {
  fs.mkdirSync('screenshots', { recursive: true });
  await page.screenshot({ path: 'screenshots/' + info.project.name + '-' + what + '.png', fullPage: false });
}
async function axe(page, label) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => v.id + ' (' + v.impact + '): ' + v.help + ' → ' + v.nodes.map((n) => n.target.join(' ')).slice(0, 5).join(', ')), label).toEqual([]);
}

test('lists states with counts and pages through jobs', async ({ page, request }, info) => {
  const q = qname(info);
  await seed(request, q);
  await open(page, '/admin', { q });
  await expect(page.locator('main h1')).toHaveText(q);
  await expect(chip(page, 'Waiting')).toContainText('30');
  await expect(chip(page, 'Failed')).toContainText('3');
  await expect(chip(page, 'Completed')).toContainText('5');
  await expect(rows(page)).toHaveCount(25);
  await expect(page.locator('.pager [role=status]')).toHaveText('1–25 of 30');
  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(rows(page)).toHaveCount(5);
  await expect(page.locator('.pager [role=status]')).toHaveText('26–30 of 30');
  await chip(page, 'Failed').click();
  await expect(rows(page)).toHaveCount(3);
  await expect(page.locator('.pager [role=status]')).toHaveText('1–3 of 3');
  await chip(page, 'Completed').click();
  await expect(rows(page)).toHaveCount(5);
  await noHorizontalScroll(page);
  await shot(page, info, 'completed');
});

test('the job drawer shows failure, logs and data; Esc closes it and restores focus', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q, s: 'failed' });
  const row = rows(page).filter({ hasText: 'card declined' }).first();
  await row.click();
  const d = drawer(page);
  await expect(d).toBeVisible();
  await expect(d.locator('.failure')).toContainText('card declined');
  await expect(d.locator('.failure li')).toHaveCount(1);
  await expect(d.locator('.logs li').first()).toBeVisible();
  await expect(d.locator('pre.json').first()).toContainText('"kind": "fail"');
  await noHorizontalScroll(page);
  await shot(page, info, 'drawer');
  await page.keyboard.press('Escape');
  await expect(d).toHaveCount(0);
  expect(await page.evaluate(() => document.activeElement && document.activeElement.tagName)).toBe('TR');
  expect(ids.failed.length).toBe(3);
});

test('hostile job names, data, failure reasons and logs render as text', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q });
  await expect(rows(page).first()).toContainText('<img src=x onerror=');
  await noXss(page);
  await open(page, '/admin', { q, s: 'failed', j: ids.xss.failed });
  const d = drawer(page);
  await expect(d.locator('.failure p')).toHaveText('"><svg onload=window.__xss=3>');
  await expect(d.locator('.logs')).toContainText('<script>window.__xss=2</script>');
  await expect(d.locator('dl.kv')).toContainText('<img src=x onerror=');
  await open(page, '/admin', { q, j: ids.xss.waiting });
  await expect(drawer(page).locator('pre.json').first()).toContainText('<script>window.__xss=2</script>');
  await noXss(page);
});

test('retry, run now and remove — with a confirmation, and not while a worker holds the job', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  const held = await (await request.post('/fixture/hold/' + q)).json();
  await open(page, '/admin', { q, s: 'failed', j: ids.failed[1] });
  await drawer(page).getByRole('button', { name: 'Retry' }).click();
  await expect(toast(page, 'Job queued for retry')).toBeVisible();
  await expect(drawer(page).locator('dl.kv')).toContainText('Waiting');
  await open(page, '/admin', { q, s: 'delayed', j: ids.delayed[0] });
  await drawer(page).getByRole('button', { name: 'Run now' }).click();
  await expect(toast(page, 'Job will run now')).toBeVisible();
  await open(page, '/admin', { q, s: 'active', j: held.active });
  await expect(drawer(page).getByRole('button', { name: 'Remove' })).toBeDisabled();
  await open(page, '/admin', { q, j: ids.waiting[5] });
  await drawer(page).getByRole('button', { name: 'Remove' }).click();
  const dlg = page.getByRole('dialog', { name: 'Remove job ' + ids.waiting[5] + '?' });
  await expect(dlg).toBeVisible();
  await dlg.getByRole('button', { name: 'Cancel' }).click();
  await expect(drawer(page)).toBeVisible();
  await drawer(page).getByRole('button', { name: 'Remove' }).click();
  await dlg.getByRole('button', { name: 'Remove' }).click();
  await expect(toast(page, 'Job removed')).toBeVisible();
  await expect(drawer(page)).toHaveCount(0);
  const r = await request.get('/admin/v1/queues/' + q + '/jobs/' + ids.waiting[5]);
  expect(r.status()).toBe(404);
});

test('queue actions: pause, add (with JSON validation), clean, delete', async ({ page, request }, info) => {
  const q = qname(info);
  await seed(request, q);
  await open(page, '/admin', { q });
  await qaction(page, 'Pause');
  await expect(toast(page, 'Queue paused')).toBeVisible();
  await expect(page.locator('.qhead .pill')).toHaveText('paused');
  await qaction(page, 'Resume');
  await expect(toast(page, 'Queue resumed')).toBeVisible();

  await page.getByRole('button', { name: 'Add job' }).click();
  const add = page.getByRole('dialog', { name: 'Add job' });
  await add.getByLabel('Data (JSON)').fill('{ not json');
  await add.getByRole('button', { name: 'Add job' }).click();
  await expect(add.locator('#addErr')).toContainText('Not valid JSON');
  await add.getByLabel('Data (JSON)').fill('{"hello": "world"}');
  await add.getByLabel('Name (optional)').fill('greeting');
  await add.getByRole('button', { name: 'Add job' }).click();
  await expect(toast(page, /Job \d+ added/)).toBeVisible();
  await expect(chip(page, 'Waiting')).toContainText('31');

  await qaction(page, 'Clean…');
  const clean = page.getByRole('dialog', { name: 'Clean ' + q });
  await clean.getByLabel('State').selectOption('completed');
  await clean.getByRole('button', { name: 'Delete jobs' }).click();
  await expect(toast(page, '5 jobs deleted')).toBeVisible();
  await expect(chip(page, 'Completed')).toContainText('0');

  await qaction(page, 'Delete queue…');
  const del = page.getByRole('dialog', { name: 'Delete queue ' + q + '?' });
  await expect(del.getByRole('button', { name: 'Delete queue' })).toBeDisabled();
  await del.getByLabel('Queue name').fill(q);
  await del.getByRole('button', { name: 'Delete queue' }).click();
  await expect(toast(page, 'Queue deleted')).toBeVisible();
  expect((await request.get('/admin/v1/queues/' + q)).status()).toBe(404);
});

test('read-only and token-protected mounts only offer what will work', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/ro', { q, s: 'failed', j: ids.failed[1] });
  await expect(page.getByRole('button', { name: 'Pause' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Add job' })).toHaveCount(0);
  await expect(drawer(page).getByRole('button', { name: 'Retry' })).toHaveCount(0);

  await guard(page);
  await page.goto('/secure#q=' + q);
  const tok = page.getByRole('dialog', { name: 'API token' });
  await expect(tok).toBeVisible();
  await tok.getByLabel('Token').fill('read-token');
  await tok.getByRole('button', { name: 'Use token' }).click();
  await expect(page.locator('main h1')).toHaveText(q);
  await expect(chip(page, 'Waiting')).toContainText('30');
  await expect(page.getByRole('button', { name: 'Pause' })).toHaveCount(0);
  await page.getByRole('button', { name: 'API token' }).click();
  await tok.getByLabel('Token').fill('admin-token');
  await tok.getByRole('button', { name: 'Use token' }).click();
  // present (behind the Actions toggle on phones, so not yet in the a11y tree there)
  await expect(page.locator('#qactions button', { hasText: 'Pause' })).toHaveCount(1);
});

test('live: a job added elsewhere shows up without reloading', async ({ page, request }, info) => {
  const q = qname(info);
  await seed(request, q);
  await open(page, '/admin', { q });
  await expect(page.locator('#live')).toHaveAttribute('data-state', 'live');
  await expect(chip(page, 'Waiting')).toContainText('30');
  const r = await request.post('/admin/v1/queues/' + q + '/jobs', { data: { data: { from: 'elsewhere' } } });
  expect(r.status()).toBe(201);
  await expect(chip(page, 'Waiting')).toContainText('31');
});

test('deep links open a job; the back button closes it', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q });
  await rows(page).nth(2).click();
  await expect(drawer(page)).toBeVisible();
  expect(page.url()).toContain('j=');
  await page.goBack();
  await expect(drawer(page)).toHaveCount(0);
  await page.goForward();
  await expect(drawer(page)).toBeVisible();
  await open(page, '/admin', { q, s: 'completed', j: ids.completed[0] });
  await expect(drawer(page).locator('#drawerTitle')).toHaveText(ids.completed[0]);
  await expect(drawer(page)).toContainText('Return value');
});

test('a flow parent shows its tree; children link back', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q, s: 'waiting-children', j: ids.flow.parent });
  const tree = drawer(page).locator('ul.tree').first();
  await expect(tree).toContainText('render-pdf');
  await expect(tree).toContainText('fetch-rates');
  await tree.getByRole('button', { name: /render-pdf/ }).click();
  await expect(drawer(page).locator('#drawerTitle')).toHaveText(ids.flow.children[0]);
  await expect(drawer(page).locator('dl.kv')).toContainText(q + ' / ' + ids.flow.parent);
});

test('schedulers tab lists and removes schedulers', async ({ page, request }, info) => {
  const q = qname(info);
  await seed(request, q);
  await open(page, '/admin', { q });
  await page.getByRole('tab', { name: 'Schedulers' }).click();
  await expect(page.locator('table.jobs')).toContainText('nightly-digest');
  await expect(page.locator('table.jobs')).toContainText('cron 0 0 2 * * *');
  await page.getByRole('button', { name: 'Remove' }).click();
  await page.getByRole('dialog').getByRole('button', { name: 'Remove' }).click();
  await expect(toast(page, 'Scheduler removed')).toBeVisible();
  await expect(page.locator('main')).toContainText('No schedulers');
});

test('keyboard: / opens a job by id, j/k move, Enter opens, Esc closes', async ({ page, request }, info) => {
  test.skip(info.project.name === 'phone', 'no hardware keyboard on the phone layout');
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q });
  await expect(rows(page)).toHaveCount(25);
  await page.locator('body').click({ position: { x: 1, y: 300 } });
  await page.keyboard.press('/');
  await expect(page.locator('#jobSearch')).toBeFocused();
  await page.keyboard.type(String(ids.waiting[3]));
  await page.keyboard.press('Enter');
  await expect(drawer(page).locator('#drawerTitle')).toHaveText(ids.waiting[3]);
  await page.keyboard.press('Escape');
  await expect(drawer(page)).toHaveCount(0);
  await page.locator('main h1').click();
  await page.keyboard.press('j');
  await page.keyboard.press('j');
  await expect(rows(page).nth(1)).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(drawer(page)).toBeVisible();
  // focus stays inside the open drawer
  for (let i = 0; i < 12; i++) await page.keyboard.press('Tab');
  expect(await page.evaluate(() => !!document.activeElement.closest('aside.drawer'))).toBe(true);
});

test('layout: queue sheet, cards and a full-width drawer on phones; no sideways scrolling', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q });
  await noHorizontalScroll(page);
  if (info.project.name === 'phone') {
    await expect(page.locator('.jobs thead')).toBeHidden();
    await page.locator('#qpick').click();
    await expect(page.locator('#side')).toBeVisible();
    await page.locator('#side').getByRole('button', { name: new RegExp(q) }).click();
    await expect(page.locator('#side')).toBeHidden();
  } else {
    await expect(page.locator('.jobs thead')).toBeVisible();
  }
  await open(page, '/admin', { q, j: ids.waiting[1] });
  const box = await drawer(page).boundingBox();
  const vw = page.viewportSize().width;
  if (info.project.name === 'phone') expect(Math.round(box.width)).toBe(vw);
  else expect(box.width).toBeLessThanOrEqual(560);
  await noHorizontalScroll(page);
});

test('theme: toggles and persists', async ({ page, request }, info) => {
  const q = qname(info);
  await seed(request, q);
  await open(page, '/admin', { q });
  const btn = page.locator('#themeBtn');
  while ((await page.locator('html').getAttribute('data-theme')) !== 'dark') await btn.click();
  await shot(page, info, 'dark');
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.locator('#themeBtn').click(); // dark → auto
  await expect(page.locator('html')).not.toHaveAttribute('data-theme', 'dark');
});

test('accessibility: no serious or critical axe violations, light and dark', async ({ page, request }, info) => {
  const q = qname(info);
  const ids = await seed(request, q);
  await open(page, '/admin', { q });
  await expect(rows(page)).toHaveCount(25);
  await axe(page, 'list');
  await open(page, '/admin', { q, s: 'failed', j: ids.failed[1] });
  await expect(drawer(page).locator('.failure')).toBeVisible();
  await axe(page, 'drawer');
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'Add job' }).click();
  await expect(page.getByRole('dialog', { name: 'Add job' })).toBeVisible();
  await axe(page, 'add dialog');
  await page.keyboard.press('Escape');
  await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
  await axe(page, 'list, dark');
});
