"use strict";
const $ = (id) => document.getElementById(id);
let token = sessionStorage.getItem("strata-key") || "",
  page = "overview",
  offset = 0,
  busy = false,
  role = "viewer";
const content = $("content"),
  modal = $("modal");
let detailReload = null;
function node(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = String(text);
  if (cls) n.className = cls;
  return n;
}
function button(text, action, cls) {
  const b = node("button", text, cls);
  b.onclick = () => Promise.resolve(action()).catch(showError);
  return b;
}
function showError(error) {
  $("notice").hidden = false;
  $("notice").textContent = error.message || String(error);
}
async function api(path, options = {}) {
  const headers = { ...options.headers };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (options.json !== undefined) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.json);
  }
  const r = await fetch(path, { ...options, headers });
  if (r.status === 401) {
    $("login").showModal();
    throw Error("Enter a valid API key to continue.");
  }
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw Error(
      typeof body.detail === "string"
        ? body.detail
        : JSON.stringify(body.detail || r.status),
    );
  }
  return r;
}
async function json(path, options) {
  return (await api(path, options)).json();
}
async function download(path, name) {
  const blob = await (await api(path)).blob(),
    url = URL.createObjectURL(blob),
    a = node("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function badge(state) {
  return node("span", state, `badge ${state}`);
}
function panel(title) {
  const p = node("section", undefined, "panel");
  p.append(node("h2", title));
  content.append(p);
  return p;
}
function table(parent, columns, rows) {
  const wrap = node("div", undefined, "table-wrap"),
    t = node("table"),
    head = node("thead"),
    tr = node("tr");
  columns.forEach((c) => tr.append(node("th", c)));
  head.append(tr);
  t.append(head);
  const body = node("tbody");
  rows.forEach((cells) => {
    const r = node("tr");
    cells.forEach((cell) => {
      const td = node("td");
      td.append(
        cell instanceof Node
          ? cell
          : document.createTextNode(String(cell ?? "—")),
      );
      r.append(td);
    });
    body.append(r);
  });
  t.append(body);
  wrap.append(t);
  parent.append(wrap);
  if (!rows.length)
    parent.append(
      node(
        "p",
        "No records yet. Create your first one to get started.",
        "empty",
      ),
    );
}
function toolbar(title, label, action) {
  const bar = node("div", undefined, "toolbar");
  bar.append(node("h2", title));
  if (label && role !== "viewer") bar.append(button(label, action, "primary"));
  content.append(bar);
}
function pagination(rows) {
  const p = node("div", undefined, "pagination");
  if (offset)
    p.append(
      button("← Previous", () => {
        offset = Math.max(0, offset - 50);
        return load();
      }),
    );
  if (rows.length === 50)
    p.append(
      button("Next →", () => {
        offset += 50;
        return load();
      }),
    );
  content.append(p);
}
function editor(title, fields, submit, help = "") {
  $("dialog-title").textContent = title;
  $("dialog-help").textContent = help;
  $("fields").replaceChildren();
  $("form-error").textContent = "";
  const controls = {};
  fields.forEach(([key, label, value, type = "text"]) => {
    const l = node("label", label),
      input = node(type === "json" ? "textarea" : "input");
    if (type !== "json") input.type = type;
    input.name = key;
    if (type !== "file")
      input.value =
        type === "json" ? JSON.stringify(value, null, 2) : String(value);
    input.required = true;
    controls[key] = [input, type];
    l.append(input);
    $("fields").append(l);
  });
  $("editor").onsubmit = async (e) => {
    e.preventDefault();
    const b = $("editor").querySelector("[type=submit]");
    b.disabled = true;
    try {
      const values = {};
      Object.entries(controls).forEach(([key, [el, type]]) => {
        values[key] =
          type === "json"
            ? JSON.parse(el.value)
            : type === "number"
              ? Number(el.value)
              : type === "file"
                ? el.files[0]
                : el.value;
      });
      await submit(values);
      modal.close();
      await load();
    } catch (e) {
      $("form-error").textContent = e.message;
    } finally {
      b.disabled = false;
    }
  };
  modal.showModal();
}
const sample = {
  name: "My compute job",
  image: "strata/python-workloads:local",
  command: [
    "python",
    "/app/main.py",
    "monte-carlo",
    "--samples",
    "100000",
    "--seed",
    "42",
  ],
  resources: { cpu: 1, memory_mb: 128 },
  max_retries: 3,
  timeout_seconds: 600,
};
function newJob() {
  editor(
    "Submit a job",
    [["specification", "Job specification", sample, "json"]],
    async (v) =>
      json("/jobs", {
        method: "POST",
        json: v.specification,
        headers: { "Idempotency-Key": crypto.randomUUID() },
      }),
    "Commands are argument arrays. Add sealed dataset inputs as {version_id, alias}; they appear at /inputs/<alias>.",
  );
}
function newCampaign() {
  const template = structuredClone(sample);
  template.command[template.command.length - 1] = "${seed}";
  editor(
    "Create a parameter campaign",
    [
      ["name", "Name", "Parameter study"],
      ["template", "Job template", template, "json"],
      ["matrix", "Parameter values", { seed: [1, 2, 3] }, "json"],
      ["repeats", "Repetitions", 1, "number"],
    ],
    (v) =>
      json("/campaigns", {
        method: "POST",
        json: v,
        headers: { "Idempotency-Key": crypto.randomUUID() },
      }),
    "Use ${parameter} in individual command arguments. Every combination becomes an independent job.",
  );
}
async function jobDetail(id) {
  page = "detail";
  detailReload = () => jobDetail(id);
  $("title").textContent = "Job details";
  content.replaceChildren();
  const [job, attempts, artifacts, events, logs] = await Promise.all([
    json(`/jobs/${id}`),
    json(`/jobs/${id}/attempts`),
    json(`/jobs/${id}/artifacts`),
    json(`/jobs/${id}/events`),
    (await api(`/jobs/${id}/logs`)).text(),
  ]);
  toolbar(job.name, "Back to jobs", () => navigate("jobs"));
  const p = panel("Execution");
  p.append(badge(job.status), node("pre", JSON.stringify(job, null, 2)));
  const a = panel("Attempt history");
  table(
    a,
    ["Attempt", "Worker", "Status", "Reason"],
    attempts.map((x) => [x.number, x.worker_id, badge(x.status), x.reason]),
  );
  const f = panel("Artifacts");
  table(
    f,
    ["File", "Bytes", "SHA-256", "Download"],
    artifacts.map((x) => [
      x.name,
      x.size,
      x.sha256,
      button("Download", () => download(x.uri, x.name)),
    ]),
  );
  panel("Logs").append(node("pre", logs || "No logs yet."));
  panel("Event history").append(node("pre", JSON.stringify(events, null, 2)));
}
async function campaignDetail(id) {
  page = "detail";
  detailReload = () => campaignDetail(id);
  $("title").textContent = "Campaign results";
  content.replaceChildren();
  const [campaign, jobs, results] = await Promise.all([
    json(`/campaigns/${id}`),
    json(`/campaigns/${id}/jobs?limit=1000`),
    json(`/campaigns/${id}/results`),
  ]);
  toolbar(campaign.name, "Back to campaigns", () => navigate("campaigns"));
  const p = panel("Progress and provenance");
  p.append(
    node("p", `${campaign.completed} of ${campaign.total} jobs completed`),
    node("pre", JSON.stringify(campaign.specification, null, 2)),
    button("Export CSV", () =>
      download(`/campaigns/${id}/results?format=csv`, `${campaign.name}.csv`),
    ),
    button("Export JSON", () =>
      download(`/campaigns/${id}/results`, `${campaign.name}.json`),
    ),
  );
  const metrics = [
    ...new Set(
      results.flatMap((r) =>
        Object.keys(r).filter(
          (k) => k.startsWith("result.") && typeof r[k] === "number",
        ),
      ),
    ),
  ];
  if (metrics.length) {
    const chart = panel("Result comparison"),
      select = node("select");
    metrics.forEach((m) => {
      const o = node("option", m);
      o.value = m;
      select.append(o);
    });
    chart.append(select);
    const host = node("div");
    chart.append(host);
    const draw = () => plot(host, results, select.value);
    select.onchange = draw;
    draw();
  }
  const t = panel("Jobs (first 1,000)");
  jobTable(t, jobs);
}
function plot(host, rows, metric) {
  host.replaceChildren();
  const ns = "http://www.w3.org/2000/svg",
    s = document.createElementNS(ns, "svg");
  s.setAttribute("viewBox", "0 0 800 300");
  s.setAttribute("role", "img");
  s.setAttribute("aria-label", `Comparison of ${metric} by job index`);
  const values = rows
    .map((r, i) => [i, r[metric]])
    .filter((x) => Number.isFinite(x[1]));
  if (!values.length) return;
  const lo = Math.min(...values.map((x) => x[1])),
    hi = Math.max(...values.map((x) => x[1]));
  const line = document.createElementNS(ns, "polyline");
  line.setAttribute(
    "points",
    values
      .map(
        ([i, v]) =>
          `${65 + (i / Math.max(1, rows.length - 1)) * 690},${250 - ((v - lo) / Math.max(1e-12, hi - lo)) * 205}`,
      )
      .join(" "),
  );
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", "#16654b");
  line.setAttribute("stroke-width", "2");
  s.append(line);
  for (const [text, x, y] of [
    [`${hi.toPrecision(5)}`, 8, 42],
    [`${lo.toPrecision(5)}`, 8, 252],
    ["Job index →", 350, 287],
  ]) {
    const t = document.createElementNS(ns, "text");
    t.textContent = text;
    t.setAttribute("x", x);
    t.setAttribute("y", y);
    t.setAttribute("font-size", "12");
    s.append(t);
  }
  host.append(s);
}
async function datasetDetail(id) {
  page = "detail";
  detailReload = () => datasetDetail(id);
  $("title").textContent = "Dataset versions";
  content.replaceChildren();
  toolbar("Dataset versions", "Create version", () =>
    editor(
      "Create a draft version",
      [["label", "Version label", "v1"]],
      async (v) => {
        await json(`/datasets/${id}/versions`, { method: "POST", json: v });
        await datasetDetail(id);
      },
    ),
  );
  const versions = await json(`/datasets/${id}/versions`);
  for (const version of versions) {
    const p = panel(`${version.label} · ${version.id}`);
    p.append(badge(version.status));
    if (version.manifest_hash)
      p.append(node("pre", `Manifest SHA-256: ${version.manifest_hash}`));
    if (version.status === "DRAFT" && role !== "viewer")
      p.append(
        button("Upload file", () =>
          editor(
            "Upload dataset file",
            [["file", "File", null, "file"]],
            async (v) => {
              await api(
                `/dataset-versions/${version.id}/files/${encodeURIComponent(v.file.name)}`,
                {
                  method: "PUT",
                  body: v.file,
                  headers: { "Content-Type": "application/octet-stream" },
                },
              );
              await datasetDetail(id);
            },
            "Files are stored by content hash. Seal this version when all files are uploaded.",
          ),
        ),
        button("Seal version", async () => {
          await json(`/dataset-versions/${version.id}/seal`, {
            method: "POST",
          });
          return datasetDetail(id);
        }),
      );
    const files = await json(
      `/dataset-versions/${version.id}/files?limit=1000`,
    );
    table(
      p,
      ["Name", "Bytes", "Actions"],
      files.map((f) => {
        const actions = node("div", undefined, "row-actions");
        actions.append(
          button("Preview", async () => {
            const data = await json(`/dataset-files/${f.id}/preview`);
            const preview = panel(`Preview · ${f.name}`);
            if (data.format === "table")
              table(preview, data.columns, data.rows);
            else preview.append(node("pre", JSON.stringify(data, null, 2)));
            preview.scrollIntoView({ behavior: "smooth" });
          }),
          button("Download", () => download(`/dataset-files/${f.id}`, f.name)),
        );
        return [f.name, f.size, actions];
      }),
    );
  }
  content.append(button("Back to datasets", () => navigate("datasets")));
}
function jobTable(p, rows) {
  table(
    p,
    ["Name", "State", "CPU / memory", "Attempts", "Actions"],
    rows.map((j) => {
      const actions = node("div", undefined, "row-actions");
      actions.append(button("Inspect", () => jobDetail(j.id)));
      if (role !== "viewer") {
        if (["FAILED", "TIMED_OUT", "CANCELLED"].includes(j.status))
          actions.append(
            button("Retry", async () => {
              await json(`/jobs/${j.id}/retry`, { method: "POST" });
              return load();
            }),
          );
        else if (j.status !== "SUCCEEDED")
          actions.append(
            button("Cancel", async () => {
              await json(`/jobs/${j.id}/cancel`, { method: "POST" });
              return load();
            }),
          );
      }
      return [
        j.name,
        badge(j.status),
        `${j.cpu_required} CPU · ${j.memory_required_mb} MB`,
        j.attempts_count,
        actions,
      ];
    }),
  );
}
async function load() {
  if (busy || page === "detail") return;
  busy = true;
  try {
    const current = page;
    let rows;
    if (page === "overview") {
      const [jobs, workers, campaigns, datasets] = await Promise.all([
        json("/jobs?limit=50"),
        json("/workers"),
        json("/campaigns?limit=50"),
        json("/datasets?limit=50"),
      ]);
      if (current !== page) return;
      content.replaceChildren();
      const cards = node("div", undefined, "cards");
      for (const [label, value, detail] of [
        [
          "Live workers",
          workers.filter((w) => w.status === "HEALTHY").length,
          "Registered execution capacity",
        ],
        ["Recent jobs", jobs.length, "Latest 50 submissions"],
        ["Campaigns", campaigns.length, "Latest 50 campaigns"],
        ["Datasets", datasets.length, "Latest 50 datasets"],
      ]) {
        const c = node("div", undefined, "card");
        c.append(
          node("small", label),
          node("span", value, "value"),
          node("small", detail),
        );
        cards.append(c);
      }
      content.append(cards);
      toolbar("Recent activity", "Submit job", newJob);
      jobTable(panel("Recent jobs"), jobs.slice(0, 10));
    } else {
      rows = await json(`/${page}?limit=50&offset=${offset}`);
      if (current !== page) return;
      content.replaceChildren();
      if (page === "jobs") {
        toolbar("Compute jobs", "Submit job", newJob);
        jobTable(panel("Job queue"), rows);
      }
      if (page === "campaigns") {
        toolbar("Campaigns", "New campaign", newCampaign);
        table(
          panel("Parameter studies and workflows"),
          ["Name", "Progress", "State", "Actions"],
          rows.map((c) => {
            const a = node("div", undefined, "row-actions");
            a.append(button("Results", () => campaignDetail(c.id)));
            if (role !== "viewer")
              a.append(
                button("Cancel", async () => {
                  await json(`/campaigns/${c.id}/cancel`, { method: "POST" });
                  return load();
                }),
                button("Retry failed", async () => {
                  const r = await json(`/campaigns/${c.id}/retry`, {
                    method: "POST",
                  });
                  if (r.errors.length)
                    throw Error(
                      `${r.errors.length} jobs could not be retried; inspect API response.`,
                    );
                  return load();
                }),
              );
            return [c.name, `${c.completed} / ${c.total}`, badge(c.status), a];
          }),
        );
      }
      if (page === "datasets") {
        toolbar("Dataset library", "New dataset", () =>
          editor("Create a dataset", [["name", "Name", "My dataset"]], (v) =>
            json("/datasets", { method: "POST", json: v }),
          ),
        );
        table(
          panel("Data catalog"),
          ["Name", "Created", "Actions"],
          rows.map((d) => [
            d.name,
            new Date(d.created_at).toLocaleString(),
            button("Versions & files", () => datasetDetail(d.id)),
          ]),
        );
      }
      if (page === "workers") {
        toolbar("Execution workers");
        table(
          panel("Cluster capacity"),
          [
            "Worker",
            "State",
            "CPU reserved / total",
            "Memory reserved / total",
            "Jobs",
            "Actions",
          ],
          rows.map((w) => [
            w.id,
            badge(w.status),
            `${w.cpu_reserved.toFixed(2)} / ${w.cpu_total}`,
            `${w.memory_reserved_mb} / ${w.memory_total_mb} MB`,
            w.running_jobs,
            role === "admin" && w.status !== "LOST"
              ? button(
                  w.status === "DRAINING" ? "Resume" : "Drain",
                  async () => {
                    await json(
                      `/workers/${w.id}/${w.status === "DRAINING" ? "resume" : "drain"}`,
                      { method: "POST" },
                    );
                    return load();
                  },
                )
              : "—",
          ]),
        );
      }
      if (page !== "workers") pagination(rows);
    }
    $("notice").hidden = true;
  } catch (e) {
    showError(e);
  } finally {
    busy = false;
  }
}
function navigate(next) {
  page = next;
  offset = 0;
  $("title").textContent = next[0].toUpperCase() + next.slice(1);
  document
    .querySelectorAll("nav button")
    .forEach((b) => b.classList.toggle("active", b.dataset.page === next));
  return load();
}
document
  .querySelectorAll("nav button")
  .forEach((b) => (b.onclick = () => navigate(b.dataset.page)));
$("refresh").onclick = () =>
  Promise.resolve(page === "detail" ? detailReload() : load()).catch(showError);
$("close").onclick = $("cancel-dialog").onclick = () => modal.close();
$("access").onclick = () => $("login").showModal();
$("login-form").onsubmit = async (e) => {
  e.preventDefault();
  token = $("key").value.trim();
  try {
    const session = await json("/session");
    role = session.role;
    sessionStorage.setItem("strata-key", token);
    $("login").close();
    $("connection").textContent = session.authentication_enabled
      ? `Connected · ${role}`
      : "Local development · admin";
    page = "overview";
    await navigate("overview");
  } catch (e) {
    $("login-error").textContent = e.message;
  }
};
(async () => {
  try {
    const session = await json("/session");
    role = session.role;
    $("connection").textContent = session.authentication_enabled
      ? `Connected · ${role}`
      : "Local development · admin";
    await navigate("overview");
  } catch (e) {
    showError(e);
  }
})();
setInterval(() => {
  if (!document.hidden && !modal.open && !$("login").open) load();
}, 5000);
