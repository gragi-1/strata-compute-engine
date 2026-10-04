"use strict";
const $ = (id) => document.getElementById(id);
let token = sessionStorage.getItem("strata-key") || "",
  page = "overview",
  offset = 0,
  busy = false,
  role = "viewer";
let individual = false,
  oidcEnabled = false,
  currentUser = null,
  project = sessionStorage.getItem("strata-project") || "";
const content = $("content"),
  modal = $("modal");
let detailReload = null;
let liveJobLogs = null,
  pollingJobLogs = false;
let runtimeWatch = null,
  runtimePolling = false;
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
  if (individual && project) headers["X-Strata-Project"] = project;
  if (options.json !== undefined) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.json);
  }
  const method = (options.method || "GET").toUpperCase();
  const route = path.split("?")[0];
  const reusable =
    options.body === undefined ||
    typeof options.body === "string" ||
    options.body instanceof ArrayBuffer;
  const safe =
    reusable &&
    (["GET", "HEAD", "OPTIONS"].includes(method) ||
      (method === "POST" &&
        (/^\/dataset-files\/[^/]+\/(query|statistics)$/.test(route) ||
          /^\/uploads\/[^/]+\/complete$/.test(route) ||
          (headers["Idempotency-Key"] &&
            /^\/(jobs|campaigns|workflows|experiments\/[^/]+\/runs|experiment-runs\/[^/]+\/replay|compute-groups(?:\/[^/]+\/retry)?|interactive-sessions(?:\/[^/]+\/(?:cells|proxy))?)$/.test(
              route,
            )))) ||
      (method === "PUT" &&
        headers["X-Chunk-SHA256"] &&
        /^\/uploads\/[^/]+\/chunks\/\d+$/.test(route)));
  const deadline = Date.now() + 10000;
  let r;
  for (let attempt = 0; attempt <= 3; attempt++) {
    let delay = Math.min(2000, 250 * 2 ** attempt) + Math.random() * 100;
    try {
      r = await fetch(path, { ...options, headers });
    } catch (error) {
      if (
        !safe ||
        error.name === "AbortError" ||
        attempt === 3 ||
        Date.now() + delay >= deadline
      )
        throw error;
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    }
    if (!safe || ![429, 502, 503, 504].includes(r.status) || attempt === 3)
      break;
    const retryAfter = r.headers.get("Retry-After");
    if (retryAfter) {
      const retryAt = /^\d+$/.test(retryAfter)
        ? Number(retryAfter) * 1000
        : Date.parse(retryAfter) - Date.now();
      if (Number.isFinite(retryAt)) delay = Math.max(delay, retryAt);
    }
    if (Date.now() + delay >= deadline) break;
    await r.body?.cancel();
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
  if (r.status === 401) {
    if (!$("login").open) $("login").showModal();
    throw Error(
      individual
        ? "Sign in to continue."
        : "Enter a valid API key to continue.",
    );
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
  wrap.tabIndex = 0;
  wrap.setAttribute("role", "region");
  wrap.setAttribute(
    "aria-label",
    `${parent.querySelector("h2")?.textContent || "Records"} table`,
  );
  columns.forEach((c) => {
    const column = node("th", c);
    column.scope = "col";
    tr.append(column);
  });
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
  // Preserve a submission's key after an ambiguous response and another Save click.
  const submissionKey = crypto.randomUUID();
  $("dialog-title").textContent = title;
  $("dialog-help").textContent = help;
  $("fields").replaceChildren();
  $("form-error").textContent = "";
  const controls = {};
  fields.forEach(([key, label, value, type = "text", required = true]) => {
    const l = node("label", label),
      input = node(type === "json" ? "textarea" : "input");
    if (type !== "json") input.type = type;
    if (type === "checkbox") input.checked = Boolean(value);
    input.name = key;
    if (type !== "file")
      input.value =
        type === "json" ? JSON.stringify(value, null, 2) : String(value);
    input.required = required;
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
          type === "checkbox"
            ? el.checked
            : type === "json"
              ? JSON.parse(el.value)
              : type === "number"
                ? Number(el.value)
                : type === "file"
                  ? el.files[0]
                  : el.value;
      });
      await submit(values, submissionKey);
      modal.close();
      await (page === "detail" && detailReload ? detailReload() : load());
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
async function approvedSample() {
  const config = await json("/execution/config");
  if (!config.approved_images.length)
    throw new Error(
      "No execution images are approved. Ask the platform administrator to configure the image allowlist.",
    );
  const example = structuredClone(sample);
  const bundled = config.approved_images.find((image) =>
    /^strata\/python-workloads[:@]/.test(image),
  );
  example.image = bundled || config.approved_images[0];
  if (!bundled) example.command = ["/path/to/your/program"];
  return example;
}
async function newJob() {
  const specification = await approvedSample();
  editor(
    "Submit a job",
    [["specification", "Job specification", specification, "json"]],
    async (v, submissionKey) =>
      json("/jobs", {
        method: "POST",
        json: v.specification,
        headers: { "Idempotency-Key": submissionKey },
      }),
    "Commands are argument arrays. Add sealed dataset inputs as {version_id, alias}; they appear at /inputs/<alias>.",
  );
}
async function newComputeGroup() {
  const job = await approvedSample();
  job.resources = { cpu: 0.5, memory_mb: 128 };
  job.max_retries = 0;
  job.command = [
    "python",
    "-c",
    "import sys,json; sys.path.insert(0,'/output/.strata'); from runtime import Collective; c=Collective(); total=c.reduce([float(c.rank+1)])[0]; c.barrier(); open('/output/reduction.json','w').write(json.dumps({'rank':c.rank,'total':total}))",
  ];
  editor(
    "Create a computation group",
    [
      ["name", "Name", "Cooperating computation"],
      ["nodes", "Distinct workers", 2, "number"],
      ["job", "Per-rank job", job, "json"],
    ],
    (values, key) =>
      json("/compute-groups", {
        method: "POST",
        json: values,
        headers: { "Idempotency-Key": key },
      }),
    "All ranks reserve distinct workers together. Import Collective from /output/.strata for ordered reductions and barriers. A rank failure cancels the group.",
  );
}
async function computeGroupDetail(id) {
  runtimeWatch = null;
  page = "detail";
  detailReload = () => computeGroupDetail(id);
  const group = await json(`/compute-groups/${id}`);
  content.replaceChildren();
  $("title").textContent = "Computation group";
  toolbar(group.name, "Back to groups", () => navigate("compute-groups"));
  content.append(
    badge(group.status),
    node(
      "p",
      `${group.nodes} ranks · ${group.next_sequence} completed collective steps`,
    ),
  );
  table(
    panel("Participants"),
    ["Rank", "Job", "State", "Actions"],
    group.members.map((j) => [
      j.rank,
      j.name,
      badge(j.status),
      button("Inspect execution", () => jobDetail(j.id)),
    ]),
  );
  if (role !== "viewer") {
    if (!["FAILED", "CANCELLED", "SUCCEEDED"].includes(group.status))
      content.append(
        button("Cancel entire group", async () => {
          await json(`/compute-groups/${id}/cancel`, { method: "POST" });
          await computeGroupDetail(id);
        }),
      );
    if (
      group.members.every((j) =>
        ["FAILED", "CANCELLED", "TIMED_OUT", "SUCCEEDED"].includes(j.status),
      )
    ) {
      const key = crypto.randomUUID();
      content.append(
        button("Run a fresh group", async () => {
          const fresh = await json(`/compute-groups/${id}/retry`, {
            method: "POST",
            headers: { "Idempotency-Key": key },
          });
          await computeGroupDetail(fresh.id);
        }),
      );
    }
  }
}
async function newInteractiveSession(kind = "python") {
  const job = await approvedSample();
  job.resources = { cpu: 0.5, memory_mb: 128 };
  job.max_retries = 0;
  job.timeout_seconds = 3600;
  job.command =
    kind === "python"
      ? ["python", "-c", "pass"]
      : [
          "python",
          "-m",
          "http.server",
          "8080",
          "--bind",
          "127.0.0.1",
          "--directory",
          "/output",
        ];
  editor(
    kind === "python" ? "Start a Python notebook" : "Start an HTTP service",
    [
      ["job", "Runtime resources, image and inputs", job, "json"],
      ["idle_seconds", "Idle timeout in seconds", 900, "number"],
      ...(kind === "service"
        ? [["service_port", "Container-local service port", 8080, "number"]]
        : []),
    ],
    (values, key) =>
      json("/interactive-sessions", {
        method: "POST",
        json: { ...values, kind },
        headers: { "Idempotency-Key": key },
      }),
    "The approved image must contain Python. Sessions keep state while running, share project quotas and stop on idle or execution deadlines. Service access uses bounded authenticated messages.",
  );
}
function sessionCellRows(target, rows, kind) {
  target.replaceChildren(node("h2", "Cells and requests · first 200"));
  table(
    target,
    ["Sequence", "State", "Input", "Output"],
    rows.map((c) => {
      const input = node(
        "pre",
        kind === "python"
          ? c.payload.code
          : `${c.payload.method} ${c.payload.path}`,
      );
      let text = (c.result.stdout || "") + (c.result.stderr || "");
      if (c.result.body_base64)
        text = new TextDecoder().decode(
          Uint8Array.from(atob(c.result.body_base64), (x) => x.charCodeAt(0)),
        );
      if (c.result.error) text += `\n${c.result.error}`;
      return [
        c.sequence + 1,
        badge(c.status),
        input,
        node("pre", text || "Waiting"),
      ];
    }),
  );
}
async function interactiveSessionDetail(id) {
  runtimeWatch = null;
  page = "detail";
  detailReload = () => interactiveSessionDetail(id);
  const [session, cells] = await Promise.all([
    json(`/interactive-sessions/${id}`),
    json(`/interactive-sessions/${id}/cells?limit=200`),
  ]);
  content.replaceChildren();
  $("title").textContent =
    session.kind === "python" ? "Python notebook" : "HTTP service";
  toolbar(session.job.name, "Back to sessions", () =>
    navigate("interactive-sessions"),
  );
  const state = node(
    "p",
    `${session.job.status} · ${session.job.cpu_required} CPU · ${session.job.memory_required_mb} MB · idle limit ${session.idle_seconds}s`,
  );
  content.append(
    state,
    button("Inspect runtime job", () => jobDetail(session.job_id)),
  );
  const closed = [
    "FAILED",
    "SUCCEEDED",
    "CANCELLED",
    "TIMED_OUT",
    "CANCEL_REQUESTED",
  ].includes(session.job.status);
  if (role !== "viewer" && !closed) {
    content.append(
      button("Stop session", async () => {
        await json(`/interactive-sessions/${id}/stop`, { method: "POST" });
        await interactiveSessionDetail(id);
      }),
    );
    const form = node("form", undefined, "runtime-editor"),
      label = node(
        "label",
        session.kind === "python" ? "Python code" : "Service path",
      ),
      input = node(session.kind === "python" ? "textarea" : "input");
    input.name = "runtime-input";
    input.required = true;
    input.value = session.kind === "python" ? "x = 21\nprint(x * 2)" : "/";
    if (session.kind === "python") input.rows = 6;
    label.append(input);
    form.append(label);
    const method = node("select"),
      body = node("textarea");
    if (session.kind === "service") {
      const methods = node("label", "HTTP method"),
        payload = node("label", "Request body (optional)");
      ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"].forEach((m) => {
        const option = node("option", m);
        option.value = m;
        method.append(option);
      });
      methods.append(method);
      payload.append(body);
      form.append(methods, payload);
    }
    const send = node(
        "button",
        session.kind === "python" ? "Run cell" : "Send request",
        "primary",
      ),
      error = node("p");
    send.type = "submit";
    error.setAttribute("role", "status");
    form.append(send, error);
    let key = crypto.randomUUID();
    form.onsubmit = async (e) => {
      e.preventDefault();
      send.disabled = true;
      error.textContent = "";
      try {
        await json(
          `/interactive-sessions/${id}/${session.kind === "python" ? "cells" : "proxy"}`,
          {
            method: "POST",
            headers: { "Idempotency-Key": key },
            json:
              session.kind === "python"
                ? { code: input.value }
                : { path: input.value, method: method.value, body: body.value },
          },
        );
        key = crypto.randomUUID();
        await pollInteractiveSession();
      } catch (e) {
        error.textContent = e.message;
      } finally {
        send.disabled = false;
      }
    };
    content.append(form);
  }
  if (session.kind === "python")
    content.append(
      button("Download notebook", () =>
        download(`/interactive-sessions/${id}/notebook`, `strata-${id}.ipynb`),
      ),
    );
  const history = panel("Cells and requests · first 200");
  sessionCellRows(history, cells, session.kind);
  runtimeWatch = { id, kind: session.kind, state, history };
}
async function pollInteractiveSession() {
  if (!runtimeWatch || page !== "detail" || runtimePolling) return;
  const watch = runtimeWatch;
  runtimePolling = true;
  try {
    const [session, cells] = await Promise.all([
      json(`/interactive-sessions/${watch.id}`),
      json(`/interactive-sessions/${watch.id}/cells?limit=200`),
    ]);
    if (runtimeWatch !== watch || page !== "detail") return;
    watch.state.textContent = `${session.job.status} · ${session.job.cpu_required} CPU · ${session.job.memory_required_mb} MB · idle limit ${session.idle_seconds}s`;
    sessionCellRows(watch.history, cells, watch.kind);
  } catch (e) {
    showError(e);
  } finally {
    runtimePolling = false;
  }
}
setInterval(() => pollInteractiveSession(), 2000);
async function newSchedule() {
  const template = await approvedSample();
  editor(
    "Create a periodic job",
    [
      ["name", "Name", "Daily experiment"],
      ["cron", "Cron expression", "0 9 * * *"],
      ["timezone", "Timezone", "UTC"],
      [
        "catch_up",
        "Run every missed occurrence after a restart",
        false,
        "checkbox",
        false,
      ],
      ["job", "Job template", template, "json"],
    ],
    (values) => json("/schedules", { method: "POST", json: values }),
    "Five cron fields: minute, hour, day, month, weekday. By default only the latest missed occurrence runs. Each execution uses the owner's current permissions and project quotas.",
  );
}
async function newWorkflow() {
  const template = await approvedSample();
  editor(
    "Create a workflow",
    [
      [
        "specification",
        "Workflow specification",
        {
          name: "Compute pipeline",
          nodes: {
            compute: template,
            cleanup: {
              ...template,
              name: "Cleanup",
              depends_on: ["compute"],
              dependency_policy: "all_terminal",
            },
          },
          expansions: {},
        },
        "json",
      ],
    ],
    (values, submissionKey) =>
      json("/workflows", {
        method: "POST",
        json: values.specification,
        headers: { "Idempotency-Key": submissionKey },
      }),
    "Nodes refer to dependencies by name. Optional expansions read a successful node's JSON parameter artifact, create jobs from a template and provide a join node for downstream work.",
  );
}
async function newWebhook() {
  const targets = await json("/webhook-targets");
  if (!targets.length)
    throw Error(
      "The platform administrator must configure an approved webhook target and signing secret first.",
    );
  editor(
    "Create an event webhook",
    [
      ["name", "Name", "Experiment completion"],
      ["target", "Approved target", targets[0]],
      [
        "events",
        "Event types",
        ["JOB_SUCCEEDED", "JOB_FAILED", "JOB_CANCELLED", "JOB_TIMED_OUT"],
        "json",
      ],
    ],
    (values) => json("/webhooks", { method: "POST", json: values }),
    `Approved targets: ${targets.join(", ")}. Deliveries are signed and retried; the receiver must deduplicate delivery IDs.`,
  );
}
async function webhookDetail(webhook, deliveryOffset = 0) {
  page = "detail";
  detailReload = () => webhookDetail(webhook, deliveryOffset);
  const deliveries = await json(
    `/webhooks/${webhook.id}/deliveries?limit=50&offset=${deliveryOffset}`,
  );
  content.replaceChildren();
  $("title").textContent = webhook.name;
  const summary = panel("Event receiver");
  summary.append(
    node(
      "p",
      `${webhook.target} · ${webhook.enabled ? "Enabled" : "Disabled"} · ${webhook.events.join(", ")}`,
    ),
  );
  if (role === "admin")
    summary.append(
      button(
        webhook.enabled ? "Disable webhook" : "Enable webhook",
        async () => {
          const updated = await json(
            `/webhooks/${webhook.id}/${webhook.enabled ? "disable" : "enable"}`,
            { method: "POST" },
          );
          return webhookDetail(updated, deliveryOffset);
        },
      ),
    );
  table(
    panel("Delivery history"),
    ["Delivery", "Event", "State", "Attempts", "Last response", "Actions"],
    deliveries.map((delivery) => [
      delivery.id,
      `${delivery.payload.type} · ${delivery.payload.job.id}`,
      badge(delivery.status),
      delivery.attempts,
      delivery.last_error || delivery.last_status || "—",
      role === "admin" && webhook.enabled && delivery.status === "DEAD"
        ? button("Retry delivery", async () => {
            await json(
              `/webhooks/${webhook.id}/deliveries/${delivery.id}/retry`,
              { method: "POST" },
            );
            return webhookDetail(webhook, deliveryOffset);
          })
        : "—",
    ]),
  );
  content.append(button("Back to webhooks", () => navigate("webhooks")));
  if (deliveryOffset)
    content.append(
      button("Previous deliveries", () =>
        webhookDetail(webhook, Math.max(0, deliveryOffset - 50)),
      ),
    );
  if (deliveries.length === 50)
    content.append(
      button("Next deliveries", () =>
        webhookDetail(webhook, deliveryOffset + 50),
      ),
    );
}
async function scheduleDetail(id, fireOffset = 0) {
  page = "detail";
  detailReload = () => scheduleDetail(id, fireOffset);
  const schedule = await json(`/schedules/${id}`);
  const fires = await json(
    `/schedules/${id}/fires?limit=50&offset=${fireOffset}`,
  );
  content.replaceChildren();
  $("title").textContent = schedule.name;
  const summary = panel("Periodic execution");
  summary.append(
    node(
      "p",
      `${schedule.cron} · ${schedule.timezone} · ${schedule.enabled ? "Enabled" : "Paused"}`,
    ),
    node(
      "p",
      `Next occurrence: ${new Date(schedule.next_run_at).toLocaleString()}`,
    ),
    node(
      "p",
      schedule.catch_up
        ? "Every missed occurrence will run, with bounded catch-up."
        : "Only the latest missed occurrence will run.",
    ),
  );
  if (schedule.last_error)
    summary.append(node("p", schedule.last_error, "empty"));
  summary.append(node("pre", JSON.stringify(schedule.specification, null, 2)));
  if (role !== "viewer")
    summary.append(
      button(
        schedule.enabled ? "Pause schedule" : "Resume schedule",
        async () => {
          await json(
            `/schedules/${id}/${schedule.enabled ? "pause" : "resume"}`,
            { method: "POST" },
          );
          return scheduleDetail(id, fireOffset);
        },
      ),
    );
  table(
    panel("Execution history"),
    ["Scheduled occurrence", "Created", "Job"],
    fires.map((fire) => [
      new Date(fire.scheduled_at).toLocaleString(),
      new Date(fire.created_at).toLocaleString(),
      button(fire.job_id, () => jobDetail(fire.job_id)),
    ]),
  );
  content.append(button("Back to schedules", () => navigate("schedules")));
  if (fireOffset)
    content.append(
      button("Previous occurrences", () =>
        scheduleDetail(id, Math.max(0, fireOffset - 50)),
      ),
    );
  if (fires.length === 50)
    content.append(
      button("Next occurrences", () => scheduleDetail(id, fireOffset + 50)),
    );
}
async function newCampaign() {
  const template = await approvedSample();
  if (template.command.includes("--seed"))
    template.command[template.command.length - 1] = "${seed}";
  else template.command.push("${seed}");
  editor(
    "Create a parameter campaign",
    [
      ["name", "Name", "Parameter study"],
      ["template", "Job template", template, "json"],
      ["matrix", "Parameter values", { seed: [1, 2, 3] }, "json"],
      ["repeats", "Repetitions", 1, "number"],
    ],
    (v, submissionKey) =>
      json("/campaigns", {
        method: "POST",
        json: v,
        headers: { "Idempotency-Key": submissionKey },
      }),
    "Use ${parameter} in individual command arguments. Every combination becomes an independent job.",
  );
}
async function jobDetail(id) {
  runtimeWatch = null;
  liveJobLogs = null;
  page = "detail";
  detailReload = () => jobDetail(id);
  $("title").textContent = "Job details";
  content.replaceChildren();
  const [job, attempts, artifacts, events, logs] = await Promise.all([
    json(`/jobs/${id}`),
    json(`/jobs/${id}/attempts`),
    json(`/jobs/${id}/artifacts`),
    json(`/jobs/${id}/events`),
    json(`/jobs/${id}/log-snapshot`),
  ]);
  toolbar(job.name, "Back to jobs", () => navigate("jobs"));
  const p = panel("Execution");
  const statusBadge = badge(job.status);
  const specification = node("dl", undefined, "execution-summary");
  const startedValue = node(
    "dd",
    job.started_at ? new Date(job.started_at).toLocaleString() : "Waiting",
  );
  const finishedValue = node(
    "dd",
    job.finished_at
      ? new Date(job.finished_at).toLocaleString()
      : "In progress",
  );
  const retriesValue = node(
    "dd",
    `${job.retry_count} used / ${job.max_retries} allowed`,
  );
  [
    ["Image", job.image],
    [
      "Resources",
      `${job.cpu_required} CPU · ${job.memory_required_mb} MB${job.gpu_required ? ` · ${job.gpu_required} GPU` : ""}`,
    ],
    ["Created", new Date(job.created_at).toLocaleString()],
    ["Started", startedValue],
    ["Finished", finishedValue],
    ["Timeout", `${job.timeout_seconds} seconds`],
    ["Retries", retriesValue],
  ].forEach(([label, value]) => {
    specification.append(
      node("dt", label),
      value instanceof Node ? value : node("dd", value),
    );
  });
  const definition = node("details");
  const originalDefinition = Object.fromEntries(
    Object.entries(job).filter(
      ([key]) =>
        ![
          "status",
          "created_at",
          "scheduled_at",
          "started_at",
          "finished_at",
          "attempts_count",
          "retry_count",
        ].includes(key),
    ),
  );
  definition.append(
    node("summary", "Full job specification & identifiers"),
    node("pre", JSON.stringify(originalDefinition, null, 2)),
  );
  p.append(
    statusBadge,
    specification,
    node("h3", "Command"),
    node("pre", job.command.join("\n")),
    definition,
  );
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
  const logsPanel = panel("Live logs"),
    logText = node("pre", logs.text || "No logs yet.");
  logText.tabIndex = 0;
  const toggle = node("input");
  toggle.type = "checkbox";
  toggle.checked = true;
  const label = node("label");
  label.append(toggle, document.createTextNode(" Follow the latest attempt"));
  const logState = node("p", `Attempt ${logs.attempt ?? "—"} · ${logs.status}`);
  logState.setAttribute("role", "status");
  logsPanel.append(label, logState, logText);
  liveJobLogs = {
    id,
    revision: logs.revision,
    logText,
    toggle,
    logState,
    statusBadge,
    startedValue,
    finishedValue,
    retriesValue,
    maxRetries: job.max_retries,
    attemptsPanel: a,
    artifactsPanel: f,
    eventsPanel: null,
    attempt: logs.attempt,
    status: logs.status,
  };
  liveJobLogs.eventsPanel = panel("Event history");
  table(
    liveJobLogs.eventsPanel,
    ["Time", "Event", "Reason"],
    events.map((event) => [
      new Date(event.created_at).toLocaleString(),
      event.kind.replaceAll("_", " ").toLowerCase(),
      event.details.reason || "—",
    ]),
  );
}
async function gpuDetail(worker) {
  page = "detail";
  detailReload = () => gpuDetail(worker);
  $("title").textContent = `Devices · ${worker.id}`;
  const devices = await json(`/workers/${worker.id}/gpus`);
  content.replaceChildren();
  toolbar("Discovered NVIDIA devices", "Back to workers", () =>
    navigate("workers"),
  );
  table(
    panel("Exclusive GPU inventory"),
    ["Device", "Model", "Memory", "State", "Attempt"],
    devices.map((device) => [
      device.id,
      device.name,
      `${device.memory_mb} MiB`,
      device.enabled
        ? device.allocated_to
          ? "Reserved"
          : "Available"
        : "Unavailable",
      device.allocated_to || "—",
    ]),
  );
}

async function poolDetail(pool) {
  page = "detail";
  detailReload = () => poolDetail(pool);
  $("title").textContent = `Pool · ${pool.id}`;
  content.replaceChildren();
  const rows = await json(`/cluster/pools/${pool.id}/workers`);
  toolbar(`Worker pool: ${pool.id}`, "Back to workers", () =>
    navigate("workers"),
  );
  table(
    panel("Provisioning history (latest 100 agents)"),
    ["Worker", "Phase", "Created", "Removed", "Last error"],
    rows.map((row) => [
      row.worker_id,
      row.phase,
      new Date(row.created_at).toLocaleString(),
      row.removed_at ? new Date(row.removed_at).toLocaleString() : "—",
      row.last_error || "—",
    ]),
  );
}
async function pollJobLogs() {
  const live = liveJobLogs;
  if (
    !live ||
    pollingJobLogs ||
    page !== "detail" ||
    !live.logText.isConnected ||
    !live.toggle.checked ||
    document.hidden ||
    $("login").open ||
    ["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"].includes(live.status)
  )
    return;
  pollingJobLogs = true;
  try {
    const snapshot = await json(
      `/jobs/${live.id}/log-snapshot?cursor=${live.revision}`,
    );
    if (live !== liveJobLogs || !live.logText.isConnected) return;
    if (snapshot.changed) {
      const stateChanged =
        snapshot.status !== live.status || snapshot.attempt !== live.attempt;
      live.revision = snapshot.revision;
      live.logText.textContent = snapshot.text || "No logs yet.";
      live.logState.textContent = `Attempt ${snapshot.attempt ?? "—"} · ${snapshot.status}`;
      live.statusBadge.textContent = snapshot.status;
      live.statusBadge.className = `badge ${snapshot.status}`;
      live.status = snapshot.status;
      live.attempt = snapshot.attempt;
      live.startedValue.textContent = snapshot.started_at
        ? new Date(snapshot.started_at).toLocaleString()
        : "Waiting";
      live.finishedValue.textContent = snapshot.finished_at
        ? new Date(snapshot.finished_at).toLocaleString()
        : "In progress";
      live.retriesValue.textContent = `${snapshot.retry_count} used / ${live.maxRetries} allowed`;
      if (stateChanged) {
        const [attempts, artifacts, events] = await Promise.all([
          json(`/jobs/${live.id}/attempts`),
          json(`/jobs/${live.id}/artifacts`),
          json(`/jobs/${live.id}/events`),
        ]);
        if (live !== liveJobLogs || !live.logText.isConnected) return;
        live.attemptsPanel.replaceChildren(node("h2", "Attempt history"));
        table(
          live.attemptsPanel,
          ["Attempt", "Worker", "Status", "Reason"],
          attempts.map((attempt) => [
            attempt.number,
            attempt.worker_id,
            badge(attempt.status),
            attempt.reason,
          ]),
        );
        live.artifactsPanel.replaceChildren(node("h2", "Artifacts"));
        table(
          live.artifactsPanel,
          ["File", "Bytes", "SHA-256", "Download"],
          artifacts.map((artifact) => [
            artifact.name,
            artifact.size,
            artifact.sha256,
            button("Download", () => download(artifact.uri, artifact.name)),
          ]),
        );
        live.eventsPanel.replaceChildren(node("h2", "Event history"));
        table(
          live.eventsPanel,
          ["Time", "Event", "Reason"],
          events.map((event) => [
            new Date(event.created_at).toLocaleString(),
            event.kind.replaceAll("_", " ").toLowerCase(),
            event.details.reason || "—",
          ]),
        );
      }
    }
  } catch (error) {
    showError(error);
  } finally {
    pollingJobLogs = false;
  }
}
setInterval(pollJobLogs, 2000);
async function experimentDetail(experiment, runOffset = 0) {
  page = "detail";
  detailReload = () => experimentDetail(experiment, runOffset);
  $("title").textContent = experiment.name;
  content.replaceChildren();
  toolbar("Experiment runs", "Submit run", () =>
    approvedSample().then((template) =>
      editor(
        "Submit an experiment run",
        [
          ["job", "Job specification", template, "json"],
          ["metadata", "Run metadata", { seed: 42 }, "json"],
        ],
        (values, submissionKey) =>
          json(`/experiments/${experiment.id}/runs`, {
            method: "POST",
            json: values,
            headers: { "Idempotency-Key": submissionKey },
          }),
        "Datasets must be sealed. Resolved image and actual input hashes are captured when execution starts.",
      ),
    ),
  );
  const runs = await json(
    `/experiments/${experiment.id}/runs?limit=50&offset=${runOffset}`,
  );
  const selected = new Set();
  const list = panel("Select runs to compare");
  table(
    list,
    ["Compare", "Run", "State", "Metrics", "Actions"],
    runs.map((run) => {
      const select = node("input");
      select.type = "checkbox";
      select.setAttribute("aria-label", `Compare run ${run.id}`);
      select.onchange = () =>
        select.checked ? selected.add(run.id) : selected.delete(run.id);
      const actions = node("div", undefined, "row-actions");
      actions.append(button("Inspect run", () => runDetail(run.id)));
      return [
        select,
        run.id,
        badge(run.status),
        JSON.stringify(run.metrics),
        actions,
      ];
    }),
  );
  list.append(
    button("Compare selected runs", async () => {
      if (selected.size < 1 || selected.size > 20)
        throw Error("Select 1–20 runs to compare.");
      const results = await json(
        `/experiment-runs/compare?ids=${Array.from(selected).join(",")}`,
      );
      const metricNames = Array.from(
        new Set(results.flatMap((run) => Object.keys(run.metrics))),
      ).sort();
      table(
        panel("Metric comparison"),
        ["Run", "State", ...metricNames],
        results.map((run) => [
          run.id,
          badge(run.status),
          ...metricNames.map((name) => run.metrics[name] ?? "—"),
        ]),
      );
    }),
  );
  content.append(button("Back to experiments", () => navigate("experiments")));
  if (runOffset > 0)
    content.append(
      button("Previous runs", () =>
        experimentDetail(experiment, Math.max(0, runOffset - 50)),
      ),
    );
  if (runs.length === 50)
    content.append(
      button("Next runs", () => experimentDetail(experiment, runOffset + 50)),
    );
}
async function runDetail(id) {
  page = "detail";
  detailReload = () => runDetail(id);
  $("title").textContent = "Experiment run";
  content.replaceChildren();
  const run = await json(`/experiment-runs/${id}`);
  const info = panel("Run provenance");
  info.append(
    badge(run.status),
    node("pre", JSON.stringify(run, null, 2)),
    button("Inspect job", () => jobDetail(run.job_id)),
    button("Export run JSON", () =>
      download(`/experiment-runs/${id}`, `run-${id}.json`),
    ),
  );
  if (role !== "viewer")
    info.append(
      button("Record metrics", () =>
        editor(
          "Record immutable summary metrics",
          [
            [
              "values",
              "Metric names and numeric values",
              { error: 0.01 },
              "json",
            ],
          ],
          (values) =>
            json(`/experiment-runs/${id}/metrics`, {
              method: "PUT",
              json: values,
            }),
        ),
      ),
      button("Replay or resume", () =>
        editor(
          "Replay or resume this run",
          [["options", "Replay options", { checkpoints: [] }, "json"]],
          (values, submissionKey) =>
            json(`/experiment-runs/${id}/replay`, {
              method: "POST",
              json: values.options,
              headers: { "Idempotency-Key": submissionKey },
            }).then((created) => runDetail(created.id)),
          "Requires a terminal attempt with a recorded image. Checkpoints use its artifact ID, job ID, name and alias; add command when the workload needs resume arguments.",
        ),
      ),
    );
  content.append(button("Back to experiments", () => navigate("experiments")));
}
async function campaignDetail(id) {
  page = "detail";
  detailReload = () => campaignDetail(id);
  $("title").textContent = "Campaign results";
  content.replaceChildren();
  const [campaign, jobs, results, expansions] = await Promise.all([
    json(`/campaigns/${id}`),
    json(`/campaigns/${id}/jobs?limit=1000`),
    json(`/campaigns/${id}/results`),
    json(`/campaigns/${id}/expansions`),
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
  if (expansions.length)
    table(
      panel("Dynamic expansions"),
      ["Name", "State", "Generated jobs", "Manifest hash", "Details"],
      expansions.map((expansion) => [
        expansion.name,
        badge(expansion.status),
        expansion.generated_count,
        expansion.manifest_sha256,
        expansion.last_error || "—",
      ]),
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
              await uploadFile(version.id, v.file);
              await datasetDetail(id);
            },
            "Uploads use verified chunks and can resume after an interruption. Select the same file again to continue. Seal after all transfers finish.",
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
          button("Explore", () => exploreDataset(f)),
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
async function uploadFile(versionId, file) {
  const upload = await json(`/dataset-versions/${versionId}/uploads`, {
    method: "POST",
    json: {
      name: file.name,
      total_bytes: file.size,
    },
  });
  $("dialog-help").textContent = `Upload ${upload.id} · verifying chunks…`;
  for (let offset = 0; offset < file.size; offset += upload.chunk_bytes) {
    const chunk = await file
      .slice(offset, offset + upload.chunk_bytes)
      .arrayBuffer();
    const digest = Array.from(
      new Uint8Array(await crypto.subtle.digest("SHA-256", chunk)),
    )
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    // Replayed chunks are checked against stored hashes, including an already completed transfer.
    await api(`/uploads/${upload.id}/chunks/${offset}`, {
      method: "PUT",
      body: chunk,
      headers: { "X-Chunk-SHA256": digest },
    });
    $("dialog-help").textContent =
      `Upload ${upload.id} · ${Math.min(offset + chunk.byteLength, file.size)} / ${file.size} bytes verified`;
  }
  return json(`/uploads/${upload.id}/complete`, { method: "POST" });
}
async function exploreDataset(file, query = { limit: 50, offset: 0 }) {
  page = "detail";
  detailReload = () => exploreDataset(file, query);
  const result = await json(`/dataset-files/${file.id}/query`, {
    method: "POST",
    json: query,
  });
  $("title").textContent = `Explore · ${file.name}`;
  content.replaceChildren();
  const controls = panel("Filter and sort");
  function select(label, options, current = "") {
    const control = node("select"),
      wrapper = node("label", label);
    for (const [value, text] of options) {
      const option = node("option", text);
      option.value = value;
      option.selected = value === current;
      control.append(option);
    }
    wrapper.append(control);
    controls.append(wrapper);
    return control;
  }
  const names = Object.keys(result.types),
    previous = query.filters?.[0];
  const column = select(
    "Filter column",
    [["", "All rows"], ...names.map((n) => [n, n])],
    previous?.column || "",
  );
  const operator = select(
    "Comparison",
    [
      ["eq", "Equals"],
      ["ne", "Does not equal"],
      ["contains", "Contains"],
      ["lt", "Less than"],
      ["lte", "At most"],
      ["gt", "Greater than"],
      ["gte", "At least"],
      ["is_null", "Is missing"],
      ["not_null", "Is present"],
    ],
    previous?.operator || "eq",
  );
  const value = node("input"),
    valueLabel = node("label", "Filter value");
  value.value = previous?.value ?? "";
  valueLabel.append(value);
  controls.append(valueLabel);
  const sorting = select(
    "Sort column",
    [["", "File order"], ...names.map((n) => [n, n])],
    query.sort_by || "",
  );
  const direction = select(
    "Sort direction",
    [
      ["ascending", "Ascending"],
      ["descending", "Descending"],
    ],
    query.descending ? "descending" : "ascending",
  );
  controls.append(
    button("Apply", () =>
      exploreDataset(file, {
        limit: 50,
        offset: 0,
        filters: column.value
          ? [
              {
                column: column.value,
                operator: operator.value,
                value: value.value,
              },
            ]
          : [],
        sort_by: sorting.value || null,
        descending: direction.value === "descending",
      }),
    ),
  );
  const data = panel(
    `Rows ${result.offset + (result.rows.length ? 1 : 0)}–${result.offset + result.rows.length}`,
  );
  table(data, result.columns, result.rows);
  if (result.truncated_cells)
    data.append(
      node(
        "p",
        "Some cells are shortened to keep this page bounded. Download the source for complete values.",
      ),
    );
  if (query.offset)
    data.append(
      button("Previous page", () =>
        exploreDataset(file, {
          ...query,
          offset: Math.max(0, query.offset - query.limit),
        }),
      ),
    );
  if (result.next_offset !== null)
    data.append(
      button("Next page", () =>
        exploreDataset(file, { ...query, offset: result.next_offset }),
      ),
    );
  const statistics = panel("Statistics over all matching rows");
  statistics.append(
    button("Calculate statistics", async () => {
      const stats = await json(`/dataset-files/${file.id}/statistics`, {
        method: "POST",
        json: query,
      });
      statistics.replaceChildren(
        node("h2", `Statistics · ${stats.rows} matching rows`),
      );
      table(
        statistics,
        [
          "Column",
          "Present",
          "Missing",
          "Minimum",
          "Maximum",
          "Mean",
          "Standard deviation",
        ],
        Object.entries(stats.columns).map(([name, s]) => [
          name,
          s.count,
          s.nulls,
          s.min,
          s.max,
          s.mean,
          s.stddev,
        ]),
      );
    }),
  );
  const numeric = result.columns.filter((n) =>
    /^(U?(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT)|FLOAT|DOUBLE|DECIMAL)/.test(
      result.types[n],
    ),
  );
  if (numeric.length) {
    const chart = panel("Chart of the current page"),
      axes = [];
    for (const [label, initial] of [
      ["Horizontal axis", 0],
      ["Vertical axis", Math.min(1, numeric.length - 1)],
    ]) {
      const axis = node("select"),
        wrapper = node("label", label);
      numeric.forEach((n, i) => {
        const option = node("option", n);
        option.value = n;
        option.selected = i === initial;
        axis.append(option);
      });
      wrapper.append(axis);
      chart.append(wrapper);
      axes.push(axis);
    }
    const host = node("div");
    chart.append(host);
    const draw = () =>
      scatterDataset(host, result, axes[0].value, axes[1].value);
    axes.forEach((axis) => {
      axis.onchange = draw;
    });
    draw();
  }
  content.append(button("Back to datasets", () => navigate("datasets")));
}
function scatterDataset(host, result, x, y) {
  host.replaceChildren();
  const points = result.rows
    .map((r) => [r[result.columns.indexOf(x)], r[result.columns.indexOf(y)]])
    .filter((p) =>
      p.every((v) => v !== null && v !== "" && Number.isFinite(Number(v))),
    )
    .map((p) => p.map(Number));
  if (!points.length) {
    host.append(node("p", "No finite points on this page."));
    return;
  }
  const ns = "http://www.w3.org/2000/svg",
    svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 800 300");
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", `${y} by ${x} for the current data page`);
  const lows = [0, 1].map((i) => Math.min(...points.map((p) => p[i]))),
    highs = [0, 1].map((i) => Math.max(...points.map((p) => p[i])));
  for (const p of points) {
    const dot = document.createElementNS(ns, "circle");
    dot.setAttribute(
      "cx",
      65 + ((p[0] - lows[0]) / Math.max(1e-12, highs[0] - lows[0])) * 690,
    );
    dot.setAttribute(
      "cy",
      250 - ((p[1] - lows[1]) / Math.max(1e-12, highs[1] - lows[1])) * 205,
    );
    dot.setAttribute("r", "4");
    dot.setAttribute("fill", "#16654b");
    svg.append(dot);
  }
  for (const [label, px, py] of [
    [`${x} · ${lows[0].toPrecision(5)} → ${highs[0].toPrecision(5)}`, 250, 285],
    [`${y}: ${highs[1].toPrecision(5)}`, 8, 25],
    [lows[1].toPrecision(5), 8, 253],
  ]) {
    const text = document.createElementNS(ns, "text");
    text.textContent = label;
    text.setAttribute("x", px);
    text.setAttribute("y", py);
    text.setAttribute("font-size", "12");
    svg.append(text);
  }
  host.append(svg);
}
function jobTable(p, rows) {
  table(
    p,
    ["Name", "State", "Resources", "Attempts", "Actions"],
    rows.map((j) => {
      const actions = node("div", undefined, "row-actions");
      actions.append(button("Inspect", () => jobDetail(j.id)));
      if (role !== "viewer") {
        if (["FAILED", "TIMED_OUT", "CANCELLED"].includes(j.status))
          actions.append(
            button("Retry", async () => {
              await json(`/jobs/${j.id}/retry`, { method: "POST" });
              return page === "detail" ? detailReload() : load();
            }),
          );
        else if (j.status !== "SUCCEEDED")
          actions.append(
            button("Cancel", async () => {
              await json(`/jobs/${j.id}/cancel`, { method: "POST" });
              return page === "detail" ? detailReload() : load();
            }),
          );
      }
      return [
        j.name,
        badge(j.status),
        j.execution_kind === "barrier"
          ? "Coordinator join"
          : `${j.cpu_required} CPU · ${j.memory_required_mb} MB${j.gpu_required ? ` · ${j.gpu_required} GPU` : ""}`,
        j.attempts_count,
        actions,
      ];
    }),
  );
}
async function load() {
  if (individual && page === "account") return accountPage();
  if (individual && !project) {
    content.replaceChildren(
      node(
        "p",
        "You have no active project. Open Account & projects to create one or ask an administrator for membership.",
      ),
    );
    return;
  }
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
      if (page === "compute-groups") {
        toolbar("Cooperating computation", "Create group", newComputeGroup);
        table(
          panel("Computation groups"),
          ["Name", "Ranks", "State", "Actions"],
          rows.map((g) => [
            g.name,
            g.nodes,
            badge(g.status),
            button("Inspect group", () => computeGroupDetail(g.id)),
          ]),
        );
      }
      if (page === "interactive-sessions") {
        toolbar("Stateful runtimes", "Start Python notebook", () =>
          newInteractiveSession(),
        );
        if (role !== "viewer")
          content.append(
            button("Start HTTP service", () =>
              newInteractiveSession("service"),
            ),
          );
        table(
          panel("Interactive sessions"),
          ["Name", "Kind", "State", "Actions"],
          rows.map((s) => [
            s.job.name,
            s.kind,
            badge(s.job.status),
            button("Open session", () => interactiveSessionDetail(s.id)),
          ]),
        );
      }
      if (page === "campaigns") {
        toolbar("Campaigns", "New campaign", newCampaign);
        if (role !== "viewer")
          content.append(button("New workflow", newWorkflow));
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
      if (page === "experiments") {
        toolbar("Experiment registry", "New experiment", () =>
          editor(
            "Create an experiment",
            [
              ["name", "Name", "Numerical study"],
              ["description", "Description", "", "text", false],
            ],
            (values) => json("/experiments", { method: "POST", json: values }),
          ),
        );
        table(
          panel("Registered studies"),
          ["Name", "Description", "Actions"],
          rows.map((experiment) => [
            experiment.name,
            experiment.description,
            button("Runs & comparison", () => experimentDetail(experiment)),
          ]),
        );
      }
      if (page === "schedules") {
        toolbar("Periodic jobs", "New schedule", newSchedule);
        table(
          panel("Durable schedules"),
          ["Name", "Cron / timezone", "State", "Next occurrence", "Actions"],
          rows.map((schedule) => [
            schedule.name,
            `${schedule.cron} · ${schedule.timezone}`,
            schedule.enabled ? "Enabled" : "Paused",
            new Date(schedule.next_run_at).toLocaleString(),
            button("History & controls", () => scheduleDetail(schedule.id)),
          ]),
        );
      }
      if (page === "webhooks") {
        toolbar(
          "Event webhooks",
          role === "admin" ? "New webhook" : null,
          newWebhook,
        );
        table(
          panel("Approved receivers"),
          ["Name", "Target", "Events", "State", "Actions"],
          rows.map((webhook) => [
            webhook.name,
            webhook.target,
            webhook.events.join(", "),
            webhook.enabled ? "Enabled" : "Disabled",
            button("Deliveries & controls", () => webhookDetail(webhook)),
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
            "GPUs reserved / total",
            "Jobs",
            "Actions",
          ],
          rows.map((w) => [
            w.id,
            badge(w.status),
            `${w.cpu_reserved.toFixed(2)} / ${w.cpu_total}`,
            `${w.memory_reserved_mb} / ${w.memory_total_mb} MB`,
            button(`${w.gpu_reserved || 0} / ${w.gpu_total || 0}`, () =>
              gpuDetail(w),
            ),
            w.running_jobs,
            (individual ? currentUser?.is_admin : role === "admin") &&
            w.status !== "LOST"
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
        if (individual ? currentUser?.is_admin : role === "admin") {
          const admission = await json("/cluster/admission");
          const controls = panel("Cluster admission");
          controls.append(
            node(
              "p",
              `New jobs: ${admission.accepting_jobs ? "Accepted" : "Paused"}. Assignments: ${admission.scheduling_enabled ? "Enabled" : "Paused"}. Running attempts continue.`,
            ),
            node("p", admission.reason || "No maintenance reason recorded."),
            button("Edit admission & reason", () =>
              editor(
                "Cluster admission",
                [
                  [
                    "accepting_jobs",
                    "Accept new jobs",
                    admission.accepting_jobs,
                    "checkbox",
                    false,
                  ],
                  [
                    "scheduling_enabled",
                    "Assign queued jobs",
                    admission.scheduling_enabled,
                    "checkbox",
                    false,
                  ],
                  [
                    "reason",
                    "Maintenance reason (up to 256 characters)",
                    admission.reason,
                    "text",
                    false,
                  ],
                ],
                (v) => json("/cluster/admission", { method: "PATCH", json: v }),
                "Running attempts, cancellation and expired-lease recovery remain available.",
              ),
            ),
            button(
              admission.accepting_jobs ? "Pause new jobs" : "Accept new jobs",
              async () => {
                await json("/cluster/admission", {
                  method: "PATCH",
                  json: {
                    accepting_jobs: !admission.accepting_jobs,
                    scheduling_enabled: admission.scheduling_enabled,
                    reason: admission.reason,
                  },
                });
                return load();
              },
            ),
            button(
              admission.scheduling_enabled
                ? "Pause assignments"
                : "Resume assignments",
              async () => {
                await json("/cluster/admission", {
                  method: "PATCH",
                  json: {
                    accepting_jobs: admission.accepting_jobs,
                    scheduling_enabled: !admission.scheduling_enabled,
                    reason: admission.reason,
                  },
                });
                return load();
              },
            ),
          );
          const operations = await json("/operations");
          const pools = await json("/cluster/pools");
          table(
            panel("Elastic worker pools"),
            [
              "Pool",
              "Policy",
              "Desired / maximum",
              "Worker allocation",
              "Controller last seen",
              "Controls",
              "History",
            ],
            pools.map((p) => [
              p.id,
              p.enabled ? "Enabled" : "Disabled",
              `${p.desired} / ${p.maximum} (hard limit ${p.hard_limit})`,
              `${p.cpu_per_worker} CPU · ${p.memory_per_worker_mb} MB`,
              `${new Date(p.last_seen_at).toLocaleString()}${p.last_error ? " · " + p.last_error : ""}`,
              button("Edit pool", () =>
                editor(
                  `Worker pool: ${p.id}`,
                  [
                    [
                      "enabled",
                      "Enable automatic provisioning",
                      p.enabled,
                      "checkbox",
                      false,
                    ],
                    ["minimum", "Minimum workers", p.minimum, "number"],
                    [
                      "maximum",
                      `Maximum workers (hard limit ${p.hard_limit})`,
                      p.maximum,
                      "number",
                    ],
                  ],
                  (v) =>
                    json(`/cluster/pools/${p.id}`, {
                      method: "PATCH",
                      json: v,
                    }),
                  "Disabling the pool drains running work before removing agents. The controller's host budget remains the hard bound.",
                ),
              ),
              button("Workers & history", () => poolDetail(p)),
            ]),
          );
          if (!pools.length)
            content.append(
              node(
                "p",
                "No host worker pools are registered. Configure strata-provisioner on a reserved Docker host partition.",
              ),
            );
          table(
            panel("Supervised operations"),
            [
              "Operation",
              "State",
              "Last success",
              "Next check",
              "Evidence / error",
            ],
            operations.map((operation) => [
              operation.name,
              operation.status,
              operation.succeeded_at
                ? new Date(operation.succeeded_at).toLocaleString()
                : "Never",
              new Date(operation.next_run_at).toLocaleString(),
              operation.last_error ||
                operation.result.snapshot ||
                "Maintenance evidence recorded",
            ]),
          );
          if (!operations.length)
            content.append(
              node(
                "p",
                "No supervised operations have run. Configure strata-ops to enable maintenance and backups.",
              ),
            );
        }
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
async function accountPage() {
  if (busy) return;
  busy = true;
  try {
    await connected();
    const credentials = await json("/auth/tokens");
    content.replaceChildren();
    const account = panel(`Account · ${currentUser.username}`);
    account.append(
      button("Change password", () =>
        editor(
          "Change password",
          [
            ["current_password", "Current password", "", "password"],
            ["new_password", "New password (12+ characters)", "", "password"],
          ],
          async (v) => {
            await api("/auth/password", { method: "POST", json: v });
            forgetSession();
          },
        ),
      ),
    );
    const tokensPanel = panel("Automation credentials");
    tokensPanel.append(
      node(
        "p",
        "Tokens are scoped to one project and cannot exceed your current membership. The secret is shown once.",
      ),
    );
    table(
      tokensPanel,
      ["Name", "Kind", "Role", "Expires", "Actions"],
      credentials.map((t) => [
        t.name,
        t.kind,
        t.role || "Session",
        new Date(t.expires_at).toLocaleString(),
        t.revoked_at
          ? "Revoked"
          : button("Revoke", async () => {
              await api(`/auth/tokens/${t.id}`, { method: "DELETE" });
              if (t.kind === "session") await connected();
              return accountPage();
            }),
      ]),
    );
    if (project)
      tokensPanel.append(
        button("Create token", () =>
          editor(
            "Create automation token",
            [
              ["name", "Name", "Notebook"],
              ["role", "Role: viewer, operator or admin", "operator"],
              [
                "lifetime_seconds",
                "Lifetime in seconds (60–7776000)",
                86400,
                "number",
              ],
            ],
            async (v) => {
              const issued = await json("/auth/tokens", {
                method: "POST",
                json: { ...v, project_id: project },
              });
              const dialog = document.createElement("dialog");
              dialog.append(
                node("h2", "Save this token now"),
                node(
                  "p",
                  "Treat this credential as a password. It will not be shown again.",
                ),
                node("pre", issued.access_token),
                button("Saved", () => {
                  dialog.close();
                  dialog.remove();
                }),
              );
              document.body.append(dialog);
              dialog.showModal();
            },
          ),
        ),
      );
    if (currentUser.is_admin) {
      const administration = panel("Platform administration");
      administration.append(
        button("Create project", () =>
          editor(
            "Create project",
            [
              ["name", "Name", "Research"],
              ["description", "Description", "Compute workspace"],
            ],
            (v) => json("/projects", { method: "POST", json: v }),
          ),
        ),
      );
      administration.append(
        button("Create user", () =>
          editor(
            "Create user",
            [
              ["username", "Username", ""],
              ["display_name", "Display name", ""],
              ["password", "Initial password (12+ characters)", "", "password"],
            ],
            (v) => json("/auth/users", { method: "POST", json: v }),
          ),
        ),
      );
      const users = await json("/auth/users?limit=1000");
      if (oidcEnabled) {
        administration.append(
          button("Link federated identity", () =>
            editor(
              "Link federated identity",
              [
                ["user_id", "Local user identifier", ""],
                ["subject", "Identity provider subject", ""],
              ],
              (values) =>
                json("/auth/oidc/identities", { method: "POST", json: values }),
              "Use the verified provider subject, not an email address or display name. Linking is explicit; accounts are never matched automatically.",
            ),
          ),
        );
        const identities = await json("/auth/oidc/identities");
        table(
          administration,
          ["Provider subject", "Local user", "State", "Actions"],
          identities.map((identity) => [
            identity.subject,
            identity.user_id,
            identity.enabled ? "Enabled" : "Disabled",
            identity.enabled
              ? button("Disable federated identity", async () => {
                  await api(`/auth/oidc/identities/${identity.id}`, {
                    method: "DELETE",
                  });
                  return accountPage();
                })
              : "—",
          ]),
        );
      }
      table(
        administration,
        ["User", "Identifier", "Status", "Actions"],
        users.map((u) => [
          u.username,
          u.id,
          u.enabled ? "Enabled" : "Disabled",
          button(u.enabled ? "Disable" : "Enable", async () => {
            await json(`/auth/users/${u.id}`, {
              method: "PATCH",
              json: { enabled: !u.enabled },
            });
            return accountPage();
          }),
        ]),
      );
    }
    if (project && role === "admin") {
      const management = panel("Active project membership");
      const members = await json(`/projects/${project}/members`);
      table(
        management,
        ["User", "Role", "Actions"],
        members.map((m) => [
          m.user.username,
          m.role,
          button("Remove", async () => {
            await api(`/projects/${project}/members/${m.user.id}`, {
              method: "DELETE",
            });
            return accountPage();
          }),
        ]),
      );
      management.append(
        button("Grant or change membership", () =>
          editor(
            "Project membership",
            [
              ["user_id", "User identifier", ""],
              ["role", "Role: viewer, operator or admin", "operator"],
            ],
            (v) =>
              json(
                `/projects/${project}/members/${encodeURIComponent(v.user_id)}`,
                { method: "PUT", json: { role: v.role } },
              ),
          ),
        ),
      );
      if (currentUser.is_admin) {
        const p = (await json("/projects")).find((p) => p.id === project);
        management.append(
          button("Edit project budgets", () =>
            editor(
              "Project budgets",
              [
                ["queue_limit", "Outstanding jobs", p.queue_limit, "number"],
                ["cpu_limit", "Concurrent CPU cores", p.cpu_limit, "number"],
                [
                  "memory_limit_mb",
                  "Concurrent memory (MiB)",
                  p.memory_limit_mb,
                  "number",
                ],
                ["gpu_limit", "Concurrent GPUs", p.gpu_limit, "number"],
                [
                  "storage_limit_bytes",
                  "Referenced storage (bytes)",
                  p.storage_limit_bytes,
                  "number",
                ],
              ],
              (v) => json(`/projects/${project}`, { method: "PATCH", json: v }),
            ),
          ),
        );
      }
      const events = await json(`/projects/${project}/audit?limit=100`);
      table(
        panel("Audit trail · first 100 events"),
        ["Time", "Action", "Actor", "Resource"],
        events.map((e) => [
          new Date(e.created_at).toLocaleString(),
          e.action,
          e.actor_id || "System",
          e.resource_id,
        ]),
      );
    }
  } catch (e) {
    showError(e);
  } finally {
    busy = false;
  }
}
function navigate(next) {
  runtimeWatch = null;
  page = next;
  offset = 0;
  $("title").textContent =
    next === "compute-groups"
      ? "Compute groups"
      : next === "interactive-sessions"
        ? "Interactive sessions"
        : next[0].toUpperCase() + next.slice(1);
  document.querySelectorAll("nav button").forEach((b) => {
    const current = b.dataset.page === next;
    b.classList.toggle("active", current);
    if (current) b.setAttribute("aria-current", "page");
    else b.removeAttribute("aria-current");
  });
  return load();
}
document
  .querySelectorAll("nav button")
  .forEach((b) => (b.onclick = () => navigate(b.dataset.page)));
$("refresh").onclick = () =>
  Promise.resolve(page === "detail" ? detailReload() : load()).catch(showError);
$("close").onclick = $("cancel-dialog").onclick = () => modal.close();
$("access").onclick = () => $("login").showModal();
async function connected() {
  if (individual) {
    const me = await json("/auth/me");
    currentUser = me.user;
    if (!me.projects.some((p) => p.id === project))
      project = me.projects[0]?.id || "";
    sessionStorage.setItem("strata-project", project);
    $("project-select").replaceChildren(
      ...me.projects.map((p) => {
        const option = node("option", p.name);
        option.value = p.id;
        option.selected = p.id === project;
        return option;
      }),
    );
    $("project-label").hidden = false;
    $("sign-out").hidden = false;
    $("account-nav").hidden = false;
    $("access").textContent = "Switch account";
  }
  const session = await json("/session");
  role = session.role;
  $("connection").textContent = session.authentication_enabled
    ? `Connected · ${currentUser?.username || role}`
    : "Local development · admin";
}
$("project-select").onchange = async () => {
  project = $("project-select").value;
  sessionStorage.setItem("strata-project", project);
  try {
    await connected();
    await navigate("overview");
  } catch (e) {
    showError(e);
  }
};
function forgetSession() {
  token = "";
  project = "";
  currentUser = null;
  role = "viewer";
  sessionStorage.removeItem("strata-key");
  sessionStorage.removeItem("strata-project");
  content.replaceChildren();
  $("sign-out").hidden = $("project-label").hidden = true;
  $("connection").textContent = "Signed out";
  $("access").textContent = "Sign in";
  if (!$("login").open) $("login").showModal();
}
$("sign-out").onclick = async () => {
  try {
    await api("/auth/logout", { method: "POST" });
  } catch (e) {
    showError(e);
  }
  forgetSession();
};
$("login-form").onsubmit = async (e) => {
  e.preventDefault();
  try {
    if (individual) {
      const session = await json("/auth/login", {
        method: "POST",
        json: {
          username: $("username").value,
          password: $("password").value,
        },
      });
      token = session.access_token;
      $("password").value = "";
    } else token = $("key").value.trim();
    await connected();
    sessionStorage.setItem("strata-key", token);
    $("login").close();
    page = "overview";
    await navigate("overview");
  } catch (e) {
    $("login-error").textContent = e.message;
  }
};
(async () => {
  try {
    const authentication = await json("/auth/config");
    individual = authentication.identity_enabled;
    oidcEnabled = authentication.oidc_enabled;
    $("oidc-login").hidden = !oidcEnabled;
    $("username-label").hidden = $("password-label").hidden = !individual;
    $("key-label").hidden = individual;
    const sso = new URLSearchParams(location.search);
    if (sso.get("sso") === "complete" && oidcEnabled) {
      const session = await json("/auth/oidc/session", { method: "POST" });
      token = session.access_token;
      sessionStorage.setItem("strata-key", token);
      history.replaceState(null, "", "/");
    } else if (sso.get("sso") === "failed") {
      history.replaceState(null, "", "/");
      $("login-error").textContent =
        sso.get("code") === "403"
          ? "Your administrator must provision this federated account."
          : "Federated sign-in failed. Start again or contact your administrator.";
    }
    $("access").textContent = individual ? "Sign in" : "Change access key";
    $("login-help").textContent = individual
      ? "Sign in with your individual account. Your administrator manages project membership."
      : "Enter an API key supplied by your administrator. Local development mode allows an empty key.";
    await connected();
    await navigate("overview");
  } catch (e) {
    showError(e);
  }
})();
setInterval(() => {
  if (!document.hidden && !modal.open && !$("login").open) load();
}, 5000);
