const POLL_MS = 2000;

const PRESETS = {
  exec: [
    "show ip interface brief",
    "show ip route",
    "show running-config",
    "show version",
    "show cdp neighbors",
  ],
  config: [
    "interface Loopback0\n ip address 10.0.0.1 255.255.255.255\n no shutdown",
    "hostname R1",
    "router ospf 1\n network 0.0.0.0 255.255.255.255 area 0",
  ],
};

const el = (id) => document.getElementById(id);
let selectedJobId = null;

async function api(path, options = {}) {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}

function currentMode() {
  return document.querySelector('input[name="mode"]:checked').value;
}

function renderPresets() {
  const box = el("presets");
  box.replaceChildren();
  for (const cmd of PRESETS[currentMode()]) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chip";
    btn.textContent = cmd.split("\n")[0];
    btn.title = cmd;
    btn.addEventListener("click", () => { el("commands").value = cmd; });
    box.appendChild(btn);
  }
}

async function loadRouters() {
  const select = el("router");
  try {
    const routers = await api("/routers");
    select.replaceChildren(...routers.map((r) => {
      const opt = document.createElement("option");
      opt.value = r.name;
      opt.textContent = `${r.name} (${r.host})`;
      return opt;
    }));
  } catch (err) {
    showMessage(`Could not load routers: ${err.message}`, true);
  }
}

function showMessage(text, isError = false) {
  const msg = el("form-message");
  msg.textContent = text;
  msg.classList.toggle("error", isError);
}

function formatTime(iso) {
  return iso ? new Date(iso).toLocaleTimeString() : "";
}

function renderJobs(jobs) {
  const list = el("job-list");
  if (!jobs.length) {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = "No jobs yet.";
    list.replaceChildren(empty);
    return;
  }
  list.replaceChildren(...jobs.map((job) => {
    const li = document.createElement("li");
    li.className = "job" + (job.job_id === selectedJobId ? " selected" : "");
    li.addEventListener("click", () => { selectedJobId = job.job_id; renderJobs(jobs); renderDetail(job); });

    const status = document.createElement("span");
    status.className = `status ${job.status || "unknown"}`;
    status.textContent = job.status || "unknown";

    const title = document.createElement("span");
    title.className = "job-title";
    title.textContent = `${job.router} · ${(job.commands || [])[0] || ""}${(job.commands || []).length > 1 ? " …" : ""}`;

    const time = document.createElement("span");
    time.className = "job-time";
    time.textContent = formatTime(job.submitted_at);

    li.append(status, title, time);
    return li;
  }));

  const selected = jobs.find((j) => j.job_id === selectedJobId);
  if (selected) renderDetail(selected);
}

function renderDetail(job) {
  el("detail-title").textContent = `Output · ${job.router}`;
  const meta = [
    `job ${job.job_id}`,
    `mode ${job.mode}`,
    `status ${job.status}`,
    job.worker ? `worker ${job.worker}` : null,
    job.finished_at ? `finished ${formatTime(job.finished_at)}` : null,
  ].filter(Boolean);
  el("detail-meta").textContent = meta.join("  ·  ");

  let text;
  if (job.status === "queued") text = "Waiting in Kafka for a worker…";
  else if (job.status === "running") text = "Worker is running the commands on the router…";
  else text = [job.error, job.output].filter(Boolean).join("\n\n") || "(no output)";
  const out = el("detail-output");
  out.textContent = text;
  out.classList.toggle("error", job.status === "failed");
}

async function poll() {
  try {
    renderJobs(await api("/jobs"));
    el("conn").textContent = "live";
    el("conn").classList.remove("down");
  } catch (err) {
    el("conn").textContent = "backend unreachable";
    el("conn").classList.add("down");
  }
}

el("job-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = el("submit");
  button.disabled = true;
  try {
    const job = await api("/jobs", {
      method: "POST",
      body: JSON.stringify({
        router: el("router").value,
        mode: currentMode(),
        commands: el("commands").value,
      }),
    });
    selectedJobId = job.job_id;
    showMessage(`Queued job ${job.job_id}`);
    await poll();
  } catch (err) {
    showMessage(err.message, true);
  } finally {
    button.disabled = false;
  }
});

document.querySelectorAll('input[name="mode"]').forEach((radio) =>
  radio.addEventListener("change", renderPresets));

renderPresets();
loadRouters();
poll();
setInterval(poll, POLL_MS);
