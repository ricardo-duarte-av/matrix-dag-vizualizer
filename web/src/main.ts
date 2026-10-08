import "./style.css";
import { api, type AppConfig, type DagNode, type RoomInfo } from "./api";
import { DagView, colorFor, type ColorBy, type Direction, type Layout, type ViewOptions } from "./graph";

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;

const els = {
  title: $("title"),
  room: $<HTMLSelectElement>("room"),
  serverRooms: $<HTMLButtonElement>("server-rooms"),
  layout: $<HTMLSelectElement>("layout"),
  direction: $<HTMLSelectElement>("direction"),
  colorBy: $<HTMLSelectElement>("color-by"),
  showAuth: $<HTMLInputElement>("show-auth"),
  showIds: $<HTMLInputElement>("show-ids"),
  live: $<HTMLInputElement>("live"),
  backfill: $<HTMLButtonElement>("backfill"),
  resolve: $<HTMLButtonElement>("resolve"),
  fit: $<HTMLButtonElement>("fit"),
  search: $<HTMLInputElement>("search"),
  empty: $("empty"),
  loading: $("loading"),
  loadingText: $("loading-text"),
  legend: $("legend"),
  panel: $("panel"),
  panelBody: $("panel-body"),
  panelClose: $("panel-close"),
  status: $("status"),
  dialog: $<HTMLDialogElement>("server-dialog"),
  serverSearch: $<HTMLInputElement>("server-search"),
  serverList: $("server-list"),
};

const PREFS_KEY = "dagviz.prefs";
type Prefs = Partial<Pick<ViewOptions, "layout" | "direction" | "colorBy" | "showAuth" | "showIds">>;

function loadPrefs(): Prefs {
  try {
    return JSON.parse(localStorage.getItem(PREFS_KEY) ?? "{}") as Prefs;
  } catch {
    return {};
  }
}
function savePrefs(p: Prefs) {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify({ ...loadPrefs(), ...p }));
  } catch {
    /* storage unavailable */
  }
}

let cfg: AppConfig;
let view: DagView;
let currentRoom = "";
let roomInfo: RoomInfo | null = null;
let stream: EventSource | null = null;
let stats = { nodes: 0, missing: 0, truncated: false, serverExt: 0, liveCount: 0 };
const nodesById = new Map<string, DagNode>();

function busy(text: string | null) {
  els.loading.classList.toggle("hidden", text === null);
  if (text) els.loadingText.textContent = text;
}

function escapeHTML(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);
}

function applyTheme() {
  if (cfg.theme === "auto") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = cfg.theme;
}

// ---- URL hash state: #room=!id&event=$id ---------------------------------

function readHash(): { room?: string; event?: string } {
  const p = new URLSearchParams(location.hash.slice(1));
  return { room: p.get("room") ?? undefined, event: p.get("event") ?? undefined };
}
function writeHash(room: string, event?: string) {
  const p = new URLSearchParams();
  if (room) p.set("room", room);
  if (event) p.set("event", event);
  history.replaceState(null, "", `#${p.toString()}`);
}

// ---- Rooms ----------------------------------------------------------------

async function refreshRooms() {
  const rooms = await api.rooms();
  const options = rooms.map((r) => {
    const o = document.createElement("option");
    o.value = r.room_id;
    o.textContent = `${r.name || r.room_id}${r.joined ? "" : " (not joined)"}`;
    o.title = r.room_id;
    return o;
  });
  const placeholder = new Option(rooms.length ? "Select a room…" : "No rooms joined yet", "");
  els.room.replaceChildren(placeholder, ...options);
  if (currentRoom && !rooms.some((r) => r.room_id === currentRoom)) {
    els.room.append(new Option(currentRoom, currentRoom));
  }
  els.room.value = currentRoom;
}

async function openRoom(roomId: string, focusEvent?: string) {
  currentRoom = roomId;
  stream?.close();
  stream = null;
  closePanel();
  writeHash(roomId, focusEvent);
  els.empty.classList.toggle("hidden", !!roomId);
  if (!roomId) {
    view.load({ room: {} as RoomInfo, nodes: [], missing: [], extremities: [], truncated: false, admin: cfg.admin });
    renderLegend();
    renderStatus();
    return;
  }
  if (els.room.value !== roomId) {
    if (![...els.room.options].some((o) => o.value === roomId)) els.room.append(new Option(roomId, roomId));
    els.room.value = roomId;
  }
  await reloadGraph("Loading event graph…");
  if (focusEvent) focus(focusEvent);
  connectStream();
}

async function reloadGraph(message: string) {
  if (!currentRoom) return;
  busy(message);
  try {
    const data = await api.dag(currentRoom, cfg.max_render_nodes);
    roomInfo = data.room;
    nodesById.clear();
    for (const n of data.nodes) nodesById.set(n.id, n);
    stats = {
      nodes: data.nodes.length,
      missing: data.missing.length,
      truncated: data.truncated,
      serverExt: data.server_extremities?.length ?? 0,
      liveCount: 0,
    };
    setAdmin(data.admin);
    view.load(data);
    renderLegend();
    renderStatus();
  } catch (e) {
    renderStatus(`Failed to load room: ${(e as Error).message}`);
  } finally {
    busy(null);
  }
}

function connectStream() {
  stream?.close();
  stream = null;
  if (!currentRoom || !els.live.checked) {
    renderStatus();
    return;
  }
  const s = api.stream(currentRoom);
  s.addEventListener("nodes", (ev) => {
    const nodes = JSON.parse((ev as MessageEvent).data) as DagNode[];
    for (const n of nodes) nodesById.set(n.id, n);
    const added = view.addLive(nodes);
    stats.nodes += added;
    stats.liveCount += added;
    renderLegend();
    renderStatus();
  });
  s.onopen = () => renderStatus();
  s.onerror = () => renderStatus();
  stream = s;
}

// ---- Server rooms dialog (Synapse admin) -----------------------------------

let searchTimer = 0;
async function searchServerRooms() {
  els.serverList.innerHTML = `<div class="overlay" style="position:static;padding:16px">Searching…</div>`;
  try {
    const res = await api.serverRooms(els.serverSearch.value.trim());
    if (!res.rooms.length) {
      els.serverList.innerHTML = `<p style="padding:12px" class="unknown">No rooms found.</p>`;
      return;
    }
    els.serverList.replaceChildren(
      ...res.rooms.map((r) => {
        const b = document.createElement("button");
        b.className = "item";
        b.innerHTML = `<div>${escapeHTML(r.name || r.canonical_alias || r.room_id)}</div>
          <div class="meta">${escapeHTML(r.room_id)} · v${escapeHTML(r.version)} · ${r.joined_members} members (${r.joined_local_members} local)</div>`;
        b.onclick = () => {
          els.dialog.close();
          void openRoom(r.room_id);
        };
        return b;
      }),
    );
  } catch (e) {
    els.serverList.innerHTML = `<p style="padding:12px" class="error">${escapeHTML((e as Error).message)}</p>`;
  }
}

// ---- Side panel -------------------------------------------------------------

function closePanel() {
  els.panel.classList.add("hidden");
  view?.clearHighlight();
}

function refList(ids: string[]): string {
  if (!ids.length) return `<li class="unknown">none</li>`;
  return ids
    .map((id) => {
      const known = nodesById.has(id);
      return `<li><a data-event="${escapeHTML(id)}" class="${known ? "" : "unknown"}">${escapeHTML(id)}</a>${known ? "" : " <span class='unknown'>(not loaded)</span>"}</li>`;
    })
    .join("");
}

async function showEvent(id: string, missing: boolean) {
  writeHash(currentRoom, id);
  els.panel.classList.remove("hidden");
  const n = nodesById.get(id);
  const children = [...nodesById.values()].filter((c) => c.prev.includes(id)).map((c) => c.id);
  const header = n
    ? `<h2>${escapeHTML(n.type)}</h2>
      <dl>
        <dt>Event</dt><dd>${escapeHTML(n.id)}</dd>
        <dt>Sender</dt><dd>${escapeHTML(n.sender)}</dd>
        ${n.state_key !== undefined ? `<dt>State key</dt><dd>${escapeHTML(n.state_key || '""')}</dd>` : ""}
        <dt>Depth</dt><dd>${n.depth}</dd>
        <dt>Time</dt><dd>${new Date(n.ts).toLocaleString()}</dd>
        ${n.summary ? `<dt>Summary</dt><dd>${escapeHTML(n.summary)}</dd>` : ""}
      </dl>
      <h3>prev_events (${n.prev.length})</h3><ul>${refList(n.prev)}</ul>
      <h3>Children (${children.length})</h3><ul>${refList(children)}</ul>
      <h3>auth_events (${n.auth.length})</h3><ul>${refList(n.auth)}</ul>`
    : `<h2>Missing event</h2><dl><dt>Event</dt><dd>${escapeHTML(id)}</dd></dl>
      <p class="unknown">Referenced by loaded events but not available ${missing && cfg.admin ? "in the graph yet. Use “Resolve missing” to pull it in." : "to this account (before join, history visibility or a gap). Synapse admin access can fill these in."}</p>
      <h3>Children (${children.length})</h3><ul>${refList(children)}</ul>`;
  els.panelBody.innerHTML = `${header}<h3>Raw PDU</h3><pre id="raw">Loading…</pre>`;
  try {
    const raw = await api.event(currentRoom, id);
    const pre = document.getElementById("raw");
    if (pre) pre.textContent = JSON.stringify(raw, null, 2);
  } catch (e) {
    const pre = document.getElementById("raw");
    if (pre) {
      pre.textContent = (e as Error).message;
      pre.classList.add("unknown");
    }
  }
}

function focus(id: string): boolean {
  if (!view.select(id)) return false;
  view.center(id);
  const missing = !nodesById.has(id);
  void showEvent(id, missing);
  return true;
}

// ---- Legend & status ----------------------------------------------------------

function renderLegend() {
  const entries = view.legend();
  if (!entries.length) {
    els.legend.innerHTML = "";
    return;
  }
  const by = els.colorBy.value as ColorBy;
  const top = entries.slice(0, 14);
  const rest = entries.slice(14).reduce((a, e) => a + e.count, 0);
  els.legend.innerHTML =
    top
      .map(
        (e) => `<div class="row"><span class="swatch" style="background:${colorFor(e.key, by)}"></span>
        ${escapeHTML(by === "type" ? e.key : e.key)}<span class="count">${e.count}</span></div>`,
      )
      .join("") +
    (rest ? `<div class="row unknown">other<span class="count">${rest}</span></div>` : "") +
    `<hr/>
    <div class="row"><span class="swatch square" style="background:var(--muted)"></span>state event</div>
    <div class="row"><span class="swatch ring"></span>forward extremity</div>
    <div class="row"><span class="swatch dashed"></span>missing event</div>
    <div class="row"><span class="swatch line"></span>prev_events</div>
    <div class="row"><span class="swatch line auth"></span>auth_events</div>`;
}

function renderStatus(error?: string) {
  const parts: string[] = [];
  if (error) parts.push(`<span class="error">${escapeHTML(error)}</span>`);
  parts.push(`${escapeHTML(cfg.user_id)} @ ${escapeHTML(cfg.homeserver)}`);
  if (cfg.admin) parts.push(`<span class="badge">Synapse admin</span>`);
  if (currentRoom && roomInfo) {
    parts.push(`room v${escapeHTML(roomInfo.version || "?")}`);
    parts.push(`${stats.nodes} events${stats.truncated ? ` (newest ${cfg.max_render_nodes} shown)` : ""}`);
    parts.push(`${stats.missing} missing`);
    if (cfg.admin) parts.push(`${stats.serverExt} server extremit${stats.serverExt === 1 ? "y" : "ies"}`);
    if (roomInfo.backfill_done) parts.push("start of history reached");
    if (stream) {
      const open = stream.readyState === EventSource.OPEN;
      parts.push(`<span class="${open ? "live" : ""}">${open ? "● live" : "○ reconnecting"}${stats.liveCount ? ` (+${stats.liveCount})` : ""}</span>`);
    }
  }
  els.status.innerHTML = parts.map((p) => `<span>${p}</span>`).join("");
  els.backfill.disabled = !currentRoom || !!roomInfo?.backfill_done;
  els.resolve.disabled = !currentRoom || stats.missing === 0;
}

function setAdmin(admin: boolean) {
  cfg.admin = admin;
  document.body.classList.toggle("admin", admin);
}

// ---- Wiring ---------------------------------------------------------------------

async function main() {
  cfg = await api.config();
  document.title = cfg.title;
  els.title.textContent = cfg.title;
  applyTheme();
  setAdmin(cfg.admin);

  const prefs = loadPrefs();
  const opts: ViewOptions = {
    layout: prefs.layout ?? cfg.default_layout,
    direction: prefs.direction ?? cfg.layout_direction,
    colorBy: prefs.colorBy ?? cfg.color_by,
    showAuth: prefs.showAuth ?? cfg.show_auth_events,
    showIds: prefs.showIds ?? cfg.show_event_ids,
    webgl: cfg.webgl,
  };
  els.layout.value = opts.layout;
  els.direction.value = opts.direction;
  els.colorBy.value = opts.colorBy;
  els.showAuth.checked = opts.showAuth;
  els.showIds.checked = opts.showIds;

  view = new DagView($("cy"), opts);
  view.onSelect = (id, missing) => {
    if (id) void showEvent(id, missing);
    else {
      els.panel.classList.add("hidden");
      writeHash(currentRoom);
    }
  };

  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => view.refreshTheme());

  els.room.onchange = () => void openRoom(els.room.value);
  els.layout.onchange = () => {
    view.setOptions({ layout: els.layout.value as Layout });
    savePrefs({ layout: els.layout.value as Layout });
  };
  els.direction.onchange = () => {
    view.setOptions({ direction: els.direction.value as Direction });
    savePrefs({ direction: els.direction.value as Direction });
  };
  els.colorBy.onchange = () => {
    view.setOptions({ colorBy: els.colorBy.value as ColorBy });
    savePrefs({ colorBy: els.colorBy.value as ColorBy });
    renderLegend();
  };
  els.showAuth.onchange = () => {
    view.setOptions({ showAuth: els.showAuth.checked });
    savePrefs({ showAuth: els.showAuth.checked });
  };
  els.showIds.onchange = () => {
    view.setOptions({ showIds: els.showIds.checked });
    savePrefs({ showIds: els.showIds.checked });
  };
  els.live.onchange = () => connectStream();
  els.fit.onclick = () => view.fitAll();
  els.panelClose.onclick = () => {
    closePanel();
    writeHash(currentRoom);
  };
  els.panelBody.addEventListener("click", (e) => {
    const a = (e.target as HTMLElement).closest<HTMLElement>("a[data-event]");
    if (a?.dataset.event && !focus(a.dataset.event)) renderStatus("That event is not loaded in the graph");
  });
  els.search.addEventListener("keydown", (e) => {
    if (e.key !== "Enter") return;
    const id = els.search.value.trim();
    if (id && !focus(id)) renderStatus(`Event ${id} is not in the loaded graph`);
  });

  els.backfill.onclick = async () => {
    if (!currentRoom) return;
    busy("Fetching older history…");
    try {
      const res = await api.backfill(currentRoom, 500);
      await reloadGraph(`Added ${res.added} events, re-rendering…`);
    } catch (e) {
      renderStatus((e as Error).message);
    } finally {
      busy(null);
    }
  };
  els.resolve.onclick = async () => {
    if (!currentRoom) return;
    busy("Resolving missing events via Synapse admin API…");
    try {
      const res = await api.resolve(currentRoom);
      await reloadGraph(`Resolved ${res.resolved} events (${res.remaining} still missing), re-rendering…`);
    } catch (e) {
      renderStatus((e as Error).message);
    } finally {
      busy(null);
    }
  };

  els.serverRooms.onclick = () => {
    els.dialog.showModal();
    els.serverSearch.focus();
    void searchServerRooms();
  };
  els.serverSearch.oninput = () => {
    clearTimeout(searchTimer);
    searchTimer = window.setTimeout(searchServerRooms, 300);
  };

  window.addEventListener("hashchange", () => {
    const h = readHash();
    if (h.room && h.room !== currentRoom) void openRoom(h.room, h.event);
    else if (h.event) focus(h.event);
  });

  renderStatus();
  await refreshRooms();
  const h = readHash();
  if (h.room) await openRoom(h.room, h.event);
  // Rooms appear as the initial sync completes; refresh the picker periodically.
  setInterval(() => void refreshRooms().catch(() => {}), 30_000);
}

main().catch((e) => {
  document.body.innerHTML = `<p class="error" style="padding:20px">Failed to start: ${escapeHTML((e as Error).message)}</p>`;
});
