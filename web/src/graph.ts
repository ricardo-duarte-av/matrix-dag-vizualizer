import cytoscape from "cytoscape";
import elk from "cytoscape-elk";
import type { DagNode, DagResponse } from "./api";

cytoscape.use(elk);

export type Layout = "depth" | "elk";
export type Direction = "TB" | "BT" | "LR" | "RL";
export type ColorBy = "type" | "sender";

export type EdgeKind = "prev" | "auth";

/** Cytoscape ID of the edge from parent to child. */
export const edgeId = (kind: EdgeKind, from: string, to: string) => `${kind}|${from}|${to}`;

export interface ViewOptions {
  layout: Layout;
  direction: Direction;
  colorBy: ColorBy;
  showAuth: boolean;
  /** Render nodes as boxes containing the event type and full event ID. */
  showIds: boolean;
  webgl: boolean;
}

// Stable colours for the most common event types; everything else is hashed.
const KNOWN_TYPE_COLORS: Record<string, string> = {
  "m.room.create": "#eab308",
  "m.room.message": "#3b82f6",
  "m.room.encrypted": "#6366f1",
  "m.room.member": "#22c55e",
  "m.room.power_levels": "#ef4444",
  "m.room.join_rules": "#f97316",
  "m.room.history_visibility": "#a855f7",
  "m.room.name": "#14b8a6",
  "m.room.topic": "#06b6d4",
  "m.room.redaction": "#64748b",
  "m.reaction": "#ec4899",
};
const PALETTE = [
  "#0ea5e9", "#84cc16", "#f43f5e", "#d946ef", "#10b981", "#f59e0b",
  "#8b5cf6", "#06b6d4", "#e11d48", "#65a30d", "#c026d3", "#0284c7",
];

function hash(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function colorFor(key: string, by: ColorBy): string {
  if (by === "type" && KNOWN_TYPE_COLORS[key]) return KNOWN_TYPE_COLORS[key];
  return PALETTE[hash(key) % PALETTE.length];
}

const shortType = (t: string) => t.replace(/^m\.room\./, "").replace(/^m\./, "");
const isState = (n: DagNode) => n.state_key !== undefined && n.state_key !== null;

function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

const SPACING = { rank: 70, sibling: 60 };
/** Distance between stacked lanes of edges routed around the graph. */
const LANE_GAP = 12;
const ROUTE_PROPS = "curve-style edge-distances segment-weights segment-distances segment-radii";

// Event-ID boxes are sized from their (monospace) text so layouts can space
// them exactly without measuring rendered labels.
const ID_FONT = 10;
const ID_CHAR_W = ID_FONT * 0.62;
const ID_LINE_H = ID_FONT * 1.35;
const ID_PAD = 8;

function idBox(lines: string[]): { idLabel: string; w: number; h: number } {
  const longest = Math.max(...lines.map((l) => l.length));
  return {
    idLabel: lines.join("\n"),
    w: Math.ceil(longest * ID_CHAR_W + ID_PAD * 2),
    h: Math.ceil(lines.length * ID_LINE_H + ID_PAD * 2),
  };
}

export class DagView {
  readonly cy: cytoscape.Core;
  private opts: ViewOptions;
  private relayoutTimer = 0;
  private hoverTimer = 0;
  private peekFrom: { zoom: number; pan: cytoscape.Position } | null = null;
  /** parent event ID → events referencing it, so late parents can be linked. */
  private children = new Map<string, [string, EdgeKind][]>();
  onSelect: (id: string | null, missing: boolean) => void = () => {};

  constructor(container: HTMLElement, opts: ViewOptions) {
    this.opts = { ...opts };
    this.cy = cytoscape({
      container,
      wheelSensitivity: 0.3,
      minZoom: 0.02,
      maxZoom: 4,
      boxSelectionEnabled: false,
      webgl: opts.webgl,
      style: this.stylesheet(),
    });
    this.cy.on("tap", "node", (e) => {
      const n = e.target as cytoscape.NodeSingular;
      this.select(n.id());
      this.onSelect(n.id(), n.hasClass("missing"));
    });
    this.cy.on("tap", (e) => {
      if (e.target === this.cy) {
        this.clearHighlight();
        this.onSelect(null, false);
      }
    });
  }

  setOptions(patch: Partial<ViewOptions>) {
    const prev = this.opts;
    this.opts = { ...this.opts, ...patch };
    if (patch.colorBy && patch.colorBy !== prev.colorBy) this.recolor();
    if (patch.showAuth !== undefined) {
      this.cy.edges(".auth").toggleClass("hidden", !this.opts.showAuth);
    }
    if (patch.showIds !== undefined && patch.showIds !== prev.showIds) {
      this.cy.style(this.stylesheet());
      this.layout(true);
      return;
    }
    if (
      (patch.layout && patch.layout !== prev.layout) ||
      (patch.direction && patch.direction !== prev.direction)
    ) {
      this.layout(true);
    }
  }

  refreshTheme() {
    this.cy.style(this.stylesheet());
    this.shapeEdges();
  }

  /** Replace the whole graph. */
  load(data: DagResponse) {
    this.cy.elements().remove();
    this.children.clear();
    this.cy.batch(() => {
      this.addNodes(data.nodes);
      for (const id of data.missing) this.ensureMissing(id);
      this.addEdges(data.nodes);
      this.markExtremities(data.extremities, data.server_extremities);
    });
    this.layout(true);
  }

  /** Add live events, returns how many were new. */
  addLive(nodes: DagNode[]): number {
    let added = 0;
    this.cy.batch(() => {
      const fresh = nodes.filter((n) => {
        const existing = this.cy.getElementById(n.id);
        if (existing.nonempty() && !existing.hasClass("missing")) return false;
        existing.remove();
        return true;
      });
      added = fresh.length;
      this.addNodes(fresh);
      for (const n of fresh) {
        for (const ref of [...n.prev, ...n.auth]) this.ensureMissing(ref);
      }
      this.addEdges(fresh);
      // Recompute "computed" extremities: nodes without children.
      this.cy.nodes(".extremity").removeClass("extremity");
      this.cy
        .nodes()
        .filter((n) => !n.hasClass("missing") && n.outgoers("edge.prev").empty())
        .addClass("extremity");
    });
    if (added) {
      clearTimeout(this.relayoutTimer);
      this.relayoutTimer = window.setTimeout(() => this.layout(false), 300);
    }
    return added;
  }

  private addNodes(nodes: DagNode[]) {
    for (const n of nodes) {
      for (const p of n.prev) this.addChild(p, n.id, "prev");
      for (const a of n.auth) this.addChild(a, n.id, "auth");
    }
    this.cy.add(
      nodes.map((n) => ({
        group: "nodes" as const,
        data: {
          id: n.id,
          label: shortType(n.type),
          ...idBox([shortType(n.type), n.id]),
          type: n.type,
          sender: n.sender,
          depth: n.depth,
          ts: n.ts,
          color: colorFor(this.opts.colorBy === "type" ? n.type : n.sender, this.opts.colorBy),
        },
        classes: [
          isState(n) ? "state" : "message",
          n.type === "m.room.create" ? "create" : "",
        ].join(" "),
      })),
    );
  }

  private addChild(parent: string, child: string, kind: EdgeKind) {
    const list = this.children.get(parent);
    if (list) list.push([child, kind]);
    else this.children.set(parent, [[child, kind]]);
  }

  private ensureMissing(id: string) {
    if (this.cy.getElementById(id).nonempty()) return;
    this.cy.add({ group: "nodes", data: { id, label: "?", depth: null, color: "", ...idBox(["missing", id]) }, classes: "missing" });
  }

  private addEdges(nodes: DagNode[]) {
    const edges: cytoscape.ElementDefinition[] = [];
    const seen = new Set<string>();
    const push = (from: string, to: string, kind: EdgeKind) => {
      if (this.cy.getElementById(from).empty() || this.cy.getElementById(to).empty()) return;
      const id = edgeId(kind, from, to);
      if (seen.has(id) || this.cy.getElementById(id).nonempty()) return;
      seen.add(id);
      edges.push({
        group: "edges",
        data: { id, source: from, target: to },
        classes: kind === "auth" && !this.opts.showAuth ? `${kind} hidden` : kind,
      });
    };
    for (const n of nodes) {
      // Edges point forward in time: parent → child.
      for (const p of n.prev) push(p, n.id, "prev");
      for (const a of n.auth) push(a, n.id, "auth");
    }
    // A newly arrived node may be the parent of nodes added earlier (e.g. it
    // replaced a "missing" placeholder), so reconnect those children too.
    for (const n of nodes) {
      for (const [child, kind] of this.children.get(n.id) ?? []) push(n.id, child, kind);
    }
    this.cy.add(edges);
  }

  private markExtremities(computed: string[], server?: string[]) {
    for (const id of computed) this.cy.getElementById(id).addClass("extremity");
    for (const id of server ?? []) this.cy.getElementById(id).addClass("server-extremity");
  }

  private recolor() {
    this.cy.batch(() => {
      this.cy.nodes().not(".missing").forEach((n) => {
        const key = this.opts.colorBy === "type" ? n.data("type") : n.data("sender");
        n.data("color", colorFor(key, this.opts.colorBy));
      });
    });
  }

  /** Counts per colour key, for the legend. */
  legend(): { key: string; color: string; count: number }[] {
    const counts = new Map<string, number>();
    this.cy.nodes().not(".missing").forEach((n) => {
      const key = this.opts.colorBy === "type" ? n.data("type") : n.data("sender");
      counts.set(key, (counts.get(key) ?? 0) + 1);
    });
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([key, count]) => ({ key, count, color: colorFor(key, this.opts.colorBy) }));
  }

  layout(fit: boolean) {
    if (this.opts.layout === "elk") {
      const dir = { TB: "DOWN", BT: "UP", LR: "RIGHT", RL: "LEFT" }[this.opts.direction];
      this.cy
        .elements()
        .not(".auth")
        .layout({
          name: "elk",
          fit: false,
          animate: false,
          stop: () => {
            this.shapeEdges();
            if (fit) this.fitRecent();
          },
          nodeDimensionsIncludeLabels: false,
          elk: {
            algorithm: "layered",
            "elk.direction": dir,
            "elk.layered.spacing.nodeNodeBetweenLayers": 40,
            "elk.spacing.nodeNode": 25,
            "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
          },
        } as cytoscape.LayoutOptions)
        .run();
      return;
    }
    this.depthLayout();
    this.shapeEdges();
    if (fit) this.fitRecent();
  }

  /**
   * Places events by depth: one rank per distinct depth, siblings (events
   * sharing a depth, i.e. forks) spread across the other axis. O(n log n).
   */
  private depthLayout() {
    const nodes = this.cy.nodes();
    // Missing nodes get a depth just above their shallowest child.
    nodes.filter(".missing").forEach((m) => {
      const childDepths = m.outgoers("node").map((c) => c.data("depth") as number).filter((d) => d != null);
      m.data("depth", childDepths.length ? Math.min(...childDepths) - 1 : 0);
    });
    const byDepth = new Map<number, cytoscape.NodeSingular[]>();
    nodes.forEach((n) => {
      const d = n.data("depth") as number;
      if (!byDepth.has(d)) byDepth.set(d, []);
      byDepth.get(d)!.push(n);
    });
    const depths = [...byDepth.keys()].sort((a, b) => a - b);
    const { direction } = this.opts;
    const horizontal = direction === "LR" || direction === "RL";
    const flip = direction === "BT" || direction === "RL" ? -1 : 1;
    const spacing = { ...SPACING };
    if (this.opts.showIds) {
      let maxW = 0;
      let maxH = 0;
      nodes.forEach((n) => {
        maxW = Math.max(maxW, n.data("w"));
        maxH = Math.max(maxH, n.data("h"));
      });
      spacing.rank = (horizontal ? maxW : maxH) + 50;
      spacing.sibling = (horizontal ? maxH : maxW) + 25;
    }
    this.cy.batch(() => {
      depths.forEach((d, rank) => {
        const group = byDepth.get(d)!;
        group.sort((a, b) => (a.data("ts") ?? 0) - (b.data("ts") ?? 0) || (a.id() < b.id() ? -1 : 1));
        group.forEach((n, i) => {
          const along = rank * spacing.rank * flip;
          const across = (i - (group.length - 1) / 2) * spacing.sibling;
          n.position(horizontal ? { x: along, y: across } : { x: across, y: along });
        });
      });
    });
  }

  private shapeEdges() {
    this.curveAuthEdges();
    this.routeEdges();
  }

  /**
   * Bends auth edges into arcs (always to the same side) so they read as a
   * different kind of link from the straight prev edges. Longer edges arc
   * higher, up to a cap.
   */
  private curveAuthEdges() {
    this.cy.batch(() => {
      this.cy.edges(".auth").forEach((e) => {
        const s = e.source().position();
        const t = e.target().position();
        const len = Math.hypot(t.x - s.x, t.y - s.y);
        // Two control points near the ends lift long edges off the line
        // quickly instead of grazing the neighbouring events. A cubic curve
        // peaks at 3/4 of the control distance.
        const height = Math.min(Math.max(len * 0.3, 25), 120);
        const w = Math.min(0.3, 120 / len);
        e.style({
          "curve-style": "unbundled-bezier",
          "edge-distances": "node-position",
          "control-point-weights": `${w} ${1 - w}`,
          "control-point-distances": `${-height / 0.75} ${-height / 0.75}`,
        } as unknown as cytoscape.Css.Edge);
      });
    });
  }

  /**
   * Routes prev edges that skip ranks (e.g. dummy events pointing at old
   * events) through lanes beside the graph instead of straight through the
   * events in between, where they would be hidden. Lanes are packed like a
   * skyline: shorter edges hug the graph, longer ones stack outside them, and
   * each edge takes whichever side of the graph is currently lower. Only
   * used with the depth layout.
   */
  private routeEdges() {
    if (this.opts.layout === "elk") {
      // ELK already reserves room for long edges (and spreads the graph over
      // several rows), so its edges stay straight.
      this.cy.edges(".routed").removeStyle(ROUTE_PROPS).removeClass("routed");
      return;
    }
    const horizontal = this.opts.direction === "LR" || this.opts.direction === "RL";
    const main = (p: cytoscape.Position) => (horizontal ? p.x : p.y);
    const perp = (p: cytoscape.Position) => (horizontal ? p.y : p.x);
    const nodes = this.cy.nodes();
    if (nodes.empty()) return;

    // Ranks are the distinct positions along the main axis (ELK: layers).
    const coords = nodes.map((n) => main(n.position())).sort((a, b) => a - b);
    const ranks: number[] = [];
    for (const c of coords) if (!ranks.length || c - ranks[ranks.length - 1] > 5) ranks.push(c);
    const rankOf = (v: number) => {
      let lo = 0;
      let hi = ranks.length - 1;
      while (lo < hi) {
        const mid = (lo + hi + 1) >> 1;
        if (ranks[mid] <= v + 5) lo = mid;
        else hi = mid - 1;
      }
      return lo;
    };

    // Skyline per side of the centre line, in half-rank slots so an edge's
    // risers (between ranks) are accounted for: slot 2r is rank r itself.
    const perps = nodes.map((n) => perp(n.position())).sort((a, b) => a - b);
    const centre = perps[perps.length >> 1];
    const sky = [new Float64Array(ranks.length * 2), new Float64Array(ranks.length * 2)];
    const pad = this.opts.showIds ? 6 : 14; // dot labels sit below the node
    nodes.forEach((n) => {
      const p = perp(n.position());
      const half = (horizontal ? n.outerHeight() : n.outerWidth()) / 2 + pad;
      const slot = rankOf(main(n.position())) * 2;
      sky[0][slot] = Math.max(sky[0][slot], centre - (p - half));
      sky[1][slot] = Math.max(sky[1][slot], p + half - centre);
    });

    const long: { e: cytoscape.EdgeSingular; lo: number; hi: number }[] = [];
    this.cy.edges(".prev").forEach((e) => {
      const a = rankOf(main(e.source().position()));
      const b = rankOf(main(e.target().position()));
      if (Math.abs(a - b) > 1) long.push({ e, lo: Math.min(a, b), hi: Math.max(a, b) });
    });
    long.sort((x, y) => x.hi - x.lo - (y.hi - y.lo));

    this.cy.batch(() => {
      this.cy.edges(".routed").removeStyle(ROUTE_PROPS).removeClass("routed");
      for (const { e, lo, hi } of long) {
        const top = [0, 0];
        for (let side = 0; side < 2; side++) {
          for (let s = lo * 2 + 1; s < hi * 2; s++) top[side] = Math.max(top[side], sky[side][s]);
        }
        const side = top[0] <= top[1] ? 0 : 1;
        const h = top[side] + LANE_GAP;
        for (let s = lo * 2 + 1; s < hi * 2; s++) sky[side][s] = h;
        const lane = centre + (side ? h : -h);

        // Segment points are offsets from the straight source→target line,
        // along its normal (-dy, dx)/len; convert the lane position to that.
        const sp = e.source().position();
        const tp = e.target().position();
        const dx = tp.x - sp.x;
        const dy = tp.y - sp.y;
        const len = Math.hypot(dx, dy);
        const normal = (horizontal ? dx : -dy) / len;
        const span = Math.abs(main(tp) - main(sp));
        const [ra, rb] = rankOf(main(sp)) === lo ? [lo, hi] : [hi, lo];
        const riser = (from: number, to: number) => Math.abs(ranks[to] - ranks[from]) / 2;
        const weights = [
          riser(ra, ra + Math.sign(rb - ra)) / span,
          1 - riser(rb, rb + Math.sign(ra - rb)) / span,
        ];
        const dists = weights.map((w) => (lane - (perp(sp) + w * (perp(tp) - perp(sp)))) / normal);
        e.addClass("routed").style({
          "curve-style": "round-segments",
          "edge-distances": "node-position",
          "segment-weights": weights.join(" "),
          "segment-distances": dists.join(" "),
          "segment-radii": 10,
        } as unknown as cytoscape.Css.Edge);
      }
    });
  }

  /** Fit the newest part of the graph rather than the whole history. */
  fitRecent(animate = false) {
    const nodes = this.cy.nodes().sort((a, b) => (b.data("depth") ?? 0) - (a.data("depth") ?? 0));
    const eles = nodes.slice(0, this.opts.showIds ? 10 : 60);
    if (animate) this.cy.animate({ fit: { eles, padding: 40 } }, { duration: 300 });
    else this.cy.fit(eles, 40);
  }

  fitAll() {
    this.cy.fit(undefined, 30);
  }

  select(id: string): boolean {
    const n = this.cy.getElementById(id);
    if (n.empty()) return false;
    this.clearHighlight();
    this.cy.elements().addClass("faded");
    const hood = n.closedNeighborhood();
    hood.removeClass("faded").addClass("highlight");
    // Edges to the selected event's children get their own colour, so they
    // stand out from its prev_events.
    n.outgoers("edge.prev").addClass("child");
    n.addClass("focus");
    return true;
  }

  center(id: string) {
    const n = this.cy.getElementById(id);
    if (n.empty()) return;
    this.cy.stop(); // e.g. a peek returning to the previous view
    this.cy.animate({ center: { eles: n }, zoom: Math.max(this.cy.zoom(), 1) }, { duration: 300 });
  }

  /**
   * Emphasise one edge and the node at its far end (e.g. while hovering a
   * reference). If that node is off-screen, after a short dwell the view peeks
   * at it (after 3 s, as this can zoom far out): fitting the whole connection when that stays readable, otherwise
   * panning to the node. unhover() returns to the previous view.
   */
  hover(edge: string, node: string) {
    this.unhover();
    const e = this.cy.getElementById(edge).addClass("hover");
    const n = this.cy.getElementById(node).addClass("hover");
    if (n.empty()) return;
    const vp = this.cy.extent();
    const nb = n.boundingBox();
    if (nb.x1 >= vp.x1 && nb.x2 <= vp.x2 && nb.y1 >= vp.y1 && nb.y2 <= vp.y2) return;
    this.hoverTimer = window.setTimeout(() => {
      this.peekFrom = { zoom: this.cy.zoom(), pan: { ...this.cy.pan() } };
      const both = e.union(e.connectedNodes()).union(n);
      const bb = both.boundingBox();
      const pad = 60;
      const fitZoom = Math.min(this.cy.width() / (bb.w + 2 * pad), this.cy.height() / (bb.h + 2 * pad));
      this.cy.stop();
      if (fitZoom >= this.cy.zoom() * 0.5) {
        this.cy.animate({ fit: { eles: both, padding: pad } }, { duration: 300 });
      } else {
        this.cy.animate({ center: { eles: n } }, { duration: 300 });
      }
    }, 3000);
  }

  /** Clears hover emphasis; restore=false keeps the current view after a peek. */
  unhover(restore = true) {
    clearTimeout(this.hoverTimer);
    this.cy.elements(".hover").removeClass("hover");
    if (this.peekFrom && restore) {
      this.cy.stop();
      this.cy.animate({ zoom: this.peekFrom.zoom, pan: this.peekFrom.pan }, { duration: 300 });
    }
    this.peekFrom = null;
  }

  clearHighlight() {
    this.cy.elements().removeClass("faded highlight focus hover child");
  }

  private stylesheet(): cytoscape.StylesheetJson {
    const fg = cssVar("--fg") || "#111";
    const muted = cssVar("--muted") || "#888";
    const edge = cssVar("--edge") || "#9ca3af";
    const auth = cssVar("--edge-auth") || "#f59e0b";
    const child = cssVar("--edge-child") || "#c026d3";
    const accent = cssVar("--accent") || "#6366f1";
    const bg = cssVar("--bg") || "#fff";
    const box = { width: "data(w)", height: "data(h)" };
    // Overrides for event-ID mode: tinted boxes outlined in the node colour;
    // extremity/focus markers move to the outline so the border keeps the colour.
    const idRules: cytoscape.StylesheetJson = !this.opts.showIds
      ? []
      : [
          {
            selector: "node",
            style: {
              ...box,
              shape: "round-rectangle",
              label: "data(idLabel)",
              "font-family": "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
              "font-size": ID_FONT,
              "text-valign": "center",
              "text-margin-y": 0,
              "text-wrap": "wrap",
              "text-max-width": "2000px",
              "text-justification": "left",
              "background-opacity": 0.15,
              "border-width": 2,
              "border-color": "data(color)",
            },
          },
          { selector: "node.state", style: { "background-opacity": 0.35 } },
          { selector: "node.create", style: { ...box, shape: "round-rectangle", "border-width": 4 } },
          { selector: "node.missing", style: { "border-color": muted, "border-style": "dashed", color: muted } },
          {
            selector: "node.extremity",
            style: { "border-color": "data(color)", "outline-width": 3, "outline-color": accent, "outline-offset": 2 },
          },
          {
            selector: "node.server-extremity",
            style: {
              ...box,
              "border-style": "solid",
              "border-color": "data(color)",
              "outline-width": 5,
              "outline-color": accent,
              "outline-offset": 2,
            },
          },
        ];
    return [
      {
        selector: "node",
        style: {
          "background-color": "data(color)",
          width: 18,
          height: 18,
          label: "data(label)",
          "font-size": 9,
          color: fg,
          "text-valign": "bottom",
          "text-margin-y": 3,
          "min-zoomed-font-size": 7,
          "border-width": 1,
          "border-color": bg,
        },
      },
      { selector: "node.state", style: { shape: "round-rectangle" } },
      { selector: "node.create", style: { shape: "star", width: 28, height: 28 } },
      {
        selector: "node.missing",
        style: {
          "background-opacity": 0,
          "border-width": 2,
          "border-style": "dashed",
          "border-color": muted,
          color: muted,
          "text-valign": "center",
          "text-margin-y": 0,
        },
      },
      { selector: "node.extremity", style: { "border-width": 3, "border-color": accent } },
      {
        selector: "node.server-extremity",
        style: { "border-width": 4, "border-color": accent, "border-style": "double", width: 24, height: 24 },
      },
      ...idRules,
      {
        selector: "edge",
        style: {
          width: 1.3,
          "line-color": edge,
          "target-arrow-color": edge,
          "target-arrow-shape": "triangle",
          "arrow-scale": 0.6,
          "curve-style": "bezier",
        },
      },
      {
        selector: "edge.auth",
        style: { "line-style": "dashed", "line-color": auth, "target-arrow-color": auth, opacity: 0.55, width: 1 },
      },
      { selector: ".hidden", style: { display: "none" } },
      { selector: ".faded", style: { opacity: 0.15 } },
      { selector: "edge.highlight", style: { width: 2.5, opacity: 1 } },
      { selector: "edge.child", style: { "line-color": child, "target-arrow-color": child } },
      // Hovered reference in the side panel; shown even if auth edges are hidden.
      {
        selector: "edge.hover",
        style: {
          display: "element",
          opacity: 1,
          width: 4,
          "line-color": accent,
          "target-arrow-color": accent,
          "z-index": 20,
        },
      },
      {
        selector: "node.hover",
        style: { opacity: 1, "underlay-color": accent, "underlay-padding": 8, "underlay-opacity": 0.45, "z-index": 20 },
      },
      {
        selector: "node.focus",
        style: this.opts.showIds
          ? { "outline-width": 4, "outline-color": fg, "outline-offset": 2, "z-index": 10 }
          : { "border-width": 4, "border-color": fg, width: 26, height: 26, "z-index": 10 },
      },
    ];
  }
}
