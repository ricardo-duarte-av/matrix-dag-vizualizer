export interface AppConfig {
  title: string;
  default_layout: "depth" | "elk";
  layout_direction: "TB" | "BT" | "LR" | "RL";
  show_auth_events: boolean;
  color_by: "type" | "sender";
  max_render_nodes: number;
  theme: "auto" | "light" | "dark";
  webgl: boolean;
  user_id: string;
  homeserver: string;
  admin: boolean;
  room_allowlist: boolean;
}

export interface RoomInfo {
  room_id: string;
  name: string;
  version: string;
  joined: boolean;
  event_count: number;
  backfill_done: boolean;
}

export interface ServerRoom {
  room_id: string;
  name: string;
  canonical_alias: string;
  joined_members: number;
  joined_local_members: number;
  version: string;
}

export interface ServerRoomsResponse {
  rooms: ServerRoom[];
  total_rooms: number;
  next_batch?: number;
}

export interface DagNode {
  id: string;
  type: string;
  state_key?: string;
  sender: string;
  depth: number;
  ts: number;
  prev: string[];
  auth: string[];
  summary?: string;
}

export interface DagResponse {
  room: RoomInfo;
  nodes: DagNode[];
  missing: string[];
  extremities: string[];
  server_extremities?: string[];
  truncated: boolean;
  admin: boolean;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error((body as { error?: string }).error ?? `${res.status} ${res.statusText}`);
  }
  return body as T;
}

const room = (id: string) => `api/rooms/${encodeURIComponent(id)}`;

export const api = {
  config: () => request<AppConfig>("api/config"),
  rooms: () => request<{ rooms: RoomInfo[] }>("api/rooms").then((r) => r.rooms),
  serverRooms: (search: string, from = 0) =>
    request<ServerRoomsResponse>(
      `api/server-rooms?limit=100&from=${from}&search=${encodeURIComponent(search)}`,
    ),
  dag: (roomId: string, limit: number) => request<DagResponse>(`${room(roomId)}/dag?limit=${limit}`),
  backfill: (roomId: string, count: number) =>
    request<{ added: number; backfill_done: boolean }>(`${room(roomId)}/backfill?count=${count}`, {
      method: "POST",
    }),
  resolve: (roomId: string, budget?: number) =>
    request<{ resolved: number; remaining: number }>(
      `${room(roomId)}/resolve${budget ? `?budget=${budget}` : ""}`,
      { method: "POST" },
    ),
  event: (roomId: string, eventId: string) =>
    request<Record<string, unknown>>(`${room(roomId)}/event?id=${encodeURIComponent(eventId)}`),
  stream: (roomId: string) => new EventSource(`${room(roomId)}/stream`),
};
