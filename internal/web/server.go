// Package web serves the REST/SSE API and the embedded frontend.
package web

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/synapseadmin"

	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/config"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/dag"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/matrix"
)

type Server struct {
	cfg    config.WebConfig
	mx     *matrix.Client
	store  *dag.Store
	static fs.FS
	log    zerolog.Logger
}

func New(cfg config.WebConfig, mx *matrix.Client, static fs.FS, log zerolog.Logger) *Server {
	return &Server{cfg: cfg, mx: mx, store: mx.Store(), static: static, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/rooms", s.handleRooms)
	mux.HandleFunc("GET /api/server-rooms", s.handleServerRooms)
	mux.HandleFunc("GET /api/rooms/{room}/dag", s.handleDAG)
	mux.HandleFunc("POST /api/rooms/{room}/backfill", s.handleBackfill)
	mux.HandleFunc("POST /api/rooms/{room}/resolve", s.handleResolve)
	mux.HandleFunc("GET /api/rooms/{room}/stream", s.handleStream)
	mux.HandleFunc("GET /api/rooms/{room}/event", s.handleEvent)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /", s.spa())

	var h http.Handler = mux
	if s.cfg.BasicAuth.Enabled() {
		h = s.basicAuth(h)
	}
	if s.cfg.BasePath != "/" {
		prefix := strings.TrimSuffix(s.cfg.BasePath, "/")
		root := http.NewServeMux()
		root.Handle(s.cfg.BasePath, http.StripPrefix(prefix, h))
		root.Handle(prefix, http.RedirectHandler(s.cfg.BasePath, http.StatusMovedPermanently))
		h = root
	}
	return h
}

func (s *Server) basicAuth(next http.Handler) http.Handler {
	user, pass := []byte(s.cfg.BasicAuth.Username), []byte(s.cfg.BasicAuth.Password)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if r.URL.Path == "/healthz" ||
			(ok && subtle.ConstantTimeCompare([]byte(u), user) == 1 && subtle.ConstantTimeCompare([]byte(p), pass) == 1) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="dagviz"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

// spa serves the built frontend, falling back to index.html for unknown paths.
func (s *Server) spa() http.Handler {
	files := http.FileServerFS(s.static)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" {
			if _, err := fs.Stat(s.static, path); err == nil {
				if strings.HasPrefix(path, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(w, r)
				return
			}
		}
		index, err := fs.ReadFile(s.static, "index.html")
		if err != nil {
			http.Error(w, "frontend not built (run `npm run build` in web/)", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(index)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func intQuery(r *http.Request, key string, def, maxVal int) int {
	v, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil || v <= 0 {
		return def
	}
	return min(v, maxVal)
}

func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"title":            s.cfg.Title,
		"default_layout":   s.cfg.DefaultLayout,
		"layout_direction": s.cfg.LayoutDirection,
		"show_auth_events": s.cfg.ShowAuthEvents,
		"show_event_ids":   s.cfg.ShowEventIDs,
		"color_by":         s.cfg.ColorBy,
		"max_render_nodes": s.cfg.MaxRenderNodes,
		"theme":            s.cfg.Theme,
		"webgl":            s.cfg.WebGL,
		"user_id":          s.mx.UserID(),
		"homeserver":       s.mx.Homeserver(),
		"admin":            s.mx.IsAdmin(),
		"room_allowlist":   len(s.mx.Config().Rooms) > 0,
	})
}

func (s *Server) handleRooms(w http.ResponseWriter, _ *http.Request) {
	rooms := s.store.Rooms()
	rooms = slices.DeleteFunc(rooms, func(r dag.RoomInfo) bool { return !s.mx.Allowed(r.ID) })
	slices.SortFunc(rooms, func(a, b dag.RoomInfo) int {
		if a.Joined != b.Joined {
			if a.Joined {
				return -1
			}
			return 1
		}
		return strings.Compare(strings.ToLower(displayName(a)), strings.ToLower(displayName(b)))
	})
	writeJSON(w, http.StatusOK, map[string]any{"rooms": rooms})
}

func displayName(r dag.RoomInfo) string {
	if r.Name != "" {
		return r.Name
	}
	return r.ID
}

func (s *Server) handleServerRooms(w http.ResponseWriter, r *http.Request) {
	if !s.mx.IsAdmin() {
		writeError(w, http.StatusForbidden, errors.New("listing server rooms requires Synapse admin"))
		return
	}
	from, _ := strconv.Atoi(r.URL.Query().Get("from"))
	resp, err := s.mx.ServerRooms(r.Context(), r.URL.Query().Get("search"), max(from, 0), intQuery(r, "limit", 50, 500))
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	resp.Rooms = slices.DeleteFunc(resp.Rooms, func(ri synapseadmin.RoomInfo) bool { return !s.mx.Allowed(string(ri.RoomID)) })
	writeJSON(w, http.StatusOK, resp)
}

// roomFromPath validates the {room} path value against the allowlist and
// admin status (non-joined rooms are only reachable as a Synapse admin).
func (s *Server) roomFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	roomID := r.PathValue("room")
	if !strings.HasPrefix(roomID, "!") {
		writeError(w, http.StatusBadRequest, errors.New("invalid room ID"))
		return "", false
	}
	if !s.mx.Allowed(roomID) {
		writeError(w, http.StatusForbidden, errors.New("room is not in the configured allowlist"))
		return "", false
	}
	if info, ok := s.store.Room(roomID); (!ok || !info.Joined) && !s.mx.IsAdmin() {
		writeError(w, http.StatusNotFound, errors.New("not joined to this room"))
		return "", false
	}
	return roomID, true
}

type dagResponse struct {
	dag.Graph
	ServerExtremities []string `json:"server_extremities,omitempty"`
	Admin             bool     `json:"admin"`
}

func (s *Server) handleDAG(w http.ResponseWriter, r *http.Request) {
	roomID, ok := s.roomFromPath(w, r)
	if !ok {
		return
	}
	if err := s.mx.Hydrate(r.Context(), roomID); err != nil {
		s.log.Warn().Err(err).Str("room_id", roomID).Msg("Initial backfill failed")
	}
	g, _ := s.store.Snapshot(roomID, intQuery(r, "limit", s.cfg.MaxRenderNodes, 1_000_000))
	resp := dagResponse{Graph: g, Admin: s.mx.IsAdmin()}
	if resp.Admin {
		if ext, err := s.mx.Extremities(r.Context(), roomID); err == nil {
			resp.ServerExtremities = ext
		} else {
			s.log.Debug().Err(err).Msg("Failed to fetch forward extremities")
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleBackfill(w http.ResponseWriter, r *http.Request) {
	roomID, ok := s.roomFromPath(w, r)
	if !ok {
		return
	}
	cfg := s.mx.Config()
	added, err := s.mx.Backfill(r.Context(), roomID, intQuery(r, "count", cfg.BackfillPageSize*5, cfg.MaxEventsPerRoom))
	info, _ := s.store.Room(roomID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"added": added, "backfill_done": info.BackfillDone})
}

func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	roomID, ok := s.roomFromPath(w, r)
	if !ok {
		return
	}
	if !s.mx.IsAdmin() {
		writeError(w, http.StatusForbidden, errors.New("resolving missing events requires Synapse admin"))
		return
	}
	budget := s.mx.Config().ResolveBudget
	resolved, remaining, err := s.mx.ResolveMissing(r.Context(), roomID, intQuery(r, "budget", budget, budget*10))
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"resolved": resolved, "remaining": remaining})
}

func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	roomID, ok := s.roomFromPath(w, r)
	if !ok {
		return
	}
	eventID := r.URL.Query().Get("id")
	if eventID == "" {
		writeError(w, http.StatusBadRequest, errors.New("missing id"))
		return
	}
	raw, found := s.store.Raw(roomID, eventID)
	if !found && s.mx.IsAdmin() {
		var err error
		if raw, err = s.mx.FetchEvent(r.Context(), eventID); err == nil {
			found = true
		}
	}
	if !found {
		writeError(w, http.StatusNotFound, errors.New("event not available"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	roomID, ok := s.roomFromPath(w, r)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	updates, cancel := s.store.Subscribe(roomID)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Disable the server write timeout for this long-lived response.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})

	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			w.Write([]byte(": ping\n\n"))
		case u := <-updates:
			data, _ := json.Marshal(u.Nodes)
			w.Write([]byte("event: nodes\ndata: "))
			w.Write(data)
			w.Write([]byte("\n\n"))
		}
		flusher.Flush()
	}
}

// Serve runs the HTTP server until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.cfg.Listen,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	s.log.Info().Str("listen", s.cfg.Listen).Str("base_path", s.cfg.BasePath).Msg("Web server started")
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
