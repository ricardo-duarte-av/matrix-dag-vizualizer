// Package config loads and validates the dagviz YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Matrix MatrixConfig `yaml:"matrix"`
	Web    WebConfig    `yaml:"web"`
	Log    LogConfig    `yaml:"log"`
}

type MatrixConfig struct {
	// Homeserver is either a full client API base URL (https://matrix.example.org)
	// or a bare server name (example.org), in which case .well-known discovery is used.
	Homeserver  string `yaml:"homeserver"`
	UserID      string `yaml:"user_id"`
	Password    string `yaml:"password"`
	AccessToken string `yaml:"access_token"`
	DeviceID    string `yaml:"device_id"`
	DeviceName  string `yaml:"device_name"`

	// Rooms restricts the visualizer to these room IDs. Empty means every joined room
	// (plus, when the account is a Synapse admin, any room on the server).
	Rooms           []string `yaml:"rooms"`
	AutoJoinInvites bool     `yaml:"auto_join_invites"`

	InitialBackfill  int           `yaml:"initial_backfill"`
	BackfillPageSize int           `yaml:"backfill_page_size"`
	MaxEventsPerRoom int           `yaml:"max_events_per_room"`
	ResolveBudget    int           `yaml:"resolve_budget"`
	SyncTimeout      time.Duration `yaml:"sync_timeout"`
	AdminCheckEvery  time.Duration `yaml:"admin_check_interval"`
	DataDir          string        `yaml:"data_dir"`
}

type WebConfig struct {
	Listen    string    `yaml:"listen"`
	BasePath  string    `yaml:"base_path"`
	Title     string    `yaml:"title"`
	BasicAuth BasicAuth `yaml:"basic_auth"`

	DefaultLayout   string `yaml:"default_layout"`
	LayoutDirection string `yaml:"layout_direction"`
	ShowAuthEvents  bool   `yaml:"show_auth_events"`
	ShowEventIDs    bool   `yaml:"show_event_ids"`
	ColorBy         string `yaml:"color_by"`
	MaxRenderNodes  int    `yaml:"max_render_nodes"`
	Theme           string `yaml:"theme"`
	WebGL           bool   `yaml:"webgl"`
}

type BasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func (b BasicAuth) Enabled() bool { return b.Username != "" || b.Password != "" }

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Default() Config {
	return Config{
		Matrix: MatrixConfig{
			DeviceName:       "Matrix DAG Visualizer",
			InitialBackfill:  500,
			BackfillPageSize: 100,
			MaxEventsPerRoom: 20000,
			ResolveBudget:    200,
			SyncTimeout:      30 * time.Second,
			AdminCheckEvery:  5 * time.Minute,
			DataDir:          "/data",
		},
		Web: WebConfig{
			Listen:          ":8080",
			BasePath:        "/",
			Title:           "Matrix DAG Visualizer",
			DefaultLayout:   "depth",
			LayoutDirection: "LR",
			ColorBy:         "type",
			MaxRenderNodes:  3000,
			Theme:           "auto",
		},
		Log: LogConfig{Level: "info", Format: "console"},
	}
}

// Load reads the YAML file at path on top of the defaults, applies environment
// overrides and validates the result.
func Load(path string) (*Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.applyEnv()
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyEnv lets secrets be provided outside the config file.
func (c *Config) applyEnv() {
	for env, dst := range map[string]*string{
		"DAGVIZ_MATRIX_HOMESERVER":       &c.Matrix.Homeserver,
		"DAGVIZ_MATRIX_USER_ID":          &c.Matrix.UserID,
		"DAGVIZ_MATRIX_PASSWORD":         &c.Matrix.Password,
		"DAGVIZ_MATRIX_ACCESS_TOKEN":     &c.Matrix.AccessToken,
		"DAGVIZ_WEB_BASIC_AUTH_USER":     &c.Web.BasicAuth.Username,
		"DAGVIZ_WEB_BASIC_AUTH_PASSWORD": &c.Web.BasicAuth.Password,
	} {
		if v, ok := os.LookupEnv(env); ok {
			*dst = v
		}
	}
}

func (c *Config) normalize() error {
	var errs []error
	if c.Matrix.Homeserver == "" {
		errs = append(errs, errors.New("matrix.homeserver is required"))
	}
	if c.Matrix.UserID == "" {
		errs = append(errs, errors.New("matrix.user_id is required"))
	}
	if c.Matrix.Password == "" && c.Matrix.AccessToken == "" {
		errs = append(errs, errors.New("either matrix.password or matrix.access_token is required"))
	}
	if c.Matrix.BackfillPageSize <= 0 || c.Matrix.BackfillPageSize > 1000 {
		errs = append(errs, errors.New("matrix.backfill_page_size must be between 1 and 1000"))
	}
	if c.Matrix.MaxEventsPerRoom <= 0 {
		errs = append(errs, errors.New("matrix.max_events_per_room must be positive"))
	}
	if c.Matrix.InitialBackfill < 0 || c.Matrix.ResolveBudget < 0 {
		errs = append(errs, errors.New("matrix.initial_backfill and matrix.resolve_budget must not be negative"))
	}
	if c.Matrix.SyncTimeout < time.Second {
		c.Matrix.SyncTimeout = time.Second
	}
	if c.Matrix.AdminCheckEvery < 10*time.Second {
		c.Matrix.AdminCheckEvery = 10 * time.Second
	}

	c.Web.BasePath = "/" + strings.Trim(c.Web.BasePath, "/")
	if c.Web.BasePath != "/" {
		c.Web.BasePath += "/"
	}
	if !oneOf(c.Web.DefaultLayout, "depth", "elk") {
		errs = append(errs, fmt.Errorf("web.default_layout must be depth or elk, got %q", c.Web.DefaultLayout))
	}
	c.Web.LayoutDirection = strings.ToUpper(c.Web.LayoutDirection)
	if !oneOf(c.Web.LayoutDirection, "TB", "BT", "LR", "RL") {
		errs = append(errs, fmt.Errorf("web.layout_direction must be TB, BT, LR or RL, got %q", c.Web.LayoutDirection))
	}
	if !oneOf(c.Web.ColorBy, "type", "sender") {
		errs = append(errs, fmt.Errorf("web.color_by must be type or sender, got %q", c.Web.ColorBy))
	}
	if !oneOf(c.Web.Theme, "auto", "light", "dark") {
		errs = append(errs, fmt.Errorf("web.theme must be auto, light or dark, got %q", c.Web.Theme))
	}
	if c.Web.MaxRenderNodes <= 0 {
		errs = append(errs, errors.New("web.max_render_nodes must be positive"))
	}
	if !oneOf(c.Log.Format, "console", "json") {
		errs = append(errs, fmt.Errorf("log.format must be console or json, got %q", c.Log.Format))
	}
	return errors.Join(errs...)
}

func oneOf(v string, options ...string) bool {
	for _, o := range options {
		if v == o {
			return true
		}
	}
	return false
}
