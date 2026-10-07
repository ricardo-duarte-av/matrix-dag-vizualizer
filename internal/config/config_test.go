package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSampleConfigLoads(t *testing.T) {
	cfg, err := Load("../../sample.config.yaml")
	if err != nil {
		t.Fatalf("sample.config.yaml: %v", err)
	}
	if cfg.Web.Listen != ":8080" || cfg.Matrix.DataDir != "/data" {
		t.Errorf("unexpected values: %+v", cfg)
	}
}

func TestDefaultsAndNormalize(t *testing.T) {
	cfg, err := Load(write(t, `
matrix:
  homeserver: example.org
  user_id: "@bot:example.org"
  password: secret
web:
  base_path: viz
  layout_direction: lr
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Web.BasePath != "/viz/" || cfg.Web.LayoutDirection != "LR" {
		t.Errorf("normalize: base_path=%q direction=%q", cfg.Web.BasePath, cfg.Web.LayoutDirection)
	}
	if cfg.Matrix.BackfillPageSize != 100 || cfg.Web.MaxRenderNodes != 3000 {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("DAGVIZ_MATRIX_PASSWORD", "from-env")
	cfg, err := Load(write(t, "matrix:\n  homeserver: example.org\n  user_id: \"@bot:example.org\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Matrix.Password != "from-env" {
		t.Errorf("password = %q", cfg.Matrix.Password)
	}
}

func TestValidation(t *testing.T) {
	_, err := Load(write(t, "web:\n  color_by: rainbow\n"))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"homeserver", "user_id", "password", "color_by"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
