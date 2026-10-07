// Command dagviz is a Matrix client that renders room event DAGs in a web UI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/config"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/dag"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/matrix"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/web"
	webui "github.com/ricardo-duarte-av/matrix-dag-vizualizer/web"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := newLogger(cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := dag.NewStore(cfg.Matrix.MaxEventsPerRoom)
	mx := matrix.New(cfg.Matrix, store, log.With().Str("component", "matrix").Logger())
	if err := mx.Connect(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Matrix")
	}
	log.Info().Str("user_id", mx.UserID().String()).Str("homeserver", mx.Homeserver()).
		Bool("synapse_admin", mx.IsAdmin()).Msg("Connected")

	srv := web.New(cfg.Web, mx, webui.FS(), log.With().Str("component", "web").Logger())

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return mx.Run(ctx) })
	g.Go(func() error { return srv.Serve(ctx) })
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal().Err(err).Msg("Exiting")
	}
	log.Info().Msg("Shut down")
}

func newLogger(cfg config.LogConfig) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	var log zerolog.Logger
	if cfg.Format == "json" {
		log = zerolog.New(os.Stdout)
	} else {
		log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime})
	}
	return log.Level(level).With().Timestamp().Logger()
}
