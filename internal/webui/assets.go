package webui

import "embed"

// Assets contains the fallback UI shipped in early development builds.
//
//go:embed assets/*
var Assets embed.FS
