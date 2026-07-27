package webui

import "embed"

// Assets contains the Vite production output embedded in the Controller binary.
//
//go:embed assets/*
var Assets embed.FS
