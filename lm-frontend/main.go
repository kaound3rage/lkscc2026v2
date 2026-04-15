package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port          string
	APIPrediction string
	APIForecast   string
	APIGatewayKey string
}


type Logger struct {
	logger *log.Logger
}

func NewLogger() *Logger {
	return &Logger{
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.logger.Printf("[INFO]  "+format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.logger.Printf("[ERROR] "+format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.logger.Printf("[WARN]  "+format, args...)
}

var appLog = NewLogger()


func loadConfig() Config {
	if err := godotenv.Load(); err != nil {
		appLog.Warn(".env file not found, reading from environment directly")
	}

	cfg := Config{
		Port:          getEnv("PORT", "3000"),
		APIPrediction: getEnv("API_PREDICTION", ""),
		APIForecast:   getEnv("API_FORECAST", ""),
		APIGatewayKey: getEnv("API_GATEWAY_KEY", ""),
	}

	if cfg.APIPrediction == "" {
		appLog.Warn("API_PREDICTION is not set")
	}
	if cfg.APIForecast == "" {
		appLog.Warn("API_FORECAST is not set")
	}
	if cfg.APIGatewayKey == "" {
		appLog.Warn("API_GATEWAY_KEY is not set")
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}


func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		appLog.Info("→ %s %s", r.Method, r.URL.Path)

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)

		duration := time.Since(start).Milliseconds()
		appLog.Info("← %s %s | %d | %dms", r.Method, r.URL.Path, rw.statusCode, duration)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}


func staticHandler(filename string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		http.ServeFile(w, r, "templates/"+filename)
	}
}


func proxyHandler(targetURL, apiKey string) http.HandlerFunc {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}


		proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, r.Body)
		if err != nil {
			appLog.Error("Failed to build proxy request to %s: %v", targetURL, err)
			http.Error(w, `{"error":"failed to build upstream request","status":"error"}`, http.StatusInternalServerError)
			return
		}

		for key, values := range r.Header {

			if isHopByHop(key) {
				continue
			}
			for _, v := range values {
				proxyReq.Header.Add(key, v)
			}
		}


		proxyReq.Header.Set("x-api-key", apiKey)
		proxyReq.Header.Set("Content-Type", "application/json")

		appLog.Info("Proxying POST → %s", targetURL)


		resp, err := client.Do(proxyReq)
		if err != nil {
			appLog.Error("Upstream request failed: %v", err)
			http.Error(w, `{"error":"upstream service unavailable","status":"error"}`, http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			if isHopByHop(key) {
				continue
			}
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}

		// Always return JSON content-type
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)

		if _, err := io.Copy(w, resp.Body); err != nil {
			appLog.Error("Failed to copy response body: %v", err)
		}
	}
}


func isHopByHop(header string) bool {
	hopByHop := []string{
		"Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "TE", "Trailers",
		"Transfer-Encoding", "Upgrade",
	}
	h := strings.ToLower(header)
	for _, hbh := range hopByHop {
		if h == strings.ToLower(hbh) {
			return true
		}
	}
	return false
}

// healthHandler returns a simple 200 OK for load-balancer health checks
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func main() {
	cfg := loadConfig()

	mux := http.NewServeMux()

	// ── Static pages ──────────────────────────────────────────────────────────
	mux.HandleFunc("/", staticHandler("index.html"))
	mux.HandleFunc("/prediction", staticHandler("prediction.html"))
	mux.HandleFunc("/forecasting", staticHandler("forecasting.html"))

	// ── Reverse proxy endpoints ───────────────────────────────────────────────
	mux.HandleFunc("/api/predict", proxyHandler(cfg.APIPrediction, cfg.APIGatewayKey))
	mux.HandleFunc("/api/forecast", proxyHandler(cfg.APIForecast, cfg.APIGatewayKey))

	// ── Health check ─────────────────────────────────────────────────────────
	mux.HandleFunc("/health", healthHandler)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	appLog.Info("lm-frontend starting on port %s", cfg.Port)
	appLog.Info("  GET  /             → templates/index.html")
	appLog.Info("  GET  /prediction   → templates/prediction.html")
	appLog.Info("  GET  /forecasting  → templates/forecasting.html")
	appLog.Info("  POST /api/predict  → %s", cfg.APIPrediction)
	appLog.Info("  POST /api/forecast → %s", cfg.APIForecast)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		appLog.Error("Server failed: %v", err)
		os.Exit(1)
	}
}