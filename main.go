/******************************************************************************
*
*  Copyright 2025 SAP SE
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
******************************************************************************/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/sapcc/go-bits/promquery"
	"gopkg.in/yaml.v3"
)

var (
	listenAddress  string
	promServerURL  string
	certPath       string
	certKeyPath    string
	configFile     string
	scrapeInterval time.Duration
)

var (
	collectorTracker = make(map[string]*prometheus.GaugeVec)
	collectorLock    sync.RWMutex // Added mutex for safe concurrent access
	registry         = prometheus.NewRegistry()
)

type LabelRuleType string

const (
	DropLabelRule LabelRuleType = "drop"
	FillLableRule LabelRuleType = "fill"
)

const exporterName = "PromQuery-Exporter"

// MetricConfig holds the structure for each metric defined in the config file.
type MetricConfig struct {
	Query        string            `yaml:"query"`
	ExportName   string            `yaml:"export_name"`
	Help         string            `yaml:"help"`
	StaticLabels map[string]string `yaml:"static_labels,omitempty"`
	LabelRules   []LabelRule       `yaml:"label_rules,omitempty"`
}

// LabelRule defines a rule for manipulating labels globally across all metrics.
type LabelRule struct {
	Type        LabelRuleType `yaml:"type"`
	LabelName   string        `yaml:"label_name,omitempty"`   // Filter by exact label_name
	LabelPrefix string        `yaml:"label_prefix,omitempty"` // Filter by prefix
}

// Config holds the overall configuration parsed from the YAML file.
type Config struct {
	Metrics    []MetricConfig `yaml:"metrics"`
	LabelRules []LabelRule    `yaml:"label_rules"` // Changed field name for clarity
}

/*
This program fetches metrics defined in a config file from a Prometheus
server, applies label rules, and re-exports them.
*/
func main() {
	// --- Configuration Loading ---
	appConfig, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("Error loading configuration from %q: %v", configFile, err)
	}
	if len(appConfig.Metrics) == 0 {
		log.Fatalf("No metric definitions found in config file %q. Exiting.", configFile)
	}
	log.Printf("Successfully loaded %d metric definitions and %d label rules from %s",
		len(appConfig.Metrics), len(appConfig.LabelRules), configFile)

	// --- Prometheus Client Setup ---
	promCfg := promquery.Config{
		ServerURL:                promServerURL,
		ClientCertificatePath:    certPath,
		ClientCertificateKeyPath: certKeyPath,
	}
	promClient, err := promCfg.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to Prometheus at %s: %v", promCfg.ServerURL, err)
	}
	log.Printf("Successfully connected to Prometheus at %s", promCfg.ServerURL)

	// --- Prometheus Exporter Server Setup ---
	registry.MustRegister(
		collectors.NewBuildInfoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	// Basic landing page
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<html>
            <head><title>PromQuery Exporter</title></head>
            <body>
            <h1>PromQuery Exporter</h1>
            <p><a href="/metrics">Metrics</a></p>
            <p><a href="/healthz">Health</a></p>
            <p><a href="/ready">Ready</a></p>
            </body>
            </html>`)
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Could add checks here later (e.g., successful connection to Prometheus)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		Registry: registry,
	}))

	log.Printf("Starting exporter server on %s", listenAddress)
	go func() {
		if err := http.ListenAndServe(listenAddress, nil); err != nil {
			log.Fatalf("Error starting metrics server: %v", err)
		}
	}()

	// --- Query Processing Loop ---
	ctx := context.Background()
	ticker := time.NewTicker(scrapeInterval)
	defer ticker.Stop()

	log.Printf("Starting metric processing loop (every %s)", scrapeInterval)
	collectMetrics(ctx, promClient, appConfig)
	for range ticker.C {
		collectMetrics(ctx, promClient, appConfig)
	}
}

// collectMetrics queries Prometheus for each metric and updates collectors concurrently.
func collectMetrics(ctx context.Context, client promquery.Client, appConfig Config) {
	collectionStart := time.Now()

	var wg sync.WaitGroup
	wg.Add(len(appConfig.Metrics))
	log.Println("Starting metrics collection cycle...")

	for i := range appConfig.Metrics {
		metricCfg := appConfig.Metrics[i]
		go func() {
			defer wg.Done()
			processSingleMetric(ctx, client, metricCfg)
		}()
	}

	wg.Wait()
	log.Printf("Metrics collection cycle finished in %s.", time.Since(collectionStart))
}

// processSingleMetric handles the querying and updating for one metric definition.
func processSingleMetric(ctx context.Context, client promquery.Client, cfg MetricConfig) {
	timeoutInterval := 60 * time.Second
	if scrapeInterval < timeoutInterval {
		timeoutInterval = scrapeInterval - 5*time.Second
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeoutInterval)
	defer cancel()

	vector, err := client.GetVector(queryCtx, cfg.Query)
	if err != nil {
		log.Printf("ERROR: Failed to query Prometheus for metric %q: %v", cfg.ExportName, err)
		return
	}
	if len(vector) == 0 {
		log.Printf("INFO: Query for %q returned no results. Skipping update.", cfg.ExportName)
		collectorLock.RLock()
		if collector, exists := collectorTracker[cfg.ExportName]; exists {
			collectorLock.RUnlock()
			collector.Reset()
			log.Printf("INFO: Reset existing collector for %q as query returned no results.", cfg.ExportName)
		} else {
			collectorLock.RUnlock()
		}
		return
	}
	rules := cfg.LabelRules
	registerOrUpdateMetric(cfg, vector, rules)
}

// registerOrUpdateMetric creates or updates the GaugeVec for a given metric definition.
func registerOrUpdateMetric(cfg MetricConfig, vector model.Vector, rules []LabelRule) { // Added rules param
	labelKeys := getLabelKeys(vector[0], cfg.StaticLabels, rules)

	// --- Safely get or create the collector ---
	collectorLock.RLock()
	gauge, exists := collectorTracker[cfg.ExportName]
	collectorLock.RUnlock()

	if !exists {
		newGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: cfg.ExportName,
			Help: cfg.Help,
		}, labelKeys)

		collectorLock.Lock()
		// Double-check if another goroutine created it while waiting for the lock
		if existingGauge, doubleCheckExists := collectorTracker[cfg.ExportName]; doubleCheckExists {
			log.Printf("INFO: Collector for %q was created concurrently. Using existing.", cfg.ExportName)
			gauge = existingGauge
		} else {
			if err := registry.Register(newGauge); err != nil {
				log.Printf("ERROR: Failed to register collector for %q: %v. Skipping metric update.", cfg.ExportName, err)
				collectorLock.Unlock()
				return
			}
			log.Printf("Successfully registered collector for %q with label keys %v.", cfg.ExportName, labelKeys)
			collectorTracker[cfg.ExportName] = newGauge
			gauge = newGauge
		}
		collectorLock.Unlock()
	}

	// Reset the collector before applying new values to remove stale series
	gauge.Reset()

	processedCount := 0
	for _, sample := range vector {
		labels := make(prometheus.Labels)
		for _, key := range labelKeys {
			if staticValue, staticOk := cfg.StaticLabels[key]; staticOk {
				labels[key] = staticValue
			} else if value, ok := sample.Metric[model.LabelName(key)]; ok {
				labels[key] = string(value)
			} else {
				labels[key] = "" // Default to empty string if label not found
			}
		}

		metricInstance, err := gauge.GetMetricWith(labels)
		if err != nil {
			// This might happen if labelKeys derived from vector[0] are inconsistent
			// with labels in other samples *after* rule application (should be rare if rules are simple drops).
			log.Printf("ERROR: Failed to get metric for %q with labels %v (derived from sample %v): %v", cfg.ExportName, labels, sample.Metric, err)
			continue
		}
		metricInstance.Set(float64(sample.Value))
		processedCount++
	}
	if processedCount > 0 {
		log.Printf("  Updated %d series for metric %q", processedCount, cfg.ExportName)
	}
}

// getLabelKeys extracts label keys from a sample, applies rules, adds static label keys, and sorts them.
func getLabelKeys(sample *model.Sample, staticLabels map[string]string, rules []LabelRule) []string { // Added rules param
	keysMap := make(map[string]struct{})
	for key := range sample.Metric {
		keysMap[string(key)] = struct{}{}
	}
	for key := range staticLabels {
		keysMap[key] = struct{}{}
	}
	return applyRulesAndFinalizeKeys(keysMap, rules)
}

// applyRulesAndFinalizeKeys takes a map of potential keys, applies rules, removes standard unwanted labels, and returns a sorted slice.
func applyRulesAndFinalizeKeys(initialKeys map[string]struct{}, rules []LabelRule) []string {
	finalKeys := make(map[string]struct{})

keyLoop:
	for key := range initialKeys {
		for _, rule := range rules {
			switch rule.Type {
			case DropLabelRule:
				if rule.LabelName != "" && rule.LabelPrefix == "" {
					if key == rule.LabelName {
						continue keyLoop
					}
				} else if rule.LabelPrefix != "" {
					if strings.HasPrefix(key, rule.LabelPrefix) {
						continue keyLoop
					}
				}
			default:
			}
		}
		finalKeys[key] = struct{}{}
	}

	for _, rule := range rules {
		switch rule.Type {
		case FillLableRule:
			if _, exists := finalKeys[rule.LabelName]; !exists {
				finalKeys[rule.LabelName] = struct{}{}
			}
		default:
		}
	}

	// --- Remove standard Prometheus labels AFTER rules ---
	delete(finalKeys, "__name__")
	delete(finalKeys, "job")
	delete(finalKeys, "instance")

	// Convert map keys to slice
	keys := make([]string, 0, len(finalKeys))
	for key := range finalKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// loadConfig reads and parses the YAML configuration file into the Config struct.
func loadConfig(path string) (Config, error) {
	var cfg Config
	if path == "" {
		return Config{}, fmt.Errorf("config file path cannot be empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file %q: %w", path, err)
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("failed to parse YAML config file %q: %w", path, err)
	}

	// Validate LabelRules
	validatedRules := make([]LabelRule, 0, len(cfg.LabelRules))
	for _, rule := range cfg.LabelRules {
		if !validateLabelRule(rule) {
			log.Printf("WARN: Invalid label rule %v in config file %q. Skipping.", rule, path)
		} else {
			validatedRules = append(validatedRules, rule)
		}
	}
	cfg.LabelRules = validatedRules

	// --- Validation ---
	validatedMetrics := make([]MetricConfig, 0, len(cfg.Metrics))
	for i, metricCfg := range cfg.Metrics {
		valid := true
		if metricCfg.Query == "" {
			log.Printf("WARN: Config metrics item %d in %q is missing required field 'query'. Skipping.", i, path)
			valid = false
		}
		if !model.IsValidMetricName(model.LabelValue(metricCfg.ExportName)) {
			log.Printf("WARN: Config metrics item %d in %q has invalid 'export_name' %q (violates Prometheus naming conventions). Skipping.", i, path, metricCfg.ExportName)
			valid = false
		}
		if metricCfg.Help == "" {
			log.Printf("WARN: Config metrics item %d (%s) in %q is missing 'help' field. Using default.", i, metricCfg.ExportName, path)
			metricCfg.Help = fmt.Sprintf("%s exported metric %s", exporterName, metricCfg.ExportName)
		}

		// validate metric's individual label rules and merge with global rules
		metricLabelRules := validatedRules
		for _, rule := range metricCfg.LabelRules {
			if !validateLabelRule(rule) {
				log.Printf("WARN: Invalid label rule %v in config file %q for metric %q. Skipping.", rule, path, metricCfg.ExportName)
			} else {
				metricLabelRules = append(metricLabelRules, rule)
			}
		}
		metricCfg.LabelRules = metricLabelRules

		if valid {
			validatedMetrics = append(validatedMetrics, metricCfg)
		}
	}
	cfg.Metrics = validatedMetrics

	return cfg, nil
}

// move label rule validation to a function
func validateLabelRule(rule LabelRule) bool {
	switch rule.Type {
	case DropLabelRule:
		if rule.LabelName == "" && rule.LabelPrefix == "" {
			return false
		}
	case FillLableRule:
		if rule.LabelName == "" {
			return false
		}
	default:
		return false
	}
	return true
}

func init() {
	flag.StringVar(&listenAddress, "listen-address", ":9547", "Address for this exporter to listen on")
	flag.StringVar(&promServerURL, "prom", "", "Prometheus server URL (e.g., http://prometheus.example.com:9090)")
	flag.StringVar(&certPath, "cert-path", "", "Path to client certificate PEM file for Prometheus TLS auth")
	flag.StringVar(&certKeyPath, "cert-key-path", "", "Path to client certificate key PEM file for Prometheus TLS auth")
	flag.StringVar(&configFile, "config", "config.yaml", "Path to the YAML configuration file defining queries and metrics")
	flag.DurationVar(&scrapeInterval, "scrape-interval", 60*time.Second, "Interval between Prometheus query cycles (e.g., 30s, 1m)") // Default increased
	flag.Parse()

	if promServerURL == "" {
		fmt.Fprintln(os.Stderr, "Error: Prometheus server URL (--prom) is required")
		flag.Usage()
		os.Exit(1)
	}
	if (certPath != "" && certKeyPath == "") || (certPath == "" && certKeyPath != "") {
		fmt.Fprintln(os.Stderr, "Error: Both --cert-path and --cert-key-path must be provided together for client TLS auth")
		flag.Usage()
		os.Exit(1)
	}
}
