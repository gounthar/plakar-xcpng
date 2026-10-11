package xapi

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the connection settings shared by the importer and the exporter.
type Config struct {
	Host        string // host[:port] of the pool master, as given in location
	Username    string
	Password    string
	TLSInsecure bool
}

// ParseConfig reads the connection keys from a connector configuration map.
func ParseConfig(config map[string]string) (*Config, error) {
	loc := strings.TrimSpace(config["location"])
	if loc == "" {
		return nil, fmt.Errorf("missing location")
	}

	u, err := url.Parse(loc)
	if err != nil {
		return nil, fmt.Errorf("invalid location %q: %w", loc, err)
	}
	if u.Scheme != Protocol {
		return nil, fmt.Errorf("invalid location %q: scheme must be %s://", loc, Protocol)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid location %q: missing host", loc)
	}

	cfg := &Config{
		Host:     u.Host,
		Username: strings.TrimSpace(config["username"]),
		Password: config["password"],
	}
	if cfg.Username == "" {
		cfg.Username = "root"
	}

	cfg.TLSInsecure, err = ParseBool(config["tls_insecure"])
	if err != nil {
		return nil, fmt.Errorf("tls_insecure: %w", err)
	}

	return cfg, nil
}

// Origin is the value reported to Plakar as the source or target of a snapshot.
func (c *Config) Origin() string {
	return c.Host
}

// ParseBool accepts an empty string as false.
func ParseBool(value string) (bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q", value)
	}
	return b, nil
}
