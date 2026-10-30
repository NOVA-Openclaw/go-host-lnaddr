package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeConfigFile writes data to a temp file with the given extension and
// returns its path.
func writeConfigFile(t *testing.T, ext, data string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config"+ext)
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// --- Group A: JSON config ---

func TestConfig_JSON_PrivateRouteHints_AbsentDefaultsFalse(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"RPCHost":"localhost:10009"}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected default false, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_True(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":true}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected true, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_False(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":false}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected false, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_Null(t *testing.T) {
	// encoding/json treats null as a no-op for a non-pointer bool field.
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":null}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected false after null, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_StringIsError(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":"true"}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for string value")
	}
}

func TestConfig_JSON_PrivateRouteHints_NumberIsError(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":1}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for numeric value")
	}
}

func TestConfig_JSON_PrivateRouteHints_ArrayIsError(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":[]}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for array value")
	}
}

func TestConfig_JSON_PrivateRouteHints_ObjectIsError(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PrivateRouteHints":{}}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for object value")
	}
}

func TestConfig_JSON_PrivateRouteHints_LowercaseKeyMatches(t *testing.T) {
	// encoding/json falls back to case-insensitive key matching.
	path := writeConfigFile(t, ".json", `{"privateroutehints":true}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected lowercase key to match and set true, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_UppercaseKeyMatches(t *testing.T) {
	path := writeConfigFile(t, ".json", `{"PRIVATEROUTEHINTS":true}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected uppercase key to match and set true, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_MisspelledKeyIsIgnored(t *testing.T) {
	// encoding/json silently ignores unmapped keys by default.
	path := writeConfigFile(t, ".json", `{"PrivatRouteHints":true}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected misspelled key to be ignored, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_DuplicateKeyLastWins(t *testing.T) {
	path := writeConfigFile(t, ".json",
		`{"PrivateRouteHints":true,"PrivateRouteHints":false}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected last duplicate value false, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_MalformedJSONIsError(t *testing.T) {
	path := writeConfigFile(t, ".json",
		`{"PrivateRouteHints":true,}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected malformed JSON error")
	}
}

func TestConfig_JSON_PrivateRouteHints_BOMPrefixedIsError(t *testing.T) {
	path := writeConfigFile(t, ".json", "\xef\xbb\xbf{\"PrivateRouteHints\":true}")
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected BOM-prefixed JSON to fail parsing")
	}
}

func TestConfig_JSON_PrivateRouteHints_LargeFileParses(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"PrivateRouteHints":true`)
	for i := 0; i < 5000; i++ {
		b.WriteString(",\"key")
		b.WriteString(strconvItoa(i))
		b.WriteString("\":\"")
		b.WriteString(strings.Repeat("x", 50))
		b.WriteString("\"")
	}
	b.WriteString("}")
	path := writeConfigFile(t, ".json", b.String())
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig large file: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected true in large file, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_JSON_PrivateRouteHints_SampleRoundTrip(t *testing.T) {
	path := writeConfigFile(t, ".json", sampleConfigJSON)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig sample: %v", err)
	}
	if cfg.RPCHost != "localhost:10009" {
		t.Fatalf("existing field changed: RPCHost=%s", cfg.RPCHost)
	}
	if cfg.AddressServerPort != 9990 {
		t.Fatalf("existing field changed: AddressServerPort=%d", cfg.AddressServerPort)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected PrivateRouteHints true from sample, got %v",
			cfg.PrivateRouteHints)
	}
}

// --- Group B: TOML config ---

func TestConfig_TOML_PrivateRouteHints_AbsentDefaultsFalse(t *testing.T) {
	path := writeConfigFile(t, ".toml", `RPCHost = "localhost:10009"`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected default false, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_True(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = true`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected true, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_False(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = false`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected false, got %v", cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_StringIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = "true"`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for string value")
	}
}

func TestConfig_TOML_PrivateRouteHints_IntegerIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = 1`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for integer value")
	}
}

func TestConfig_TOML_PrivateRouteHints_ArrayIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = [true]`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for array value")
	}
}

func TestConfig_TOML_PrivateRouteHints_InlineTableIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = {}`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected type-mismatch error for inline-table value")
	}
}

func TestConfig_TOML_PrivateRouteHints_LowercaseKeyMatches(t *testing.T) {
	// Deviation from TEST-CASES-ISSUE-284.md B8: BurntSushi/toml v1.3.2
	// matches struct tags case-insensitively in this project, so a
	// lowercase key is mapped to PrivateRouteHints. We assert the
	// observed library behavior rather than the documented guess.
	path := writeConfigFile(t, ".toml", `privateroutehints = true`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected lowercase TOML key to match and set true, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_MisspelledKeyIsIgnored(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHint = true`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected misspelled key to be ignored, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_DuplicateKeyIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", "PrivateRouteHints = true\nPrivateRouteHints = false")
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected duplicate-key parse error in TOML")
	}
}

func TestConfig_TOML_PrivateRouteHints_KeyInNostrSectionIsIgnored(t *testing.T) {
	path := writeConfigFile(t, ".toml", `[Nostr]
PrivateRouteHints = true`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != false {
		t.Fatalf("expected key in [Nostr] to be ignored, got %v",
			cfg.PrivateRouteHints)
	}
	if cfg.Nostr == nil {
		t.Fatalf("expected Nostr section to parse without corruption")
	}
}

func TestConfig_TOML_PrivateRouteHints_MalformedTOMLIsError(t *testing.T) {
	path := writeConfigFile(t, ".toml", `PrivateRouteHints = true
unterminated = "oops`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatalf("expected malformed TOML error")
	}
}

func TestConfig_TOML_PrivateRouteHints_TrailingComment(t *testing.T) {
	path := writeConfigFile(t, ".toml",
		`PrivateRouteHints = true # enable private route hints`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected true with trailing comment, got %v",
			cfg.PrivateRouteHints)
	}
}

func TestConfig_TOML_PrivateRouteHints_SampleRoundTrip(t *testing.T) {
	path := writeConfigFile(t, ".toml", sampleConfigTOML)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig sample: %v", err)
	}
	if cfg.RPCHost != "localhost:10009" {
		t.Fatalf("existing field changed: RPCHost=%s", cfg.RPCHost)
	}
	if cfg.AddressServerPort != 9990 {
		t.Fatalf("existing field changed: AddressServerPort=%d", cfg.AddressServerPort)
	}
	if cfg.PrivateRouteHints != true {
		t.Fatalf("expected PrivateRouteHints true from sample, got %v",
			cfg.PrivateRouteHints)
	}
}

// small helper to avoid importing strconv just for this file
func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for n := i; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}

const sampleConfigJSON = `{
  "RPCHost": "localhost:10009",
  "InvoiceMacaroonPath": "/lnd/macaroonpath/invoices.macaroon",
  "TLSCertPath": "/home/alice/.lnd/tls.cert",
  "WorkingDir": "/home/alice/.go-host-lnaddr",
  "ExternalURL": "https://sendmesats.com",
  "ListAllURLs": true,
  "LightningAddresses": ["tips@sendmesats.com"],
  "MinSendableMsat": 1000,
  "MaxSendableMsat": 100000000,
  "MaxCommentLength": 150,
  "Tag": "payRequest",
  "Metadata": [
    ["text/plain", "Welcome to sendmesats.com"],
    ["text/identifier", "tips@sendmesats.com"]
  ],
  "Thumbnail": "/path/to/thumbnail.[jpeg|png]",
  "SuccessMessage": "Thank you!",
  "InvoiceCallback": "https://sendmesats.com/invoice/",
  "AddressServerPort": 9990,
  "PrivateRouteHints": true,
  "Nostr": {
    "names": {"myNostrUsername": "npub1h....."},
    "relays": {"b9b.....": ["wss://my.relay.com"]}
  },
  "Zaps": {"Npub": "npub1...", "Nsec": "nsec1..."}
}`

const sampleConfigTOML = `RPCHost = "localhost:10009"
InvoiceMacaroonPath = "/lnd/macaroonpath/invoices.macaroon"
TLSCertPath = "/home/alice/.lnd/tls.cert"
WorkingDir = "/home/alice/.go-host-lnaddr"
ExternalURL = "https://sendmesats.com"
ListAllURLs = true
LightningAddresses = ["tips@sendmesats.com"]
MinSendableMsat = 1000
MaxSendableMsat = 100000000
MaxCommentLength = 150
Tag = "payRequest"
Metadata = [
  ["text/plain", "Welcome to sendmesats.com"],
  ["text/identifier", "tips@sendmesats.com"],
]
Thumbnail = "/path/to/thumbnail.[jpeg|png]"
SuccessMessage = "Thank you!"
InvoiceCallback = "https://sendmesats.com/invoice/"
AddressServerPort = 9990
PrivateRouteHints = true

[Nostr]
  [Nostr.names]
  myNostrUsername = "npub1h....."

  [Nostr.relays]
  "b9b....." = ["wss://my.relay.com"]

[Zaps]
Npub = "npub1..."
Nsec = "nsec1..."
`
