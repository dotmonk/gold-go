package gold

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

// RequestInterceptor processes incoming requests before operation execution.
// It can modify the request, validate auth, add context, or reject the request.
type RequestInterceptor func(*http.Request) (*http.Request, error)

// ResponseInterceptor processes outgoing responses from operations.
// It can transform the result, add headers, log, or reject the response.
type ResponseInterceptor func(http.ResponseWriter, *http.Request, any) (any, error)

// UploadedFile represents a file uploaded via multipart form data.
type UploadedFile struct {
	FieldName    string
	Filename     string
	ContentType  string
	Size         int64
	TempFilePath string
}

// CreateReadStream returns a new reader for the uploaded file.
func (uf *UploadedFile) CreateReadStream() (io.ReadCloser, error) {
	return newFileReadCloser(uf.TempFilePath)
}

// ReadAsBuffer reads the entire uploaded file into memory.
func (uf *UploadedFile) ReadAsBuffer() ([]byte, error) {
	f, err := newFileReadCloser(uf.TempFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Options for the gold framework.
type Options struct {
	Port                   int
	Dev                    bool
	GeneratedClientPath    string
	FrontendDir            string
	WorkDir                string
	ViteConfig             string
	VitePort               int
	MaxMultipartBytes      int64
	MaxMultipartFieldBytes int64
	MultipartTmpDir        string
	RequestInterceptors    []RequestInterceptor
	ResponseInterceptors   []ResponseInterceptor
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		Port:                   3000,
		Dev:                    false,
		GeneratedClientPath:    "frontend/client.ts",
		FrontendDir:            "static",
		WorkDir:                ".",
		ViteConfig:             "vite.config.ts",
		VitePort:               5173,
		MaxMultipartBytes:      8 * 1024 * 1024 * 1024, // 8 GB
		MaxMultipartFieldBytes: 1024 * 1024,            // 1 MB
		MultipartTmpDir:        "/tmp/gold-uploads",
	}
}

// Void can be used as an operation return type to generate a TypeScript void return.
type Void struct{}

// OperationMeta describes a registered operation.
type OperationMeta struct {
	Namespace string
	Name      string
	Summary   string
}

// ParamInfo describes a function parameter.
type ParamInfo struct {
	Name       string
	TypeText   string
	IsOptional bool
	Kind       string // "body" or "file"
}

// RouteInfo describes a registered route (function).
type RouteInfo struct {
	MethodName   string
	Namespace    string
	Params       []ParamInfo
	ReturnType   string
	IsSSE        bool
	SSEEventType string
	DispatchKey  string
}

type operationRuntime struct {
	route           RouteInfo
	hasFiles        bool
	inputType       reflect.Type
	outputType      reflect.Type
	streamEventType reflect.Type
	requestInvoker  func(ctx context.Context, payload map[string]any, files map[string]any) (any, error)
	streamInvoker   func(ctx context.Context, payload map[string]any, files map[string]any) (<-chan any, error)
}

// Operation describes one typed backend endpoint.
type Operation struct {
	runtime operationRuntime
}

// App is the gold-go application runtime.
type App struct {
	options Options
	ops     map[string]operationRuntime
	routes  []RouteInfo
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.Port == 0 {
		opts.Port = defaults.Port
	}
	if opts.GeneratedClientPath == "" {
		opts.GeneratedClientPath = defaults.GeneratedClientPath
	}
	if opts.FrontendDir == "" {
		opts.FrontendDir = defaults.FrontendDir
	}
	if opts.WorkDir == "" {
		opts.WorkDir = defaults.WorkDir
	}
	if opts.ViteConfig == "" {
		opts.ViteConfig = defaults.ViteConfig
	}
	if opts.VitePort == 0 {
		opts.VitePort = defaults.VitePort
	}
	if opts.MaxMultipartBytes == 0 {
		opts.MaxMultipartBytes = defaults.MaxMultipartBytes
	}
	if opts.MaxMultipartFieldBytes == 0 {
		opts.MaxMultipartFieldBytes = defaults.MaxMultipartFieldBytes
	}
	if opts.MultipartTmpDir == "" {
		opts.MultipartTmpDir = defaults.MultipartTmpDir
	}
	return opts
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = []rune(strings.ToLower(string(runes[0])))[0]
	return string(runes)
}

func parseJSONTag(tag string, fallback string) (name string, omitempty bool, ignore bool) {
	if tag == "" {
		return lowerFirst(fallback), false, false
	}
	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return lowerFirst(fallback), false, false
	}
	if parts[0] == "-" {
		return "", false, true
	}
	name = parts[0]
	if name == "" {
		name = lowerFirst(fallback)
	}
	for _, p := range parts[1:] {
		if p == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty, false
}

func isUploadedFileType(t reflect.Type) bool {
	if t == reflect.TypeOf(UploadedFile{}) {
		return true
	}
	if t.Kind() == reflect.Pointer && t.Elem() == reflect.TypeOf(UploadedFile{}) {
		return true
	}
	if t.Kind() == reflect.Slice {
		elt := t.Elem()
		return elt == reflect.TypeOf(UploadedFile{}) || (elt.Kind() == reflect.Pointer && elt.Elem() == reflect.TypeOf(UploadedFile{}))
	}
	return false
}

func typeScriptType(t reflect.Type) string {
	if t == nil {
		return "void"
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(Void{}) {
		return "void"
	}
	if t == reflect.TypeOf(UploadedFile{}) {
		return "File | Blob"
	}
	if t.PkgPath() == "time" && t.Name() == "Time" {
		return "string"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.String:
		return "string"
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return "number[]"
		}
		return typeScriptType(t.Elem()) + "[]"
	case reflect.Map:
		return "Record<string, unknown>"
	case reflect.Struct:
		if t.Name() != "" {
			return t.Name()
		}
		return inlineStructType(t)
	case reflect.Interface:
		return "unknown"
	default:
		return "unknown"
	}
}

func inlineStructType(t reflect.Type) string {
	parts := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		name, omitempty, ignore := parseJSONTag(f.Tag.Get("json"), f.Name)
		if ignore {
			continue
		}
		tsType := typeScriptType(f.Type)
		opt := ""
		if omitempty {
			opt = "?"
		}
		parts = append(parts, name+opt+": "+tsType)
	}
	return "{ " + strings.Join(parts, "; ") + " }"
}

func collectNamedStructs(t reflect.Type, out map[string]reflect.Type) {
	if t == nil {
		return
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(UploadedFile{}) || t == reflect.TypeOf(Void{}) {
		return
	}

	switch t.Kind() {
	case reflect.Struct:
		if t.Name() != "" && t.PkgPath() != "time" {
			if _, exists := out[t.Name()]; !exists {
				out[t.Name()] = t
			}
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			collectNamedStructs(f.Type, out)
		}
	case reflect.Slice, reflect.Array:
		collectNamedStructs(t.Elem(), out)
	case reflect.Map:
		collectNamedStructs(t.Elem(), out)
	}
}

func inputParams(inType reflect.Type) []ParamInfo {
	for inType.Kind() == reflect.Pointer {
		inType = inType.Elem()
	}
	if inType.Kind() != reflect.Struct {
		return nil
	}

	params := make([]ParamInfo, 0, inType.NumField())
	for i := 0; i < inType.NumField(); i++ {
		f := inType.Field(i)
		if f.PkgPath != "" {
			continue
		}
		name, omitempty, ignore := parseJSONTag(f.Tag.Get("json"), f.Name)
		if ignore {
			continue
		}
		kind := "body"
		ts := typeScriptType(f.Type)
		if isUploadedFileType(f.Type) {
			kind = "file"
			ts = "File | Blob"
			if f.Type.Kind() == reflect.Slice {
				ts = "(File | Blob)[]"
			}
		}
		params = append(params, ParamInfo{Name: name, TypeText: ts, IsOptional: omitempty, Kind: kind})
	}
	return params
}

func buildDispatchKey(meta OperationMeta) string {
	return meta.Namespace + "." + meta.Name
}

func validateMeta(meta OperationMeta) error {
	if strings.TrimSpace(meta.Namespace) == "" {
		return errors.New("operation namespace is required")
	}
	if strings.TrimSpace(meta.Name) == "" {
		return errors.New("operation name is required")
	}
	return nil
}

func sortedRouteCopy(routes []RouteInfo) []RouteInfo {
	out := append([]RouteInfo(nil), routes...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].MethodName < out[j].MethodName
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}

func decodeJSONPayload(payload map[string]any, into any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, into)
}

func injectFiles(target any, files map[string]any) {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		if field.PkgPath != "" {
			continue
		}
		name, _, ignore := parseJSONTag(field.Tag.Get("json"), field.Name)
		if ignore {
			continue
		}
		fv := v.Field(i)
		if !fv.CanSet() {
			continue
		}
		val, ok := files[name]
		if !ok {
			continue
		}

		switch fv.Type() {
		case reflect.TypeOf(UploadedFile{}):
			switch t := val.(type) {
			case UploadedFile:
				fv.Set(reflect.ValueOf(t))
			case []UploadedFile:
				if len(t) > 0 {
					fv.Set(reflect.ValueOf(t[0]))
				}
			}
		default:
			if fv.Kind() == reflect.Slice && fv.Type().Elem() == reflect.TypeOf(UploadedFile{}) {
				switch t := val.(type) {
				case UploadedFile:
					fv.Set(reflect.ValueOf([]UploadedFile{t}))
				case []UploadedFile:
					fv.Set(reflect.ValueOf(t))
				}
			}
		}
	}
}

func copyFormFileToTemp(fieldName string, header *multipart.FileHeader, tmpDir string) (UploadedFile, error) {
	reader, err := header.Open()
	if err != nil {
		return UploadedFile{}, err
	}
	defer reader.Close()

	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return UploadedFile{}, err
	}

	tmp, err := os.CreateTemp(tmpDir, "gold-upload-*")
	if err != nil {
		return UploadedFile{}, err
	}
	defer tmp.Close()

	written, err := io.Copy(tmp, reader)
	if err != nil {
		return UploadedFile{}, err
	}

	return UploadedFile{
		FieldName:    fieldName,
		Filename:     header.Filename,
		ContentType:  header.Header.Get("Content-Type"),
		Size:         written,
		TempFilePath: filepath.Clean(tmp.Name()),
	}, nil
}

func newFileReadCloser(path string) (io.ReadCloser, error) {
	return os.Open(path)
}
