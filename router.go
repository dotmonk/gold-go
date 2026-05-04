package gold

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func New(opts Options) *App {
	app := &App{options: normalizeOptions(opts), ops: map[string]operationRuntime{}, routes: []RouteInfo{}}
	RegisterAll(app)
	return app
}

func Request[In any, Out any](meta OperationMeta, handler func(context.Context, In) (Out, error)) Operation {
	if err := validateMeta(meta); err != nil {
		panic(err)
	}
	var inZero In
	var outZero Out
	inType := reflectTypeOf(inZero)
	outType := reflectTypeOf(outZero)

	route := RouteInfo{
		MethodName:   meta.Name,
		Namespace:    meta.Namespace,
		Params:       inputParams(inType),
		ReturnType:   typeScriptType(outType),
		IsSSE:        false,
		SSEEventType: "",
		DispatchKey:  buildDispatchKey(meta),
	}

	hasFiles := false
	for _, p := range route.Params {
		if p.Kind == "file" {
			hasFiles = true
			break
		}
	}

	return Operation{runtime: operationRuntime{
		route:      route,
		hasFiles:   hasFiles,
		inputType:  inType,
		outputType: outType,
		requestInvoker: func(ctx context.Context, payload map[string]any, files map[string]any) (any, error) {
			var in In
			if err := decodeJSONPayload(payload, &in); err != nil {
				var empty any
				return empty, err
			}
			injectFiles(&in, files)
			return handler(ctx, in)
		},
	}}
}

func Stream[In any, Ev any](meta OperationMeta, handler func(context.Context, In, chan<- Ev) error) Operation {
	if err := validateMeta(meta); err != nil {
		panic(err)
	}
	var inZero In
	var evZero Ev
	inType := reflectTypeOf(inZero)
	evType := reflectTypeOf(evZero)

	route := RouteInfo{
		MethodName:   meta.Name,
		Namespace:    meta.Namespace,
		Params:       inputParams(inType),
		ReturnType:   typeScriptType(evType),
		IsSSE:        true,
		SSEEventType: typeScriptType(evType),
		DispatchKey:  buildDispatchKey(meta),
	}

	return Operation{runtime: operationRuntime{
		route:           route,
		inputType:       inType,
		streamEventType: evType,
		streamInvoker: func(ctx context.Context, payload map[string]any, files map[string]any) (<-chan any, error) {
			var in In
			if err := decodeJSONPayload(payload, &in); err != nil {
				return nil, err
			}
			injectFiles(&in, files)

			typedCh := make(chan Ev)
			out := make(chan any)

			go func() {
				defer close(typedCh)
				_ = handler(ctx, in, typedCh)
			}()

			go func() {
				defer close(out)
				for {
					select {
					case <-ctx.Done():
						return
					case ev, ok := <-typedCh:
						if !ok {
							return
						}
						out <- ev
					}
				}
			}()

			return out, nil
		},
	}}
}

func reflectTypeOf[T any](sample T) reflect.Type {
	t := reflect.TypeOf(sample)
	if t != nil {
		return t
	}
	return reflect.TypeOf((*T)(nil)).Elem()
}

func (a *App) Register(op Operation) {
	key := op.runtime.route.DispatchKey
	if _, exists := a.ops[key]; exists {
		panic("duplicate operation key: " + key)
	}
	a.ops[key] = op.runtime
	a.routes = append(a.routes, op.runtime.route)
}

func (a *App) parsePayload(r *http.Request) (map[string]any, map[string]any, []string, error) {
	filesToDelete := []string{}
	payload := map[string]any{}
	files := map[string]any{}

	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		r.Body = http.MaxBytesReader(nil, r.Body, a.options.MaxMultipartBytes)
		if err := r.ParseMultipartForm(a.options.MaxMultipartBytes); err != nil {
			return nil, nil, filesToDelete, err
		}

		for k, values := range r.MultipartForm.Value {
			if len(values) == 1 {
				payload[k] = coerceFormValue(values[0])
				continue
			}
			arr := make([]any, 0, len(values))
			for _, v := range values {
				arr = append(arr, coerceFormValue(v))
			}
			payload[k] = arr
		}

		for fieldName, list := range r.MultipartForm.File {
			uploads := make([]UploadedFile, 0, len(list))
			for _, header := range list {
				uf, err := copyFormFileToTemp(fieldName, header, a.options.MultipartTmpDir)
				if err != nil {
					return nil, nil, filesToDelete, err
				}
				uploads = append(uploads, uf)
				filesToDelete = append(filesToDelete, uf.TempFilePath)
			}
			if len(uploads) == 1 {
				files[fieldName] = uploads[0]
			} else {
				files[fieldName] = uploads
			}
		}
		return payload, files, filesToDelete, nil
	}

	if r.Body == nil {
		return payload, files, filesToDelete, nil
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, filesToDelete, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return payload, files, filesToDelete, nil
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, nil, filesToDelete, err
	}
	return payload, files, filesToDelete, nil
}

func coerceFormValue(v string) any {
	trim := strings.TrimSpace(v)
	if strings.HasPrefix(trim, "{") || strings.HasPrefix(trim, "[") || trim == "true" || trim == "false" || trim == "null" {
		var out any
		if err := json.Unmarshal([]byte(trim), &out); err == nil {
			return out
		}
	}
	return v
}

func (a *App) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Run request interceptors
	for _, interceptor := range a.options.RequestInterceptors {
		var err error
		r, err = interceptor(r)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusUnauthorized)
			return
		}
	}

	payload, files, filesToDelete, err := a.parsePayload(r)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Invalid payload: %s"}`, err), http.StatusBadRequest)
		return
	}
	defer func() {
		for _, p := range filesToDelete {
			_ = os.Remove(p)
		}
	}()

	methodKey, ok := payload["__method"].(string)
	if !ok {
		http.Error(w, `{"error":"Missing __method"}`, http.StatusBadRequest)
		return
	}

	op, exists := a.ops[methodKey]
	if !exists {
		http.Error(w, fmt.Sprintf(`{"error":"Unknown method: %s"}`, methodKey), http.StatusNotFound)
		return
	}
	if op.route.IsSSE {
		http.Error(w, `{"error":"Method is streaming; use /gold_sse"}`, http.StatusBadRequest)
		return
	}
	if op.requestInvoker == nil {
		http.Error(w, `{"error":"Request handler not configured"}`, http.StatusInternalServerError)
		return
	}

	result, err := op.requestInvoker(r.Context(), payload, files)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Run response interceptors
	for _, interceptor := range a.options.ResponseInterceptors {
		var err error
		result, err = interceptor(w, r, result)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}

	if result == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
	}
}

func (a *App) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Run request interceptors
	for _, interceptor := range a.options.RequestInterceptors {
		var err error
		r, err = interceptor(r)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusUnauthorized)
			return
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	payload, files, filesToDelete, err := a.parsePayload(r)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Invalid payload: %s"}`, err), http.StatusBadRequest)
		return
	}
	defer func() {
		for _, p := range filesToDelete {
			_ = os.Remove(p)
		}
	}()

	methodKey, ok := payload["__method"].(string)
	if !ok {
		http.Error(w, `{"error":"Missing __method"}`, http.StatusBadRequest)
		return
	}

	op, exists := a.ops[methodKey]
	if !exists {
		http.Error(w, fmt.Sprintf(`{"error":"Unknown method: %s"}`, methodKey), http.StatusNotFound)
		return
	}
	if !op.route.IsSSE || op.streamInvoker == nil {
		http.Error(w, `{"error":"Method is not streaming"}`, http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"Streaming unsupported"}`, http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ch, err := op.streamInvoker(ctx, payload, files)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			raw, err := json.Marshal(event)
			if err != nil {
				fmt.Fprintf(w, "event: error\ndata: %s\n\n", `{"error":"Failed to encode SSE event"}`)
				flusher.Flush()
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", string(raw))
			flusher.Flush()
		}
	}
}

func renderInterface(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t.Name() == "" || t == reflect.TypeOf(Void{}) || t == reflect.TypeOf(UploadedFile{}) {
		return ""
	}

	lines := []string{"export interface " + t.Name() + " {"}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		name, omitempty, ignore := parseJSONTag(f.Tag.Get("json"), f.Name)
		if ignore {
			continue
		}
		opt := ""
		if omitempty {
			opt = "?"
		}
		lines = append(lines, "  "+name+opt+": "+typeScriptType(f.Type)+";")
	}
	lines = append(lines, "}")
	return strings.Join(lines, "\n")
}

func renderRequestMethod(route RouteInfo) string {
	params := make([]string, 0, len(route.Params))
	hasFile := false
	for _, p := range route.Params {
		opt := ""
		if p.IsOptional {
			opt = "?"
		}
		params = append(params, p.Name+opt+": "+p.TypeText)
		if p.Kind == "file" {
			hasFile = true
		}
	}
	paramSig := strings.Join(params, ", ")

	if hasFile {
		lines := []string{"    " + route.MethodName + "(" + paramSig + "): ApiResponse<" + route.ReturnType + "> {"}
		lines = append(lines, "      const _form = new FormData();")
		lines = append(lines, "      _form.append(\"__method\", \""+route.DispatchKey+"\");")
		for _, p := range route.Params {
			if p.Kind == "file" {
				if strings.HasSuffix(p.TypeText, "[]") {
					lines = append(lines, "      if ("+p.Name+" != null) for (const _f of "+p.Name+") _form.append(\""+p.Name+"\", _f as Blob);")
				} else {
					lines = append(lines, "      if ("+p.Name+" != null) _form.append(\""+p.Name+"\", "+p.Name+" as Blob);")
				}
			} else {
				lines = append(lines, "      if ("+p.Name+" != null) _form.append(\""+p.Name+"\", typeof "+p.Name+" === 'string' ? "+p.Name+" : JSON.stringify("+p.Name+"));")
			}
		}
		lines = append(lines,
			"      return fetch('/gold_request', { method: 'POST', body: _form }).then(async (r) => {",
			"        if (!r.ok) throw new Error(`HTTP ${r.status}`);",
			"        if ('"+route.ReturnType+"' === 'void') return undefined as unknown as "+route.ReturnType+";",
			"        const _text = await r.text();",
			"        if (!_text) return undefined as unknown as "+route.ReturnType+";",
			"        return JSON.parse(_text) as "+route.ReturnType+";",
			"      });",
			"    },",
		)
		return strings.Join(lines, "\n")
	}

	bodyParts := []string{"__method: \"" + route.DispatchKey + "\""}
	for _, p := range route.Params {
		bodyParts = append(bodyParts, p.Name+": "+p.Name)
	}
	bodyExpr := "{ " + strings.Join(bodyParts, ", ") + " }"

	lines := []string{
		"    " + route.MethodName + "(" + paramSig + "): ApiResponse<" + route.ReturnType + "> {",
		"      return fetch('/gold_request', {",
		"        method: 'POST',",
		"        headers: { 'Content-Type': 'application/json' },",
		"        body: JSON.stringify(" + bodyExpr + "),",
		"      }).then(async (r) => {",
		"        if (!r.ok) throw new Error(`HTTP ${r.status}`);",
		"        if ('" + route.ReturnType + "' === 'void') return undefined as unknown as " + route.ReturnType + ";",
		"        const _text = await r.text();",
		"        if (!_text) return undefined as unknown as " + route.ReturnType + ";",
		"        return JSON.parse(_text) as " + route.ReturnType + ";",
		"      });",
		"    },",
	}
	return strings.Join(lines, "\n")
}

func renderSSEMethod(route RouteInfo) string {
	params := make([]string, 0, len(route.Params)+2)
	bodyParts := []string{"__method: \"" + route.DispatchKey + "\""}
	for _, p := range route.Params {
		opt := ""
		if p.IsOptional {
			opt = "?"
		}
		params = append(params, p.Name+opt+": "+p.TypeText)
		bodyParts = append(bodyParts, p.Name+": "+p.Name)
	}
	params = append(params, "callback: (data: "+route.SSEEventType+") => void", "onError?: (err: Event) => void")
	bodyExpr := "{ " + strings.Join(bodyParts, ", ") + " }"

	lines := []string{
		"    " + route.MethodName + "(" + strings.Join(params, ", ") + "): () => void {",
		"      const _ctrl = new AbortController();",
		"      fetch('/gold_sse', {",
		"        method: 'POST',",
		"        headers: { 'Content-Type': 'application/json' },",
		"        body: JSON.stringify(" + bodyExpr + "),",
		"        signal: _ctrl.signal,",
		"      }).then(async (res) => {",
		"        if (!res.ok || !res.body) { if (onError) onError(new Event('error')); return; }",
		"        const reader = res.body.getReader();",
		"        const decoder = new TextDecoder();",
		"        let buf = '';",
		"        while (true) {",
		"          const { done, value } = await reader.read();",
		"          if (done) break;",
		"          buf += decoder.decode(value, { stream: true });",
		"          const lines = buf.split('\\n');",
		"          buf = lines.pop() ?? '';",
		"          for (const line of lines) {",
		"            if (line.startsWith('data: ')) callback(JSON.parse(line.slice(6)) as " + route.SSEEventType + ");",
		"          }",
		"        }",
		"      }).catch(() => { if (!_ctrl.signal.aborted && onError) onError(new Event('error')); });",
		"      return () => _ctrl.abort();",
		"    },",
	}
	return strings.Join(lines, "\n")
}

func (a *App) generateClient() error {
	routes := sortedRouteCopy(a.routes)

	collected := map[string]reflect.Type{}
	for _, op := range a.ops {
		collectNamedStructs(op.inputType, collected)
		collectNamedStructs(op.outputType, collected)
		collectNamedStructs(op.streamEventType, collected)
	}

	names := make([]string, 0, len(collected))
	for name := range collected {
		names = append(names, name)
	}
	sort.Strings(names)

	lines := []string{
		"// AUTO-GENERATED by gold-go -- do not edit manually",
		"// Generated on " + time.Now().Format(time.RFC3339),
		"",
		"export type ApiResponse<T> = Promise<T>;",
		"",
	}

	for _, name := range names {
		iface := renderInterface(collected[name])
		if iface == "" {
			continue
		}
		lines = append(lines, iface)
		lines = append(lines, "")
	}

	byNamespace := map[string][]RouteInfo{}
	for _, route := range routes {
		byNamespace[route.Namespace] = append(byNamespace[route.Namespace], route)
	}

	nsNames := make([]string, 0, len(byNamespace))
	for ns := range byNamespace {
		nsNames = append(nsNames, ns)
	}
	sort.Strings(nsNames)

	lines = append(lines, "const _api = {")
	for _, ns := range nsNames {
		lines = append(lines, "  "+ns+": {")
		routesInNS := byNamespace[ns]
		sort.Slice(routesInNS, func(i, j int) bool { return routesInNS[i].MethodName < routesInNS[j].MethodName })
		for _, route := range routesInNS {
			if route.IsSSE {
				lines = append(lines, renderSSEMethod(route))
			} else {
				lines = append(lines, renderRequestMethod(route))
			}
		}
		lines = append(lines, "  },")
	}
	lines = append(lines, "} as const;", "", "export default _api;")
	for _, ns := range nsNames {
		lines = append(lines, "export const "+ns+" = _api."+ns+";")
	}

	outputPath := filepath.Join(a.options.WorkDir, a.options.GeneratedClientPath)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outputPath, []byte(strings.Join(lines, "\n")), 0o644)
}

func firstAvailablePort(start int) (int, error) {
	for p := start; p < start+20; p++ {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		if err != nil {
			continue
		}
		_ = ln.Close()
		return p, nil
	}
	return 0, fmt.Errorf("no available Vite port in range %d-%d", start, start+19)
}

func (a *App) startVite(ctx context.Context) (*exec.Cmd, *httputil.ReverseProxy, int, error) {
	vitePort, err := firstAvailablePort(a.options.VitePort)
	if err != nil {
		return nil, nil, 0, err
	}

	viteURL, err := url.Parse("http://127.0.0.1:" + strconv.Itoa(vitePort))
	if err != nil {
		return nil, nil, 0, err
	}
	proxy := httputil.NewSingleHostReverseProxy(viteURL)

	args := []string{"vite", "--host", "127.0.0.1", "--port", strconv.Itoa(vitePort), "--strictPort", "--logLevel", "silent"}
	if a.options.ViteConfig != "" {
		args = append(args, "--config", a.options.ViteConfig)
	}

	cmd := exec.CommandContext(ctx, "npx", args...)
	cmd.Dir = a.options.WorkDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, 0, err
	}

	go func() {
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		res, err := http.Get(viteURL.String())
		if err == nil {
			_ = res.Body.Close()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	return cmd, proxy, vitePort, nil
}

func (a *App) ListenAndServe() error {
	if err := a.generateClient(); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/gold_healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/gold_request", a.handleRequest)
	mux.HandleFunc("/gold_sse", a.handleSSE)

	var viteCmd *exec.Cmd
	var viteProxy *httputil.ReverseProxy
	var viteCancel context.CancelFunc
	vitePort := a.options.VitePort
	if a.options.Dev {
		ctx, cancel := context.WithCancel(context.Background())
		viteCancel = cancel
		cmd, proxy, actualVitePort, err := a.startVite(ctx)
		if err != nil {
			cancel()
			return err
		}
		vitePort = actualVitePort
		proxy.ModifyResponse = func(resp *http.Response) error {
			contentType := strings.ToLower(resp.Header.Get("Content-Type"))
			if !strings.Contains(contentType, "text/html") {
				return nil
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			_ = resp.Body.Close()

			const reloadScript = `<script>(function(){var wasDown=false;setInterval(function(){fetch('/gold_healthz',{cache:'no-store'}).then(function(){if(wasDown){location.reload();}wasDown=false;}).catch(function(){wasDown=true;});},1000);})();</script>`
			html := string(body)
			lower := strings.ToLower(html)
			if idx := strings.Index(lower, "</head>"); idx >= 0 {
				html = html[:idx] + reloadScript + html[idx:]
			} else {
				html += reloadScript
			}

			resp.Body = io.NopCloser(strings.NewReader(html))
			resp.ContentLength = int64(len(html))
			resp.Header.Set("Content-Length", strconv.Itoa(len(html)))
			resp.Header.Del("Content-Encoding")
			return nil
		}
		viteCmd = cmd
		viteProxy = proxy
	}

	frontendDir := filepath.Join(a.options.WorkDir, a.options.FrontendDir)
	indexPath := filepath.Join(frontendDir, "index.html")
	staticHandler := http.FileServer(http.Dir(frontendDir))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gold_healthz" || r.URL.Path == "/gold_request" || r.URL.Path == "/gold_sse" {
			http.NotFound(w, r)
			return
		}
		if a.options.Dev && viteProxy != nil {
			r.Header.Set("Accept-Encoding", "identity")
			viteProxy.ServeHTTP(w, r)
			return
		}
		full := filepath.Join(frontendDir, filepath.Clean(r.URL.Path))
		if st, err := os.Stat(full); err == nil && !st.IsDir() {
			staticHandler.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, indexPath)
	})

	fmt.Printf("gold-go: registered %d routes\n", len(a.routes))
	fmt.Printf("gold-go: generated %s\n", filepath.Join(a.options.WorkDir, a.options.GeneratedClientPath))
	fmt.Printf("gold-go: server running on http://localhost:%d\n", a.options.Port)
	if a.options.Dev {
		fmt.Printf("gold-go: dev mode enabled (vite via backend proxy on :%d)\n", vitePort)
	}

	server := &http.Server{Addr: fmt.Sprintf(":%d", a.options.Port), Handler: mux}

	shutdownVite := func() {
		if viteCancel != nil {
			viteCancel()
		}
		if viteCmd != nil && viteCmd.Process != nil {
			_ = viteCmd.Process.Kill()
		}
	}

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	errCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err == nil || err == http.ErrServerClosed {
			errCh <- nil
			return
		}
		errCh <- err
	}()

	select {
	case <-sigCtx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("gold-go: shutdown error: %v", err)
		}
		shutdownVite()
		return nil
	case err := <-errCh:
		shutdownVite()
		return err
	}
}
