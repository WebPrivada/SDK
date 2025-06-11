package http

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type TokenInfo struct {
    Token string
    Expiration int64
}

// HttpRequest representa una petición HTTP (similar a la versión C)
type HttpRequest struct {
	Method      string
	Path        string
	Body        string
	ClientIP    string
	Headers     string
	Username    string
	Password    string
	BearerToken string
}

// HttpResponse representa una respuesta HTTP (similar a la versión C)
type HttpResponse struct {
	StatusCode int
	Body       string
}

// HttpHandler es el tipo para los manejadores de ruta (similar a la versión C)
type HttpHandler func(HttpRequest) HttpResponse

var (
	handlers      = make(map[string]HttpHandler)
	handlersMutex sync.RWMutex
)

func parseBasicAuth(authHeader string) (string, string, bool) {
	if !strings.HasPrefix(authHeader, "Basic ") {
		return "", "", false
	}

	decoded, err := base64.StdEncoding.DecodeString(authHeader[6:])
	if err != nil {
		return "", "", false
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func parseBearerToken(authHeader string) (string, bool) {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return "", false
	}
	return authHeader[7:], true
}

// getClientIP obtiene la IP real del cliente
func getClientIP(r *http.Request) string {
    if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
        ips := strings.Split(xff, ",")
        ip := strings.TrimSpace(ips[0])
        if ip != "" {
            return ip
        }
    }

    if xri := r.Header.Get("X-Real-IP"); xri != "" {
        return xri
    }

    if ra := r.RemoteAddr; ra != "" {
        ip, _, err := net.SplitHostPort(ra)
        if err == nil {
            return ip
        }
        return ra
    }

    return ""
}

func getHeadersString(r *http.Request) string {
	headers, _ := json.Marshal(r.Header)
	return string(headers)
}


// StartServer inicia el servidor HTTP/HTTPS (similar a la versión C)
func StartServer(port string, enableFilter int, certFile string, keyFile string) {
	portStr := port

	var handler http.Handler = http.DefaultServeMux
	if enableFilter == 1 {
		handler = ipFilterMiddleware(handler)
	}

	// Configurar el mux principal con nuestros handlers
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handlersMutex.RLock()
		routeHandler, exists := handlers[r.URL.Path]
		handlersMutex.RUnlock()

		if !exists {
			http.NotFound(w, r)
			return
		}

		// Configurar content-type por defecto
		w.Header().Set("Content-Type", "application/json")

		// Manejar el body
		var body []byte
		var err error

		if r.ContentLength > 0 {
		    // Validar Content-Type solo para métodos que usualmente llevan body
		    if r.Method != http.MethodGet && r.Method != http.MethodHead {
		        contentType := r.Header.Get("Content-Type")
		        if !strings.HasPrefix(contentType, "application/json") {
		            sendErrorResponse(w, http.StatusUnsupportedMediaType,
		                "Content-Type must be application/json")
		            return
		        }
		    }

		    // Leer body
		    body, err = io.ReadAll(r.Body)
		    if err != nil {
		        sendErrorResponse(w, http.StatusBadRequest,
		            "Error reading request body")
		        return
		    }
		    defer r.Body.Close()

		    // Verificar JSON solo si hay contenido y es requerido
		    if len(body) > 0 && r.Method != http.MethodGet && r.Method != http.MethodHead {
		        if !json.Valid(body) {
		            sendErrorResponse(w, http.StatusBadRequest,
		                "Invalid JSON format")
		            return
		        }
		    }
		}

		// Procesar autenticación
		authHeader := r.Header.Get("Authorization")
		username, password, bearerToken := "", "", ""

		if authHeader != "" {
			if u, p, ok := parseBasicAuth(authHeader); ok {
				username, password = u, p
			} else if token, ok := parseBearerToken(authHeader); ok {
				bearerToken = token
			}
		}

		// Crear request para el handler
		req := HttpRequest{
			Method:      r.Method,
			Path:        r.URL.Path,
			Body:        string(body),
			ClientIP:    getClientIP(r),
			Headers:     getHeadersString(r),
			Username:    username,
			Password:    password,
			BearerToken: bearerToken,
		}

		// Llamar al handler
		response := routeHandler(req)

		// Manejar respuesta
		w.WriteHeader(response.StatusCode)
		if response.Body != "" {
			w.Write([]byte(response.Body))
		}
	})

	// Aplicar middleware si está habilitado
	if enableFilter == 1 {
		handler = ipFilterMiddleware(mux)
	} else {
		handler = mux
	}

	server := &http.Server{
		Addr:    ":" + portStr,
		Handler: handler,
	}

	go func() {
		var err error
		if certFile != "" && keyFile != "" {
			err = server.ListenAndServeTLS(certFile, keyFile)
		} else {
			err = server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			panic("Error al iniciar el servidor: " + err.Error())
		}
	}()
}

// RegisterHandler registra un manejador para una ruta específica (similar a la versión C)
func RegisterHandler(path string, handler HttpHandler) {
	handlersMutex.Lock()
	defer handlersMutex.Unlock()
	handlers[path] = handler
}

// Funciones auxiliares
func sendErrorResponse(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (r *HttpRequest) GetMethod() string {
    return r.Method
}

func (r *HttpRequest) GetPath() string {
    return r.Path
}

func (r *HttpRequest) GetBody() string {
    return r.Body
}

func (r *HttpRequest) GetClientIP() string {
    return r.ClientIP
}

func (r *HttpRequest) GetHeaders() string {
    return r.Headers
}

func (r *HttpRequest) GetHeaderValue(key string) string {
    // Parsear los headers del JSON almacenado en Headers
    var headers map[string][]string
    err := json.Unmarshal([]byte(r.Headers), &headers)
    if err != nil {
        return ""
    }

    // Buscar el header (case-insensitive)
    key = strings.ToLower(key)
    for name, values := range headers {
        if strings.ToLower(name) == key && len(values) > 0 {
            return values[0]
        }
    }
    return ""
}

func (r *HttpRequest) GetUsername() string {
    return r.Username
}

func (r *HttpRequest) GetPassword() string {
    return r.Password
}

func (r *HttpRequest) GetBearerToken() string {
    return r.BearerToken
}

func CreateResponse(statusCode int, body string) HttpResponse {
    return HttpResponse{
    	StatusCode: statusCode,
    	Body:       body,
   	}
}

// ListManager gestiona las listas de IPs
type ListManager struct {
	whitelist map[string]bool
	blacklist map[string]bool
	mu        sync.RWMutex
}

var ipListManager = &ListManager{
	whitelist: make(map[string]bool),
	blacklist: make(map[string]bool),
}

func AddToWhitelist(ip string) int {
	if net.ParseIP(ip) == nil {
		return 0 // IP inválida
	}

	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	ipListManager.whitelist[ip] = true
	// Si está en blacklist, la quitamos
	delete(ipListManager.blacklist, ip)
	
	return 1 // Éxito
}

func RemoveFromWhitelist(ip string) int {	
	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	delete(ipListManager.whitelist, ip)
	return 1
}

func AddToBlacklist(ip string) int {
	if net.ParseIP(ip) == nil {
		return 0 // IP inválida
	}

	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	ipListManager.blacklist[ip] = true
	// Si está en whitelist, la quitamos
	delete(ipListManager.whitelist, ip)
	
	return 1 // Éxito
}

func RemoveFromBlacklist(ip string) int {
	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	delete(ipListManager.blacklist, ip)
	return 1
}

func IsWhitelisted(ip string) int {
	ipListManager.mu.RLock()
	defer ipListManager.mu.RUnlock()
	
	if _, exists := ipListManager.whitelist[ip]; exists {
		return 1
	}
	return 0
}

func IsBlacklisted(ip string) int {
	ipListManager.mu.RLock()
	defer ipListManager.mu.RUnlock()
	
	if _, exists := ipListManager.blacklist[ip]; exists {
		return 1
	}
	return 0
}

// Middleware para verificación de IP
func ipFilterMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := getClientIP(r)
		
		ipListManager.mu.RLock()
		defer ipListManager.mu.RUnlock()
		
		// Primero verificar blacklist
		if _, blacklisted := ipListManager.blacklist[clientIP]; blacklisted {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		
		// Si hay whitelist, verificar
		if len(ipListManager.whitelist) > 0 {
			if _, whitelisted := ipListManager.whitelist[clientIP]; !whitelisted {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
		}
		
		next.ServeHTTP(w, r)
	})
}

func LoadWhitelist(ips string) {
	ipList := strings.Split(ips, ",")
	
	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	ipListManager.whitelist = make(map[string]bool)
	for _, ip := range ipList {
		ip = strings.TrimSpace(ip)
		if net.ParseIP(ip) != nil {
			ipListManager.whitelist[ip] = true
		}
	}
}

func LoadBlacklist(ips string) {
	ipList := strings.Split(ips, ",")
	
	ipListManager.mu.Lock()
	defer ipListManager.mu.Unlock()
	
	ipListManager.blacklist = make(map[string]bool)
	for _, ip := range ipList {
		ip = strings.TrimSpace(ip)
		if net.ParseIP(ip) != nil {
			ipListManager.blacklist[ip] = true
		}
	}
}


























// TokenManager gestiona tokens de autenticación
type TokenManager struct {
	tokens      map[string]time.Time // token -> expiration time
	secretKey   string               // clave secreta para validación (opcional)
	mu          sync.RWMutex
	tokenExpiry time.Duration       // duración por defecto de los tokens
}

var tokenManager = &TokenManager{
	tokens:    make(map[string]time.Time),
	secretKey: "default-secret-key", // Cambiar en producción
	tokenExpiry: 24 * time.Hour,    // 24 horas por defecto
}

func SetTokenSecretKey(key string) {
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	tokenManager.secretKey = key
}

func SetDefaultTokenExpiry(seconds int64) {
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	tokenManager.tokenExpiry = time.Duration(seconds) * time.Second
}

func GenerateToken() string {
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	// Generar un token único (en producción usar un método más seguro)
	token := fmt.Sprintf("%x-%x-%x", 
		time.Now().UnixNano(), 
		rand.Int63(), 
		rand.Int63())
	
	expiration := time.Now().Add(tokenManager.tokenExpiry)
	tokenManager.tokens[token] = expiration
	
	return token
}

func ValidateToken(token string) int {    
    tokenManager.mu.RLock()
    defer tokenManager.mu.RUnlock()
    
    expiration, exists := tokenManager.tokens[token]
    if !exists {
        return 0 // Token no existe
    }
    
    if time.Now().After(expiration) {
        return -1 // Token expirado
    }
    
    // Token válido, calcular margen adicional (1ms) para evitar falsos positivos
    if time.Until(expiration) <= time.Millisecond {
        return 0 // Considerar como expirado si está muy cerca
    }
    
    return 1 // Token válido
}

func InvalidateToken(token string) {
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	
	delete(tokenManager.tokens, token)
}

func GetTokenExpiration(token string) int64 {
    tokenManager.mu.RLock()
    defer tokenManager.mu.RUnlock()
    
    if expiration, exists := tokenManager.tokens[token]; exists {
        if time.Now().Before(expiration) {
            return expiration.Unix()
        }
    }
    
    return 0
}

func SetTokenExpiration(token string, expiration int64) int {
	expTime := time.Unix(expiration, 0)
	
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	
	if _, exists := tokenManager.tokens[token]; exists {
		tokenManager.tokens[token] = expTime
		return 1
	}
	
	return 0
}

func CleanExpiredTokens() int {
	tokenManager.mu.Lock()
	defer tokenManager.mu.Unlock()
	
	count := 0
	now := time.Now()
	
	for token, expiration := range tokenManager.tokens {
		if now.After(expiration) {
			delete(tokenManager.tokens, token)
			count++
		}
	}
	
	return int(count)
}

func GetTokenInfo(token string) *TokenInfo {
	tokenManager.mu.RLock()
	defer tokenManager.mu.RUnlock()
	
	if expiration, exists := tokenManager.tokens[token]; exists {
		return &TokenInfo{
			Token:      token,
			Expiration: expiration.Unix(),
		}
	}
	
	return nil
}

func IsTokenValid(token string) int {
    return ValidateToken(token)
}

func GetTokenRemainingTime(token string) float64 {
    tokenManager.mu.RLock()
    defer tokenManager.mu.RUnlock()
    
    if expiration, exists := tokenManager.tokens[token]; exists {
        remaining := time.Until(expiration).Seconds()
        return remaining
    }    
    return -1.0 // Token no encontrado
}
