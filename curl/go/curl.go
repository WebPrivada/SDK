package curl

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"encoding/base64"
)

func makeRequest(method, url, body, headersStr string) string{
	var goBody []byte
	if body != "" {
		goBody = []byte(body)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(goBody))
	if err != nil {
		return ""
	}
	if headersStr != "" {
		for _, line := range strings.Split(headersStr, "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue // Formato inválido, ignorar
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			req.Header.Add(key, value)
		}
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(respBody)
}

func Header(key, value string) string{
	header := key+": " + value
	return header
}

func HeaderAuthToken(token string) string {
	header := "Authorization: Bearer " + token
	return header
}

func HeaderAuthBasic(user, pass string) string {
	authString := user + ":" + pass
	encoded := base64.StdEncoding.EncodeToString([]byte(authString))
	header := "Authorization: Basic " + encoded
	return header
}

func Get(url, headers, body string) string {
	return makeRequest("GET", url, body, headers)
}

func Post(url, headers, body string) string {
	return makeRequest("POST", url, body, headers)
}

func Put(url, headers, body string) string {
	return makeRequest("PUT", url, body, headers)
}

func Patch(url, headers, body string) string {
	return makeRequest("PATCH", url, body, headers)
}

func Delete(url, headers, body string) string {
	return makeRequest("DELETE", url, body, headers)
}

func Head(url, headers, body string) string {
	return makeRequest("HEAD", url, body, headers)
}

func Options(url, headers, body string) string {
	return makeRequest("OPTIONS", url, body, headers)
}

