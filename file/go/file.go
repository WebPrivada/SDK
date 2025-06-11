package file

import (
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"strings"
	"os"
)

func WBFile(b64Str, outputPath string) error {
	data, err := base64.StdEncoding.DecodeString(b64Str)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(outputPath, data, 0644)
}

func WTFile(textStr, outputPath string) error {
	return ioutil.WriteFile(outputPath, []byte(textStr), 0644)
}

func RBFile(inputPath string) string {
	data, err := ioutil.ReadFile(inputPath)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

func RTFile(inputPath string) string {
	data, err := ioutil.ReadFile(inputPath)
	if err != nil {
		return ""
	}
	return string(data)
}

func CreateDir(path string) error {
	return os.MkdirAll(path, 0755)
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func ListFiles(dirPath string) []string {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil
	}
	
	var fileNames []string
	for _, file := range files {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}
	return fileNames
}

func GetContentTypeFile(b64Str string) string {
	data, err := base64.StdEncoding.DecodeString(b64Str)
	if err != nil || len(data) < 12 {
		return "application/octet-stream"
	}

	contentType := http.DetectContentType(data[:12])
	
	switch {
	case strings.HasPrefix(contentType, "text/plain") && len(data) > 0:
		if isLikelyJSON(data) {
			return "application/json"
		}
		if isLikelyXML(data) {
			return "application/xml"
		}
	case strings.HasPrefix(contentType, "application/octet-stream"):
		if isPDF(data) {
			return "application/pdf"
		}
	}
	return contentType
}


// Funciones auxiliares para detección más precisa
func isPDF(data []byte) bool {
	return len(data) > 4 && string(data[:4]) == "%PDF"
}

func isLikelyJSON(data []byte) bool {
	firstChar := strings.TrimSpace(string(data[:1]))
	return firstChar == "{" || firstChar == "["
}

func isLikelyXML(data []byte) bool {
	str := strings.TrimSpace(string(data[:32]))
	return strings.HasPrefix(str, "<?xml") || 
	       strings.HasPrefix(str, "<html") || 
	       strings.HasPrefix(str, "<!DOCTYPE html")
}
