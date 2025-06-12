package ftp

import (
    "bytes"
    "encoding/base64"
    "fmt"
    "io"
    "net"
    "net/url"
    "path/filepath"
    "strconv"
    "strings"
    "time"
    "github.com/pkg/sftp"
    "golang.org/x/crypto/ssh"
)

const (
    maxFileSize = 90 * 1024 * 1024 // 90MB limit
    timeout     = 30 * time.Second
)

// Error codes
const (
    ErrEmptyData        = -1
    ErrEmptyURL         = -2
    ErrInvalidScheme    = -3
    ErrMissingHostUser  = -4
    ErrMissingPath      = -5
    ErrConnectionFailed = -6
    ErrInitialRead      = -7
    ErrUserSend         = -8
    ErrUserAuth         = -9
    ErrPassSend         = -10
    ErrPassAuth         = -11
    ErrMkdirFailed      = -12
    ErrMkdirResponse    = -13
    ErrPasvMode         = -14
    ErrSizeCommand      = -15
    ErrSizeResponse     = -16
    ErrFileConflict     = -17
    ErrCwdCommand       = -18
    ErrCwdResponse      = -19
    ErrTypeCommand      = -20
    ErrStorCommand      = -21
    ErrDataTransfer     = -22
    ErrTransferConfirm  = -23
    ErrAsciiMode        = -24
    ErrSftpConnection   = -25
    ErrSftpClient       = -26
    ErrSftpOperation    = -27
)

func parsePASV(resp string) (string, error) {
    start := strings.Index(resp, "(")
    end := strings.Index(resp, ")")
    if start == -1 || end == -1 {
        return "", fmt.Errorf("formato PASV inválido")
    }
    parts := strings.Split(resp[start+1:end], ",")
    if len(parts) < 6 {
        return "", fmt.Errorf("formato PASV inválido")
    }
    ip := strings.Join(parts[0:4], ".")
    p1, err1 := strconv.Atoi(parts[4])
    p2, err2 := strconv.Atoi(parts[5])
    if err1 != nil || err2 != nil {
        return "", fmt.Errorf("puerto PASV inválido")
    }
    port := p1*256 + p2
    return fmt.Sprintf("%s:%d", ip, port), nil
}

func isSFTP(urlStr string) bool {
    u, err := url.Parse(urlStr)
    if err != nil {
        return false
    }
    return u.Scheme == "sftp"
}

func createSFTPClient(ftpUrl string) (*sftp.Client, error) {
    u, err := url.Parse(ftpUrl)
    if err != nil {
        return nil, fmt.Errorf("error parsing URL: %v", err)
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host

    if !strings.Contains(host, ":") {
        host += ":22"
    }

    config := &ssh.ClientConfig{
        User: user,
        Auth: []ssh.AuthMethod{
            ssh.Password(pass),
        },
        HostKeyCallback: ssh.InsecureIgnoreHostKey(),
        Timeout:         timeout,
    }

    conn, err := ssh.Dial("tcp", host, config)
    if err != nil {
        return nil, fmt.Errorf("error code %d: failed to connect to SFTP server: %v", ErrSftpConnection, err)
    }

    client, err := sftp.NewClient(conn)
    if err != nil {
        return nil, fmt.Errorf("error code %d: failed to create SFTP client: %v", ErrSftpClient, err)
    }

    return client, nil
}

func GetFTPFile(ftpUrl string) string {
    if ftpUrl == "" {
        return ""
    }

    if isSFTP(ftpUrl) {
        client, err := createSFTPClient(ftpUrl)
        if err != nil {
            return ""
        }
        defer client.Close()

        u, _ := url.Parse(ftpUrl)
        path := u.Path

        file, err := client.Open(path)
        if err != nil {
            return ""
        }
        defer file.Close()

        limitedReader := &io.LimitedReader{R: file, N: maxFileSize}
        var buffer bytes.Buffer
        if _, err := io.Copy(&buffer, limitedReader); err != nil {
            return ""
        }

        if limitedReader.N <= 0 || buffer.Len() == 0 {
            return ""
        }

        encoded := base64.StdEncoding.EncodeToString(buffer.Bytes())
        return encoded
    }

    // Original FTP implementation
    u, err := url.Parse(ftpUrl)
    if err != nil || u.Scheme != "ftp" {
        return ""
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := u.Path

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return ""
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return ""
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "TYPE I\r\n"); err != nil {
        return ""
    }
    conn.Read(buf[:])

    if _, err := fmt.Fprintf(conn, "PASV\r\n"); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return ""
    }

    pasvResp := string(buf[:n])
    dataAddr, err := parsePASV(pasvResp)
    if err != nil {
        return ""
    }

    dataConn, err := net.DialTimeout("tcp", dataAddr, timeout)
    if err != nil {
        return ""
    }
    defer dataConn.Close()
    dataConn.SetDeadline(time.Now().Add(timeout))

    if _, err := fmt.Fprintf(conn, "RETR %s\r\n", path); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "150") {
        return ""
    }

    limitedReader := &io.LimitedReader{R: dataConn, N: maxFileSize}
    var buffer bytes.Buffer
    if _, err := io.Copy(&buffer, limitedReader); err != nil {
        return ""
    }

    if limitedReader.N <= 0 {
        return ""
    }

    if buffer.Len() == 0 {
        return ""
    }

    encoded := base64.StdEncoding.EncodeToString(buffer.Bytes())
    return encoded
}

func GetFTPText(ftpUrl string) string {
    if ftpUrl == "" {
        return ""
    }

    if isSFTP(ftpUrl) {
        client, err := createSFTPClient(ftpUrl)
        if err != nil {
            return ""
        }
        defer client.Close()

        u, _ := url.Parse(ftpUrl)
        path := u.Path

        file, err := client.Open(path)
        if err != nil {
            return ""
        }
        defer file.Close()

        limitedReader := &io.LimitedReader{R: file, N: maxFileSize}
        var buffer bytes.Buffer
        if _, err := io.Copy(&buffer, limitedReader); err != nil {
            return ""
        }

        if limitedReader.N <= 0 || buffer.Len() == 0 {
            return ""
        }

        text := buffer.String()
        text = strings.ReplaceAll(text, "\r\n", "\n")
        text = strings.TrimSpace(text)
        return text
    }

    // Original FTP implementation
    u, err := url.Parse(ftpUrl)
    if err != nil || u.Scheme != "ftp" {
        return ""
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := u.Path

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return ""
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return ""
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return ""
    }

    if _, err := fmt.Fprintf(conn, "TYPE A\r\n"); err != nil {
        return ""
    }
    conn.Read(buf[:])

    if _, err := fmt.Fprintf(conn, "PASV\r\n"); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return ""
    }

    pasvResp := string(buf[:n])
    dataAddr, err := parsePASV(pasvResp)
    if err != nil {
        return ""
    }

    dataConn, err := net.DialTimeout("tcp", dataAddr, timeout)
    if err != nil {
        return ""
    }
    defer dataConn.Close()
    dataConn.SetDeadline(time.Now().Add(timeout))

    if _, err := fmt.Fprintf(conn, "RETR %s\r\n", path); err != nil {
        return ""
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "150") {
        return ""
    }

    limitedReader := &io.LimitedReader{R: dataConn, N: maxFileSize}
    var buffer bytes.Buffer
    if _, err := io.Copy(&buffer, limitedReader); err != nil {
        return ""
    }

    if limitedReader.N <= 0 {
        return ""
    }

    if buffer.Len() == 0 {
        return ""
    }

    text := buffer.String()
    text = strings.ReplaceAll(text, "\r\n", "\n")
    text = strings.TrimSpace(text)
    return text
}

func PutFTPFile(base64Data, ftpUrl string) error {
    if base64Data == "" {
        return fmt.Errorf("error code %d: datos vacíos", ErrEmptyData)
    }
    if ftpUrl == "" {
        return fmt.Errorf("error code %d: URL vacía", ErrEmptyURL)
    }

    data, err := base64.StdEncoding.DecodeString(base64Data)
    if err != nil {
        return fmt.Errorf("error decodificando base64: %v", err)
    }

    if isSFTP(ftpUrl) {
        client, err := createSFTPClient(ftpUrl)
        if err != nil {
            return err
        }
        defer client.Close()

        u, _ := url.Parse(ftpUrl)
        path := u.Path

        // Create parent directories if they don't exist
        dir := filepath.Dir(path)
        if dir != "." {
            if err := client.MkdirAll(dir); err != nil {
                return fmt.Errorf("error code %d: failed to create directories: %v", ErrSftpOperation, err)
            }
        }

        file, err := client.Create(path)
        if err != nil {
            return fmt.Errorf("error code %d: failed to create file: %v", ErrSftpOperation, err)
        }
        defer file.Close()

        if _, err := file.Write(data); err != nil {
            return fmt.Errorf("error code %d: failed to write file: %v", ErrSftpOperation, err)
        }

        return nil
    }

    // Original FTP implementation
    u, err := url.Parse(ftpUrl)
    if err != nil {
        return fmt.Errorf("error analizando URL: %v", err)
    }
    if u.Scheme != "ftp" {
        return fmt.Errorf("error code %d: URL no es FTP", ErrInvalidScheme)
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := u.Path

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return fmt.Errorf("error code %d: falta host o usuario", ErrMissingHostUser)
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return fmt.Errorf("error code %d: conexión fallida: %v", ErrConnectionFailed, err)
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: lectura inicial fallida: %v", ErrInitialRead, err)
    }

    if _, err = fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return fmt.Errorf("error code %d: error enviando usuario: %v", ErrUserSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return fmt.Errorf("error code %d: autenticación de usuario fallida", ErrUserAuth)
    }

    if _, err = fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return fmt.Errorf("error code %d: error enviando contraseña: %v", ErrPassSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return fmt.Errorf("error code %d: autenticación de contraseña fallida", ErrPassAuth)
    }

    if _, err = fmt.Fprintf(conn, "TYPE I\r\n"); err != nil {
        return fmt.Errorf("error code %d: error configurando modo binario", ErrTypeCommand)
    }
    conn.Read(buf[:])

    if _, err = fmt.Fprintf(conn, "PASV\r\n"); err != nil {
        return fmt.Errorf("error code %d: error entrando en modo pasivo", ErrPasvMode)
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: error leyendo respuesta PASV", ErrPasvMode)
    }

    pasvResp := string(buf[:n])
    dataAddr, err := parsePASV(pasvResp)
    if err != nil {
        return fmt.Errorf("error code %d: error analizando modo pasivo: %v", ErrPasvMode, err)
    }

    dataConn, err := net.DialTimeout("tcp", dataAddr, timeout)
    if err != nil {
        return fmt.Errorf("error code %d: error conexión de datos: %v", ErrConnectionFailed, err)
    }
    defer dataConn.Close()
    dataConn.SetDeadline(time.Now().Add(timeout))

    if _, err = fmt.Fprintf(conn, "STOR %s\r\n", path); err != nil {
        return fmt.Errorf("error code %d: error iniciando transferencia", ErrStorCommand)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "150") {
        return fmt.Errorf("error code %d: error preparando servidor", ErrDataTransfer)
    }

    _, err = io.Copy(dataConn, bytes.NewReader(data))
    if err != nil {
        return fmt.Errorf("error code %d: error enviando datos: %v", ErrDataTransfer, err)
    }
    dataConn.Close()

    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "226") {
        return fmt.Errorf("error code %d: error confirmando transferencia", ErrTransferConfirm)
    }

    return nil
}

func PutFTPText(textData, ftpUrl string) error {
    if textData == "" {
        return fmt.Errorf("error code %d: texto vacío", ErrEmptyData)
    }
    if ftpUrl == "" {
        return fmt.Errorf("error code %d: URL vacía", ErrEmptyURL)
    }

    if isSFTP(ftpUrl) {
        client, err := createSFTPClient(ftpUrl)
        if err != nil {
            return err
        }
        defer client.Close()

        u, _ := url.Parse(ftpUrl)
        path := u.Path

        // Create parent directories if they don't exist
        dir := filepath.Dir(path)
        if dir != "." {
            if err := client.MkdirAll(dir); err != nil {
                return fmt.Errorf("error code %d: failed to create directories: %v", ErrSftpOperation, err)
            }
        }

        file, err := client.Create(path)
        if err != nil {
            return fmt.Errorf("error code %d: failed to create file: %v", ErrSftpOperation, err)
        }
        defer file.Close()

        normalizedText := strings.ReplaceAll(textData, "\n", "\r\n")
        if _, err := fmt.Fprintf(file, normalizedText); err != nil {
            return fmt.Errorf("error code %d: failed to write file: %v", ErrSftpOperation, err)
        }

        return nil
    }

    // Original FTP implementation
    u, err := url.Parse(ftpUrl)
    if err != nil {
        return fmt.Errorf("error analizando URL: %v", err)
    }
    if u.Scheme != "ftp" {
        return fmt.Errorf("error code %d: URL no es FTP", ErrInvalidScheme)
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := u.Path

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return fmt.Errorf("error code %d: falta host o usuario", ErrMissingHostUser)
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return fmt.Errorf("error code %d: conexión fallida: %v", ErrConnectionFailed, err)
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: lectura inicial fallida: %v", ErrInitialRead, err)
    }

    if _, err = fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return fmt.Errorf("error code %d: error enviando usuario: %v", ErrUserSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return fmt.Errorf("error code %d: autenticación de usuario fallida", ErrUserAuth)
    }

    if _, err = fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return fmt.Errorf("error code %d: error enviando contraseña: %v", ErrPassSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return fmt.Errorf("error code %d: autenticación de contraseña fallida", ErrPassAuth)
    }

    if _, err = fmt.Fprintf(conn, "TYPE A\r\n"); err != nil {
        return fmt.Errorf("error code %d: error configurando modo ASCII", ErrAsciiMode)
    }
    conn.Read(buf[:])

    if _, err = fmt.Fprintf(conn, "PASV\r\n"); err != nil {
        return fmt.Errorf("error code %d: error entrando en modo pasivo", ErrPasvMode)
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: error leyendo respuesta PASV", ErrPasvMode)
    }

    pasvResp := string(buf[:n])
    dataAddr, err := parsePASV(pasvResp)
    if err != nil {
        return fmt.Errorf("error code %d: error analizando modo pasivo: %v", ErrPasvMode, err)
    }

    dataConn, err := net.DialTimeout("tcp", dataAddr, timeout)
    if err != nil {
        return fmt.Errorf("error code %d: error conexión de datos: %v", ErrConnectionFailed, err)
    }
    defer dataConn.Close()
    dataConn.SetDeadline(time.Now().Add(timeout))

    if _, err = fmt.Fprintf(conn, "STOR %s\r\n", path); err != nil {
        return fmt.Errorf("error code %d: error iniciando transferencia", ErrStorCommand)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "150") {
        return fmt.Errorf("error code %d: error preparando servidor", ErrDataTransfer)
    }

    normalizedText := strings.ReplaceAll(textData, "\n", "\r\n")
    _, err = fmt.Fprintf(dataConn, normalizedText)
    if err != nil {
        return fmt.Errorf("error code %d: error enviando datos: %v", ErrDataTransfer, err)
    }
    dataConn.Close()

    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "226") {
        return fmt.Errorf("error code %d: error confirmando transferencia", ErrTransferConfirm)
    }

    return nil
}

func CreateFTPDir(ftpUrl string) error {
    if ftpUrl == "" {
        return fmt.Errorf("error code %d: URL vacía", ErrEmptyURL)
    }

    if isSFTP(ftpUrl) {
        client, err := createSFTPClient(ftpUrl)
        if err != nil {
            return err
        }
        defer client.Close()

        u, _ := url.Parse(ftpUrl)
        path := strings.TrimPrefix(u.Path, "/")

        if path == "" {
            return fmt.Errorf("error code %d: falta path del directorio", ErrMissingPath)
        }

        // Check if path exists as a file
        if stat, err := client.Stat(path); err == nil {
            if !stat.IsDir() {
                return fmt.Errorf("error code %d: ya existe como archivo (conflicto)", ErrFileConflict)
            }
            return nil // Already exists as directory
        }

        // Create the directory
        if err := client.MkdirAll(path); err != nil {
            return fmt.Errorf("error code %d: error creando directorio: %v", ErrMkdirFailed, err)
        }

        return nil
    }

    // Original FTP implementation
    u, err := url.Parse(ftpUrl)
    if err != nil {
        return fmt.Errorf("error analizando URL: %v", err)
    }
    if u.Scheme != "ftp" {
        return fmt.Errorf("error code %d: URL no es FTP", ErrInvalidScheme)
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := strings.TrimPrefix(u.Path, "/")

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return fmt.Errorf("error code %d: falta host o usuario", ErrMissingHostUser)
    }

    if path == "" {
        return fmt.Errorf("error code %d: falta path del directorio", ErrMissingPath)
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return fmt.Errorf("error code %d: conexión fallida: %v", ErrConnectionFailed, err)
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: lectura inicial fallida: %v", ErrInitialRead, err)
    }

    if _, err = fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return fmt.Errorf("error code %d: error enviando usuario: %v", ErrUserSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return fmt.Errorf("error code %d: autenticación de usuario fallida", ErrUserAuth)
    }

    if _, err = fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return fmt.Errorf("error code %d: error enviando contraseña: %v", ErrPassSend, err)
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return fmt.Errorf("error code %d: autenticación de contraseña fallida", ErrPassAuth)
    }

    // Verificar si ya existe como archivo
    if _, err = fmt.Fprintf(conn, "SIZE %s\r\n", path); err != nil {
        return fmt.Errorf("error code %d: error enviando comando SIZE", ErrSizeCommand)
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: error leyendo respuesta SIZE", ErrSizeResponse)
    }
    sizeResp := string(buf[:n])
    if strings.HasPrefix(sizeResp, "213") {
        return fmt.Errorf("error code %d: ya existe como archivo (conflicto)", ErrFileConflict)
    }

    // Verificar si ya existe como directorio
    if _, err = fmt.Fprintf(conn, "CWD %s\r\n", path); err != nil {
        return fmt.Errorf("error code %d: error enviando comando CWD", ErrCwdCommand)
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: error leyendo respuesta CWD", ErrCwdResponse)
    }
    cwdResp := string(buf[:n])
    if strings.HasPrefix(cwdResp, "250") {
        // Volver al directorio anterior
        _, _ = fmt.Fprintf(conn, "CDUP\r\n")
        return nil // Ya existe como directorio
    }

    // Crear el directorio
    if _, err = fmt.Fprintf(conn, "MKD %s\r\n", path); err != nil {
        return fmt.Errorf("error code %d: error enviando comando MKD", ErrMkdirFailed)
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return fmt.Errorf("error code %d: error leyendo respuesta MKD", ErrMkdirResponse)
    }
    resp := string(buf[:n])
    if !strings.HasPrefix(resp, "257") {
        return fmt.Errorf("error code %d: error creando directorio", ErrMkdirFailed)
    }

    return nil
}

func ListFTPFiles(dirPath string) []string {
    if dirPath == "" {
        return nil
    }

    if isSFTP(dirPath) {
        client, err := createSFTPClient(dirPath)
        if err != nil {
            return nil
        }
        defer client.Close()

        u, _ := url.Parse(dirPath)
        path := u.Path

        files, err := client.ReadDir(path)
        if err != nil {
            return nil
        }

        var fileNames []string
        for _, file := range files {
            fileNames = append(fileNames, file.Name())
        }

        return fileNames
    }

    // Original FTP implementation
    u, err := url.Parse(dirPath)
    if err != nil || u.Scheme != "ftp" {
        return nil
    }

    user := u.User.Username()
    pass, _ := u.User.Password()
    host := u.Host
    path := u.Path

    if !strings.Contains(host, ":") {
        host += ":21"
    }

    if host == "" || user == "" {
        return nil
    }

    conn, err := net.DialTimeout("tcp", host, timeout)
    if err != nil {
        return nil
    }
    defer conn.Close()
    conn.SetDeadline(time.Now().Add(timeout))

    var buf [1024]byte
    n, err := conn.Read(buf[:])
    if err != nil {
        return nil
    }

    if _, err := fmt.Fprintf(conn, "USER %s\r\n", user); err != nil {
        return nil
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "331") {
        return nil
    }

    if _, err := fmt.Fprintf(conn, "PASS %s\r\n", pass); err != nil {
        return nil
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "230") {
        return nil
    }

    if _, err := fmt.Fprintf(conn, "TYPE A\r\n"); err != nil {
        return nil
    }
    conn.Read(buf[:])

    if _, err := fmt.Fprintf(conn, "PASV\r\n"); err != nil {
        return nil
    }
    n, err = conn.Read(buf[:])
    if err != nil {
        return nil
    }

    pasvResp := string(buf[:n])
    dataAddr, err := parsePASV(pasvResp)
    if err != nil {
        return nil
    }

    dataConn, err := net.DialTimeout("tcp", dataAddr, timeout)
    if err != nil {
        return nil
    }
    defer dataConn.Close()
    dataConn.SetDeadline(time.Now().Add(timeout))

    if _, err := fmt.Fprintf(conn, "LIST %s\r\n", path); err != nil {
        return nil
    }
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "150") {
        return nil
    }

    var buffer bytes.Buffer
    limitedReader := &io.LimitedReader{R: dataConn, N: maxFileSize}
    if _, err := io.Copy(&buffer, limitedReader); err != nil {
        return nil
    }

    dataConn.Close()
    n, err = conn.Read(buf[:])
    if err != nil || !strings.HasPrefix(string(buf[:n]), "226") {
        return nil
    }

    lines := strings.Split(buffer.String(), "\n")
    var files []string
    for _, line := range lines {
        line = strings.TrimSpace(line)
        if line == "" {
            continue
        }
        parts := strings.Fields(line)
        if len(parts) > 0 {
            files = append(files, parts[len(parts)-1])
        }
    }

    return files
}