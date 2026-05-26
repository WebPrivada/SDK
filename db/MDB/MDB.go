package MDB

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	STRC "github.com/WebPrivada/SDK/db/STRUCTURES"
)

var (
	jsonRegex   = regexp.MustCompile(`(?i)\(JSON\[([a-z0-9_,BLOB()\s]+)\]`)
	blobPattern = regexp.MustCompile(`(?i)BLOB\(([a-z0-9_]+)\)`)

	callPattern    = regexp.MustCompile(`(?i)^call\s+([a-z0-9_]+(?:\.[a-z0-9_]+)?)\s*\(JSON\[([a-z0-9_,BLOB()\s]+)\]\)$`)
	insertcPattern = regexp.MustCompile(`(?i)^insert\s+into\s+([a-z0-9_.]+(?:\.[a-z0-9_]+)?)\s*\(([a-z0-9_,\sBLOB()]+)\)\s*values\s*\(JSON\[([a-z0-9_,BLOB()\s]+)\]\)$`)
	insertPattern  = regexp.MustCompile(`(?i)^insert\s+into\s+([a-z0-9_.]+(?:\.[a-z0-9_]+)?)\s*values\s*\(JSON\[([a-z0-9_,BLOB()\s]+)\]\)$`)
	selectPattern  = regexp.MustCompile(`(?i)^select\s+([a-z0-9_]+(?:\.[a-z0-9_]+)?)\s*\(JSON\[([a-z0-9_,BLOB()\s]+)\]\)$`)
)

// OpenConnection opens a new database connection
func OpenConnection(driver, conexion string) (*sql.DB, error) {
	db, err := sql.Open(driver, conexion)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// SqlRunOnConn executes a query on an existing connection
func SqlRunOnConn(db *sql.DB, query string, args ...any) STRC.InternalResult {
	rows, err := db.Query(query, args...)
	if err != nil {
		return STRC.InternalResult{
			Json:     createErrorJSON(fmt.Sprintf("Error en la consulta SQL: %v", err)),
			Is_error: 1,
			Is_empty: 0,
		}
	}
	defer rows.Close()

	return buildResultFromRows(rows, query)
}

// buildResultFromRows centraliza la construcción del JSON a partir de un *sql.Rows.
// Se usa tanto desde SqlRunOnConn como desde SqlRunInternal para evitar duplicación.
func buildResultFromRows(rows *sql.Rows, query string) STRC.InternalResult {
	var resultsets []string
	resultSetCount := 0

	for {
		columns, err := rows.Columns()
		if err != nil {
			return STRC.InternalResult{
				Json:     createErrorJSON(fmt.Sprintf("Error al obtener columnas: %v", err)),
				Is_error: 1,
				Is_empty: 0,
			}
		}

		// Verificar si hay un campo llamado "JSON" (case insensitive)
		hasJSONField := false
		jsonFieldIndex := -1
		for i, col := range columns {
			if strings.ToUpper(col) == "JSON" {
				hasJSONField = true
				jsonFieldIndex = i
				break
			}
		}

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return STRC.InternalResult{
				Json:     createErrorJSON(fmt.Sprintf("Error al obtener tipos de columna: %v", err)),
				Is_error: 1,
				Is_empty: 0,
			}
		}

		// Pre-detectamos qué columnas son BLOB para no hacer strings.Contains por fila
		isBlob := make([]bool, len(columns))
		for i, ct := range colTypes {
			if strings.Contains(ct.DatabaseTypeName(), "BLOB") {
				isBlob[i] = true
			}
		}

		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(sql.RawBytes)
		}

		var buf bytes.Buffer
		rowCount := 0
		buf.WriteByte('[')

		for rows.Next() {
			if rowCount > 0 {
				buf.WriteByte(',')
			}

			if err := rows.Scan(values...); err != nil {
				return STRC.InternalResult{
					Json:     createErrorJSON(fmt.Sprintf("Error al escanear fila: %v", err)),
					Is_error: 1,
					Is_empty: 0,
				}
			}

			if hasJSONField {
				// Si hay un campo JSON, usamos solo ese campo (tal cual viene de la BD).
				// buf.Write copia los bytes, así que es seguro respecto al reuso de RawBytes.
				rb := *(values[jsonFieldIndex].(*sql.RawBytes))
				if rb == nil {
					buf.WriteString("null")
				} else {
					if !json.Valid(rb) {
						return STRC.InternalResult{
							Json:     createErrorJSON("El campo JSON no contiene un JSON válido"),
							Is_error: 1,
							Is_empty: 0,
						}
					}
					buf.Write(rb)
				}
			} else {
				// Construimos un map[string]interface{} y dejamos que json.Marshal
				// se encargue de escapar correctamente todos los caracteres
				// (comillas dobles, backslash, \n, \r, \t, controles, UTF-8, etc.).
				// string(rb) copia los bytes y base64.EncodeToString también, por lo
				// que no hay aliasing con la memoria que reutiliza el driver.
				row := make(map[string]interface{}, len(columns))
				for i, col := range columns {
					rb := *(values[i].(*sql.RawBytes))
					if rb == nil {
						row[col] = nil
						continue
					}
					if isBlob[i] {
						row[col] = base64.StdEncoding.EncodeToString(rb)
					} else {
						row[col] = string(rb)
					}
				}

				encoded, err := json.Marshal(row)
				if err != nil {
					return STRC.InternalResult{
						Json:     createErrorJSON(fmt.Sprintf("Error al serializar fila a JSON: %v", err)),
						Is_error: 1,
						Is_empty: 0,
					}
				}
				buf.Write(encoded)
			}
			rowCount++
		}

		buf.WriteByte(']')

		if err = rows.Err(); err != nil {
			return STRC.InternalResult{
				Json:     createErrorJSON(fmt.Sprintf("Error después de iterar filas: %v", err)),
				Is_error: 1,
				Is_empty: 0,
			}
		}

		// Solo agregamos el resultset si tiene filas o es el primer resultset
		if rowCount > 0 || resultSetCount == 0 {
			resultsets = append(resultsets, buf.String())
			resultSetCount++
		}

		// Pasamos al siguiente resultset si existe
		if !rows.NextResultSet() {
			break
		}
	}

	// Construimos la respuesta final
	if len(resultsets) > 1 {
		combined := "[" + strings.Join(resultsets, ",") + "]"
		return STRC.InternalResult{
			Json:     combined,
			Is_error: 0,
			Is_empty: 0,
		}
	} else if len(resultsets) == 1 && strings.Contains(resultsets[0], ":") {
		return STRC.InternalResult{
			Json:     resultsets[0],
			Is_error: 0,
			Is_empty: 0,
		}
	} else if isNonReturningQuery(query) {
		return STRC.InternalResult{
			Json:     createSuccessJSON(),
			Is_error: 0,
			Is_empty: 1,
		}
	}
	return STRC.InternalResult{
		Json:     "[]",
		Is_error: 0,
		Is_empty: 1,
	}
}

// SqlRunInternal abre una conexión, ejecuta y cierra. Mantiene la firma original.
func SqlRunInternal(driver, conexion, query string, args ...any) STRC.InternalResult {

	if len(args) == 1 { // solo un argumento
		if sjson, ok := args[0].(string); ok { // ese argumento debe ser string
			// Validamos la query del tipo: ...(JSON[col1,col2,BLOB(col3),...])
			if jsonRegex.MatchString(query) {
				if isJSON(sjson) {
					return sqlruninternalwithJSON(driver, conexion, query, sjson)
				}
				return STRC.InternalResult{
					Json:     createErrorJSON("El query esperaba un JSON valido"),
					Is_error: 1,
					Is_empty: 0,
				}
			}
		}
	}

	db, err := sql.Open(driver, conexion)
	if err != nil {
		return STRC.InternalResult{
			Json:     createErrorJSON(fmt.Sprintf("Error al abrir conexión: %v", err)),
			Is_error: 1,
			Is_empty: 0,
		}
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return STRC.InternalResult{
			Json:     createErrorJSON(fmt.Sprintf("Error al conectar a la base de datos: %v", err)),
			Is_error: 1,
			Is_empty: 0,
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return STRC.InternalResult{
			Json:     createErrorJSON(fmt.Sprintf("Error en la consulta SQL: %v", err)),
			Is_error: 1,
			Is_empty: 0,
		}
	}
	defer rows.Close()

	return buildResultFromRows(rows, query)
}

func isNonReturningQuery(query string) bool {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(queryUpper, "INSERT ") ||
		strings.HasPrefix(queryUpper, "UPDATE ") ||
		strings.HasPrefix(queryUpper, "DELETE ") ||
		strings.HasPrefix(queryUpper, "REPLACE ") ||
		strings.HasPrefix(queryUpper, "DROP ") ||
		strings.HasPrefix(queryUpper, "CREATE ") ||
		strings.HasPrefix(queryUpper, "ALTER ") ||
		strings.HasPrefix(queryUpper, "TRUNCATE ") ||
		strings.HasPrefix(queryUpper, "CALL ")
}

func createErrorJSON(message string) string {
	errResp := STRC.ErrorResponse{Error: message}
	jsonData, _ := json.Marshal(errResp)
	return string(jsonData)
}

func createSuccessJSON() string {
	successResp := STRC.SuccessResponse{Status: "OK"}
	jsonData, _ := json.Marshal(successResp)
	return string(jsonData)
}

func sqlruninternalwithJSON(driver, conexion, query, jsonStr string) STRC.InternalResult {
	result, err := runSQLInternal(driver, conexion, query, jsonStr)
	if err != nil {
		errorJson, _ := json.Marshal(STRC.ErrorResponse{Error: err.Error()})
		return STRC.InternalResult{
			Json:     string(errorJson),
			Is_error: 1,
			Is_empty: 0,
		}
	}

	if len(result) == 0 {
		return STRC.InternalResult{
			Json:     `{"message":"no data found"}`,
			Is_error: 0,
			Is_empty: 1,
		}
	}

	firstItem := result[0]
	firstItemJson, err := json.Marshal(firstItem)
	if err != nil {
		errorJson, _ := json.Marshal(STRC.ErrorResponse{Error: err.Error()})
		return STRC.InternalResult{
			Json:     string(errorJson),
			Is_error: 1,
			Is_empty: 0,
		}
	}

	return STRC.InternalResult{
		Json:     string(firstItemJson),
		Is_error: 0,
		Is_empty: 0,
	}
}

// Función interna que mantiene la lógica original
func runSQLInternal(driver string, connection string, query string, jsonStr string) ([]map[string]interface{}, error) {
	normalizedQuery := strings.TrimSpace(strings.TrimSuffix(query, ";"))

	queryType, params, blobParams, err := parseQuery(normalizedQuery)
	if err != nil {
		return nil, err
	}

	var jsonArray []map[string]interface{}
	var jsonObject map[string]interface{}

	if strings.TrimSpace(jsonStr) != "" && strings.TrimSpace(jsonStr)[0] == '[' {
		if err := json.Unmarshal([]byte(jsonStr), &jsonArray); err != nil {
			return nil, fmt.Errorf("error al parsear JSON array: %v", err)
		}
		if len(jsonArray) == 0 {
			return nil, errors.New("el array JSON está vacío")
		}
		if err := validateParams(params, blobParams, jsonArray[0]); err != nil {
			return nil, err
		}
	} else if strings.TrimSpace(jsonStr) != "" {
		if err := json.Unmarshal([]byte(jsonStr), &jsonObject); err != nil {
			return nil, fmt.Errorf("error al parsear JSON: %v", err)
		}
		if err := validateParams(params, blobParams, jsonObject); err != nil {
			return nil, err
		}
		jsonArray = []map[string]interface{}{jsonObject}
	} else {
		jsonArray = []map[string]interface{}{make(map[string]interface{})}
	}

	db, err := sql.Open(driver, connection)
	if err != nil {
		return nil, fmt.Errorf("error al conectar a la base de datos: %v", err)
	}
	defer db.Close()

	baseQuery, _ := buildQuery(queryType, params, blobParams, jsonArray[0])

	return executeBatchInsert(db, baseQuery, params, blobParams, jsonArray)
}

// parseQuery identifica el tipo de consulta y extrae parámetros normales y BLOB
func parseQuery(query string) (string, []string, []string, error) {
	normalizedQuery := strings.TrimSpace(strings.TrimSuffix(query, ";"))

	patterns := []struct {
		regex     *regexp.Regexp
		queryType string
	}{
		{callPattern, "call"},
		{insertcPattern, "insert_with_columns"},
		{insertPattern, "insert_without_columns"},
		{selectPattern, "select_function"},
	}

	for _, pattern := range patterns {
		matches := pattern.regex.FindStringSubmatch(normalizedQuery)
		if len(matches) > 0 {
			paramStr := matches[len(matches)-1]

			// Extraer parámetros BLOB primero
			blobMatches := blobPattern.FindAllStringSubmatch(paramStr, -1)
			blobParams := make([]string, 0)
			for _, m := range blobMatches {
				blobParams = append(blobParams, m[1])
				paramStr = strings.Replace(paramStr, m[0], m[1], 1)
			}

			params := strings.Split(paramStr, ",")
			for i := range params {
				params[i] = strings.TrimSpace(params[i])
			}

			switch pattern.queryType {
			case "insert_with_columns":
				columns := strings.Split(matches[2], ",")
				for i := range columns {
					columns[i] = strings.TrimSpace(columns[i])
				}
				return fmt.Sprintf("%s:%s:%s", pattern.queryType, matches[1], strings.Join(columns, ",")), params, blobParams, nil

			case "select_function_alias":
				return fmt.Sprintf("%s:%s:%s", pattern.queryType, matches[1], matches[3]), params, blobParams, nil

			default:
				return fmt.Sprintf("%s:%s", pattern.queryType, matches[1]), params, blobParams, nil
			}
		}
	}

	return "", nil, nil, errors.New("formato de consulta no soportado")
}

// buildQuery construye la consulta SQL con placeholders
func buildQuery(queryType string, params []string, blobParams []string, jsonData map[string]interface{}) (string, []interface{}) {
	parts := strings.Split(queryType, ":")
	qType := parts[0]

	args := make([]interface{}, len(params))
	for i, param := range params {
		if isBlobParam(param, blobParams) {
			if str, ok := jsonData[param].(string); ok {
				decoded, err := base64.StdEncoding.DecodeString(str)
				if err != nil {
					decoded = []byte(str)
				}
				args[i] = decoded
			} else {
				args[i] = []byte{}
			}
		} else {
			args[i] = jsonData[param]
		}
	}

	if len(params) == 0 {
		return "", nil
	}
	placeholders := strings.Repeat("?,", len(params)-1) + "?"

	switch qType {
	case "call":
		return fmt.Sprintf("CALL %s(%s)", parts[1], placeholders), args
	case "insert_with_columns":
		return fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", parts[1], parts[2], placeholders), args
	case "insert_without_columns":
		return fmt.Sprintf("INSERT INTO %s VALUES(%s)", parts[1], placeholders), args
	case "select_function":
		return fmt.Sprintf("SELECT %s(%s)", parts[1], placeholders), args
	case "select_function_alias":
		return fmt.Sprintf("SELECT %s(%s) AS %s", parts[1], placeholders, parts[2]), args
	default:
		return "", nil
	}
}

// validateParams valida que los parámetros existan en el JSON
func validateParams(params []string, blobParams []string, jsonData map[string]interface{}) error {
	for _, param := range params {
		if _, exists := jsonData[param]; !exists {
			return fmt.Errorf("parámetro faltante en JSON: '%s'", param)
		}
	}
	return nil
}

func executeBatchInsert(db *sql.DB, baseQuery string, params []string, blobParams []string, jsonArray []map[string]interface{}) ([]map[string]interface{}, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("error al iniciar transacción: %v", err)
	}

	stmt, err := tx.Prepare(baseQuery)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error al preparar consulta: %v", err)
	}
	defer stmt.Close()

	var totalRows int64
	var lastInsertId int64

	for i, item := range jsonArray {
		args := make([]interface{}, len(params))
		for j, param := range params {
			if isBlobParam(param, blobParams) {
				val, exists := item[param]
				if !exists || val == nil {
					args[j] = nil
					continue
				}

				strVal, ok := val.(string)
				if !ok {
					tx.Rollback()
					return nil, fmt.Errorf("el valor para BLOB %s debe ser string (base64) o null", param)
				}

				decoded, err := base64.StdEncoding.DecodeString(strVal)
				if err != nil {
					tx.Rollback()
					return nil, fmt.Errorf("error decodificando base64 para %s: %v", param, err)
				}
				args[j] = decoded
			} else {
				args[j] = item[param]
			}
		}

		res, err := stmt.Exec(args...)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("error al insertar registro %d: %v", i+1, err)
		}

		if i == 0 {
			lastInsertId, _ = res.LastInsertId()
		}
		rowsAffected, _ := res.RowsAffected()
		totalRows += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error al confirmar transacción: %v", err)
	}

	return []map[string]interface{}{
		{
			"last_insert_id":   lastInsertId,
			"rows_affected":    totalRows,
			"records_inserted": len(jsonArray),
		},
	}, nil
}

// Función auxiliar para verificar si un parámetro es BLOB
func isBlobParam(param string, blobParams []string) bool {
	for _, p := range blobParams {
		if p == param {
			return true
		}
	}
	return false
}

func isJSON(jsonStr string) bool {
	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()
	var dummy interface{}
	return decoder.Decode(&dummy) == nil
}
