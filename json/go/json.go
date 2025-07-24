package jsonlib

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"reflect"
)

type JsonResult struct {
	Value   string
	Is_Valid bool
	Error   error
}

type JsonArrayResult struct {
	Items   []string
	Is_Valid bool
	Error   error
}

func ParseJSON(jsonStr string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func GetJSONValue(jsonStr, key string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	value, exists := data[key]
	if !exists {
		result.Is_Valid = false
		result.Error = fmt.Errorf("clave '%s' no encontrada", key)
		return result
	}

	switch v := value.(type) {
	case string:
		result.Is_Valid = true
		result.Value = v
		return result
	case json.Number:
		result.Is_Valid = true
		result.Value = v.String()
		return result
	case bool:
		result.Is_Valid = true
		result.Value = strconv.FormatBool(v)
		return result
	default:
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al codificar valor: %w", err)
			return result
		}
		result.Is_Valid = true
		result.Value = strings.TrimSpace(buf.String())
		return result
	}
}

func GetArrayLength(jsonStr string) JsonResult {
	var result JsonResult

	if len(jsonStr) == 0 || jsonStr[0] != '[' {
		result.Is_Valid = false
		result.Error = errors.New("no es un arreglo JSON válido")
		return result
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		result.Is_Valid = false
		result.Error = errors.New("arreglo JSON inválido")
		return result
	}

	count := 0
	for decoder.More() {
		var dummy interface{}
		if err := decoder.Decode(&dummy); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al contar elementos: %w", err)
			return result
		}
		count++
	}

	result.Is_Valid = true
	result.Value = strconv.Itoa(count)
	return result
}

func GetArrayItem(jsonStr string, index int) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		result.Is_Valid = false
		result.Error = errors.New("arreglo JSON inválido")
		return result
	}

	currentIndex := 0
	for decoder.More() {
		if currentIndex == index {
			var item interface{}
			if err := decoder.Decode(&item); err != nil {
				result.Is_Valid = false
				result.Error = fmt.Errorf("error al obtener elemento: %w", err)
				return result
			}

			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(item); err != nil {
				result.Is_Valid = false
				result.Error = fmt.Errorf("error al codificar elemento: %w", err)
				return result
			}

			result.Is_Valid = true
			result.Value = strings.TrimSpace(buf.String())
			return result
		}

		// Saltar este elemento
		var dummy interface{}
		if err := decoder.Decode(&dummy); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al saltar elemento: %w", err)
			return result
		}
		currentIndex++
	}

	result.Is_Valid = false
	result.Error = errors.New("índice fuera de rango")
	return result
}

func GetJSONKeys(jsonStr string) JsonArrayResult {
	var result JsonArrayResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}

	result.Is_Valid = true
	result.Items = keys
	return result
}

func GetJSONValueByPath(jsonStr, path string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	current := data
	pathParts := strings.Split(path, ".")
	for _, part := range pathParts {
		if part == "" {
			continue
		}

		switch v := current.(type) {
		case map[string]interface{}:
			val, exists := v[part]
			if !exists {
				result.Is_Valid = false
				result.Error = fmt.Errorf("ruta '%s' no encontrada", part)
				return result
			}
			current = val
		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(v) {
				result.Is_Valid = false
				result.Error = fmt.Errorf("índice de arreglo inválido '%s'", part)
				return result
			}
			current = v[index]
		default:
			result.Is_Valid = false
			result.Error = fmt.Errorf("no se puede navegar por la ruta '%s'", part)
			return result
		}
	}

	switch v := current.(type) {
	case string:
		result.Is_Valid = true
		result.Value = v
		return result
	case json.Number:
		result.Is_Valid = true
		result.Value = v.String()
		return result
	case bool:
		result.Is_Valid = true
		result.Value = strconv.FormatBool(v)
		return result
	case nil:
		result.Is_Valid = true
		result.Value = "null"
		return result
	default:
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(current); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al codificar valor: %w", err)
			return result
		}
		result.Is_Valid = true
		result.Value = strings.TrimSpace(buf.String())
		return result
	}
}

func GetArrayItems(jsonStr string) JsonArrayResult {
	var result JsonArrayResult

	if len(jsonStr) == 0 || jsonStr[0] != '[' {
		result.Is_Valid = false
		result.Error = errors.New("no es un arreglo JSON válido")
		return result
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		result.Is_Valid = false
		result.Error = errors.New("arreglo JSON inválido")
		return result
	}

	var items []string
	for decoder.More() {
		var item interface{}
		if err := decoder.Decode(&item); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al obtener elementos: %w", err)
			return result
		}

		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(item); err != nil {
			result.Is_Valid = false
			result.Error = fmt.Errorf("error al codificar elemento: %w", err)
			return result
		}

		items = append(items, strings.TrimSpace(buf.String()))
	}

	result.Is_Valid = true
	result.Items = items
	return result
}

func CreateEmptyJSON() JsonResult {
	return JsonResult{
		Is_Valid: true,
		Value:   "{}",
	}
}

func CreateEmptyArray() JsonResult {
	return JsonResult{
		Is_Valid: true,
		Value:   "[]",
	}
}

func AddStringToJSON(jsonStr, key, value string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	data[key] = value

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func AddNumberToJSON(jsonStr, key string, value float64) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	data[key] = value

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func AddBooleanToJSON(jsonStr, key string, value bool) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	data[key] = value

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func AddJSONToJSON(parentJson, key, childJson string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(parentJson)))
	decoder.UseNumber()

	var parentData map[string]interface{}
	if err := decoder.Decode(&parentData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON padre: %w", err)
		return result
	}

	childDecoder := json.NewDecoder(bytes.NewReader([]byte(childJson)))
	childDecoder.UseNumber()

	var childData interface{}
	if err := childDecoder.Decode(&childData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON hijo: %w", err)
		return result
	}

	parentData[key] = childData

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(parentData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON combinado: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func AddItemToArray(jsonArray, item string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonArray)))
	decoder.UseNumber()

	var arrayData []interface{}
	if err := decoder.Decode(&arrayData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar arreglo JSON: %w", err)
		return result
	}

	itemDecoder := json.NewDecoder(bytes.NewReader([]byte(item)))
	itemDecoder.UseNumber()

	var itemData interface{}
	if err := itemDecoder.Decode(&itemData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar elemento: %w", err)
		return result
	}

	arrayData = append(arrayData, itemData)

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(arrayData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar arreglo actualizado: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func RemoveKeyFromJSON(jsonStr, key string) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar JSON: %w", err)
		return result
	}

	if _, exists := data[key]; !exists {
		result.Is_Valid = false
		result.Error = fmt.Errorf("clave '%s' no encontrada", key)
		return result
	}

	delete(data, key)

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON actualizado: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func RemoveItemFromArray(jsonArray string, index int) JsonResult {
	var result JsonResult

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonArray)))
	decoder.UseNumber()

	var arrayData []interface{}
	if err := decoder.Decode(&arrayData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar arreglo JSON: %w", err)
		return result
	}

	if index < 0 || index >= len(arrayData) {
		result.Is_Valid = false
		result.Error = errors.New("índice fuera de rango")
		return result
	}

	arrayData = append(arrayData[:index], arrayData[index+1:]...)

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(arrayData); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar arreglo actualizado: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func MergeJSON(json1, json2 string) JsonResult {
	var result JsonResult

	decoder1 := json.NewDecoder(bytes.NewReader([]byte(json1)))
	decoder1.UseNumber()

	var data1 map[string]interface{}
	if err := decoder1.Decode(&data1); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar primer JSON: %w", err)
		return result
	}

	decoder2 := json.NewDecoder(bytes.NewReader([]byte(json2)))
	decoder2.UseNumber()

	var data2 map[string]interface{}
	if err := decoder2.Decode(&data2); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al analizar segundo JSON: %w", err)
		return result
	}

	for key, value := range data2 {
		data1[key] = value
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data1); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al codificar JSON combinado: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = strings.TrimSpace(buf.String())
	return result
}

func IsValidJSON(jsonStr string) bool {
	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()

	var dummy interface{}
	return decoder.Decode(&dummy) == nil
}


func ValidateJSON(jsonStr, schemaStr string) JsonResult {
	var result JsonResult

	// Primero validamos que ambos strings sean JSON válidos
	if !IsValidJSON(jsonStr) {
		result.Is_Valid = false
		result.Error = errors.New("JSON de entrada no es válido")
		return result
	}

	if !IsValidJSON(schemaStr) {
		result.Is_Valid = false
		result.Error = errors.New("JSON de esquema no es válido")
		return result
	}

	// Parseamos ambos JSON
	var data, schema interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al parsear JSON de entrada: %w", err)
		return result
	}

	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("error al parsear JSON de esquema: %w", err)
		return result
	}

	// Validamos la estructura
	if err := validateStructure(data, schema); err != nil {
		result.Is_Valid = false
		result.Error = fmt.Errorf("validación fallida: %w", err)
		return result
	}

	result.Is_Valid = true
	result.Value = "JSON válido según el esquema"
	return result
}

// validateStructure compara recursivamente los datos con el esquema
func validateStructure(data, schema interface{}) error {
	switch schemaTyped := schema.(type) {
	case map[string]interface{}:
		// El esquema es un objeto, los datos también deben serlo
		dataTyped, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("se esperaba un objeto")
		}

		// Verificar que todas las claves del esquema existan en los datos
		for key := range schemaTyped {
			if _, exists := dataTyped[key]; !exists {
				return fmt.Errorf("falta la clave '%s'", key)
			}
		}

		// Verificar que no haya claves adicionales en los datos
		for key := range dataTyped {
			if _, exists := schemaTyped[key]; !exists {
				return fmt.Errorf("clave adicional '%s' no permitida", key)
			}
		}

		// Validar recursivamente cada valor
		for key, schemaValue := range schemaTyped {
			if err := validateStructure(dataTyped[key], schemaValue); err != nil {
				return fmt.Errorf("en clave '%s': %w", key, err)
			}
		}

	case []interface{}:
		// El esquema es un array, los datos también deben serlo
		dataTyped, ok := data.([]interface{})
		if !ok {
			return fmt.Errorf("se esperaba un array")
		}

		// Si el esquema del array está vacío, no validamos los tipos de los elementos
		if len(schemaTyped) == 0 {
			return nil
		}

		// Validar que todos los elementos del array coincidan con el primer elemento del esquema
		// (asumimos que el esquema define el tipo esperado para todos los elementos)
		for i, item := range dataTyped {
			if err := validateStructure(item, schemaTyped[0]); err != nil {
				return fmt.Errorf("en índice %d del array: %w", i, err)
			}
		}

	default:
		// Para valores primitivos, solo verificamos que los tipos coincidan
		dataType := reflect.TypeOf(data)
		schemaType := reflect.TypeOf(schema)
		if dataType != schemaType {
			return fmt.Errorf("tipo incorrecto, se esperaba %s pero se obtuvo %s", schemaType, dataType)
		}
	}

	return nil
}
