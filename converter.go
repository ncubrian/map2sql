package map2sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"reflect"
	"regexp"

	"github.com/arstd/log"
)

type Converter interface {
	Insert(tblname string, doc map[string]interface{}) (string, error)
	Update(tblname, queryKey string, doc map[string]interface{}) (string, error)
	Delete(tblname, queryKey string, doc map[string]interface{}) (string, error)
	appendDate(buf *bytes.Buffer, date string)
}

func (c *BaseConverter) appendDate(buf *bytes.Buffer, date string) {
	if c.child != nil {
		c.child.appendDate(buf, date)
	}
}

type MysqlConverter struct {
	Converter
}

func (c *MysqlConverter) appendDate(buf *bytes.Buffer, date string) {
	buf.WriteString("'")
	buf.WriteString(strings.Replace(date, "'", "''", -1))
	buf.WriteString("'")
}

type OracleConverter struct {
	Converter
}

func (c *OracleConverter) appendDate(buf *bytes.Buffer, date string) {
	buf.WriteString("to_date('")
	buf.WriteString(strings.Replace(date, "'", "''", -1))
	buf.WriteString("','yyyy-MM-dd hh24:mi:ss')")
}

type BaseConverter struct {
	confModelMap map[string]interface{}
	child Converter
}

type mode func(Converter) Converter

func ModeMysql() mode {
	return func(c Converter) Converter {
		mysqlConverter := new(MysqlConverter)
		mysqlConverter.Converter = c
		return mysqlConverter
    }
}

func ModeOracle() mode {
    return func(c Converter) Converter {
		oracleConverter := new(OracleConverter)
		oracleConverter.Converter = c
		return oracleConverter
    }
}

func NewConverter(config string, m mode) (Converter, error) {
	confModelMap, err := parseConfModel(config)
	if err != nil {
		return nil, err
	}

	baseConverter := &BaseConverter {
		confModelMap: confModelMap,
	}
	c := m(baseConverter)
	baseConverter.child = c
	return c, nil
}

func parseConfModel(fileName string) (map[string]interface{}, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Errorf("read config file %s error: %s", fileName, err)
		return nil, err
	}

	// remove comments
	re := regexp.MustCompile(`\s*//\s.+|/\*.+?\*/`)
	content = re.ReplaceAll(content, []byte(""))

	var m map[string]interface{}
	err = json.Unmarshal(content, &m)
	if err != nil {
		log.Errorf("config file `%s` parser error: %s", fileName, err)
		return nil, err
	}
	return m, nil
}

func parseTagVal(tv string) (val string, ty string) {
	vals := strings.Split(tv, ",")
	if len(vals) == 2 {
		return vals[0], vals[1]
	} else {
		return "", ""
	}
}

func (c *BaseConverter)appendWhereClause(tag string, sqlBuf *bytes.Buffer, queryVal interface{}) {
	sqlV, sqlT := parseTagVal(tag)
	sqlBuf.WriteString(" WHERE ")
	sqlBuf.WriteString(sqlV)
	sqlBuf.WriteString("=")
	c.appendModelToSQL(sqlT, queryVal, sqlBuf)
}

func (c *BaseConverter)appendModelToSQL(sqlType string, value interface{}, valsBuf *bytes.Buffer) {
	switch value.(type) {
	case nil:
		valsBuf.WriteString("NULL")
	case int:
		if sqlType == "string" {
			valsBuf.WriteString("'")
			valsBuf.WriteString(strconv.Itoa(value.(int)))
			valsBuf.WriteString("'")
		} else {
			valsBuf.WriteString(strconv.Itoa(value.(int)))
		}
	case string:
		if sqlType == "DATE" {
			c.appendDate(valsBuf, value.(string))
		} else if sqlType == "string" {
			valsBuf.WriteString("'")
			valsBuf.WriteString(strings.Replace(value.(string), "'", "''", -1))
			valsBuf.WriteString("'")
		} else {
			valsBuf.WriteString(value.(string))
		}
	case int64:
		if sqlType == "string" {
			valsBuf.WriteString("'")
			valsBuf.WriteString(strconv.Itoa(int(value.(int64))))
			valsBuf.WriteString("'")
		} else {
			valsBuf.WriteString(strconv.Itoa(int(value.(int64))))
		}
	case bool:
		if sqlType == "string" {
			valsBuf.WriteString("'")
			valsBuf.WriteString(strconv.FormatBool(value.(bool)))
			valsBuf.WriteString("'")
		} else {
			valsBuf.WriteString(strconv.FormatBool(value.(bool)))
		}
	case float64:
		if sqlType == "string" {
			valsBuf.WriteString("'")
			valsBuf.WriteString(strconv.FormatFloat(value.(float64), 'f', -1, 64))
			valsBuf.WriteString("'")
		} else {
			valsBuf.WriteString(strconv.FormatFloat(value.(float64), 'f', -1, 64))
		}
	default:
		log.Error("[ERR]: Model element type unsupported.")
	}
}

func (c *BaseConverter)appendSqlInsert(fieldsBuf *bytes.Buffer, valsBuf *bytes.Buffer, fieldName string, fieldType string, value interface{}) {
	fieldsBuf.WriteString(fieldName)
	fieldsBuf.WriteString(",")
	c.appendModelToSQL(fieldType, value, valsBuf)
	valsBuf.WriteString(",")
}

/**
 * generate a complete INSERT SQL statement
 * tblmap: config map used to describe doc -> sql field mapping
 * m: the kv map representing doc
 * parentMapV: the higher level map field name of an embeded map field
 * fieldsBuf: buffer containing fields name in the INSERT SQL statement
 * valsBuf: buffer containing fields value in the INSERT SQL statement
 * return: number of fields in the generated INSERT SQL statement
 */
func (c *BaseConverter)mapToSqlInsert(tblmap map[string]interface{}, m map[string]interface{}, parentMapV string, fieldsBuf *bytes.Buffer, valsBuf *bytes.Buffer) (int, error) {
	fieldsNum := 0

	for key, value := range tblmap {
		mapV := key
		if parentMapV != "" {
			mapV = parentMapV + "." + mapV
		}

		if vMap, ok := value.(map[string]interface{}); ok {
			if m == nil { // the whole map is empty
				n, err := c.mapToSqlInsert(vMap, nil, "", fieldsBuf, valsBuf)
				if err != nil {
					return -1, err
				}
				fieldsNum += n
			} else if m[mapV] == nil { // a map field is empty
				n, err := c.mapToSqlInsert(vMap, m, mapV, fieldsBuf, valsBuf)
				if err != nil {
					return -1, err
				}
				fieldsNum += n
			} else {
				mapT := reflect.TypeOf(m[mapV]).String()
				if o, ok := m[mapV].(map[string]interface{}); ok {
					n, err := c.mapToSqlInsert(vMap, o, "", fieldsBuf, valsBuf)
					if err != nil {
						return -1, err
					}
					fieldsNum += n
				} else {
					return -1, fmt.Errorf("[ERR]: %s type(%s) error in collection", mapV, mapT)
				}
			}
			continue
		}

		sqlV, sqlT := parseTagVal(value.(string))

		if m == nil {
			continue
		} else if m[mapV] == nil {
			if _, ok := m[mapV]; ok { // "key":null, key exists，value is null
				c.appendSqlInsert(fieldsBuf, valsBuf, sqlV, sqlT, nil)
				fieldsNum++
			}
			continue
		}

		c.appendSqlInsert(fieldsBuf, valsBuf, sqlV, sqlT, m[mapV])
		fieldsNum++
	}

	return fieldsNum, nil
}

/**
 * Convert a kv map doc into a complete INSERT SQL statement
 * tblname: table name in the INSERT SQL statement
 * doc: the kv map doc to be converted
 */
func (c *BaseConverter)Insert(tblname string, doc map[string]interface{}) (string, error) {
	tblmap := c.confModelMap[tblname].(map[string]interface{})

	sqlBuf := bytes.NewBufferString("INSERT INTO ")
	if sqlBuf == nil {
		log.Error("[ERR]: NewBufferString Out of memory.")
		return "", nil
	}

	sqlBuf.WriteString(tblname)
	sqlBuf.WriteString(" ")

	fieldsBuf := bytes.NewBufferString("(")
	if fieldsBuf == nil {
		log.Error("[ERR]: NewBufferString Out of memory.")
		return "", nil
	}

	valsBuf := bytes.NewBufferString("VALUES(")
	if valsBuf == nil {
		log.Error("[ERR]: NewBufferString Out of memory.")
		return "", nil
	}

	fieldsNum, err := c.mapToSqlInsert(tblmap, doc, "", fieldsBuf, valsBuf)
	if err != nil {
		log.Error(err)
		return "", nil
	}

	if fieldsNum > 0 {
		fieldsBuf.Truncate(fieldsBuf.Len() - 1)
		fieldsBuf.WriteString(")")

		sqlBuf.WriteString(fieldsBuf.String())
		sqlBuf.WriteString(" ")

		valsBuf.Truncate(valsBuf.Len() - 1)
		valsBuf.WriteString(")")

		sqlBuf.WriteString(valsBuf.String())
		return sqlBuf.String(), nil
	}
	return "", fmt.Errorf("Nothing to INSERT.")
}

func (c *BaseConverter)appendSqlUpdate(sqlBuf *bytes.Buffer, fieldName string, fieldType string, value interface{}) {
	sqlBuf.WriteString(fieldName)
	sqlBuf.WriteString("=")
	c.appendModelToSQL(fieldType, value, sqlBuf)
	sqlBuf.WriteString(",")
}

/**
 * generate a complete UPDATE SQL statement
 * tblmap: config map used to describe doc -> sql field mapping
 * m: the kv map representing doc
 * parentMapV: the higher level map field name of an embeded map field
 * sqlBuf: buffer containing fields update value in the UPDATE SQL statement
 * return: number of fields in the generated INSERT SQL statement
 */
func (c *BaseConverter)mapToSqlUpdate(tblmap map[string]interface{}, m map[string]interface{}, parentMapV string, sqlBuf *bytes.Buffer) (int, error) {
	fieldsNum := 0

	for key, value := range tblmap {
		mapV := key
		if parentMapV != "" {
			mapV = parentMapV + "." + mapV
		}

		if vMap, ok := value.(map[string]interface{}); ok {
			if m == nil {
				n, err := c.mapToSqlUpdate(vMap, nil, "", sqlBuf)
				if err != nil {
					return -1, err
				}
				fieldsNum += n
			} else if m[mapV] == nil {
				n, err := c.mapToSqlUpdate(vMap, m, mapV, sqlBuf)
				if err != nil {
					return -1, err
				}
				fieldsNum += n
			} else {
				mapT := reflect.TypeOf(m[mapV]).String()
				if o, ok := m[mapV].(map[string]interface{}); ok {
					n, err := c.mapToSqlUpdate(vMap, o, "", sqlBuf)
					if err != nil {
						return -1, err
					}
					fieldsNum += n
				} else {
					return -1, fmt.Errorf("[ERR]: %s type(%s) error in collection", mapV, mapT)
				}
			}
			continue
		}

		sqlV, sqlT := parseTagVal(value.(string))

		if m == nil || m[mapV] == nil {
			c.appendSqlUpdate(sqlBuf, sqlV, sqlT, nil)
			fieldsNum++
			continue
		}

		c.appendSqlUpdate(sqlBuf, sqlV, sqlT, m[mapV])
		fieldsNum++
	}

	return fieldsNum, nil
}

/**
 * Convert a kv map doc into a complete UPDATE SQL statement
 * tblname: table name in the UPDATE SQL statement
 * queryKey: the key in the config map used to get the query field name in the where clause
 * doc: the kv map doc to be converted
 */
func (c *BaseConverter)Update(tblname, queryKey string, doc map[string]interface{}) (string, error) {
	tblmap := c.confModelMap[tblname].(map[string]interface{})

	if queryVal, ok := doc[queryKey]; ok {
		sqlBuf := bytes.NewBufferString("UPDATE ")
		if sqlBuf == nil {
			log.Error("[ERR]: NewBufferString Out of memory.")
			return "", nil
		}

		sqlBuf.WriteString(tblname)
		sqlBuf.WriteString(" SET ")

		fieldsNum, err := c.mapToSqlUpdate(tblmap, doc, "", sqlBuf)
		if err != nil {
			log.Error(err)
			return "", err
		}

		if fieldsNum > 0 {
			sqlBuf.Truncate(sqlBuf.Len() - 1)
			if key, ok := tblmap[queryKey]; ok {
				c.appendWhereClause(key.(string), sqlBuf, queryVal)
				return sqlBuf.String(), nil
			}
		}
	}
	return "", fmt.Errorf("Nothing to Update.")
}

/**
 * generate a complete DELETE SQL statement
 * tblname: table name in the DELETE SQL statement
 * queryKey: the key in the config map used to get the query field name in the where clause
 * doc: the kv map doc to be converted
 */
func (c *BaseConverter)Delete(tblname, queryKey string, doc map[string]interface{}) (string, error) {
	tblmap := c.confModelMap[tblname].(map[string]interface{})

	if queryVal, ok := doc[queryKey]; ok {
		sqlBuf := bytes.NewBufferString("DELETE FROM ")
		if sqlBuf == nil {
			log.Error("[NewBufferString Out of memory.")
			return "", nil
		}
		sqlBuf.WriteString(tblname)
		if key, ok := tblmap[queryKey]; ok {
			c.appendWhereClause(key.(string), sqlBuf, queryVal)
			return sqlBuf.String(), nil
		}
	}
	return "", fmt.Errorf("Nothing to Update.")
}
