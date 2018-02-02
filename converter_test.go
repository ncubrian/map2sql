package map2sql

import (
	"testing"
	"github.com/arstd/log"
)

func TestMysqlInsert(t *testing.T) {
	c, err := NewConverter("model.js", ModeMysql())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Insert("FOOBAR", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}

func TestMysqlUpdate(t *testing.T) {
	c, err := NewConverter("model.js", ModeMysql())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Update("FOOBAR", "name", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}

func TestMysqlDelete(t *testing.T) {
	c, err := NewConverter("model.js", ModeMysql())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Delete("FOOBAR", "name", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}

func TestOracleInsert(t *testing.T) {
	c, err := NewConverter("model.js", ModeOracle())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Insert("FOOBAR", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}

func TestOracleUpdate(t *testing.T) {
	c, err := NewConverter("model.js", ModeOracle())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Update("FOOBAR", "name", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}

func TestOracleDelete(t *testing.T) {
	c, err := NewConverter("model.js", ModeOracle())
	if err != nil {
		log.Error(err)
		t.FailNow()
	}

	m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
	sql, err := c.Delete("FOOBAR", "name", m)
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(sql)
}
