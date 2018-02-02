Introduction
===
##### A library to convert a key-value map into a sql statement.

Features
===
* Support Mysql and Oracle sql statement
* Support basic insert, update and delete statement
* Support embeded map structure

Install
===
go get github.com/ncubrian/map2sql

Quick Start
===
##### First of all, create a map to sql config file which looks like this.

```
{
	"FOOBAR":{
		"name": "NAME,string",
		"age": "AGE,int",
		"birthday": "BIRTHDAY,DATE",
		"career": {
			"company": "CAREER_COMPANY,string"
		}
	}
}
```
The config file has to be in json format.

##### Create a converter and do the convertion. Like this

```
c, _ := NewConverter("model.js", ModeOracle())
m := map[string]interface{} {
		"name": "Diana",
		"age": 12,
		"birthday": "2018-02-01 12:34:56",
		"career": map[string]interface{} {
			"company": "flicker",
		},
	}
sql, _ := c.Insert("FOOBAR", m)
```
##### More usage could be found in the test file.
