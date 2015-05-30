package influxdbclient

import "github.com/stretchr/testify/assert"
import "testing"
import "time"

var fields = map[string]interface{}{
	"rsc": 3711,
	"r":   2138,
	"gri": 1908,
	"adg": 912,
}

var fields2 = map[string]interface{}{
	"rst": 3711,
	"r":   2138,
	"gri": 1908,
	"adg": 912,
}

var tags = map[string]string{
	"test": "yes",
}

func Test_BadConnect(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8087", "testdb", "admin", "admin")
	err := testDB.Connect()
	assert.NotNil(t, err, "We are expecting error and got one")
}

func Test_GoodConnect(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "admin", "admin")
	err := testDB.Connect()
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_CreateDB(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "admin", "admin")
	testDB.Connect()
	_, err := testDB.CreateDB("testdb")

	assert.Nil(t, err, "We are expecting no errors and got one")
}

// func Test_AddPoint(t *testing.T) {
// 	testDB := InitSession()
// 	testDB.AddPoint("test", time.Now(), fields, tags)
// 	assert.Equal(t, len(testDB.points), 1)
// }

func Test_WritePoints(t *testing.T) {
	testDB := InitSession()
	testDB.AddPoint("test", time.Now(), fields, tags)
	testDB.AddPoint("test2", time.Now(), fields, tags)

	err := testDB.WritePoints()
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_ReadPoints(t *testing.T) {
	testDB := InitSession()
	testDB.AddPrecisePoint("test", time.Now(), fields2, tags, "n")
	time.Sleep(100 * time.Millisecond)
	testDB.AddPrecisePoint("test", time.Now(), fields, tags, "n")
	time.Sleep(100 * time.Millisecond)
	testDB.WritePoints()

	time.Sleep(1000 * time.Millisecond)
	_, err := testDB.ReadPoints("test", "*")
	assert.Nil(t, err, "We are expecting error and got one")
}

func Test_ShowDB(t *testing.T) {
	testDB := InitSession()
	testDB.CreateDB("titi")
	_, err := testDB.ShowDB()
	assert.Nil(t, err, "We are expecting error and got one")
}

func Test_ExistDB(t *testing.T) {
	testDB := InitSession()
	testDB.CreateDB("testdb2")
	check, err := testDB.ExistDB("testdb2")
	assert.Nil(t, err, "We are expecting error and got one")
	assert.Equal(t, check, true, "they should be equal")
}

func Test_DropDB(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "admin", "admin")
	testDB.Connect()
	_, err := testDB.DropDB("testdb")

	assert.Nil(t, err, "We are expecting error and got one")
}

func InitSession() (db *InfluxDB) {
	db = NewInfluxDB("localhost", "8086", "testdb", "admin", "admin")
	db.Connect()
	return
}
