package influxdbclient

import "github.com/stretchr/testify/assert"
import "testing"
import "fmt"

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

// func Test_BadConnect(t *testing.T) {
// 	testDB := NewInfluxDB()
// 	err := testDB.InitSession("locallhost", "8087", "testdb", "root", "root")
// 	assert.NotNil(t, err, "We are expecting error and didn't got one")
// }

func Test_GoodConnect(t *testing.T) {
	testDB := NewInfluxDB()
	err := testDB.InitSession("localhost:8086", "testdb", "root", "root")
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_CreateDB(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	err := testDB.CreateDB("testdb")

	assert.Nil(t, err, "We are expecting no error and got one")
}

func Test_AddPoint(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	testDB.SetDataSerie("test", []string{"col1", "col2"})
	testDB.AddPoint("test", "23:55:28,13-MAY-2015", []string{"10", "20"})
	assert.Equal(t, testDB.PointsCount("test"), 1)
}
func Test_WritePoints(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	testDB.SetDataSerie("test2", []string{"col1", "col2"})
	testDB.AddPoint("test2", "23:55:28,13-MAY-2015", []string{"10", "20"})
	testDB.AddPoint("test2", "23:55:38,13-MAY-2015", []string{"11", "21"})
	testDB.AddPoint("test2", "23:55:38,13-MAY-2015", []string{"211", "321"})
	err := testDB.WritePoints("test2")
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_ReadAllPoints(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	_, err := testDB.ReadAllPoints("*", "_test2")
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_ReadPoints(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	_, err := testDB.ReadPoints("*", "_test2", "23:55:00,13-MAY-2015", "23:56:00,13-MAY-2015", "")
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_BuildStats(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	result, err := testDB.ReadAllPoints("*", "_test2")
	stats := testDB.BuildStats(result)
	fmt.Println(stats)
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_DropDB(t *testing.T) {
	testDB := NewInfluxDB()
	testDB.InitSession("localhost:8086", "testdb", "root", "root")
	err := testDB.DropDB("testdb")

	assert.Nil(t, err, "We are expecting no error and got one")
}
