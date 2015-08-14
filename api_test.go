package influxdbclient

import "github.com/stretchr/testify/assert"
import "testing"
import "fmt"
import "time"

var fields = map[string]interface{}{
	"a": 3711.0,
	"b": 2138.0,
	"c": 1908.0,
	"d": 912.0,
}

var fields2 = map[string]interface{}{
	"a": 3711.0,
	"b": 2138.0,
	"c": 1908.0,
	"d": 912.0,
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
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_CreateDB(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	res, err := testDB.CreateDB("testdb")
	assert.Nil(t, err, "We are expecting no error and got one")
	assert.NotNil(t, res, "We are expecting no error and got one")
}

func Test_AddPoint(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	ti, _ := ConvertTimeStamp("23:55:28,13-MAY-2015", "Europe/Paris")
	testDB.AddPoint("test", ti, fields, tags)
	assert.Nil(t, err, "We are expecting no error and got one")
	assert.Equal(t, testDB.PointsCount(), int64(1))
}
func Test_WritePoints(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	ti, _ := ConvertTimeStamp("23:55:28,13-MAY-2015", "Europe/Paris")
	testDB.AddPoint("test", ti, fields, tags)
	testDB.AddPoint("test2", ti, fields, tags)
	ti, _ = ConvertTimeStamp("23:55:28,13-MAY-2015", "Europe/Paris")
	testDB.AddPoint("test", ti, fields2, tags)
	testDB.AddPoint("test2", ti, fields2, tags)
	err = testDB.WritePoints()
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_ReadPoints(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	filters := new(Filters)
	_, err = testDB.ReadPoints("a", filters, "test", "test2", "23:50:00,13-MAY-2015", "23:59:00,13-MAY-2015", "")
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_BuildStats(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	filters := new(Filters)
	result, err := testDB.ReadPoints("a", filters, "test", "test2", "23:50:00,13-MAY-2015", "23:59:00,13-MAY-2015", "")
	stats := testDB.BuildStats(result)
	fmt.Println(stats)
	assert.Nil(t, err, "We are expecting no errors and got one")
}

func Test_DropDB(t *testing.T) {
	testDB := NewInfluxDB("localhost", "8086", "testdb", "root", "root")
	err := testDB.Connect()
	res, err := testDB.DropDB("testdb")

	assert.Nil(t, err, "We are expecting no error and got one")
	assert.NotNil(t, res, "We are expecting no error and got one")

}

const timeformat = "15:04:05,02-Jan-2006"

func ConvertTimeStamp(s string, tz string) (time.Time, error) {
	var err error
	var loc *time.Location
	if len(tz) > 0 {
		loc, err = time.LoadLocation(tz)
		if err != nil {
			loc = time.FixedZone("Europe/Paris", 2*60*60)
		}
	} else {
		timezone, _ := time.Now().In(time.Local).Zone()
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			loc = time.FixedZone("Europe/Paris", 2*60*60)
		}
	}

	t, err := time.ParseInLocation(timeformat, s, loc)
	return t, err
}
