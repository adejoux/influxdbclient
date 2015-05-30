package influxdbclient

import "github.com/influxdb/influxdb/client"
import "fmt"
import "time"
import "strings"
import "strconv"
import "encoding/json"

//
// DataSerie structure
// contains the columns and points to insert in InfluxDB
//

type DataSerie struct {
	Columns  []string
	PointSeq int
	Points   [50][]interface{}
}

//
// influxDB structure
// contains the main structures and methods used to parse nmon files and upload data in Influxdb
//

type InfluxDB struct {
	Client      *client.Client
	MaxPoints   int
	DataSeries  map[string]DataSerie
	TextContent string
	Label       string
	debug       bool
	starttime   int64
	stoptime    int64
}

// initialize a Influx structure
func NewInfluxDB() *InfluxDB {
	return &InfluxDB{DataSeries: make(map[string]DataSerie), MaxPoints: 50}

}

func (db *InfluxDB) GetColumns(serie string) []string {
	return db.DataSeries[serie].Columns
}

func (db *InfluxDB) GetFilteredColumns(serie string, filter string) []string {
	var res []string
	for _, field := range db.DataSeries[serie].Columns {
		if strings.Contains(field, filter) {
			res = append(res, field)
		}
	}
	return res
}

// func (db *InfluxDB) AppendText(text string) {
// 	db.TextContent += ReplaceComma(text)
// }

func (db *InfluxDB) SetDebug(debug bool) {
	db.debug = true
}

func (db *InfluxDB) CreateDB(dbname string) (err error) {
	if err = db.Client.CreateDatabase(dbname); err != nil {
		return
	}
	return
}

func (db *InfluxDB) DropDB(dbname string) (err error) {
	if err = db.Client.DeleteDatabase(dbname); err != nil {
		return
	}
	return
}

func (db *InfluxDB) ShowDB() (databases []string, err error) {
	dblist, err := db.Client.GetDatabaseList()
	for _, v := range dblist {
		databases = append(databases, v["name"].(string))
	}

	return
}

func (db *InfluxDB) ExistDB(dbname string) (check bool) {
	check = false
	dbs, err := db.ShowDB()
	if err != nil {
		return
	}

	//checking if database exists
	for _, v := range dbs {
		if v == dbname {
			check = true
		}
	}
	return
}

func (db *InfluxDB) SetDataSerie(name string, columns []string) {
	dataserie := db.DataSeries[name]
	dataserie.Columns = columns
	db.DataSeries[name] = dataserie
}

func (db *InfluxDB) AddPoint(serie string, strtimestamp string, elems []string) {

	timestamp, err := ConvertTimeStamp(strtimestamp)

	if err != nil {
		if db.debug {
			fmt.Printf("Skipping point.Unable to convert timestamp : %s\n", strtimestamp)
		}
		return
	}

	dataSerie := db.DataSeries[serie]

	if len(dataSerie.Columns) == 0 {
		//fmt.Printf("No defined fields for %s. No datas inserted\n", serie)
		return
	}

	if len(dataSerie.Columns) != len(elems) {
		return
	}

	point := []interface{}{}
	point = append(point, timestamp)
	for i := 0; i < len(elems); i++ {
		// try to convert string to integer
		value, err := strconv.ParseFloat(elems[i], 64)
		if err != nil {
			//if not working, use string
			point = append(point, elems[i])
		} else {
			//send integer if it worked
			point = append(point, value)
		}
	}

	dataSerie.Points[dataSerie.PointSeq] = point
	dataSerie.PointSeq += 1
	db.DataSeries[serie] = dataSerie
}

func (db *InfluxDB) WritePoints(serie string) (err error) {

	dataSerie := db.DataSeries[serie]
	series := &client.Series{}

	series.Name = db.Label + "_" + serie

	series.Columns = append([]string{"time"}, dataSerie.Columns...)

	for i := 0; i < len(dataSerie.Points); i++ {
		if dataSerie.Points[i] == nil {
			break
		}
		series.Points = append(series.Points, dataSerie.Points[i])
	}

	if err = db.Client.WriteSeriesWithTimePrecision([]*client.Series{series}, "s"); err != nil {
		data, err2 := json.Marshal(series)
		if err2 != nil {
			return err2
		}
		fmt.Printf("%s\n", data)
		return
	}
	return
}

func (db *InfluxDB) PointsCount(serie string) int {
	return db.DataSeries[serie].PointSeq
}

func (db *InfluxDB) NewPoints(serie string) {
	dataSerie := db.DataSeries[serie]
	dataSerie.PointSeq = 0
	db.DataSeries[serie] = dataSerie
}

func (db *InfluxDB) InitSession(host string, database string, user string, pass string) (err error) {
	dbclient, err := client.NewClient(&client.ClientConfig{
		Host:     host,
		Username: user,
		Password: pass,
		Database: database,
	})

	if err != nil {
		return
	}

	dbclient.DisableCompression()
	db.Client = dbclient
	return
}

const timeformat = "15:04:05,02-Jan-2006"

func ConvertTimeStamp(s string) (int64, error) {
	timezone, _ := time.Now().In(time.Local).Zone()
	loc, err := time.LoadLocation(timezone)

	if err != nil {
		loc = time.FixedZone("Europe/Paris", 2*60*60)
	}

	t, err := time.ParseInLocation(timeformat, s, loc)
	return t.Unix(), err
}
