package influxdbclient

import "github.com/influxdb/influxdb/client"
import "fmt"
import "sort"
import "time"

//import "math"
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
// DataSet structure
// contains the columns and points to analyze for statistics
//

type DataSet struct {
	Name       string
	TimeStamps []float64
	Datas      map[string][]float64
}

type DataStats struct {
	Min    float64
	Max    float64
	Mean   float64
	Median float64
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
	db.debug = debug
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
		if db.debug {
			fmt.Printf("No defined fields for %s. No datas inserted\n", serie)
		}
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
	dataSerie.PointSeq++
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
		return
	}
	return
}

func (db *InfluxDB) PointsCount(serie string) int {
	return db.DataSeries[serie].PointSeq
}

func (db *InfluxDB) MaxPointsCount(serie string) bool {
	if db.DataSeries[serie].PointSeq == db.MaxPoints {
		return true
	}
	return false
}

func (db *InfluxDB) ClearPoints(serie string) {
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

func (db *InfluxDB) ReadPoints(fields string, serie string, from string, to string, function string) (ds *DataSet, err error) {
	cmd := buildQuery(fields, serie, from, to, function)
	if db.debug {
		fmt.Printf("query: %s\n", cmd)
	}
	res, err := db.Client.Query(cmd)
	if err != nil {
		return
	}
	fmt.Println(res)
	ds = ConvertToDataSet(res)
	return
}

const querytimeformat = "2006-01-02 15:04:05"

func buildQuery(fields string, serie string, from string, to string, function string) (query string) {
	if len(function) > 0 {
		query = fmt.Sprintf("select %s(%s) from %s", function, fields, serie)
	} else {
		query = fmt.Sprintf("select %s from %s", fields, serie)
	}

	if len(from) == 0 {
		return
	}

	fromUnix, _ := ConvertTimeStamp(from)
	fromtime := time.Unix(fromUnix, 0).UTC().Format(querytimeformat)
	if len(to) == 0 {
		query = fmt.Sprintf("%s where time > '%s'", query, fromtime)
		return
	}
	toUnix, _ := ConvertTimeStamp(to)

	totime := time.Unix(toUnix, 0).UTC().Format(querytimeformat)
	query = fmt.Sprintf("%s where time > '%s' and time < '%s'", query, fromtime, totime)
	return
}

func (db *InfluxDB) ReadAllPoints(fields string, serie string) (ds *DataSet, err error) {
	cmd := fmt.Sprintf("select %s from %s", fields, serie)
	res, err := db.Client.Query(cmd)
	if err != nil {
		return
	}

	ds = ConvertToDataSet(res)
	return
}

func NewDataSet(length int, fields []string) *DataSet {
	ds := DataSet{TimeStamps: make([]float64, length), Datas: make(map[string][]float64)}

	for _, fieldname := range fields {
		ds.Datas[fieldname] = make([]float64, length)
	}
	return &ds
}

func ConvertToDataSet(res []*client.Series) *DataSet {
	if len(res) == 0 {
		return new(DataSet)
	}

	ds := NewDataSet(len(res[0].Points), res[0].Columns[2:])

	ds.Name = res[0].Name

	for i, row := range res[0].Points {

		ds.TimeStamps[i] = row[0].(float64)

		for j, field := range row {
			if j == 0 {
				continue
			}
			if j == 1 {
				continue
			}
			fieldname := res[0].Columns[j]
			if field != nil {
				val, _ := field.(float64)
				ds.Datas[fieldname][i] = val
			}
		}
	}
	return ds
}

func (db *InfluxDB) BuildStats(ds *DataSet) (stats map[string]DataStats) {
	stats = make(map[string]DataStats)
	for name, data := range ds.Datas {
		length := len(data)

		//sorting data
		sort.Float64s(data)
		var stat DataStats
		stat.Min = data[0]
		stat.Max = data[length-1]
		stat.Mean = Mean(data)
		if length%2 == 0 {
			stat.Median = Mean(data[length/2-1 : length/2+1])
		} else {
			stat.Median = float64(data[length/2])
		}
		stats[name] = stat
	}
	return
}

func Sum(data []float64) (sum float64) {
	for _, n := range data {
		sum += n
	}
	return sum
}

func Mean(data []float64) (mean float64) {
	sum := Sum(data)
	return sum / float64(len(data))
}
