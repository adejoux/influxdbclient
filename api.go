package influxdbclient

import "github.com/influxdb/influxdb/client"
import "net/url"
import "fmt"
import "sort"
import "time"
import "encoding/json"

//
// DataSet structure
// contains the columns and points to analyze for statistics
//

type DataSet struct {
	Name       string
	TimeStamps []time.Time
	Datas      map[string][]float64
}

type DataStat struct {
	Name   string
	Min    float64
	Max    float64
	Mean   float64
	Median float64
	Length int
}

type DataStats []DataStat

func (ds *DataStats) FieldSort(field string) {
	switch field {
	case "name":
		sort.Sort(NameDataStats{*ds})
	case "min":
		sort.Sort(MinDataStats{*ds})
	case "max":
		sort.Sort(MaxDataStats{*ds})
	case "median":
		sort.Sort(MedianDataStats{*ds})
	default:
		sort.Sort(MeanDataStats{*ds})
	}
}

func (slice DataStats) Len() int {
	return len(slice)
}

func (slice DataStats) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type NameDataStats struct{ DataStats }

func (slice NameDataStats) Less(i, j int) bool {
	return slice.DataStats[i].Name > slice.DataStats[j].Name
}

type MinDataStats struct{ DataStats }

func (slice MinDataStats) Less(i, j int) bool {
	return slice.DataStats[i].Min > slice.DataStats[j].Min
}

type MaxDataStats struct{ DataStats }

func (slice MaxDataStats) Less(i, j int) bool {
	return slice.DataStats[i].Max > slice.DataStats[j].Max
}

type MeanDataStats struct{ DataStats }

func (slice MeanDataStats) Less(i, j int) bool {
	return slice.DataStats[i].Mean > slice.DataStats[j].Mean
}

type MedianDataStats struct{ DataStats }

func (slice MedianDataStats) Less(i, j int) bool {
	return slice.DataStats[i].Median > slice.DataStats[j].Median
}

//
// influxDB structure
// contains the main structures and methods used to parse nmon files and upload data in Influxdb
//

type InfluxDB struct {
	host   string
	port   string
	db     string
	user   string
	pass   string
	debug  bool
	count  int64
	points []client.Point
	con    *client.Client
}

// initialize a Influx structure
func NewInfluxDB(host string, port string, database string, user string, pass string) *InfluxDB {
	return &InfluxDB{host: host,
		port:   port,
		db:     database,
		user:   user,
		pass:   pass,
		points: make([]client.Point, 10000),
		count:  0}
}

// queryDB convenience function to query the database
func (db *InfluxDB) queryDB(cmd string, dbname string) (res []client.Result, err error) {
	query := client.Query{
		Command:  cmd,
		Database: dbname,
	}
	if response, err := db.con.Query(query); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}
	return
}

// query convenience function to query Influxdb
func (db *InfluxDB) query(cmd string) (res []client.Result, err error) {
	query := client.Query{
		Command: cmd,
	}

	if response, err := db.con.Query(query); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}
	return
}

// func (db *InfluxDB) AppendText(text string) {
// 	db.TextContent += ReplaceComma(text)
// }

func (db *InfluxDB) SetDebug(debug bool) {
	db.debug = debug
}

func (db *InfluxDB) CreateDB(dbname string) (res []client.Result, err error) {
	cmd := fmt.Sprintf("create database %s", dbname)
	res, err = db.queryDB(cmd, dbname)
	return
}

func (db *InfluxDB) DropDB(dbname string) (res []client.Result, err error) {
	cmd := fmt.Sprintf("drop database %s", dbname)
	res, err = db.queryDB(cmd, dbname)
	return
}

func (db *InfluxDB) ShowDB() (databases []string, err error) {
	cmd := fmt.Sprintf("show databases")
	res, err := db.query(cmd)
	if err != nil {
		return
	}

	if db.debug == true {
		fmt.Println(res)
	}

	if res == nil {
		return
	}

	for _, dbs := range res[0].Series[0].Values {
		for _, db := range dbs {
			if str, ok := db.(string); ok {
				databases = append(databases, str)
			}
		}
	}
	return
}

func (db *InfluxDB) ExistDB(dbname string) (check bool, err error) {
	dbs, err := db.ShowDB()
	check = false

	if err != nil {
		return
	}

	for _, val := range dbs {
		if dbname == val {
			check = true
			return
		}
	}
	return
}

func (db *InfluxDB) AddPoint(measurement string, timestamp time.Time, fields map[string]interface{}, tags map[string]string) {
	db.AddPrecisePoint(measurement, timestamp, fields, tags, "s")
}

func (db *InfluxDB) AddPrecisePoint(measurement string, timestamp time.Time, fields map[string]interface{}, tags map[string]string, precision string) {

	point := client.Point{
		Measurement: measurement,
		Fields:      fields,
		Tags:        tags,
		Time:        timestamp,
		Precision:   precision,
	}

	if len(tags) > 0 {
		point.Tags = tags
	}

	db.points[db.count] = point
	db.count += 1
}

func (db *InfluxDB) WritePoints() (err error) {
	bps := client.BatchPoints{
		Points:           db.points[:db.count],
		Database:         db.db,
		RetentionPolicy:  "default",
		WriteConsistency: client.ConsistencyAny,
	}

	_, err = db.con.Write(bps)
	return
}

func (db *InfluxDB) PointsCount() int64 {
	return db.count
}

func (db *InfluxDB) ClearPoints() {
	db.count = 0
}

func (db *InfluxDB) Connect() error {
	u, err := url.Parse(fmt.Sprintf("http://%s:%s", db.host, db.port))
	if err != nil {
		return err
	}

	conf := client.Config{
		URL:      *u,
		Username: db.user,
		Password: db.pass,
	}

	db.con, err = client.NewClient(conf)
	if err != nil {
		return err
	}

	dur, ver, err := db.con.Ping()
	if err != nil {
		return err
	}

	if db.debug == true {
		fmt.Printf("time : %v, version : %s\n", dur, ver)
	}

	return err
}

func (db *InfluxDB) ReadPoints(fields string, serie string, from string, to string, function string) (ds *DataSet, err error) {
	cmd := db.buildQuery(fields, serie, from, to, function)
	if db.debug {
		fmt.Printf("query: %s\n", cmd)
	}
	res, err := db.queryDB(cmd, db.db)
	if err != nil {
		return
	}
	ds = ConvertToDataSet(res)
	return
}

func (db *InfluxDB) buildQuery(fields string, serie string, from string, to string, function string) (query string) {
	if len(function) > 0 {
		query = fmt.Sprintf("select %s(\"%s\") from \"%s\"", function, fields, serie)
	} else {
		query = fmt.Sprintf("select \"%s\" from \"%s\"", fields, serie)
		if db.debug {
			fmt.Printf("query : %s \n", query)
		}
	}

	if len(from) == 0 {
		return
	}

	if len(to) == 0 {
		query = fmt.Sprintf("%s where time > '%s'", query, from)
		return
	}

	query = fmt.Sprintf("%s where time > '%s' and time < '%s'", query, from, to)
	return
}

func NewDataSet(length int, fields []string) *DataSet {
	ds := DataSet{TimeStamps: make([]time.Time, length), Datas: make(map[string][]float64)}

	for _, fieldname := range fields {
		ds.Datas[fieldname] = make([]float64, length)
	}
	return &ds
}

func ConvertToDataSet(res []client.Result) *DataSet {
	ds := NewDataSet(len(res[0].Series[0].Values), res[0].Series[0].Columns)

	ds.Name = res[0].Series[0].Name

	for i, row := range res[0].Series[0].Values {

		t, _ := time.Parse(time.RFC3339, row[0].(string))

		ds.TimeStamps[i] = t

		for j, field := range row {
			if j == 0 {
				continue
			}

			fieldname := res[0].Series[0].Columns[j]
			if field != nil {
				val, _ := field.(json.Number).Float64()
				ds.Datas[fieldname][i] = val
			}
		}
	}
	return ds
}

func (db *InfluxDB) BuildStats(ds *DataSet) (stats DataStats) {
	for name, data := range ds.Datas {
		length := len(data)

		//sorting data
		sort.Float64s(data)
		var stat DataStat
		stat.Name = name
		stat.Min = data[0]
		stat.Max = data[length-1]
		stat.Mean = Mean(data)
		stat.Length = length
		if length%2 == 0 {
			stat.Median = Mean(data[length/2-1 : length/2+1])
		} else {
			stat.Median = float64(data[length/2])
		}
		stats = append(stats, stat)
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
