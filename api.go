package influxdbclient

import "github.com/influxdb/influxdb/client"
import "net/url"
import "time"
import "fmt"
import "encoding/json"

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

type DataSet struct {
	Name       string
	TimeStamps []time.Time
	Datas      map[string][]float64
}

func NewInfluxDB(host string, port string, database string, user string, pass string) *InfluxDB {
	return &InfluxDB{host: host, port: port, db: database, user: user, pass: pass, points: make([]client.Point, 1000), count: 0}
}

func (db *InfluxDB) SetDebug(debug bool) {
	db.debug = true
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

func (db *InfluxDB) AddPoint(name string, timestamp time.Time, fields map[string]interface{}, tags map[string]string) {
	db.AddPrecisePoint(name, timestamp, fields, tags, "s")
}

func (db *InfluxDB) AddPrecisePoint(name string, timestamp time.Time, fields map[string]interface{}, tags map[string]string, precision string) {

	point := client.Point{
		Name:      name,
		Fields:    fields,
		Tags:      tags,
		Time:      timestamp,
		Precision: precision,
	}

	if len(tags) > 0 {
		point.Tags = tags
	}

	db.points[db.count] = point
	db.count += 1
}

func (db *InfluxDB) WritePoints() (err error) {
	bps := client.BatchPoints{
		Points:          db.points[:db.count],
		Database:        db.db,
		RetentionPolicy: "default",
	}

	_, err = db.con.Write(bps)
	return
}

func (db *InfluxDB) PointsCount() int64 {
	return db.count + 1
}

func (db *InfluxDB) NewPoints() {
	db.count = 0
}

func NewDataSet(length int, fields []string) *DataSet {
	ds := DataSet{TimeStamps: make([]time.Time, length), Datas: make(map[string][]float64)}

	for i, fieldname := range fields {
		if i == 0 {
			continue
		}
		ds.Datas[fieldname] = make([]float64, length)
	}
	return &ds
}

func (db *InfluxDB) ReadPoints(serie string, fields string) (ds *DataSet, err error) {
	cmd := fmt.Sprintf("select %s from %s", fields, serie)
	res, err := db.queryDB(cmd, db.db)
	if err != nil {
		return
	}

	ds = ConvertToDataSet(res)
	return
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
