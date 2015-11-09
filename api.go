package influxdbclient

import "github.com/influxdb/influxdb/client"
import "net/url"
import "fmt"
import "time"

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
		Precision:			"s",
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

func (db *InfluxDB) ListMeasurement(filters *Filters) (tset *TextSet, err error) {

	query := "SHOW MEASUREMENTS"
	var fQuery FilterQuery
	if len(*filters) > 0 {
		fQuery.AddFilters(filters)
	}
	if len(fQuery.Content) > 0 {
		query += " WHERE " + fQuery.Content
	}
	if db.debug {
		fmt.Printf("query: %s\n", query)
	}
	res, err := db.queryDB(query, db.db)
	if err != nil {
		return
	}
	return ConvertToTextSet(res), err

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

func (db *InfluxDB) ReadPoints(fields string, filters *Filters, groupby string, serie string, from string, to string, function string) (ds []*DataSet, err error) {
	cmd := buildQuery(fields, filters, groupby, serie, from, to, function)
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
