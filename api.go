package influxdbclient

import "github.com/influxdb/influxdb/client"
import "net/url"
import "time"
import "fmt"
import "encoding/json"


type InfluxDB struct {
    host     string
    port     string
    db       string
    user     string
    pass     string
    debug    bool
    points   []client.Point
    con      *client.Client
}

type DataSet struct {
    Name    string
    TimeStamps []time.Time
    Datas map[string][]float64
}

func NewInfluxDB(host string, port string, database string, user string, pass string) *InfluxDB {
    return &InfluxDB{host: host, port: port, db: database, user: user, pass: pass}
}

func (db *InfluxDB) SetDebug(debug bool) {
    db.debug = debug
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

func (db *InfluxDB) AddPoint(name string, timestamp time.Time, fields map[string]interface{}) {
    db.AddPrecisePoint(name, timestamp, fields, "s")
}

func (db *InfluxDB) AddPrecisePoint(name string, timestamp time.Time, fields map[string]interface{}, precision string) {
    point := client.Point{
        Name: name,
        Fields: fields,
        Timestamp: timestamp,
        Precision: precision,
    }

    db.points = append(db.points, point)
    if db.debug == true {
      fmt.Println(db.points)
    }
}

func (db *InfluxDB) WritePoints() (err error) {
    bps := client.BatchPoints{
        Points:          db.points,
        Database:        db.db,
        RetentionPolicy: "default",
    }

    _, err = db.con.Write(bps)
    return
}

func NewDataSet(length int, fields []string) *DataSet {
    ds := DataSet{TimeStamps: make([]time.Time, length), Datas: make(map[string][]float64)}

    for i, fieldname := range fields {
        if i == 0 {
            continue
        }
        ds.Datas[fieldname] = make([]float64,length)
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

func ConvertToDataSet(res []client.Result) (*DataSet) {
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
    fmt.Println(ds)
    return ds
}
