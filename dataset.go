package influxdbclient

import "github.com/influxdb/influxdb/client"
import "time"
import "encoding/json"

//
// DataSet structure
// contains the columns and points to analyze for statistics
//
type DataSet struct {
	Name       string
	TimeStamps []time.Time
	Tags       map[string]string
	Datas      map[string][]float64
}

func NewDataSet(length int, fields []string) *DataSet {
	ds := DataSet{TimeStamps: make([]time.Time, length), Datas: make(map[string][]float64)}

	for _, fieldname := range fields {
		ds.Datas[fieldname] = make([]float64, length)
	}
	return &ds
}

func ConvertToDataSet(res []client.Result) (dsets []*DataSet) {
	if len(res[0].Series) == 0 {
		return
	}

	for _, serie := range res[0].Series {
		ds := NewDataSet(len(serie.Values), serie.Columns[1:])

		ds.Name = serie.Name
		ds.Tags = serie.Tags
		for i, row := range serie.Values {

			t, _ := time.Parse(time.RFC3339, row[0].(string))

			ds.TimeStamps[i] = t

			for j, field := range row {
				if j == 0 {
					continue
				}

				fieldname := serie.Columns[j]
				if field != nil {
					val, _ := field.(json.Number).Float64()
					ds.Datas[fieldname][i] = val
				}
			}
		}
		dsets = append(dsets, ds)
	}
	return
}
