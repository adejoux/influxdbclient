package influxdbclient

import "github.com/influxdata/influxdb/client"

type TextSet struct {
	Name  string
	Tags  map[string]string
	Datas []string
}

func ConvertToTextSet(res []client.Result) (tset *TextSet) {
	if len(res) == 0 {
		return
	}

	if len(res[0].Series) == 0 {
		return
	}
	serie := res[0].Series[0]
	tset = new(TextSet)

	tset.Name = serie.Name
	tset.Tags = serie.Tags

	for _, row := range serie.Values {
		for _, field := range row {
			tset.Datas = append(tset.Datas, field.(string))
		}
	}
	return tset
}
