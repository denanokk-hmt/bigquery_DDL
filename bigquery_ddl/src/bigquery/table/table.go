/* =================================
BigQueryのTableの共通構造
その他、Tableを扱う共通項目や処理
* ================================= */
package table

import (
	//共通処理
	"context"
	"fmt"
	"strings"

	CONFIG "bwing.app/src/config"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

var ()

//Datastore Load result
type LoadDataResults struct {
	Results interface{}
}

/* =================================
//Get GCP ProjectID
* ================================= */
func GetProjectId() string {
	return CONFIG.GetConfigEnv(CONFIG.ENV_GCP_PROJECT_ID)
}

/* =================================
//結果箱の空の中身
* ================================= */
func NewLoadDataResults(p interface{}) LoadDataResults {
	var r LoadDataResults
	r.Results = p
	return r
}

/* =================================
//Get ROUTIN LastUpdate
* ================================= */
type RoutineLastUpdate struct {
	RoutineName string
	LastUpdate  civil.DateTime
}

func GetRoutineLastUpdate(dataset, sourceId string) (civil.DateTime, error) {
	info_sql := `SELECT routine_name, 
			DATETIME(last_altered, 'Asia/Tokyo') AS last_altered_jst`
	info_sql += " FROM `" + dataset + ".INFORMATION_SCHEMA.ROUTINES`"
	info_sql += " WHERE routine_name = '" + sourceId + "'"
	info_sql = strings.ReplaceAll(info_sql, "\n", "")
	info_sql = strings.ReplaceAll(info_sql, "\t", "")

	var dt civil.DateTime

	//BigQueryのclientを生成
	projectId := GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return dt, err
	}
	defer bqClient.Close()

	//該当DDL実行前のの最終更新日時を取得
	qs := bqClient.Query(info_sql)
	qs.QueryConfig.UseStandardSQL = true
	it, err := qs.Read(bqCtx)
	if err != nil {
		return dt, err
	}

	//BigQueryのレコードを格納する箱を準備(カラムごとに配列される)
	var valuess [][]bigquery.Value
	for {
		var values []bigquery.Value

		//これ以上結果が存在しない場合には、iterator.Doneを返して、ループ離脱
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		//エラーハンドル
		if err != nil {
			fmt.Println(err)
			return dt, err
		}
		valuess = append(valuess, values)
	}
	if len(valuess) == 1 {
		dt = valuess[0][1].(civil.DateTime)
	}

	return dt, nil
}
