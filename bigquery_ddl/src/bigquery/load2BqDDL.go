/*======================
SQLを実行してDDLを更新する
========================*/
package bigquery

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"

	TABLE "bwing.app/src/bigquery/table"
)

//Inerface
type Load2BqDDL struct {
	Dataset  string
	SourceId string
	SQL      string
}

///////////////////////////////////////////////////
/* ===========================================
//取得したSQLファイルからBigQueryのDDLを更新する
* =========================================== */
func (l *Load2BqDDL) RunBqQuery() (interface{}, error) {

	var err error

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return nil, err
	}
	defer bqClient.Close()

	//更新前のProcedureのLastUpdate(tableは考慮外)を取得
	dt1, err := TABLE.GetRoutineLastUpdate(l.Dataset, l.SourceId)
	if err != nil {
		return nil, err
	}

	//SQLを実行
	q := bqClient.Query(l.SQL)
	jr, err := q.Run(bqCtx)
	if err != nil {
		return nil, err
	}

	//更新前のProcedureのLastUpdate(tableは考慮外)を取得
	dt2, err := TABLE.GetRoutineLastUpdate(l.Dataset, l.SourceId)
	if err != nil {
		return nil, err
	}

	//ProcedureのLastUpdateを比較して更新されているかを確認
	if !dt1.IsZero() {
		if !dt2.After(dt1) {
			err := errors.New("ストアドプロシージャが更新されていない可能性があります。BigQueryの実行履歴を確認してください。")
			return nil, err
		}
	}

	return jr, nil
}
