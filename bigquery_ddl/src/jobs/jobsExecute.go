/*======================
ETL Jobの処理のまとめやく
========================*/
package jobs

import (
	"fmt"
	"os"

	BQ "bwing.app/src/bigquery"
	CONFIG "bwing.app/src/config"
	LOGGING "bwing.app/src/log"
)

type JobsExecute struct {
	SqlRepo, Dataset, SourceId string
}

///////////////////////////////////////////////////
/* ===========================================
//Contentで指定されたログのETLを行う
=========================================== */
func (j JobsExecute) ExecuteJobs() (interface{}, error) {

	//コマンドライン引数のバリデーション
	err := ValidationReqArgs()
	if err != nil {
		return nil, err
	}

	//コマンドライン引数の値を出力
	args := CONFIG.GetConfigArgsAllString()
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Args", fmt.Sprintf("%+v", args)))

	//SQLファイル読み込み
	path := "ddl/" + j.SqlRepo + "/" + j.Dataset + "/" + j.SourceId + ".sql"
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	//SQLを文字列化
	sql := fmt.Sprintln(string(bytes))
	fmt.Println(sql)

	//SQLを取得しBigQueryへロードするJobを実行
	bq := &BQ.Load2BqDDL{Dataset: j.Dataset, SourceId: j.SourceId, SQL: sql}
	lr, err := bq.RunBqQuery()
	if err != nil {
		return nil, err
	}

	//形骸リターン処理
	return lr, nil
}
