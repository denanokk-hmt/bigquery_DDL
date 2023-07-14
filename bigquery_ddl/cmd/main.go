/* =================================
サーバー起動時の起点ファイル
* ================================= */
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	CONFIG "bwing.app/src/config"
	ERR "bwing.app/src/error"
	JOBS "bwing.app/src/jobs"
	LOGGING "bwing.app/src/log"
	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* ===========================================
	args:Jobsの場合に利用
	key:value形式で記述
	1st(dataset), 2nd(contents)は必須
	2nd(contents)のvalueは、sourceカンマつなぎ
	コマンドライン引数の設定内容
* =========================================== */
func main() {

	/*------------------------------------------------
	共通前準備
	------------------------------------------------*/
	cdt := time.Now() //処理開始時間

	//実行されているタスクのIndexを取得
	taskNum := os.Getenv("CLOUD_RUN_TASK_INDEX")
	//attemptNum := os.Getenv("CLOUD_RUN_TASK_ATTEMPT")

	//実行するJobのIndexを決定
	index, _ := strconv.Atoi(taskNum)
	force_index := CONFIG.GetConfigArgs(CONFIG.ARGS_FORCE_INDEX)
	if force_index != "" {
		index, _ = strconv.Atoi(force_index) //強制的content実行
	}
	fmt.Println(index)

	//実行するsqlreo, dataset, source_idを取得
	sqlRepo := CONFIG.GetConfigArgs(CONFIG.ARGS_SQL_REPO)
	dataset := CONFIG.GetConfigArgs(CONFIG.ARGS_DATASET)
	sourceId := CONFIG.GetConfigArgs(CONFIG.ARGS_SOURCE_ID)

	var sourceIds []string

	if sourceId != "*" {
		sourceIds = append(sourceIds, sourceId)
	} else {
		//dataset配下のすべてのSQLファイルを取得
		dir := "ddl/" + sqlRepo + "/" + dataset + "/"
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			ERR.ErrorLoggingWithStackTrace(errors.WithStack(fmt.Errorf("[dir:%s][Error:%w]", dir, err)))
			log.Fatal(err)
			return
		}

		//先頭がアンダースコアではなく、末尾に「.sql」を持つファイルに絞り込む
		for _, f := range files {
			str1 := f.Name()[0:1]
			if str1 != "_" {
				re := regexp.MustCompile(".sql$")
				n := re.ReplaceAllString(f.Name(), "")
				sourceIds = append(sourceIds, n)
			}
		}
	}

	/*------------------------------------------------
	DDL Update
	------------------------------------------------*/
	for _, s := range sourceIds {

		//DDL情報をセット
		j := &JOBS.JobsExecute{
			SqlRepo:  sqlRepo,
			Dataset:  dataset,
			SourceId: s,
		}

		//Jobを実行してSQLを実行
		lr, err := j.ExecuteJobs()
		if err != nil {
			ERR.ErrorLoggingWithStackTrace(errors.WithStack(fmt.Errorf("[dataset:%s][source_id:%s][Error:%w]", dataset, s, err)))
			//log.Fatal(err)
			//return
			continue
		}
		r := lr.(*bigquery.Job)

		//Jobの結果を取りまとめる
		exeSec := time.Since(cdt).Seconds() //処理時間計測
		responseOutput := fmt.Sprintf("【LOADED】[dataset:%s][source_id:%s][ExeSec:%f][JobResult:%v", dataset, s, exeSec, r)

		//Jobの結果をロギング
		fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "【SUCCESS】JobReport", fmt.Sprintf("%+v", responseOutput)))
	}
}
