/* =================================
サーバーのConfigを設定する
* ================================= */
package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	LOGGING "bwing.app/src/log"
)

var (
	SYSTEM_COMPONENT_VERSION = "1.0.0_bd"

	ENV_PORT            = "Port"
	ENV_GCP_PROJECT_ID  = "GcpProjectId"
	ENV_SERVER_CODE     = "ServerCode"
	ENV_APPLI_NAME      = "AppliName"
	ENV_ENV             = "Env"
	ENV_SERVICE_OR_JOBS = "ServiceOrJobs"

	//これより以下は、Jobsのコマンドライン引数に利用
	//コマンドラインの引数指定方法は、key:value 形式で行う
	ARGS_SQL_REPO    = "sql_repo"    //SQLの格納リポジトリ名
	ARGS_DATASET     = "dataset"     //BQのデータセット名
	ARGS_SOURCE_ID   = "source_id"   //テーブル、VIEW、Procedureなどの対象DDLのID
	ARGS_FORCE_INDEX = "force_index" //強制的に実行する位置を指定
)

var configMapEnv map[string]string  //環境変数の箱
var configMapArgs map[string]string //CMDライン引数の箱

///////////////////////////////////////////////////
//起動時にGCP ProjectID、NS, Kindを登録する
func init() {

	//Set environ values
	NewConfigEnv()

	//Set CMD Args values
	NewConfigArgs()

	//output message finish config settings.
	initString := fmt.Sprintf("[Project:%s][ServerCode:%s][Appli:%s][Env:%s]",
		configMapEnv[ENV_GCP_PROJECT_ID],
		configMapEnv[ENV_SERVER_CODE],
		configMapEnv[ENV_APPLI_NAME],
		configMapEnv[ENV_ENV])
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Flight-LogBook INIT", initString))

	//Verion print
	initString = fmt.Sprintf("Flight-LogBook component version :%s", SYSTEM_COMPONENT_VERSION)
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Flight-LogBook STARTING", initString))

}

///////////////////////////////////////////////////
/* =================================
	環境変数の格納
		$PORT
		$GCP_PROJECT_ID
		$SERVER_CODE
		$APPLI_NAME
		$ENV
		$SERVICE_OR_JOBS
* ================================= */
func NewConfigEnv() {

	//環境変数をMapping
	configMapEnv = make(map[string]string)
	configMapEnv[ENV_PORT] = os.Getenv("PORT")
	configMapEnv[ENV_GCP_PROJECT_ID] = os.Getenv("GCP_PROJECT_ID")
	configMapEnv[ENV_SERVER_CODE] = os.Getenv("SERVER_CODE")
	configMapEnv[ENV_APPLI_NAME] = os.Getenv("APPLI_NAME")
	configMapEnv[ENV_ENV] = os.Getenv("ENV")
}

///////////////////////////////////////////////////
/* =================================
	環境変数の返却
* ================================= */
func GetConfigEnv(name string) string {
	return configMapEnv[name]
}
func GetConfigEnvAll() map[string]string {
	return configMapEnv
}

///////////////////////////////////////////////////
/* =================================
	コマンドライン引数の格納
		force_index:強制的実行
			"0"or "1" or ...
		dataset:
		source_id:
* ================================= */
func NewConfigArgs() {

	//起動時の引数から取得
	flag.Parse()
	args := flag.Args()

	//コマンドライン引数をMapping
	err := SetConfigMapArgs(args)
	if err != nil {
		log.Fatal(err)
	}
}

///////////////////////////////////////////////////
/* =================================
コマンドライン引数を"key:value"に分割しMapping、格納する
* ================================= */
func SetConfigMapArgs(args []string) error {

	if len(configMapArgs) == 0 {
		configMapArgs = make(map[string]string)
	}

	for _, a := range args {
		if a == "" {
			continue
		}

		//コロンでスプリットをしkey-valueに変換
		key := strings.Split(a, ":")[0]
		value := strings.Split(a, ":")[1]

		//Mapping
		switch key {
		case ARGS_SQL_REPO:
			configMapArgs[ARGS_SQL_REPO] = value
		case ARGS_DATASET:
			configMapArgs[ARGS_DATASET] = value
		case ARGS_SOURCE_ID:
			configMapArgs[ARGS_SOURCE_ID] = value
		case ARGS_FORCE_INDEX:
			configMapArgs[ARGS_FORCE_INDEX] = value
		default:
			return fmt.Errorf("【Error】[func:%s][Args:%s]", "src/config/config.go/setConfigMapArgs()", "コマンドライン引数のキーが一致しません。")
		}
	}
	return nil
}

///////////////////////////////////////////////////
/* =================================
	コマンドライン引数の返却
* ================================= */
func GetConfigArgs(name string) string {
	return configMapArgs[name]
}
func GetConfigArgsAll() map[string]string {
	return configMapArgs
}
func GetConfigArgsAllString() string {
	var s string
	for k, v := range configMapArgs {
		s += k + ":" + v + ", "
	}
	return s
}
func GetConfigArgsAllKeyValue() (key []string, val []string) {
	var retK []string
	var retV []string
	for k, v := range configMapArgs {
		retK = append(retK, k)
		retV = append(retV, v)
	}
	return retK, retV
}
