/*======================
Cloud-Run Jobsからの
設定値を取得
設定値についての処理
========================*/
package jobs

import (
	"fmt"

	CONFIG "bwing.app/src/config"
)

///////////////////////////////////////////////////
/* ===========================================
contentに応じたValidation
	dataset::値の有無
	source_id::検査しない→switchのdefaultでエラー
	force_index::検査なし
=========================================== */
func ValidationReqArgs() error {

	//引数コマンドラインの値を取得
	configs := CONFIG.GetConfigArgsAll()

	//Validation
	for key, val := range configs {
		switch key {
		case
			CONFIG.ARGS_SQL_REPO,
			CONFIG.ARGS_DATASET,
			CONFIG.ARGS_SOURCE_ID:
			//値の有無を確認
			if val == "" {
				return fmt.Errorf("【Valid Error】[Args:%s][msg:%s]", key, "値が設定されていません。")
			}
		}
	}

	return nil
}
