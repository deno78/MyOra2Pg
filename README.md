# MyOra2Pg 概要・使い方

## 概要
MyOra2Pgは、OracleデータベースからPostgreSQLへの移行を支援するツールです。
Groovyスクリプトを利用して、テーブルデータのエクスポート・インポートを行います。
中間ファイルなどを生成せず、インメモリでデータコピーを行うため、高速にデータ移行できるのが特徴です。

## 前提条件

本ツールではテーブル定義の移行／作成は行いません。
別途、移行先DBに同等のテーブルを作成しておいてください。

## 必要なファイル・ライブラリ
- `db.properties`：OracleおよびPostgreSQLの接続情報を記載します。
- `table_list.txt`：移行対象のテーブル名を記載します。
- `lib/`：必要なJDBCドライバ（ojdbc8, postgresql）とGroovyライブラリを格納します。
- `jar_download.bat` : 必要なJarファイル一式をMaven Repoから取得します。

## 使い方
1. `db.properties` を編集し、Oracle/PostgreSQLの接続情報を設定します。
2. `table_list.txt` に移行したいテーブル名を記載します。
3. コマンドプロンプトで `migrate.bat` を実行します。
   ```
   migrate.bat
   ```
4. ログは `logs/` フォルダに出力されます。

## 主なファイル
- `PostgresMigrator.groovy`：移行処理のメインスクリプト
- `UiMain.groovy`：GUIによる操作が可能なメイン画面

## 注意事項
- Java（JRE）8以上がインストールされている必要があります。
- JDBCドライバ（ojdbc8, postgresql）が `lib/` フォルダに配置されていることを確認してください。

## ライセンス
本ツールはMITライセンスで公開されています。
