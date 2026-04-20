## 1. データフロー名

- データフロー名称: DigitizeVisionForInferenceClient
- NiFi JSONファイル: DigitizeVisionForInferenceClient.json
- バージョン: 1.0
- 改版日: 2026-03-30
- ライセンス： MIT License

## 2. データフローの概要

- 当データフローは、digitize-vision-sdkの推論プログラム(MCP Server)をサーバーとするクライアント側のデータフローです。
- 図面（画像ファイル）をインプットとして推論プログラムへ推論をリクエストします。推論結果（レスポンス・画像ファイル）を受領後はベクタライズ処理を行い、デジタイズ結果データ（ベクトルデータ）として出力します。
- 

## 3. データフローの入出力仕様
インプットデータ（ParameterContext：Input_Images_Dir、MCP_Url)
以下の２つのデータをInputPathで指定するディレクトリへ格納する。
- Input_Images_Dir：対象図面の画像ファイル。
- MCP_Url：推論プログラム（MCP Server）のリクエスト先URL。

アウトプットデータ（ParameterContext：Output_Images_Dir、Output_Geojson_Dir)
- Output_Images_Dir：推論プログラム実行結果（２値化画像）
- Output_Geojson_Dir：推論プログラム実行結果のベクタライズ後データ（ベクトルデータ）

