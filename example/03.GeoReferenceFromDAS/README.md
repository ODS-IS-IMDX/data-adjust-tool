## 1. データフロー名

- データフロー名称: GeoReferenceFromDAS
- NiFi JSONファイル: GeoReferenceFromDAS.json
- バージョン: 1.0
- 改版日: 2026-03-30
- ライセンス： MIT License

## 2. データフローの概要

- 図面デジタイズ支援システムのデジタイズ結果出力ファイル仕様（ジオメトリとGCP）を用いて、
- 絶対座標を付与したGISデータ（ジオメトリ）を作成する。

## 3. データフローの入出力仕様
インプットデータ（ParameterContext：input_dir)
以下の２つのデータをinput_dirで指定するディレクトリへ格納する。
- 対象図面のGCP座標を持つGeoJSON。当データフローのファイル命名規則は『【画像ファイル名】.gcp.json』。
- 対象図面のデジタイズ結果(線データ)を持つGeoJSON。当データフローのファイルの命名規則は『【画像ファイル名】.geom.json』。

アウトプットデータ（ParameterContext：output_dir)
- 絶対座標を付与した対象図面のデジタイズ結果(線データ)のGISデータ
- (ジオメトリのみ・シェープファイル)

