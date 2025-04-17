# データ整備ツール

## 概要
本リポジトリでは、Apache NiFiのカスタムプロセッサを実装し、地下インフラ情報のデータ整備を支援するための機能を提供します。本ツールは、インフラ管理DXシステムと連携し、地下埋設物の情報を効率的に処理・統合することを目的としています。

| 機能名                | 機能概要                                                 |
| --------------------- | -------------------------------------------------------- |
| 図面から設備抽出      | 図面データを解析し、地下埋設物情報をシェープファイルに変換します。 |
| CADから設備抽出       | CADデータを解析し、地下埋設物のGISデータを生成します。       |
| 位置補正ツール        | GISデータに基づき、高精度の位置補正を適用します。          |
| 3D都市モデル変換      | GISデータを基に、地下埋設物の3D都市モデル（PLATEAU準拠）を生成します。 |
| 空間ID変換           | 3D都市モデルを空間IDフォーマットへ変換します。             |
| 画像処理機能         | 図面や画像データを解析し、地下埋設物の情報を抽出します。     |

## リポジトリ利用方法

本リポジトリのカスタムプロセッサを利用するには、NiFi環境にデプロイする必要があります。以下の手順に従ってください。

## 1. Apache NiFiのインストール
下記を参照してください  
docs/02.Setup    

## 2. リポジトリのコード配置
1. 本リポジトリのコードを任意のディレクトリに配置します。
   ```sh
   git clone <リポジトリURL> 任意のディレクトリ
   ```
2. Nifiのディレクトリへのコピー  
任意の場所に取得した下記ディレクトリをNifiのpythonディレクトリにコピーします。  
- api
- extensions

## ディレクトリ構造

本プロジェクトのディレクトリ構造は以下の通りです。

```
├  api         # データ整備ツールの機能群（プロセッサ群）が使用するモジュールを格納しています。
├  extensions  # データ整備ツールの機能群（カスタムプロセッサ）が格納されています。
└  docs
    ├ 01.setup
    │  └─ データ整備ツールのセットアップガイド
    ├ 02.manual
    │  └─ データ整備ツールの利用者向けマニュアル
    ├ 03.reference
    │  ├─ NiFi式言語一覧
    │  ├─ データ加工処理逆引き一覧
    │  ├─ データ定義の解説
    │  ├─ リファレンス
    │  └─ 用語集
    └ 04.exercises
        ├─ プロセス設計演習１～CityGML編～
        ├─ プロセス設計演習２～空間ID編～
```

## 問い合わせに関して
本リポジトリは配布を目的としており、IssueやPull Requestを受け付けておりません。

## ライセンス
本リポジトリはMITライセンスで提供されています。
ソースコードおよび関連ドキュメントの著作権はエヌ・ティ・ティ・インフラネット株式会社に帰属します。

### 依存ライブラリとライセンス

本ツールでは以下の外部ライブラリを使用しています。
各ライブラリのライセンス詳細は、それぞれの公式リポジトリを参照してください。

| ライブラリ名             | ライセンス              | リポジトリ                                             |
|-------------------------|------------------------|--------------------------------------------------------|
| EasyOCR                 | Apache License 2.0     | [GitHub](https://github.com/JaidedAI/EasyOCR)         |
| ezdxf                   | MIT License            | [GitHub](https://github.com/mozman/ezdxf)             |
| Fiona                   | BSD 3-Clause License   | [GitHub](https://github.com/Toblerity/Fiona)          |
| GDAL                    | MIT License            | [GitHub](https://github.com/OSGeo/gdal)               |
| GeoPandas               | BSD 3-Clause License   | [GitHub](https://github.com/geopandas/geopandas)      |
| lxml                    | BSD 3-Clause License   | [GitHub](https://github.com/lxml/lxml)                |
| mojimoji                | Apache License 2.0     | [GitHub](https://github.com/studio-ousia/mojimoji)    |
| Numba                   | BSD 2-Clause License   | [GitHub](https://github.com/numba/numba)              |
| NumPy                   | BSD 3-Clause License   | [GitHub](https://github.com/numpy/numpy)              |
| opencv-python-headless  | MIT License            | [GitHub](https://github.com/opencv/opencv-python)     |
| pandas                  | BSD 3-Clause License   | [GitHub](https://github.com/pandas-dev/pandas)        |
| Pillow                  | MIT-CMU License        | [GitHub](https://github.com/python-pillow/Pillow)     |
| pygltflib               | MIT License            | [GitHub](https://github.com/avaturn/pygltflib)        |
| pyproj                  | MIT License            | [GitHub](https://github.com/pyproj4/pyproj)           |
| Python Tesseract        | Apache License 2.0     | [GitHub](https://github.com/madmaze/pytesseract)      |
| Rasterio                | BSD 3-Clause License   | [GitHub](https://github.com/mapbox/rasterio)          |
| Rtree                   | MIT License            | [GitHub](https://github.com/Toblerity/rtree)          |
| scikit-learn            | BSD 3-Clause License   | [GitHub](https://github.com/scikit-learn/scikit-learn)|
| SciPy                   | BSD 3-Clause License   | [GitHub](https://github.com/scipy/scipy)              |
| Shapely                 | BSD 3-Clause License   | [GitHub](https://github.com/Toblerity/Shapely)        |
| TKY2JGD                 | BSD 3-Clause License   | [GitHub](https://github.com/mugwort-rc/TKY2JGD)       |
| PyTorch                 | BSD 3-Clause License   | [GitHub](https://github.com/pytorch/pytorch)          |
| tripy                   | MIT License            | [GitHub](https://github.com/linuxlewis/tripy)         |

## 免責事項
本リポジトリの内容は予告なく変更・削除する可能性があります。
本リポジトリの利用により生じた損失及び損害等について、いかなる責任も負わないものとします。
