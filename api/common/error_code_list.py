# MIT License
# 
# Copyright (c) 2025 NTT InfraNet
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from enum import Enum


class ErrorCodeList(Enum):
    """エラーコードを定義"""
    EC00001 = "EC00001"
    EC00002 = "EC00002"
    EC00003 = "EC00003"
    EC00004 = "EC00004"
    EC00005 = "EC00005"
    EC00006 = "EC00006"
    EC00007 = "EC00007"
    EC00008 = "EC00008"
    EC00009 = "EC00009"
    EC00010 = "EC00010"
    EC00011 = "EC00011"
    EC00012 = "EC00012"
    EC00013 = "EC00013"
    EC00014 = "EC00014"
    EC00015 = "EC00015"
    EC00016 = "EC00016"
    EC00017 = "EC00017"
    EC00018 = "EC00018"
    EC00019 = "EC00019"
    EC00020 = "EC00020"
    EC00021 = "EC00021"
    EC00022 = "EC00022"
    EC00023 = "EC00023"
    EC00024 = "EC00024"
    EC00025 = "EC00025"
    ER00001 = "ER00001"
    ER00002 = "ER00002"
    ER00003 = "ER00003"
    ER00004 = "ER00004"
    ER00005 = "ER00005"
    ER00006 = "ER00006"
    ER00007 = "ER00007"
    ER00008 = "ER00008"
    ED00001 = "ED00001"
    ED00002 = "ED00002"
    ED00003 = "ED00003"
    ED00004 = "ED00004"
    ED00005 = "ED00005"
    ED00006 = "ED00006"
    ED00007 = "ED00007"
    ED00008 = "ED00008"
    ED00009 = "ED00009"
    ED00010 = "ED00010"
    ED00011 = "ED00011"
    ED00012 = "ED00012"
    ED00013 = "ED00013"
    ED00014 = "ED00014"
    ED00015 = "ED00015"
    ED00016 = "ED00016"
    ED00017 = "ED00017"
    ED00018 = "ED00018"
    ED00019 = "ED00019"
    ED00020 = "ED00020"
    ED00021 = "ED00021"
    ED00022 = "ED00022"
    ED00023 = "ED00023"
    ED00024 = "ED00024"
    ED00025 = "ED00025"
    ED00026 = "ED00026"
    ED00027 = "ED00027"
    ED00028 = "ED00028"
    ED00029 = "ED00029"
    ED00030 = "ED00030"
    ED00031 = "ED00031"
    ED00032 = "ED00032"
    ED00033 = "ED00033"
    ED00034 = "ED00034"
    ED00035 = "ED00035"
    ED00036 = "ED00036"
    ED00037 = "ED00037"
    ED00038 = "ED00038"
    ED00039 = "ED00039"
    ED00040 = "ED00040"
    ED00041 = "ED00041"
    ED00042 = "ED00042"
    ED00043 = "ED00043"
    ED00044 = "ED00044"
    ED00045 = "ED00045"
    ED00046 = "ED00046"
    ED00047 = "ED00047"
    ED00048 = "ED00048"
    ED00049 = "ED00049"
    ED00050 = "ED00050"
    ED00051 = "ED00051"
    ED00052 = "ED00052"
    ED00053 = "ED00053"
    ED00054 = "ED00054"
    ED00055 = "ED00055"
    ED00056 = "ED00056"
    ED00057 = "ED00057"
    ED00058 = "ED00058"
    ED00059 = "ED00059"
    ED00060 = "ED00060"
    ED00061 = "ED00061"
    ED00062 = "ED00062"
    ED00063 = "ED00063"
    ED00064 = "ED00064"
    ED00065 = "ED00065"
    ED00066 = "ED00066"
    ED00067 = "ED00067"
    ED00068 = "ED00068"
    ED00069 = "ED00069"
    ED00070 = "ED00070"
    ED00071 = "ED00071"
    ED00072 = "ED00072"
    ED00073 = "ED00073"
    ED00074 = "ED00074"
    ED00075 = "ED00075"
    ED00076 = "ED00076"
    ED00077 = "ED00077"
    ED00078 = "ED00078"
    ED00079 = "ED00079"
    ED00080 = "ED00080"
    ED00081 = "ED00081"


class ErrorCodeTable:

    """
    エラーコードを定義
    """
    TBL = {
        ErrorCodeList.EC00001: "データが空です。",
        ErrorCodeList.EC00002: "ジオメトリが空または存在しません。",
        ErrorCodeList.EC00003: "無効なフィールドが存在します。",
        ErrorCodeList.EC00004: "レコード数が一致しません。",
        ErrorCodeList.EC00005: "FIDが一致しません。",
        ErrorCodeList.EC00006: "形式が正しくありません。",
        ErrorCodeList.EC00007: "エンコードが正しくありません。",
        ErrorCodeList.EC00008: "データ型が一致しません。",
        ErrorCodeList.EC00009: "列名称が存在しません。",
        ErrorCodeList.EC00010: "対象となるジオメトリが存在しません。",
        ErrorCodeList.EC00011: "対象となるデータが不足しています。",
        ErrorCodeList.EC00012: "境界値を超えています。",
        ErrorCodeList.EC00013: "重複しています。",
        ErrorCodeList.EC00014: "対象となる属性が存在しません。",
        ErrorCodeList.EC00015: "データがシリアライズされていません。",
        ErrorCodeList.EC00016: "Value列にbyte配列のデータが含まれていません。",
        ErrorCodeList.EC00017: "対象外のジオメトリタイプです。",
        ErrorCodeList.EC00018: "Value列の要素数が異なります。",
        ErrorCodeList.EC00019: "ジオメトリにNaNが含まれています。",
        ErrorCodeList.EC00020: "無効なPoint: 座標値がNaNを含んでいます。",
        ErrorCodeList.EC00021: "無効なLineString: 2点未満です。",
        ErrorCodeList.EC00022: "無効なLineString: NaN座標を含んでいます。",
        ErrorCodeList.EC00023: "無効なPolygon: 4点未満です。",
        ErrorCodeList.EC00024: "無効なPolygon: 閉じていません。",
        ErrorCodeList.EC00025: "無効なPolygon: NaN座標を含んでいます。",
        ErrorCodeList.ER00001: "入力データの拡張子が適切ではありません。",
        ErrorCodeList.ER00002: "画像からFieldSetFileに変換できませんでした。",
        ErrorCodeList.ER00003: "入力したFSF Target SrcがFieldSetFileに設定されていません。",
        ErrorCodeList.ER00004: "FieldSetFileにcolor_spaceが設定されていません。",
        ErrorCodeList.ER00005: "入力データの型がndarray、list、tuple、bytes以外です。",
        ErrorCodeList.ER00006: "入力データが空です。",
        ErrorCodeList.ER00007: "入力データがNoneです。",
        ErrorCodeList.ER00008: "入力データにnp.nanが含まれています。",
        ErrorCodeList.ED00001: "GCPから作られたTINの座標配列が反時計回りになっています。",
        ErrorCodeList.ED00002: "入力データが配列ではありません。",
        ErrorCodeList.ED00003: "入力データがジオメトリではありません。",
        ErrorCodeList.ED00004: "入力データがフィールドではありません。",
        ErrorCodeList.ED00005: "入力データがGeoDataFrameではありません。",
        ErrorCodeList.ED00006: "フィールド値の型が適切ではありません。",
        ErrorCodeList.ED00007: "フィールド値が数字ではありません。",
        ErrorCodeList.ED00008: "座標配列にZ値が存在しません。",
        ErrorCodeList.ED00009: "指定したデータ定義の区切り文字が誤っています。",
        ErrorCodeList.ED00010: "データ定義がCSV形式ではありません。",
        ErrorCodeList.ED00011: "データ定義に必要なカラムが存在しません。",
        ErrorCodeList.ED00012: "データ定義に出力必須としているフィールドがFieldSetFileに存在しません。",
        ErrorCodeList.ED00013: "プロパティで指定したDWH名がFieldSetFileに存在しません。",
        ErrorCodeList.ED00014: "座標配列が、マルチパッチになっていません。",
        ErrorCodeList.ED00015: "入力データが3Dの座標配列ではありません。",
        ErrorCodeList.ED00016: "指定したディレクトリ(フォルダ)が存在しません。",
        ErrorCodeList.ED00017: "無効なジオメトリが含まれています。（自己交差したライン、重複頂点 等）",
        ErrorCodeList.ED00018: "ジオメトリにZ値が含まれていません。",
        ErrorCodeList.ED00019: "ジオメトリにNoneが含まれています。",
        ErrorCodeList.ED00020: "ジオメトリのZ値が既定値(-9999)になっています。",
        ErrorCodeList.ED00021: "プロパティで指定した入力データの形と異なります。",
        ErrorCodeList.ED00022: "GeoDataFrameに存在しないフィールドがデータ定義に記載されています。",
        ErrorCodeList.ED00023: "データ定義の'データ型'列にint, float, str, object以外のものが記載されています。",
        ErrorCodeList.ED00024: "FlowFileのAttributeにデータ定義が設定されていません。",
        ErrorCodeList.ED00025: "プロパティで指定したCRS(EPSGコード)の値が正しくありません。",
        ErrorCodeList.ED00026: "ジオメトリがポイントではありません。",
        ErrorCodeList.ED00027: "元データがポイントではないため、座標配列をポイントに変換できません。",
        ErrorCodeList.ED00028: "元データがラインではないため、座標配列をラインに変換できません。",
        ErrorCodeList.ED00029: "元データがポリゴンではないため、座標配列をポリゴンに変換できません。",
        ErrorCodeList.ED00030: "FieldSetFile内に、ポリゴンの座標配列がありません",
        ErrorCodeList.ED00031: "指定したDWHのデータは、ポリゴンの座標配列ではありません。",
        ErrorCodeList.ED00032: "ポリゴンの座標配列に、無効なジオメトリが含まれています。",
        ErrorCodeList.ED00033: "CSV形式で指定するプロパティが、CSV形式で書かれていません。",
        ErrorCodeList.ED00034: "プロパティで指定するプロパティに必須カラムがありません。",
        ErrorCodeList.ED00035: "プロパティで指定した文字コードが正しくありません。",
        ErrorCodeList.ED00036: "入力データのCSVが、データフレームに変換できません。",
        ErrorCodeList.ED00037: "プロパティで指定したカラムは、入力データのカラムに存在しません。",
        ErrorCodeList.ED00038: "プロパティで指定したパスにパラメータファイルがありません。",
        ErrorCodeList.ED00039: "必要なAttribute[crs]がありません。",
        ErrorCodeList.ED00040: "ファイルタイプが[-2]のときは、'属性値'に既定値を設定してください。",
        ErrorCodeList.ED00041: "右記のプロパティは整数で入力してください。",
        ErrorCodeList.ED00042: "右記のプロパティは数値で入力してください。",
        ErrorCodeList.ED00043: "データ定義のファイルタイプ列に+の値がありません。",
        ErrorCodeList.ED00044: "データ定義の'ファイルタイプ'の値が+1の項目は、ポイント以外を受け付けません。",
        ErrorCodeList.ED00045: "データ定義の'ファイルタイプ'の値が+2の項目は、ライン以外を受け付けません。",
        ErrorCodeList.ED00046: "データ定義の'ファイルタイプ'の値が+3の項目は、ポリゴン以外を受け付けません。",
        ErrorCodeList.ED00047: "マルチパッチの構成点が4つ作られていません。",
        ErrorCodeList.ED00048: "マルチパッチの始点と終点が一致しません。",
        ErrorCodeList.ED00049: "マルチパッチが自己交差しています。",
        ErrorCodeList.ED00050: "プロパティで指定した拡張子のファイルが見つかりません。",
        ErrorCodeList.ED00051: "ディレクトリ内のファイルからDEM情報を取得できません。",
        ErrorCodeList.ED00052: "データ定義の'流通項目名'に[オブジェクトID]が必要です。",
        ErrorCodeList.ED00053: "0以下の値には対応していません。",
        ErrorCodeList.ED00054: "入力データがDataFrameではありません。",
        ErrorCodeList.ED00055: "座標変換に失敗しました。入力された地物の範囲と指定されたパラメータファイル（.par）の範囲が一致しません。",
        ErrorCodeList.ED00056: "プロパティで指定した、図郭の内包判定用座標取得用流通項目名が、データ定義の'流通項目名'に存在しません。",
        ErrorCodeList.ED00057: "座標配列に同一構成点が存在します。",
        ErrorCodeList.ED00058: "右記のプロパティには0以下の数値は入力できません。",
        ErrorCodeList.ED00059: "フィールドにNull（None）が含まれています。",
        ErrorCodeList.ED00060: "TargetGCPとBaseGCPの座標数が等しくありません。",
        ErrorCodeList.ED00061: "FlowFileのAttributeにレイヤ名[layer_name]が設定されていません。",
        ErrorCodeList.ED00062: "CSV形式でのデータ変換に失敗しました。GeoDataFrameの構造に問題がある可能性があります。データの整合性を確認してください。",
        ErrorCodeList.ED00063: "GeoJSON形式でのデータ変換に失敗しました。GeoDataFrameの構造に問題がある可能性があります。データの整合性を確認してください。",
        ErrorCodeList.ED00064: "Geopackage形式でのデータ変換に失敗しました。GeoDataFrameの構造に問題がある可能性があります。データの整合性を確認してください。",
        ErrorCodeList.ED00065: "国土基本図図郭コード内に、ジオメトリが存在しません。",
        ErrorCodeList.ED00066: "中心線となる座標配列の指定が誤っています。",
        ErrorCodeList.ED00067: "GeoDataFrameの一部ジオメトリからジオメトリのタイプを取得できませんでした。GeoDataFrameの構造に問題がある可能性があります。",
        ErrorCodeList.ED00068: "ジオメトリの変換に失敗しました。入力データに不適切な値が含まれている可能性があります。",
        ErrorCodeList.ED00069: "Shapefileの読み込みに失敗しました。入力データに問題がある可能性があります。",
        ErrorCodeList.ED00070: "GeoJSONの読み込みに失敗しました。入力データに問題がある可能性があります。",
        ErrorCodeList.ED00071: "空間IDのCSVに想定されるカラムが付与されていません。",
        ErrorCodeList.ED00072: "入力データに、プロパティで指定した空間IDの区切り文字が存在しません。",
        ErrorCodeList.ED00073: "空間IDの形式が正しくありません。",
        ErrorCodeList.ED00074: "座標配列が2次元配列ではありません。",
        ErrorCodeList.ED00075: "入力データが座標配列ではありません。",
        ErrorCodeList.ED00076: "[FID]というフィールドを指定してください。",
        ErrorCodeList.ED00077: "データ定義には、ファイルタイプに-1もしくは-2を指定したフィールドの記載が必要です。",
        ErrorCodeList.ED00078: "FIDが必須項目ですが、FIDがDwh列に存在しません。",
        ErrorCodeList.ED00079: "CSV形式で指定したプロパティに予期しない列数のデータが含まれています。",
        ErrorCodeList.ED00080: "CSV形式で指定したプロパティに無効な値が含まれています。",
        ErrorCodeList.ED00081: "プロパティで指定した値が正しくありません。プロパティを確認してください。",
    }

    @classmethod
    def get_error_message(cls, code):
        # エラーコードに対応するメッセージを返す
        message = cls.TBL.get(code, "存在しないエラーコードです。")
        return message
