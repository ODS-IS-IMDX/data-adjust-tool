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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 指定された深さ情報とジオメトリデータに基づき、ポイントから垂直ラインを生成する処理を行う。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import io
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
LineString = import_module("shapely.geometry").LineString

MILLIMETER = "ミリメートル(mm)"
METER = "メートル(m)"


class ConvertPointToLineFromDepth(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        Pointジオメトリを深さのデータを用いて、LineStringジオメトリに変換する。
                        ①input: Pointのジオメトリが格納されたリストと深さを持った2行のFieldSetFile。
                        ②output: LineStringジオメトリが格納された、1行のFieldSetFile。
                      '''
        tags = ['Geometry', 'Python']

    # 深さのDWH
    DEPTH_DWH_NAME = PropertyDescriptor(
        name="Depth DWH Name",
        description="入力データの深さのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # geometryのDWH
    GEOMETRY_DWH_NAME = PropertyDescriptor(
        name="Geometry DWH Name",
        description="入力データのジオメトリのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 深さデータの単位(ミリorメートル)の指定
    SPECIFY_UNIT = PropertyDescriptor(
        name="Specify Unit",
        description="深さデータの単位(ミリorメートル)",
        allowable_values=[MILLIMETER, METER],
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [DEPTH_DWH_NAME,
                            GEOMETRY_DWH_NAME,
                            SPECIFY_UNIT]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要
            プロパティで入力した値を取得する関数
        引数
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値
            depth_dwh_name: 深さのDwh名
            geometry_dwh_name: geometryのDwh名
        """

        # 各プロパティで設定した値を取得
        depth_dwh_name = context.getProperty(
            self.DEPTH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        geometry_dwh_name = context.getProperty(
            self.GEOMETRY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        specify_unit = context.getProperty(
            self.SPECIFY_UNIT).evaluateAttributeExpressions(flowfile).getValue()

        return depth_dwh_name, geometry_dwh_name, specify_unit

    def get_flowfile_and_transform_each_list(self, flowfile, depth_dwh_name, geometry_dwh_name):
        """
        概要
            flowfileからデータを取得し、指定した DWH 名に基づき、デシリアライズしたデータをリストを取得する関数

        引数
            flowfile: プロセッサに入ってくるデータ
            depth_dwh_name: 深さの DWH 名。これに一致するデータを抽出してデシリアライズを行う
            geometry_dwh_name: ジオメトリの DWH 名。これに一致するデータを抽出し、必要なリストを取得する

        戻り値
            depth_dwh_value: depth_dwh_name に一致する DWH の Value 列をデシリアライズした結果
            geometry_dwh_value: geometry_dwh_name に一致する DWH の Value 列のリスト
            geometry_dwh_list: geometry_dwh_name に一致する DWH の Dwh 列のリスト
            geometry_type_list: geometry_dwh_name に一致する DWH の Type 列のリスト
        """

        # flowfileからCSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # depth_dwh_name geometry_dwh_name に一致するDwhのValue列を取得し、pickleからデシリアライズ
        depth_dwh_value = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == depth_dwh_name, "Value"].values[0]))
        geometry_dwh_value = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_name, "Value"].values[0]))

        # geometry_dwh_name に一致する Dwh と Type のリストを取得
        geometry_dwh_list = field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_name, "Dwh"].tolist(
        )
        geometry_type_list = field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_name, "Type"].tolist(
        )

        return depth_dwh_value, geometry_dwh_value, geometry_dwh_list, geometry_type_list

    def change_to_vertical_line_from_point(self, geometry_dwh_value, depth_dwh_value, specify_unit):
        """
        概要
            ジオメトリデータと深さデータを基に、各ポイントから深さに応じた垂直ラインを作成する関数

        引数
            geometry_dwh_value: ジオメトリデータのリスト(GeoSeries)
            depth_dwh_value: 深さデータのリスト(タプル)

        戻り値
            vertical_line_geometry_list: 生成された垂直ラインのリスト
        """

        vertical_line_geometry_list = []
        # ジオメトリのデータを numpy 配列として取得
        geometry_points_numpy = NSP.get_geometries_points_numpy(
            geometry_dwh_value)

        # ジオメトリデータの長さを取得し、その範囲でループ処理を行う
        target_range = range(len(geometry_points_numpy))

        # ジオメトリデータの各ポイントに対して垂直ラインを作成し、リストに追加
        if specify_unit == MILLIMETER:

            vertical_line_geometry_list = [
                LineString([
                    # ラインの始点: 現在のジオメトリポイントの x, y 座標と z 座標
                    (geometry_points_numpy[i][1], geometry_points_numpy[i]
                     [2], geometry_points_numpy[i][3]),
                    # ラインの終点: 同じ x, y 座標で、深さの値を引いた z 座標
                    (geometry_points_numpy[i][1], geometry_points_numpy[i][2],
                     geometry_points_numpy[i][3] - (depth_dwh_value[i][1] / 1000))
                ]) for i in target_range
            ]

        # 単位がメートルの場合s
        elif specify_unit == METER:

            vertical_line_geometry_list = [
                LineString([
                    # ラインの始点: 現在のジオメトリポイントの x, y 座標と z 座標
                    (geometry_points_numpy[i][1], geometry_points_numpy[i]
                     [2], geometry_points_numpy[i][3]),
                    # ラインの終点: 同じ x, y 座標で、深さの値を引いた z 座標
                    (geometry_points_numpy[i][1], geometry_points_numpy[i][2],
                     geometry_points_numpy[i][3] - (depth_dwh_value[i][1]))
                ]) for i in target_range
            ]

        return vertical_line_geometry_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:
            # 各プロパティで設定した値を取得
            depth_dwh_name, \
                geometry_dwh_name, \
                specify_unit\
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            depth_dwh_value, \
                geometry_dwh_value, \
                geometry_dwh_list, \
                geometry_type_list\
                = WM.calc_func_time(self.logger)(self.get_flowfile_and_transform_each_list)(flowfile,
                                                                                            depth_dwh_name,
                                                                                            geometry_dwh_name)

            # 深さに基づく垂直ラインを作成
            changed_vertical_line_geometry_list\
                = WM.calc_func_time(self.logger)(self.change_to_vertical_line_from_point)(geometry_dwh_value,
                                                                                          depth_dwh_value,
                                                                                          specify_unit)

            # set_field_set_file 関数で デコードデシリアライズする際、GeoSeriesをデコードデシリアライズするため二重 list に
            output_geometry_value_list = [changed_vertical_line_geometry_list]

            # FieldSetFileを作成する
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(geometry_dwh_list,
                                                                         geometry_type_list,
                                                                         output_geometry_value_list)

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship='success', contents=output_field_set_file)

            # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship='failure')
