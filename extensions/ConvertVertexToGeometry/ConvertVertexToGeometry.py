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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 属性として取得したx,yの座標データをgeometryに変換し
# geometryのリストを返す
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
Point = import_module("shapely.geometry").Point
LineString = import_module("shapely.geometry").LineString
Polygon = import_module("shapely.geometry").Polygon

# 定数を定義
MILLIMETER = "ミリメートル(mm)"
CENTIMETER = "センチメートル(cm)"
METER = "メートル(m)"


class ConvertVertexToGeometry(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        x,yの座標データを持ったDataFrameからジオメトリを作成する。
                        ①input: x,yの座標データを持ったDataFrameとジオメトリ判定用項目を持ったDataFrameの、2行のFieldSetFile。
                        ②output: ジオメトリを持った、1つ以上のFieldSetFile。
                      """
        tags = ['DataFrame', 'Geometry', 'Python']

    # 座標データ(DataFrame)のDWH
    VERTEX_DWH_NAME = PropertyDescriptor(
        name="Vertex DWH Name",
        description="座標の頂点データ(データフレーム)のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # ジオメトリ判定用項目を持ったDataFrameのDWH
    HEADER_DWH_NAME = PropertyDescriptor(
        name="Header DWH Name",
        description="ヘッダ(データフレーム)のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力時のポリゴンジオメトリのDWH
    OUTPUT_POLYGONZ_DWH_NAME = PropertyDescriptor(
        name="Output PolygonZ DWH Name",
        description="出力データのPolygonのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # 出力時のポイントジオメトリのDWH
    OUTPUT_POINTZ_DWH_NAME = PropertyDescriptor(
        name="Output PointZ DWH Name",
        description="出力データのPointのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # 出力時のラインジオメトリのDWH
    OUTPUT_LINESTRINGZ_DWH_NAME = PropertyDescriptor(
        name="Output LineStringZ DWH Name",
        description="出力データのLineStringのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # データフレームのX座標格納カラム名
    X_COLUMN_NAME = PropertyDescriptor(
        name="X Column Name",
        description="データフレームのX座標格納カラム名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # データフレームのY座標格納カラム名
    Y_COLUMN_NAME = PropertyDescriptor(
        name="Y Column Name",
        description="データフレームのY座標格納カラム名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # ジオメトリのタイプを決定する項目名
    GEOMETRY_TYPE_COLUMN_NAME = PropertyDescriptor(
        name="Geometry Type Column Name",
        description="ジオメトリのタイプを決定する項目名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # ジオメトリ判定項目のLineString番号
    DATA_STRUCTURE_LINESTRING = PropertyDescriptor(
        name="Data Structure LineString",
        description="ジオメトリのタイプを決定する項目の中でLineStringを意味する番号",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # ジオメトリ判定項目のPolygon番号
    DATA_STRUCTURE_POLYGON = PropertyDescriptor(
        name="Data Structure Polygon",
        description="ジオメトリのタイプを決定する項目の中でPolygonを意味する番号",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # ジオメトリ判定項目のPoint番号
    DATA_STRUCTURE_POINT = PropertyDescriptor(
        name="Data Structure Point",
        description="ジオメトリのタイプを決定する項目の中でPointを意味する番号",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # 座標データの単位(ミリorメートル)の指定
    SPECIFY_UNIT = PropertyDescriptor(
        name="Specify Unit",
        description="座標データの単位(ミリ、センチ、メートル)",
        allowable_values=[MILLIMETER, CENTIMETER, METER],
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [VERTEX_DWH_NAME,
                            HEADER_DWH_NAME,
                            OUTPUT_POLYGONZ_DWH_NAME,
                            OUTPUT_POINTZ_DWH_NAME,
                            OUTPUT_LINESTRINGZ_DWH_NAME,
                            X_COLUMN_NAME,
                            Y_COLUMN_NAME,
                            GEOMETRY_TYPE_COLUMN_NAME,
                            DATA_STRUCTURE_LINESTRING,
                            DATA_STRUCTURE_POLYGON,
                            DATA_STRUCTURE_POINT,
                            SPECIFY_UNIT,
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            center_dwh_name: 座標データ(DataFrame)のDWH
            output_center_dwh_name: 出力時の座標データ(DataFrame)のDWH
            new_findex_dwh_name: 出力時 results型データのDWH
            x_column_name: 座標データ(DataFrame)のX座標格納カラム名
            y_column_name: 座標データ(DataFrame)のY座標格納カラム名
            break_flag_dwh_name: geometryの区分を判別するためのデータのDWH
            select_geometry_to_create: どのgeometryを作成するか
            specify_unit: 座標データの単位(ミリorメートル)の指定
        """

        # プロパティで設定した値を取得
        vertex_dwh_name = context.getProperty(
            self.VERTEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        header_dwh_name = context.getProperty(
            self.HEADER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_polygonz_dwh_name = context.getProperty(
            self.OUTPUT_POLYGONZ_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_pointz_dwh_name = context.getProperty(
            self.OUTPUT_POINTZ_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_linestringz_dwh_name = context.getProperty(
            self.OUTPUT_LINESTRINGZ_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        x_column_name = context.getProperty(
            self.X_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        y_column_name = context.getProperty(
            self.Y_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        geometry_type_column_name = context.getProperty(
            self.GEOMETRY_TYPE_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        data_structure_linestring = context.getProperty(
            self.DATA_STRUCTURE_LINESTRING).evaluateAttributeExpressions(flowfile).getValue()

        data_structure_polygon = context.getProperty(
            self.DATA_STRUCTURE_POLYGON).evaluateAttributeExpressions(flowfile).getValue()

        data_structure_point = context.getProperty(
            self.DATA_STRUCTURE_POINT).evaluateAttributeExpressions(flowfile).getValue()

        specify_unit = context.getProperty(
            self.SPECIFY_UNIT).evaluateAttributeExpressions(flowfile).getValue()

        return vertex_dwh_name, \
            header_dwh_name, \
            output_polygonz_dwh_name, \
            output_pointz_dwh_name, \
            output_linestringz_dwh_name, \
            x_column_name, \
            y_column_name, \
            geometry_type_column_name, \
            data_structure_linestring, \
            data_structure_polygon, \
            data_structure_point, \
            specify_unit

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで設定した値を取得
            vertex_dwh_name, \
                header_dwh_name, \
                output_polygonz_dwh_name, \
                output_pointz_dwh_name, \
                output_linestringz_dwh_name, \
                x_column_name, \
                y_column_name, \
                geometry_type_column_name, \
                data_structure_linestring, \
                data_structure_polygon, \
                data_structure_point, \
                specify_unit\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file \
                = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            field_set_file_data_frame \
                = pd.read_csv(io.StringIO(input_field_set_file))

            try:
                # field_set_file_data_frame から抽出 バイトデータからvertex_dataframeを復元する
                vertex_dataframe \
                    = pickle.loads(base64.b64decode(field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == vertex_dwh_name, "Value"].values[0]))

                # field_set_file_data_frame から抽出 バイトデータからheader_dataframeを復元する
                header_dataframe \
                    = pickle.loads(base64.b64decode(field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == header_dwh_name, "Value"].values[0]))

            except Exception:
                self.logger.error(traceback.format_exc())

            # Findex列だけをコピー
            vertex_dataframe_copy = vertex_dataframe[['Findex']].copy()

            try:
                # x列 y列から(x, y)を取り、Zを0追加し、(x, y, z)のタプルを作成
                vertex_dataframe_copy["xyz"] = vertex_dataframe.apply(
                    lambda row: (row[x_column_name], row[y_column_name], 0), axis=1)

            except Exception:
                # エラーをログに出力する
                self.logger.error(
                    f'X,Yのカラムが違います。確認してください。: {x_column_name} {y_column_name}')

            # SPECIFY_UNITが'millimeter'の場合、xyのタプル内の値をメートルに変換
            if specify_unit == MILLIMETER:

                vertex_dataframe_copy["xyz"] = vertex_dataframe_copy["xyz"].apply(
                    lambda xyz: (xyz[0] / 1000, xyz[1] / 1000, xyz[2]))

            elif specify_unit == CENTIMETER:
                vertex_dataframe_copy['xyz'] = vertex_dataframe_copy['xyz'].apply(
                    lambda xyz: (xyz[0] / 100, xyz[1] / 100, xyz[2]))

            # Findexでグループ化し、xyzをリストにまとめる　(1, 1) (2, 2) → [(1, 1), (2, 2), (3, 3), (4, 4)]
            geometry_dataframe = vertex_dataframe_copy.groupby(
                'Findex', as_index=False)["xyz"].agg(list)

            # ジオメトリのDataFrameに、ヘッダのDataFrameを結合
            merge_dataframe = geometry_dataframe.merge(
                header_dataframe, on='Findex')

            # LineStringZとPolygonZ,PointZのリストを作成
            line_strings = []
            polygons = []
            points = []

            # データ型を１行目から型を取得。
            first_row_type = type(
                merge_dataframe[geometry_type_column_name].iloc[0])

            # DataFrameを行ずつループ
            for _, row in merge_dataframe.iterrows():

                # linestringのデータ構造値がある場合。
                if data_structure_linestring:

                    # データ構造列の値が指定されたlinestring値だった場合。「xyz」列の値をlistに追加
                    if row[geometry_type_column_name] == first_row_type(data_structure_linestring):
                        line_strings.append(LineString(row['xyz']))

                if data_structure_point:
                    # データ構造列の値が指定されたpoint値だった場合。「xyz」列の値をlistに追加
                    if row[geometry_type_column_name] == first_row_type(data_structure_point):
                        points.append(Point(row['xyz']))

                if data_structure_polygon:
                    # データ構造列の値が指定されたpolygon値だった場合。「xyz」列の値をlistに追加
                    if row[geometry_type_column_name] == first_row_type(data_structure_polygon):
                        polygons.append(Polygon(row['xyz']))

            # output_field_set_file作成用のlist
            dwh_list = []
            type_list = []
            value_list = []

            # ジオメトリのlistが作成されたものだけ、output用のlistに格納
            if line_strings:
                dwh_list.append(output_linestringz_dwh_name)
                type_list.append("geometry")
                value_list.append(line_strings)

            if points:
                dwh_list.append(output_pointz_dwh_name)
                type_list.append("geometry")
                value_list.append(points)

            if polygons:
                dwh_list.append(output_polygonz_dwh_name)
                type_list.append("geometry")
                value_list.append(polygons)

            # set_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                                           type_list,
                                                                                           value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
