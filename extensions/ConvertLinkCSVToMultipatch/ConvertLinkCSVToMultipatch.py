# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
# LinkDataのCSVから空間IDとオブジェクトIDを取得し、
# ボクセルのマルチパッチとして出力する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import traceback
import zipfile
import base64
import pickle

from importlib import import_module

f_get_point_on_spatial_id = import_module("SpatialId.shape.point").f_get_point_on_spatial_id
const = import_module("SpatialId.common.object").enum

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")

# Constants
NEW_FINDEX = 'newFindex'
FINDEX = 'Findex'
ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"

class ConvertLinkCSVToMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """LinkDataのCSVからボクセルのマルチパッチに変換する。"""
        tags = ["SpatialID", "ObjectID", "Multipatch", "Python"]

    # 出力データのマルチパッチのCRSコード
    OUTPUT_CRS = PropertyDescriptor(
        name="Output CRS",
        description="出力データのマルチパッチのCRSコード",
        default_value="${crs}",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #空間IDのカラム名
    INPUT_SPATIAL_ID_COLUMN_NAME = PropertyDescriptor(
        name="Input Spatial ID Column Name",
        description="空間IDのカラム名",
        default_value="空間ID",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # オブジェクトIDのカラム名
    INPUT_OBJECT_ID_COLUMN_NAME = PropertyDescriptor(
        name="Input Object ID Column Name",
        description="オブジェクトIDのカラム名",
        default_value="オブジェクトID",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力データのマルチパッチのDWH名
    OUTPUT_MULTIPATCH_DWH_NAME = PropertyDescriptor(
        name="Output Multipatch DWH Name",
        description="出力データのマルチパッチのDWH名",
        default_value="${filename:append('/coordinates_マルチパッチ')}",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # 出力データのオブジェクトIDのDWH名
    OUTPUT_OBJECT_ID_DWH_NAME = PropertyDescriptor(
        name="Output Object ID DWH Name",
        description="出力データのオブジェクトIDのDWH名",
        default_value="${filename:append('/オブジェクトID')}",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ZIP圧縮するかどうかのフラグ（圧縮するまたは圧縮しない）デフォルトは"圧縮しない"
    OUTPUT_ZIP_FLAG = PropertyDescriptor(
        name="Output ZIP Flag",
        description="出力結果をZIP圧縮するかどうかのフラグ",
        default_value=ZIP_COMPRESSION_DISABLED,
        allowable_values=[ZIP_COMPRESSION_ENABLED, ZIP_COMPRESSION_DISABLED],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [OUTPUT_CRS,
                            INPUT_SPATIAL_ID_COLUMN_NAME,
                            INPUT_OBJECT_ID_COLUMN_NAME,
                            OUTPUT_MULTIPATCH_DWH_NAME,
                            OUTPUT_OBJECT_ID_DWH_NAME,
                            OUTPUT_ZIP_FLAG
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
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            output_crs: 出力データのcrs
            input_spatial_id_column_name: LinkDataCSVの空間IDカラム名
            input_object_id_column_name: LinkDataCSVのオブジェクトIDカラム名
            output_multipatch_dwh_name: 出力マルチパッチのDWH名
            output_object_id_dwh_name: 出力オブジェクトIDのDWH名
        """
        # 空間IDのCRS
        output_crs = context.getProperty(self.OUTPUT_CRS).evaluateAttributeExpressions(flowfile).getValue()

        # 空間IDのCRS
        input_spatial_id_column_name = context.getProperty(self.INPUT_SPATIAL_ID_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力時のgeometryのdwh名
        input_object_id_column_name = context.getProperty(self.INPUT_OBJECT_ID_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 空間IDのCRS
        output_multipatch_dwh_name = context.getProperty(self.OUTPUT_MULTIPATCH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力時のgeometryのdwh名
        output_object_id_dwh_name = context.getProperty(self.OUTPUT_OBJECT_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # ZIP圧縮するかどうかのフラグ
        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return output_crs, input_spatial_id_column_name, input_object_id_column_name, output_multipatch_dwh_name, output_object_id_dwh_name, output_zip_flag
    
    def create_vertex_list(self, spatial_id_in_key, crs_code):
        """
        概要:
            空間IDの頂点の座標を取得する関数。

        引数:
            spatial_id_in_key: 1つの空間ID
            crs_code: プロパティで入力したCRS

        戻り値:
            vertex_list: 頂点(8点)の座標が格納されたリスト。
        """

        # 空間ID1つ分のボクセルの頂点8個を取得。緯度経度まで取れてしまう。
        vertex_8_points\
            = WM.calc_func_time(self.logger, False)\
                        (f_get_point_on_spatial_id)(spatial_id=spatial_id_in_key,
                                                    option=const.Point_Option.VERTEX, 
                                                    crs=crs_code)

        # 緯度経度を除くためのリストを作成
        vertex_list = []

        # ボクセルの頂点の座標をリストに格納
        vertex_list = [(vertex_8_points[i].x, vertex_8_points[i].y, vertex_8_points[i].alt)
                            for i in range(len(vertex_8_points))]
        
        return vertex_list

    def create_triangle_coords_list(self, vertex_list):
        """
        概要:
            頂点の座標から各面に対して2つずつ、
            計12個のマルチパッチ(三角形)を作成する関数。

        引数:
            vertex_list: 頂点(8点)の座標が格納されたリスト。

        戻り値:
            triangle_coords_list: 12個の三角形の3点の座標が格納されたリスト。
        """

        # ボクセルをマルチパッチにするための三角形格納用リスト
        triangle_coords_list = []

        # 各面に作成する三角形の3点
        triangle_coords_list.append([vertex_list[7], vertex_list[6], vertex_list[4]])
        triangle_coords_list.append([vertex_list[5], vertex_list[4], vertex_list[6]])

        triangle_coords_list.append([vertex_list[3], vertex_list[2], vertex_list[7]])
        triangle_coords_list.append([vertex_list[6], vertex_list[7], vertex_list[2]])

        triangle_coords_list.append([vertex_list[2], vertex_list[1], vertex_list[6]])
        triangle_coords_list.append([vertex_list[5], vertex_list[6], vertex_list[1]])

        triangle_coords_list.append([vertex_list[1], vertex_list[0], vertex_list[5]])
        triangle_coords_list.append([vertex_list[4], vertex_list[5], vertex_list[0]])

        triangle_coords_list.append([vertex_list[0], vertex_list[3], vertex_list[4]])
        triangle_coords_list.append([vertex_list[7], vertex_list[4], vertex_list[3]])

        triangle_coords_list.append([vertex_list[0], vertex_list[1], vertex_list[3]])
        triangle_coords_list.append([vertex_list[2], vertex_list[3], vertex_list[1]])

        return triangle_coords_list

    def arrange_multipatch_array(self, triangle_coords_list, new_findex):
        """
        概要:
            三角形の座標データから、multipatch_arrayの形を整える関数。

        引数:
            triangle_coords_list: 12個の三角形の3点の座標が格納されたリスト。
            new_findex: 空間ID1つ分に対応するfindex

        戻り値:
            multipatch_array: 調整を施した配列。48行*8カラム
        """

        # NCP.add_vertex_normalのパラメータに合わせるためにndarrayに変換
        multipatch_array = np.array(triangle_coords_list)

        # 法線ベクトルを付与
        temporary_multipatch_array = WM.calc_func_time(self.logger, False)(NCP.add_vertex_normal)(multipatch_array)

        # ポリゴンを閉じるために先頭の点を4点目に設定
        multipatch_array = np.concatenate(
            [temporary_multipatch_array, temporary_multipatch_array[:, 0:1, :]], axis=1
                ).reshape((len(temporary_multipatch_array)*4, 6))

        # マルチパッチジオメトリid生成
        multipatch_geometry_id_array = np.repeat(np.arange(0,len(temporary_multipatch_array)), 4).reshape(len(multipatch_array), 1)

        # 地物id生成
        feature_id_array = np.array(len(multipatch_array) * [new_findex]).reshape(len(multipatch_array), 1)

        # # id+xyz+multi_idの形に結合
        multipatch_array = np.concatenate([feature_id_array, multipatch_array, multipatch_geometry_id_array],axis = 1)

        return multipatch_array

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            # プロパティで入力した値の取得。
            output_crs, \
                input_spatial_id_column_name, \
                input_object_id_column_name, \
                output_multipatch_dwh_name, \
                output_object_id_dwh_name, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # Flowfileから、csv形式のfield_set_fileを取得。
            input_link_csv = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            csv_data_frame = pd.read_csv(io.StringIO(input_link_csv))

            # 1行目のデータを変数に格納
            spatial_id_list = csv_data_frame[input_spatial_id_column_name].to_list()
            object_id_list = csv_data_frame[input_object_id_column_name].to_list()

            # マルチパッチごとに格納するためのリスト。
            multi_patch_list = []

            # オブジェクトIDを属性として扱うためのリスト
            field_object_id_list = []

            # fieldsetfileにする際のリストを作成
            dwh_name_list = []
            type_list = []
            value_list = []

            # 空間ID1つごとに処理
            for index in range(len(spatial_id_list)):

                # 空間IDの頂点を取得する。
                vertex_list = WM.calc_func_time(self.logger)(self.create_vertex_list)(spatial_id_list[index], output_crs)

                # マルチパッチの三角形の頂点が12個格納されたリストを作成。
                triangle_coords_list = WM.calc_func_time(self.logger)(self.create_triangle_coords_list)(vertex_list)

                # multipatch_arrayを必要な形に調整。
                multipatch_array = WM.calc_func_time(self.logger)(self.arrange_multipatch_array)(triangle_coords_list, index)

                # マルチパッチ1つごとにリストに格納。
                multi_patch_list.append(multipatch_array)

                # オブジェクトIDを属性として扱うためのリストに格納
                field_object_id_list.append((float(index), object_id_list[index]))
                # field_object_id_list.append((float(index), str(index)))

            # リストにいれた各マルチパッチを一本のndarrayに結合。
            coordinates_array = np.concatenate(multi_patch_list, 0)

            # マルチパッチの行
            dwh_name_list.append(output_multipatch_dwh_name)
            type_list.append("geometry")
            value_list.append(coordinates_array)

            # # オブジェクトIDの行
            dwh_name_list.append(output_object_id_dwh_name)
            type_list.append("object")
            value_list.append(field_object_id_list)

            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_name_list,
                                                                         type_list,
                                                                         value_list)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:
    

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"crs": output_crs})

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
