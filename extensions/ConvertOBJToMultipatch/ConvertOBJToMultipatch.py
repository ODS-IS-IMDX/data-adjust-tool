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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# OBJの入荷プロセッサ。
# OBJを入荷し、マルチパッチ、FID、Findexを出力する
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")

# 定数
# 頂点index
OBJ_COORDINATE_INDEX = 0

# 法線index
OBJ_NORMAL_INDEX = 2

class ConvertOBJToMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        OBJファイルをマルチパッチ配列とフィールド型のFIDに変換する。
                        ①input: 任意のOBJファイル。
                        ②output: マルチパッチ、FIDを持った2行のFieldSetFile。
                      """
        tags = ["CSV", "DataFrame", "Python"]

    # インプットOBJファイルのデコードに用いる文字コード
    INPUT_OBJ_ENCODING_CODE = PropertyDescriptor(
        name="Input OBJ Encoding Code",
        description="OBJファイルの文字コード",
        default_value="utf-8",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # 出力マルチパッチのDWH名
    OUTPUT_MULTIPATCH_DWH_NAME = PropertyDescriptor(
        name="Output Multipatch DWH Name",
        description="出力マルチパッチのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 出力FIDのDWH名
    OUTPUT_FID_DWH_NAME = PropertyDescriptor(
        name="Output FID DWH Name",
        description="出力FIDのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [INPUT_OBJ_ENCODING_CODE,
                            OUTPUT_MULTIPATCH_DWH_NAME,
                            OUTPUT_FID_DWH_NAME]

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
            flowfile: プロセッサに渡されるデータ

        戻り値:
            input_obj_encoding_code: 入荷OBJの文字コード
            output_multipatch_dwh_name: 出力マルチパッチのDWH名
            output_fid_dwh_name: 出力マルチパッチのFIDのDWH名
            output_findex_dwh_name 出力マルチパッチのFindexのDWH名
        """
        input_obj_encoding_code = context.getProperty(
            self.INPUT_OBJ_ENCODING_CODE).evaluateAttributeExpressions(flowfile).getValue()

        output_multipatch_dwh_name = context.getProperty(
            self.OUTPUT_MULTIPATCH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_fid_dwh_name = context.getProperty(
            self.OUTPUT_FID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return input_obj_encoding_code, output_multipatch_dwh_name, output_fid_dwh_name

    #objファイルのテキスト全体から対象要素を抽出
    #f(座標を選ぶインデックス)抽出用のメソッド
    def get_obj_from_surface_string(self, string_list, target_string, split_string=' '):
        
        #先頭検索対象文字列の文字列数
        target_string_length = len(target_string)
        
        #先頭から検索対象文字列分切り出して同じならsplitして格納
        target_string_list = [string_list[i].split(split_string)[1:] for i in range(len(string_list)) if string_list[i][0:target_string_length] == target_string]
        
        return target_string_list
        
    #objファイルのf情報から構成点の組み合わせだけを抽出し3点ずつの組み合わせに変換
    def get_index_from_obj_string(self, coordinate_index_list, split_string = '/', target_index = 0):

        #結果格納用List
        result_index_list = []

        #listの内の文字列を'/'でsplitして先頭のみ抽出
        for temp_list in coordinate_index_list:
            target_index_list = [temp_string.split(split_string)[target_index] for temp_string in temp_list]
            
            
            #3点ずつ選んで結果格納
            [result_index_list.append([target_index_list[0], target_index_list[i+1], target_index_list[i+2]]) for i in range(len(target_index_list)-2)]

        #objファイルのインデックスは1始まりなので-1する
        return np.array(result_index_list, dtype=np.int64) - 1

    def close_polygon_array(self, coordinate_index_array, coordinate_array):
        # ポリゴンとして閉じるために終点に始点を設定する
        closed_coordinate_index_array = np.concatenate([coordinate_index_array, coordinate_index_array[:,0:1]], axis=1)
        result_coordinates_array = coordinate_array[closed_coordinate_index_array.flatten()]

        return closed_coordinate_index_array, result_coordinates_array

    def assign_ids_to_multipatch_vertices(self, coordinate_index_array, coordinates_array, normal_result_array):
        #multi_id マルチパッチごとのid　地物idごとに0から振られる　今回はすべて0
        multipatch_id_array  = np.zeros((len(coordinates_array), 1))

        #地物idを4点ずつ付与
        multipatch_geometry_id_array = np.repeat(np.arange(0,len(coordinate_index_array)), 4).reshape(len(coordinates_array), 1)

        #マルチパッチの配列
        multipatch_coordinates_array = np.concatenate([multipatch_geometry_id_array, coordinates_array, normal_result_array, multipatch_id_array], axis = 1)

        return multipatch_geometry_id_array, multipatch_coordinates_array

    def create_fid_from_id_array(self, multipatch_geometry_id_array):
        #データ流通基盤のidの配列生成
        unique_id_array = np.unique(multipatch_geometry_id_array).astype(np.int64)

        feature_id_list = [(i, i) for i in range(len(unique_id_array))]

        return feature_id_list

    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            input_obj_encoding_code, \
                output_multipatch_dwh_name, \
                output_fid_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileからobjのテキストファイルを取得
            input_obj_string = flowfile.getContentsAsBytes().decode(input_obj_encoding_code)

            # ------------------------------------------------------------------------------------------------------------
            # ジオメトリの取得
            # ------------------------------------------------------------------------------------------------------------
            # objファイルを1行ずつlistに格納
            obj_string_list = input_obj_string.splitlines()

            # 頂点座標のlistを取得
            vertex_coordinate_list = self.get_obj_from_surface_string(obj_string_list, 'v ')

            # 頂点座標をarrayへ変換
            vertex_coordinate_array = np.array(vertex_coordinate_list, dtype=np.float64)

            # 組み合わせ情報を取得
            coordinate_index_list = self.get_obj_from_surface_string(obj_string_list, 'f ')

            # 頂点の組み合わせ情報をMultiPatchのインデックスへ変換
            vertex_coordinate_index_array = self.get_index_from_obj_string(coordinate_index_list,
                                                                           target_index=OBJ_COORDINATE_INDEX)

            # ポリゴンとして閉じるために終点に始点を設定する
            closed_vertex_coordinate_index_array, \
                result_coordinates_array = self.close_polygon_array(vertex_coordinate_index_array, 
                                                                    vertex_coordinate_array)
            # ------------------------------------------------------------------------------------------------------------

            # ------------------------------------------------------------------------------------------------------------
            # 法線取得
            # ------------------------------------------------------------------------------------------------------------
            # 法線座標のlistを取得
            normal_coordinate_list = self.get_obj_from_surface_string(obj_string_list, 'vn ')

            # 法線座標をarrayへ変換
            normal_coordinate_array = np.array(normal_coordinate_list, dtype=np.float64)

            #組み合わせ情報をMultiPatchのインデックスへ変換
            normal_coordinate_index_array = self.get_index_from_obj_string(coordinate_index_list,
                                                                           target_index=OBJ_NORMAL_INDEX)

            #ポリゴンとして閉じるために終点に始点を設定する
            closed_normal_coordinate_index_array, \
                normal_result_array = self.close_polygon_array(normal_coordinate_index_array, 
                                                               normal_coordinate_array)
            #------------------------------------------------------------------------------------------------------------

            # id付与
            multipatch_geometry_id_array, result_coordinates_array = \
                self.assign_ids_to_multipatch_vertices(closed_vertex_coordinate_index_array, result_coordinates_array, normal_result_array)

            feature_id_list \
                = self.create_fid_from_id_array(multipatch_geometry_id_array)

            #
            output_dwh_name_list = [output_multipatch_dwh_name, output_fid_dwh_name]
            geometry_type_list = ["geometry", "int"]
            coordinates_array_list = [result_coordinates_array, feature_id_list]

            # マルチパッチをフィールドセットファイルに加工
            output_field_set_file = (PBP.set_field_set_file)(
                output_dwh_name_list, geometry_type_list, coordinates_array_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
