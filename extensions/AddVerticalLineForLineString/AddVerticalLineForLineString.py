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
# coordinatesに対して、Z座標が異なる同一X, Y座標の頂点間に垂直線を追加と
# 属性情報更新のためのresults形式データの作成を行う
# --------------------------------------------------------------------------------------------

# python 標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

np = import_module("numpy")
pd = import_module("pandas")


class AddVerticalLineForLineString(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列に対して縦線処理を行う。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 座標配列と処理前後のインデックスデータを持った2行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Results', 'Python']

    # coordinates_array(座標配列)のDWH
    OUTPUT_RESULTS_NAME = PropertyDescriptor(
        name="Output Index Data DWH Name",
        description="出力するインデックス更新データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    property_descriptors = [OUTPUT_RESULTS_NAME]

    def __init__(self, **kwargs):
        pass

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
            output_results_name: 出力時のresults形式データのDWH
        """

        # プロパティで設定した値を取得
        output_results_name = context.getProperty(
            self.OUTPUT_RESULTS_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_results_name

    def create_connected_line_geometry(self, coordinates_array, round_decimal=3):
        """
        概要:
            指定された座標配列に基づいて、Z座標が異なる同一のX, Y座標の頂点間に縦のラインを追加

        引数:
            coordinates_array: 頂点の座標情報を含む NumPy配列[id, X, Y, Z]
            round_decimal: 座標の小数点以下の桁数。デフォルトは3。座標をこの桁数に丸めてから処理を行う

        戻り値:
            coordinates_array: 列元の座標配列と新しく追加された縦線の座標が含まれる更新された座標配か元の座標配列
            reference_new_id_list: 参照される新しいIDのリスト
            new_geometry_id_list: 新しく作成された縦線のジオメトリのIDリスト
        """

        # mm未満切り捨て
        coordinates_array = np.trunc(
            (coordinates_array * 10**round_decimal)) / 10**round_decimal

        # 始点のインデックスを取得
        point_start_index_list = list(np.where(
            (coordinates_array[:, 0:1] == np.roll(coordinates_array, 4)[:, 0:1]) == False)[0])

        # 終点のインデックスを取得
        point_end_index_list = list(np.where(
            (coordinates_array[:, 0:1] == np.roll(coordinates_array, -4)[:, 0:1]) == False)[0])

        # 端点のインデックス配列を作成
        tip_point_index_list = np.array(
            point_start_index_list + point_end_index_list)

        # 同一の頂点を持つインデックスの組み合わせを格納する配列を定義
        target_edge_point_index_list = []

        # 既存geometryのidから連番になるよう新しく作成するlineの最初のidを取得
        new_geometry_id = len(set(coordinates_array[:, 0]))

        # 端点のインデックスを要素数分繰り返す
        for tip_point_index in tip_point_index_list:

            # X,Yが同一の頂点を特定し取得
            target_point_equality_index = np.where(np.all(
                coordinates_array[tip_point_index, 1:3] == coordinates_array[:, 1:3], axis=1) == True)[0]

            # 同一の頂点が2つ以上の場合
            if len(target_point_equality_index) >= 2:

                # Z値を取得
                z_value_array = coordinates_array[target_point_equality_index, 3]

                # Z値の昇順に座標を並び替えた時のインデックスで頂点のインデックスを並び替える
                target_point_equality_index = target_point_equality_index[np.argsort(
                    z_value_array)]

                # (len(target_point_equality_index)-1)回分インデックスを選択して追加
                [target_edge_point_index_list.append(
                    [target_point_equality_index[i], target_point_equality_index[i+1]]) for i in range(len(target_point_equality_index) - 1)]

        # ユニークな配列に変更
        target_edge_point_index_unique_list = np.unique(
            target_edge_point_index_list, axis=0)

        # 返却用の配列を定義
        vertical_line_geometry_list = []
        reference_new_id_list = []
        new_geometry_id_list = []

        # 要素数分繰り返す
        for target_index_list in target_edge_point_index_unique_list:

            # インデックスをもとに頂点座標を取得
            target_geometry_list = coordinates_array[target_index_list]

            # ユニークな配列を作成する　id無し
            edit_target_geometry_unique_list = np.unique(
                target_geometry_list[np.argsort(target_geometry_list[:, 3])[::-1]][:, 1:4], axis=0)

            # 頂点の数が2つ以上の場合
            if len(edit_target_geometry_unique_list) >= 2:

                # z値を比較し、z値が最大の座標を取得
                max_id_value = max(target_geometry_list[:, -1])

                # zが最大のID値に対応する行を取得
                max_id_row = target_geometry_list[target_geometry_list[:, -1]
                                                  == max_id_value]

                # その行の最初の列の値（ID列の値）を取得
                mix_id = max_id_row[0, 0]

                # id をリストに追加(後の属性更新で使用)
                reference_new_id_list.append(mix_id.astype(int))

                # 片方の座標とidをラインジオメトリの配列に格納する
                edit_target_geometry_with_id1 = np.hstack(
                    (np.array([new_geometry_id]), edit_target_geometry_unique_list[0]))

                vertical_line_geometry_list.append(
                    edit_target_geometry_with_id1)

                # もう片方の座標とidをラインジオメトリの配列に格納する
                edit_target_geometry_with_id2 = np.hstack(
                    (np.array([new_geometry_id]), edit_target_geometry_unique_list[-1]))

                vertical_line_geometry_list.append(
                    edit_target_geometry_with_id2)

                # 新しく追加した縦線のidを取得
                new_geometry_id_list.append(new_geometry_id)

                # 次のidに
                new_geometry_id += 1

        if vertical_line_geometry_list:

            # 作成した縦線
            vertical_line_array = np.vstack(vertical_line_geometry_list)

            # 元の配列と縦線の配列で再構成
            processed_coordinates_array = np.vstack(
                (coordinates_array, vertical_line_array))

            return processed_coordinates_array, reference_new_id_list, new_geometry_id_list

        # 作成した縦線がなかった場合 input の配列をそのまま返す
        else:

            return coordinates_array, reference_new_id_list, new_geometry_id_list

    def create_results_data(self, input_coordinates_array_ids_list, new_geometry_id_list, geometry_value_coordinates_array, reference_new_id_list):
        """
        概要:
            inputのcoordinatesから取得したidと追加した縦線のidからresults形式データ作成

        引数:
            input_coordinates_array_ids_list: inputのcoordinates_arrayのIDが格納されたリスト
            new_geometry_id_list: 新しく作成されたジオメトリのIDのリスト
            geometry_value_coordinates_array: ジオメトリの座標配列
            reference_new_id_list: 新しいジオメトリのIDが参照する元のジオメトリの ID リスト
        戻り値:
            results_data_list: 作成したresults形式データ
        """

        # attributes の findex を取得
        attributes_findex = input_coordinates_array_ids_list

        # 新しく追加された縦線の ID を追加
        new_attributes_findex = attributes_findex + new_geometry_id_list

        # coordinates_array の findex を取得し、リストに
        coordinates_array_id_list = geometry_value_coordinates_array[:, 0].tolist(
        )

        # 構成点ごとにIDを保持して、順番を保持したまま重複を除くため list → dict → list
        unique_coordinates_array_id_list = list(
            dict.fromkeys(coordinates_array_id_list))

        # 参照するための findex
        reference_id_list = unique_coordinates_array_id_list + reference_new_id_list

        # findex(属性ID) と findex(属性IDを参照し属性を付けるための参照元ID) の df
        findex_and_findex_dataframe = pd.DataFrame({
            'attributes_findex': new_attributes_findex,
            'findex': reference_id_list
        })

        # 外部で属性の更新を行うためのresults継起しデータを作成
        results_data_list = list(zip(
            findex_and_findex_dataframe['attributes_findex'], findex_and_findex_dataframe['findex']))

        return results_data_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値取得
            output_results_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # field_set_file取得
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # outputで使用
            dwh_list = []
            type_list = []
            dwh_list.append(target_dwh)
            type_list.append(target_type)

            # results形式データ作成のためID取得
            input_coordinates_array_ids = np.unique(
                geometry_value_coordinates_array[:, 0])
            input_coordinates_array_ids_list = input_coordinates_array_ids.tolist()

            # Z値が異なる同一の頂点に対して頂点間に縦のラインを作成する
            processed_coordinates_array, \
                reference_new_id_list, \
                new_geometry_id_list\
                = WM.calc_func_time(self.logger)(self.create_connected_line_geometry)(geometry_value_coordinates_array)

            # results形式データ作成
            results_data_list = WM.calc_func_time(self.logger)(self.create_results_data)(input_coordinates_array_ids_list,
                                                                                         new_geometry_id_list,
                                                                                         geometry_value_coordinates_array,
                                                                                         reference_new_id_list,
                                                                                         )

            dwh_list.append(output_results_name)
            type_list.append("results")

            output_value_list = []
            output_value_list.append(np.array(processed_coordinates_array))
            output_value_list.append(results_data_list)

            # set_field_set_file : pickle によるシリアライズ、base64 でのデコード、utf-8 でのエンコード処理
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                                           type_list,
                                                                                           output_value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
