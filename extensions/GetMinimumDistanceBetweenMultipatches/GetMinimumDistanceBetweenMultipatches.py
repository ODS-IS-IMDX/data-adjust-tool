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
# 距離のフィールドに基づき、各メッシュ領域内で距離が最小の座標データを抽出する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io
from collections import defaultdict

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")


class GetMinimumDistanceBetweenMultipatches(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        距離のフィールドに基づき、各メッシュ領域内で距離が最小の座標データを抽出する。
                        ①input: 座標配列および距離のフィールドを含む2行以上のFieldSetFile。
                                座標配列と距離のフィールドは必須。必要に応じて任意の関連属性を追加することも可能。
                        ②output: 座標配列および距離のフィールドを含む2行以上のFieldSetFile。
                                 inputに関連属性が含まれていた場合、それらも出力に含まれる。
                      """

        tags = ["Distance", "Mesh", "Python"]

    # データ定義ファイルの区切り文字列
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # 離隔距離のフィールドの流通項目名
    DISTANCE_DISTRIBUTION_NAME = PropertyDescriptor(
        name="Distance Distribution Name",
        description="離隔距離のフィールドの流通項目名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # メッシュの分割単位(メートル)
    MESH_UNIT = PropertyDescriptor(
        name="Mesh Unit",
        description="メッシュの分割単位(メートル)",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            DISTANCE_DISTRIBUTION_NAME,
                            MESH_UNIT
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで設定した値を取得

        Parameters:
            context: プロセッサの設定値
            flowfile: プロセッサに入るデータ

        Returns:
            data_definition_delimiter: データ定義ファイルの区切り文字
            distance_distribution_name: 離隔距離のフィールドの流通項目名
            mesh_unit: メッシュの分割単位(メートル)
        """

        # データ定義ファイルの区切り文字
        data_definition_delimiter = context.getProperty(self.DATA_DEFINITION_DELIMITER).getValue()

        # 離隔距離のフィールドのDWH名
        distance_distribution_name = context.getProperty(
            self.DISTANCE_DISTRIBUTION_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # メッシュの分割単位(メートル)
        mesh_unit = float(context.getProperty(
            self.MESH_UNIT).evaluateAttributeExpressions(flowfile).getValue())

        return data_definition_delimiter, distance_distribution_name, mesh_unit

    def get_flowfile(self, flowfile):
        """
        flowfileから、attributeからDataDefinitionの値を取得し、
        contentのFieldSetFileをDataFrameに変換

        Parameters:
            flowfile: プロセッサに入ってくるデータ

        Returns:
            data_definition_stream: データ定義ファイル(csv形式)
            field_set_file_dataframe: FieldSetFileから作成したDataFrame
        """

        # FlowFileから"DataDefinition"属性の内容を取得
        data_definition = flowfile.getAttribute("DataDefinition")

        # data_definitionをpathとして扱う
        data_definition_stream = io.StringIO(data_definition)

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        return data_definition_stream, field_set_file_data_frame


    def add_column_to_array(self, coordinates_array, new_column_array):
        """
        座標配列と追加用配列を整形・結合して、1つの配列にまとめる

        Parameters:
            coordinates_array: 座標配列※構成点2点のラインからなる座標配列のみ対応
            new_column_array: 追加する列の1次元配列

        Returns:
            np.ndarray: [ID, x, y, z, distance] の形になった座標配列
        """

        # 追加用配列を2回複製して形状を調整（座標配列との結合のためのZ）
        expanded_distance = np.stack([new_column_array, new_column_array], axis=1).flatten().reshape(len(coordinates_array), 1)

        # 座標配列と追加用配列を横方向に連結（列を追加）
        return np.concatenate([coordinates_array, expanded_distance], axis=1)


    def create_boolean_mask_for_ids(self, total_rows, target_id_array):
        """
        指定したIDに対応する行のみTrueにするマスクを作成

        Parameters:
            total_rows: 全体の行数(int)
            target_id_array: 抽出対象のID配列

        Returns:
            mask: bool配列
        """

        # 属性データ(DataFrame)の行数に合わせた論理値配列（Falseで初期化）
        mask = np.zeros(total_rows, dtype=bool)

        # 最小距離IDに該当する行をTrueにセット
        mask[target_id_array.astype(int)] = True

        return mask

    def get_mesh_id_dict(self, coordinates_xyz_array, mesh_unit):
        # -----------------------------------------------------------------------------------------------------------
        #メッシュidごとに座標を振り分ける
        #引数1:座標配列 2次元配列 [構成点]>[id + xyz] np.float64
        #      yより後の要素はなくてもよい 別の情報が設定させれていてもよい
        #      要素には、少なくともid + xy座標を設定すること
        #引数2:メッシュの単位 座標配列と単位を合わせること float
        #戻り値:defaultdict Key:メッシュのid 'x@y' str, Value:座標配列(numpy.array)が要素のList
        # -----------------------------------------------------------------------------------------------------------

        #座標の存在領域の左上を原点とするメッシュidをx,yそれぞれで取得
        x_mesh_array = np.floor((coordinates_xyz_array[:,1] - coordinates_xyz_array[:,1].min()) / mesh_unit)
        y_mesh_array = np.floor((coordinates_xyz_array[:,2].max() - coordinates_xyz_array[:,2]) / mesh_unit)

        #x,yのメッシュidを結合 xy平面メッシュidのlistを生成
        mesh_id_list = [str(x_mesh_array[i]) + '@' + str(y_mesh_array[i]) for i in range(len(x_mesh_array))]

        #xy平面メッシュidをKey、座標配列
        mesh_id_default_dict = defaultdict(list)
        [mesh_id_default_dict[mesh_id_list[mi]].append(coordinates_xyz_array[mi]) for mi in range(len(mesh_id_list))]

        return mesh_id_default_dict


    def get_smallest_distance_in_mesh(self, default_dict, column_index):
        # -----------------------------------------------------------------------------------------------------------
        #辞書型配列のValueに設定されたListから指定列の最小値インデックスを持つ要素を取り出す
        #引数1:default_dict or dictionary Keyにxyメッシュid('x@y' str)を想定 Valueに座標配列(numpy.array, np.float64)が要素のList
        #引数2:最小値取得対象列インデックス int
        #戻り値:最小値を持つ座標配列 2次元 np.float64 [構成点]>[id + xyz]
        # -----------------------------------------------------------------------------------------------------------

        #結果格納用List
        result_list = []

        #KeyごとにValueを取得しその中で指定列の最小値を持つデータのみ抽出
        for temp_key, temp_value in default_dict.items():
            #List→numpy.arrayへ
            temp_value_array = np.array(temp_value)

            #最小値の配列を抽出
            result_list.append(temp_value_array[np.argmin(temp_value_array[:,column_index])])

        #結果をnumpy.array
        return np.array(result_list)

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            data_definition_delimiter, \
                distance_distribution_name, \
                mesh_unit\
                = self.get_property(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  flowfileから、attributeからDataDefinitionの値を取得し、contentのFieldSetFileをDataFrameに変換
            # -----------------------------------------------------------------------------------------------------------
            data_definition_stream, \
                field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # データ定義からジオメトリと属性の情報を取得し、対応するリストを返す
            # -----------------------------------------------------------------------------------------------------------
            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                attribute_file_type_list, \
                attribute_dwh_file_name_list, \
                attribute_distribution_name_list, \
                attribute_const_value_list, \
                attribute_data_type_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stream,
                                                                                         data_definition_delimiter=data_definition_delimiter)

            # -----------------------------------------------------------------------------------------------------------
            #  座標配列をfield_set_file_dataframeから取得
            # -----------------------------------------------------------------------------------------------------------
            coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, geometry_dwh_file_name_list[0])

            # 座標配列から重複要素を取り除いたIDを取得
            coordinates_id_array = np.unique(
                coordinates_array[:, 0])

            # -----------------------------------------------------------------------------------------------------------
            # データ定義ファイルから属性のDataFrameを作成
            # -----------------------------------------------------------------------------------------------------------
            attribute_feature_id_array, \
                all_attribute_dataframe\
                = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe_datatype)(field_set_file_dataframe,
                                                                                          attribute_dwh_file_name_list,
                                                                                          attribute_distribution_name_list,
                                                                                          attribute_const_value_list,
                                                                                          attribute_file_type_list,
                                                                                          attribute_data_type_list,
                                                                                          len(coordinates_id_array))

            # -----------------------------------------------------------------------------------------------------------
            # distance_arrayを2回複製して形状を調整、座標配列と距離配列を横方向に連結（列を追加）
            # -----------------------------------------------------------------------------------------------------------
            # 指定した距離のフィールド名(distance_distribution_name)に対応する列をNumPy配列として取得
            distance_array = all_attribute_dataframe[distance_distribution_name].to_numpy(dtype = np.float64)

            coordinates_array_with_distance\
                = WM.calc_func_time(self.logger)(self.add_column_to_array)(coordinates_array,
                                                                           distance_array)

            # -----------------------------------------------------------------------------------------------------------
            # メッシュIDごとに座標配列を分類
            # -----------------------------------------------------------------------------------------------------------
            target_dict = self.get_mesh_id_dict(coordinates_array_with_distance,
                                                mesh_unit)

            # -----------------------------------------------------------------------------------------------------------
            # メッシュ内で距離が最小の座標のみ抽出
            # -----------------------------------------------------------------------------------------------------------
            # 最小値判定の対象列（座標配列の5列目、距離のフィールドがここに入っている想定）
            start_col = 4
            target_array = self.get_smallest_distance_in_mesh(target_dict, start_col)

            # 抽出された座標配列から重複要素を取り除いたIDを取得
            unique_id_array = np.unique(target_array[:,0])

            # -----------------------------------------------------------------------------------------------------------
            # 属性データ(DataFrame)の行数に合わせた論理値配列（Falseで初期化）を作成し、最小距離IDに該当する行をTrueにセット
            # -----------------------------------------------------------------------------------------------------------
            bool_array = WM.calc_func_time(self.logger)(self.create_boolean_mask_for_ids)(len(all_attribute_dataframe),
                                                                                          unique_id_array)

            # 座標配列のうち、抽出対象IDの行のみ取得
            filtered_coordinates_array = coordinates_array[np.isin(coordinates_array[:, 0], unique_id_array)]

            filtered_coordinates_array_copy = filtered_coordinates_array.copy()

            # 抽出対象IDでフィルタリングした座標配列から重複要素を取り除いたIDを取得
            unique_ids = np.unique(filtered_coordinates_array_copy[:, 0])

            # IDの振り直し
            filtered_coordinates_array_copy[:, 0] = np.repeat(np.arange(len(unique_ids)), np.full_like(unique_ids, 2, dtype=int))

            # 属性DataFrameも抽出対象IDの行だけにフィルタリング
            filtered_all_attribute_dataframe = all_attribute_dataframe[bool_array]

            # 出力用に属性データを (インデックス, 値) のタプルリストに変換（NiFi用フォーマット）
            output_attributes_list = [
                list(zip(range(len(filtered_all_attribute_dataframe)), filtered_all_attribute_dataframe[col].tolist()))
                for col in filtered_all_attribute_dataframe.columns
            ]

            # -----------------------------------------------------------------------------------------------------------
            # 出力データの設定
            # -----------------------------------------------------------------------------------------------------------
            output_geometry_type = "geometry"

            output_value_list = [filtered_coordinates_array_copy] + output_attributes_list
            output_dwh_name_list = geometry_dwh_file_name_list + attribute_dwh_file_name_list
            output_type_list = [output_geometry_type] + attribute_data_type_list

            # -----------------------------------------------------------------------------------------------------------
            # 出力用FieldSetFileを生成
            # -----------------------------------------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
