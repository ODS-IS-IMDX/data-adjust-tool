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

# ----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 縦の中心線を構成する座標の情報をもとに、属性を付加しCityGMLの形式に変換する。
# NoThematicとは中心線の情報を含まないことを意味する。
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import xml.etree.ElementTree as ET
from xml.dom import minidom
import traceback
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
np = import_module("numpy")
pd = import_module("pandas")


# -----------------------------------------------------------------------------------------------------------
# 固定値
# -----------------------------------------------------------------------------------------------------------
# テーマティックシェープの流通項目List
THEMATICSHAPE_GEOMETRY_LIST = ['uro:shape',
                               'gml:Point',
                               'gml:posList'
                               ]

XML_ATTRIBUTE_DELIMITER = '|'


class ConvertMultiPointCoordinatesToCityGML(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、中心線のあるMultiPointからCityGML形式のファイルを作成する。
                        ①input: 出力したいデータ項目を持ったFieldSetFile。
                        ②output: 国土基本図郭ごとのCityGMLファイルを持った、1行以上のFieldSetFile。
                      """
        tags = ['Coordinates', 'CityGML', 'gml', 'xml', 'Python']

    # データ定義ファイルの区切り文字列
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True
    )

    # coordinates_array(座標配列)のDWH
    CENTER_DWH_NAME = PropertyDescriptor(
        name="Center DWH Name",
        description="入力データの座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # gml_idのDWH
    GML_ID_DWH_NAME = PropertyDescriptor(
        name="gml_id DWH Name",
        description="入力データのgml_idのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 蓋中心点_coordinates(蓋MH座標)のDWH
    MANHOLE_COORDINATE_DWH_NAME = PropertyDescriptor(
        name="Manhole Coordinate DWH Name",
        description="入力データの蓋中心点のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # results形式データのDWH
    RESULTS_DWH_NAME = PropertyDescriptor(
        name="Index Data DWH Name",
        description="入力データのインデックス更新データのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 建物の面情報を示すタグを設定
    FEATURE_TAG_STRING = PropertyDescriptor(
        name="Feature Tag String",
        description="建物の面情報を示すタグ",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        default_value="gml:CompositeSurface"
    )

    # 地図情報2500レベルの図郭コードを設定
    LEVEL2500_UNIT_CODE_STRING = PropertyDescriptor(
        name="Level2500 Unit Code String",
        description="地図情報2500レベルの図郭コード",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="${unit_code}",
    )

    # 出力時のCRS（座標参照系）を設定
    OUTPUT_TARGET_CRS_STRING = PropertyDescriptor(
        name="Output Target CRS String",
        description="出力データの座標のCRS(epsgコード)",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="${crs}",
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            CENTER_DWH_NAME,
                            GML_ID_DWH_NAME,
                            MANHOLE_COORDINATE_DWH_NAME,
                            RESULTS_DWH_NAME,
                            FEATURE_TAG_STRING,
                            LEVEL2500_UNIT_CODE_STRING,
                            OUTPUT_TARGET_CRS_STRING
                            ]

    def __init__(self, **kwargs):
        pass

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
            data_definition_delimiter: データ定義ファイルの区切り文字
            center_dwh_name: 中心線のDWH
            gml_id_dwh_name: gml_idのDWH
            manhole_coordinate_dwh_name: 蓋中心点_coordinates(蓋MH座標)のDWH
            results_dwh_name: results形式データのDWH
            feature_tag_string: ジオメトリループ用tag
            level2500_unit_code_string: 出力対象レベル2500国土基本図図郭コード文字列郡
            output_target_crs_string: 出力対象CRS
        """

        # データ定義ファイルの区切り文字
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        # 中心線のDWH
        center_dwh_name = context.getProperty(
            self.CENTER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # gml_idのDWH
        gml_id_dwh_name = context.getProperty(
            self.GML_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 蓋中心点_coordinates(蓋MH座標)のDWH
        manhole_coordinate_dwh_name = context.getProperty(
            self.MANHOLE_COORDINATE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # results形式データのDWH
        results_dwh_name = context.getProperty(
            self.RESULTS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # ジオメトリループ用tag
        feature_tag_string = context.getProperty(
            self.FEATURE_TAG_STRING).getValue()

        # 出力対象レベル2500国土基本図図郭コード文字列郡
        level2500_unit_code_string = context.getProperty(
            self.LEVEL2500_UNIT_CODE_STRING).evaluateAttributeExpressions(flowfile).getValue()

        # 出力対象CRS
        output_target_crs_string = context.getProperty(
            self.OUTPUT_TARGET_CRS_STRING).evaluateAttributeExpressions(flowfile).getValue()

        return data_definition_delimiter, center_dwh_name, gml_id_dwh_name, manhole_coordinate_dwh_name, results_dwh_name, feature_tag_string, level2500_unit_code_string, output_target_crs_string

    def get_flowfile(self, flowfile, results_dwh_name):
        """
        概要:
            flowfileから、DataDefinitionというattributeのキーから値を取得し、(データ定義ファイル)
            次に、input_field_set_fileをデータフレームに加工する関数

        引数:
            results_dwh_name: results形式データのDWH
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            data_definition_stream: データ定義ファイル(CSV形式)
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
            results_data: results形式のデータ
        """

        # flowfileから"DataDefinition"属性の内容を取得する
        data_definition = flowfile.getAttribute("DataDefinition")

        # data_definitionを、pathとして扱う。
        data_definition_stream = io.StringIO(data_definition)

        # flowfileから、CSV形式のFieldSetFileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        results_data = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == results_dwh_name, "Value"].values[0]))

        return data_definition_stream, field_set_file_data_frame, results_data

    def create_coordinates_id_array_and_dict_from_coordinates_array(self, field_set_file_data_frame, geometry_dwh_file_name_list):
        """
        概要:
            ジオメトリ配列を取得し、マルチパッチのIDをキーとし、その座標データを値とした辞書に変換する関数。

        引数:
            field_set_file_data_frame: FieldSetFileをDataFrameに加工したデータ
            geometry_dwh_file_name_list: データ定義ファイルのgeometryのDWHファイル名が格納されたlist

        戻り値:
            coordinates_id_array: geometry_value_coordinates_arrayのIDだけの配列
            coordinates_dict: マルチパッチのIDをキーとし、その座標データを値とした辞書
        """

        # field_set_file_data_frame からジオメトリ値の配列を抽出
        geometry_value_coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_file_name_list[0], "Value"].values[0]))

        # geometry_value_coordinates_array から重複した要素を取り除いた座標IDを取得
        coordinates_id_array = np.unique(
            geometry_value_coordinates_array[:, 0])

        # coordinates_dict を生成
        coordinates_dict = {coordinates_id_array[i]: geometry_value_coordinates_array[list(np.where(
            geometry_value_coordinates_array[:, 0] == coordinates_id_array[i])[0])] for i in range(len(coordinates_id_array))}

        return coordinates_id_array, coordinates_dict

    def create_point_dict_from_field_set_file_data_frame(self, field_set_file_data_frame, center_dwh_name):
        """
        概要:
            field_set_file_data_frameの指定されたDWH列から中心線の座標配列を抽出し、
            IDをキーとした中心線の辞書を作成する

        引数:
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
            center_dwh_name: 中心線のDWH

        戻り値:
            point_id_array: 抽出された中心線のIDを格納した numpy 配列
            point_dict: 中心線のIDをキーとし、その座標データを値とした辞書
        """

        # Pointはは偶数[::2, :]番目を取得する。
        point_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == center_dwh_name, "Value"].values[0]))[::2, :]
        point_id_array = np.unique(point_array[:, 0])
        point_dict = {point_id_array[i]: point_array[list(np.where(
            point_array[:, 0] == point_id_array[i])[0]), 1:4] for i in range(len(point_id_array))}

        return point_id_array, point_dict

    def get_gml_id_array_from_field_set_file_data_frame(self, field_set_file_data_frame, gml_id_dwh_name):
        """
        概要:
            field_set_file_data_frameの指定されたDWH列からgml_idリストを抽出し、numpy配列として返す

        引数:
            field_set_file_data_frame: DataFrameに加工したFieldSetFile

        戻り値:
            gml_id_dwh_name: gml_idのDWH
            gml_id_array: 抽出されたgml_idの配列
        """

        gml_id_list = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == gml_id_dwh_name, "Value"].values[0]))
        gml_id_array = np.array(gml_id_list, dtype=object)

        return gml_id_array

    # -----------------------------------------------------------------------------------------------------------
    # 概要 　: field_set_file_data_frameの指定されたDWH列からMHIDリストの2番目の要素を抽出し、numpy配列として返す
    # 引数　 : field_set_file_data_frame - DataFrameに加工したFieldSetFile
    # 　　   : mhid_dwh_name             - MHIDのDWH
    # 戻り値 : ultipatch_id_array        - 抽出されたMHIDの配列
    # -----------------------------------------------------------------------------------------------------------
    def get_mhid_array_from_field_set_file_data_frame(self, field_set_file_data_frame, mhid_dwh_name):

        # 指定されたDWH列に対応するMHidリストを取得
        multipatch_id_list = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == mhid_dwh_name, "Value"].values[0]))

        # 取得したMHidリストをnumpy配列に変換
        multipatch_id_array = np.array(multipatch_id_list, dtype=object)

        # リストの2番目の要素を取得
        multipatch_id_array = multipatch_id_array[:, 1]

        return multipatch_id_array

    # -----------------------------------------------------------------------------------------------------------
    # 概要 　: field_set_file_data_frameの指定されたDWH列から蓋MH座標リストのを抽出し、numpy配列として返す
    # 引数　 : field_set_file_data_frame   - DataFrameに加工したFieldSetFile
    # 　　   : manhole_coordinate_dwh_name - 蓋MH座標のDWH
    # 戻り値 : mh_cover_coordinates_array  - 抽出された蓋MH座標の配列
    # -----------------------------------------------------------------------------------------------------------
    def get_manhole_coordinate_from_field_set_file_data_frame(self, field_set_file_data_frame, manhole_coordinate_dwh_name):

        # 指定されたDwh列に対応する蓋MHの座標を取得
        mh_cover_coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == manhole_coordinate_dwh_name, "Value"].values[0]))

        return mh_cover_coordinates_array

    # -----------------------------------------------------------------------------------------------------------
    # 概要 　: field_set_file_data_frameの指定されたDwh列から蓋中心点_MHIDのリストの2番目の要素を抽出し、numpy配列として返す
    # 引数　 : field_set_file_data_frame                - DataFrameに加工したFieldSetFile
    # 　　   : manhole_cover_center_point_mhid_dwh_name - 蓋中心点_MHIDのDWH
    # 戻り値 : mh_cover2_multipatch_id_array            - 抽出された蓋中心点_MHIDの配列
    # -----------------------------------------------------------------------------------------------------------
    def get_manhole_cover_center_point_mhid_from_field_set_file_data_frame(self, field_set_file_data_frame, manhole_cover_center_point_mhid_dwh_name):

        # 指定されたDwh列に対応する蓋中心点_MHIDリストを取得
        mh_cover2_multipatch_id_list = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == manhole_cover_center_point_mhid_dwh_name, "Value"].values[0]))

        # 取得した蓋中心点_MHIDリストをnumpy配列に変換
        mh_cover2_multipatch_id_list = np.array(
            mh_cover2_multipatch_id_list, dtype=object)

        # リストの2番目の要素を取得
        mh_cover2_multipatch_id_array = mh_cover2_multipatch_id_list[:, 1]

        return mh_cover2_multipatch_id_array

    def output_target_extraction_specifically_feature_bool(self, all_attribute_dataframe, point_id_array, coordinates_dict, gml_id_array, feature_bool):
        """
        概要:
            与えられた条件(feature_bool)に基づいて特定の属性データ (target_attribute_dataframe)とジオメトリデータ(target_coordinates_array)を抽出する

        引数:
            all_attribute_dataframe: 全ての属性データを含むDataFrame
            point_id_array: ラインストリングのIDを格納した配列
            coordinates_dict: ジオメトリの座標情報を持つ辞書
            gml_id_array: gml_idを格納した配列
            feature_bool: 出力対象とするフラグを示すブール型の配列

        戻り値:
            target_attribute_dataframe: 出力対象となる属性データのDataFrame
            target_id_array: 出力対象となるラインストリングのIDを格納した配列
            target_coordinates_array: 出力対象となるジオメトリの座標情報を持つ配列
            target_gml_id_array: 出力対象となるgml_idを格納した配列
        """

        # 出力対象抽出
        target_attribute_dataframe = all_attribute_dataframe[feature_bool].reset_index(
            drop=True)
        target_id_array = point_id_array[feature_bool].copy()
        target_coordinates_array = np.concatenate(
            [coordinates_dict[target_id_array[i]] for i in range(len(target_id_array))])
        target_gml_id_array = gml_id_array[feature_bool].copy()

        return target_attribute_dataframe, target_id_array, target_coordinates_array, target_gml_id_array

    def set_bound_By(self, output_element_core, target_plateau_crs, target_coordinates_array):
        """
        概要:
            指定された情報を使用してboundBy要素を設定し、更新された出力要素を返す関数。

        引数:
            output_element_core: 出力要素のコア部分。boundBy要素が追加
            target_plateau_crs: 目標の座標参照系（CRS）を示す文字列。
            target_coordinates_array: 出力対象の座標配列

        戻り値:
            output_element_core: 更新された出力要素のコア部分。boundBy要素が追加された状態
        """

        # boundBy設定
        boundby_unit = ET.SubElement(output_element_core, DDC.BOUNDBY_LIST[0])
        boundby_element = ET.SubElement(boundby_unit, DDC.BOUNDBY_LIST[1])

        boundby_element.set('srsDimension', '3')
        boundby_element.set(
            'srsName', r'http://www.opengis.net/def/crs/EPSG/0/' + target_plateau_crs)

        min_attribute_element = ET.Element(DDC.LOWER_STRING)
        max_attribute_element = ET.Element(DDC.UPPER_STRING)

        boundby_element.append(min_attribute_element)
        boundby_element.append(max_attribute_element)

        min_geometry_list = map(
            str, list(np.min(target_coordinates_array[:, 1:4], axis=0)))
        max_geometry_list = map(
            str, list(np.max(target_coordinates_array[:, 1:4], axis=0)))

        min_attribute_element.text = " ".join(min_geometry_list)
        max_attribute_element.text = " ".join(max_geometry_list)

        return output_element_core

    def extract_feature_string_and_geometry_str_to_list(self, geometry_distribution_name_list, feature_type_string, feature_tag_string):
        """
        概要:
            geometry_distribution_name_list から特定の地物と座標値の部分を抽出する。
            feature_type_string と feature_tag_string を使って、地物の開始位置と座標値の開始位置を特定する。
            feature_string_list には地物の文字列部分が、geometry_string_list には座標値の文字列部分が格納される。

        引数:
            geometry_distribution_name_list: ジオメトリの分布情報を表すリストとなり、各要素は文字列で空白区切りで構造化されている。
            feature_type_string: ジオメトリの種類を示す文字列
            feature_tag_string: ジオメトリのタグを示す文字列

        戻り値:
            feature_string_list: 地物を表す文字列のリスト
            geometry_string_list: 座標値を表す文字列のリスト
        """

        member_list = [temp.split(' ')
                       for temp in geometry_distribution_name_list][0]
        member_list = member_list[member_list.index(feature_type_string)+1:]

        # 地物ループ位置のインデックス
        feature_loop_index = member_list.index(feature_tag_string)

        # 地物の塊を表すところまで
        feature_string_list = member_list[0:feature_loop_index+1]

        # 座標値を入れる部分
        geometry_string_list = member_list[feature_loop_index+1:]

        return feature_string_list, geometry_string_list

    def get_coordinates_by_id(self, target_coordinates_array):
        """
        概要:
            target_coordinates_array から特定の列（xyz座標）を抽出し、 geometry_array として取得
            geometry_arrayをreshape し、各地物ごとに4つの座標セット（ID + xyz座標）が16個の要素で表される形のマルチパッチデータ
            マルチパッチデータから地物IDを取得し、各地物IDごとに構成点のxyz座標をマッピングした辞書 id_coordinate_dict を作成

        引数:
            target_coordinates_array: 座標情報を含むnumpy配列で、地物ごとの座標データが含まれる

        戻り値:
            id_coordinate_dict: キーとして地物ID、値としてその地物IDに属する座標データを持つ辞書データ
        """

        # npy読み込み時点のデータはここ
        # 4点1行の組み合わせにreshape + xyz座標のみ取得
        # [マルチパッチ]>[4点 id xyzが4つで16個の要素]
        geometry_array = target_coordinates_array[:, 1:4]

        # 座標値のみ
        geometry_array = geometry_array.reshape(int(len(geometry_array)/4), 12)
        multipatch_array2 = target_coordinates_array.reshape(
            int(len(target_coordinates_array)/4), 32)

        # IDのみ
        id_array = multipatch_array2[:, 0]

        # unique_ID
        id_unique_array = np.unique(id_array)

        # ここで地物IDごとのインデックス取得

        # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
        id_coordinate_dict = {id_unique_array[i]: geometry_array[list(np.where(
            id_array == id_unique_array[i])[0]), :] for i in range(len(id_unique_array))}

        return id_coordinate_dict

    def extract_attribute_arrays_from_target_attribute_dataframe(self, target_attribute_dataframe, feature_type_string):
        """
        概要:
            target_attribute_dataframe から属性データを抽出し、それぞれの属性に対応する配列（リスト）を取得

        引数:
            target_attribute_dataframe: Pandas の DataFrame。属性データが含まれる
            feature_type_string: 属性のタイプを示す文字列。この文字列が列名の一部として使用され、それ以降の属性名と値を特定。

        戻り値:
            attribute_array_list: 各属性に対応するデータを含むリスト
        """

        # target_attribute_dataframeのカラム名を取得
        all_attribute_name_list = target_attribute_dataframe.columns.values

        # 各カラム名を空白 ' ' を区切り文字として分割し、リスト化
        all_attribute_name_list = [temp_list.split(
            ' ') for temp_list in all_attribute_name_list]

        # 各カラム名リストに対して、feature_type_string が最初に出現する位置の次の要素（すなわち、属性名とその後の情報）を取得。これにより、各属性の名前を抽出する。
        all_attribute_name_list = [
            temp[temp.index(feature_type_string)+1:] for temp in all_attribute_name_list]

        # 属性値が設定されたarrayが要素のリスト
        # ★to_numpy注意 数値が勝手に小数に置き換わらないようにdtype=objectとする
        attribute_array_list = [target_attribute_dataframe[temp_column].to_numpy(
            dtype=object) for temp_column in target_attribute_dataframe.columns.values]

        return all_attribute_name_list, attribute_array_list

    def create_feature_element(self, output_element_core, feature_string, feature_type_string, target_gml_id_array, index):
        """
        概要:
            target_attribute_dataframe から属性データを抽出し、それぞれの属性に対応する配列（リスト）を取得

        引数:
            output_element_core: 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            feature_string: 地物の要素名を表す文字列
            feature_type_string: 地物の種類を表す文字列
            target_gml_id_array: 地物のgml_idの配列。各地物には一意のgml_idが設定されている
            feature_string_list: 地物の要素名から構成されるリスト。これは、地物の構造を表す階層的な要素のリスト

        戻り値:
            element_dict: 各要素の名前をキーとし、その要素のXML要素（Element）を値とする辞書
            output_element_tree: 地物のXML要素を表し、この要素は、親要素である
        """

        # element格納用defaultdict
        # Key:要素文字列 uro:...など
        # Value:Element
        element_dict = defaultdict(str)

        # 地物単位の要素追加
        output_element_tree_unit = ET.SubElement(
            output_element_core, feature_string)
        # 地物の地下埋設物種類
        output_element_tree = ET.SubElement(
            output_element_tree_unit, feature_type_string)

        # gml_idを地物IDとして設定
        output_element_tree.set('gml:id', target_gml_id_array[index, 1])

        return element_dict, output_element_tree

    def decide_to_add_attribute_to_tag(self, attribute_split_list, output_element_tree, attribute_array_list, all_attribute_name_list_index, index):
        """
        概要:
            指定された属性情報を使用して新しいXML要素を作成し、出力の要素ツリーに追加する関数。

        引数:
            attribute_split_list: 分割された属性のリスト
            output_element_tree: 出力のXML要素ツリーに新しい要素を追加するためのルート要素。
            attribute_array_list: 属性の配列リスト
            all_attribute_name_list_index: 全属性名リストのインデックス。
            index: id_coordinate_dictのインデックス

        戻り値:
            attribute_element: 新しく作成されたXML要素。
        """

        attribute_element = ET.Element(attribute_split_list[0][0])
        output_element_tree.append(attribute_element)
        attribute_element.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

        return attribute_element

    def append_attribute_to_tag(self, element_dict, attribute_split_list, attribute_array_list, all_attribute_name_list_index, index):
        """
        概要:
            要素の辞書に対して、指定されたタグに属性を追加する関数

        引数:
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            attribute_split_list: 分割された属性のリスト
            attribute_array_list: 属性の配列リスト
            all_attribute_name_list_index: 全属性名リストのインデックス
            index: id_coordinate_dictのインデックス

        戻り値:
            already_element_tree: 指定されたタグとテキストを持つ新しいXML要素
        """

        already_element_tree = ET.SubElement(
            element_dict[attribute_split_list[-2][0]], attribute_split_list[-1][0])
        already_element_tree.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

        return already_element_tree

    def add_attribute_to_tag_in_element_dict(self, attribute_split_list, attribute_array_list, all_attribute_name_list_index, index, element_dict, output_element_tree):
        """
        概要:
            要素の辞書に特定のタグに属性を追加する関数。

        引数:
            attribute_split_list: 分割された属性のリスト
            attribute_array_list: 属性配列のリスト
            all_attribute_name_list_index: 全属性名リストのインデックス
            index: id_coordinate_dictのインデックス
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            output_element_tree: 出力の要素ツリー

        戻り値:
            attribute_subelement: 属性の副要素
        """

        attribute_element, attribute_subelement, element_dict = WM.calc_func_time(
            self.logger, False)(NSP.create_element2)(attribute_split_list, element_dict)
        output_element_tree.append(attribute_element)
        attribute_subelement.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

        return attribute_subelement

    def xml_element_to_string_and_add_list(self, output_element_core, xml_value_list, type_list, dwh_list, target_unit_code_list, target_prefix, target_plateau_crs, target_unit_code_list_index):
        """
        概要:
            output_element_core を XML 文字列に変換し、それを指定されたリストに追加する処理を行う関数

        引数:
            output_element_core         - 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            xml_value_list              - output用に用意した空リスト
            type_list                   - output用に用意した空リスト
            dwh_list                    - output用に用意した空リスト
            target_unit_code_list       - 分割された地図情報2500レベルの図郭コードのリスト
            target_prefix               - 地下埋設物接頭辞(固定値)
            target_plateau_crs          - CRS
            target_unit_code_list_index - target_unit_code_list の要素のインデックス

        戻り値:
            dwh_list                    - XML文字列が追加された後のDWHファイル名のリスト
            type_list                   - 追加されたXML文字列のデータ型のリスト
            xml_value_list              - 追加されたXML文字列のリスト
        """

        # 全階層文字列へ
        xmlstr = ET.tostring(
            output_element_core, short_empty_elements=False, encoding='UTF-8', method='xml')

        # 別の機能でxml読み込み
        xmlstr = minidom.parseString(xmlstr)

        # インデントを指定して成形
        xmlstr = xmlstr.toprettyxml(indent="\t")

        # 改行文字をWindows用に変換
        xmlstr = xmlstr.replace("\n", "\r\n")

        xml_value_list.append(xmlstr)

        xml_type = "str"
        type_list.append(xml_type)

        # ファイルを指定して出力 ファイル名についてはPLATEAU標準製品仕様書 9.6_地下埋設物における特記事項参照
        # [図郭コード]_[地物型]_[CRS]_[オプション]
        # 国土基本図図郭コード + 'unf' + 10170(6677第9系) デリミタは'_'
        gml_file_name = '_'.join(
            [target_unit_code_list[target_unit_code_list_index], target_prefix, target_plateau_crs])

        dwh_list.append(gml_file_name)

        return dwh_list, type_list, xml_value_list

    # -----------------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:

            data_definition_delimiter, \
                center_dwh_name, \
                gml_id_dwh_name, \
                manhole_coordinate_dwh_name, \
                results_dwh_name, \
                feature_tag_string, \
                level2500_unit_code_string, \
                output_target_crs_string, \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            data_definition_stream, \
                field_set_file_data_frame, \
                results_data\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile, results_dwh_name)

            # データ定義から構成点データと属性項目ファイル群のDWHファイル名、属性名リスト取得
            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                attribute_file_type_list, \
                attribute_dwh_file_name_list, \
                attribute_distribution_name_list, \
                attribute_const_value_list, \
                attribute_data_type_list, \
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stream,
                                                                                         data_definition_delimiter=data_definition_delimiter)

            # -----------------------------------------------------------------------------------------------------------
            # field_set_dataframeからcoordinates_arrayを抜き出し、coordinates_dictに変換する
            # -----------------------------------------------------------------------------------------------------------
            coordinates_id_array, \
                coordinates_dict\
                = WM.calc_func_time(self.logger)(self.create_coordinates_id_array_and_dict_from_coordinates_array)(field_set_file_data_frame,
                                                                                                                   geometry_dwh_file_name_list)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】中心点npy取得 地上の点のみ抽出
            # -----------------------------------------------------------------------------------------------------------
            point_id_array, \
                point_dict\
                = WM.calc_func_time(self.logger)(self.create_point_dict_from_field_set_file_data_frame)(field_set_file_data_frame,
                                                                                                        center_dwh_name)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
            # -----------------------------------------------------------------------------------------------------------
            attribute_feature_id_array, \
                all_attribute_dataframe\
                = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe_datatype)(field_set_file_data_frame,
                                                                                          attribute_dwh_file_name_list,
                                                                                          attribute_distribution_name_list,
                                                                                          attribute_const_value_list,
                                                                                          attribute_file_type_list,
                                                                                          attribute_data_type_list,
                                                                                          len(
                                                                                              coordinates_id_array),
                                                                                          encoding="utf-8",
                                                                                          input_file_type=1  # dump指定
                                                                                          )

            # -----------------------------------------------------------------------------------------------------------
            # 出力対象図郭取得
            # -----------------------------------------------------------------------------------------------------------
            target_unit_code_list\
                = level2500_unit_code_string.split(DDC.UNIT_CODE_DELIMITER)

            # -----------------------------------------------------------------------------------------------------------
            # 出力対象CRS取得
            # -----------------------------------------------------------------------------------------------------------
            target_plateau_crs\
                = DDC.PLATEAU_CRS_DICT[output_target_crs_string]

            # -----------------------------------------------------------------------------------------------------------
            # gml_id取得
            # -----------------------------------------------------------------------------------------------------------
            gml_id_array\
                = WM.calc_func_time(self.logger)(self.get_gml_id_array_from_field_set_file_data_frame)(field_set_file_data_frame,
                                                                                                     gml_id_dwh_name)

            # 指定されたDwh列に対応する蓋MH座標リストを取得
            mh_cover_coordinates_array = WM.calc_func_time(self.logger)(self.get_manhole_coordinate_from_field_set_file_data_frame)(field_set_file_data_frame,
                                                                                                                                    manhole_coordinate_dwh_name)

            # 全体のマルチパッチID : 全体の蓋MHのID
            try:

                link_dict = {}
                for polygon_id, point_id in results_data:
                    if polygon_id not in link_dict:
                        link_dict[polygon_id] = []
                    link_dict[polygon_id].append(point_id)

            except Exception as e:
                self.logger.error(traceback.format_exc())

            # output用にリストに追加
            dwh_list = []
            type_list = []
            xml_value_list = []

            # 出力対象図郭ごとに処理
            for target_unit_code_list_index in range(len(target_unit_code_list)):

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】出力対象図郭情報取得 本当は図郭名が正しいか判定したい
                # -----------------------------------------------------------------------------------------------------------
                unit_origin_array, \
                    level_mesh_array\
                    = WM.calc_func_time(self.logger)(NSP.get_unit_origin)(
                        target_unit_code_list[target_unit_code_list_index])

                # -----------------------------------------------------------------------------------------------------------
                # 【抽出】国土基本図図郭内に存在するlasの座標を抽出 bool配列なので属性も同様に抽出
                # -----------------------------------------------------------------------------------------------------------
                point_bool = [WM.calc_func_time(self.logger)
                              (NSP.judge_citygml)(point_dict[point_id_array[i]][:, :2],
                                                  unit_origin_array,
                                                  level_mesh_array)
                              for i in range(len(point_id_array))]

                # 出力対象がなければ次へ
                if np.any(point_bool) == True:
                    pass
                else:
                    continue

                # 出力対象抽出
                target_attribute_dataframe, \
                    target_id_array, \
                    target_coordinates_array, \
                    target_gml_id_array\
                    = WM.calc_func_time(self.logger)(self.output_target_extraction_specifically_feature_bool)(all_attribute_dataframe,
                                                                                                              point_id_array,
                                                                                                              coordinates_dict,
                                                                                                              gml_id_array,
                                                                                                              point_bool
                                                                                                              )

                # -----------------------------------------------------------------------------------------------------------
                feature_string = [temp.split(DDC.XML_TAG_DELIMITER)
                                  for temp in geometry_distribution_name_list][0][0]
                feature_type_string = [temp.split(
                    DDC.XML_TAG_DELIMITER) for temp in geometry_distribution_name_list][0][1]

                # -----------------------------------------------------------------------------------------------------------
                # 変換処理
                # -----------------------------------------------------------------------------------------------------------
                # 親ノード作成
                output_element_core = ET.Element(DDC.CORE_STRING)

                # ここの固定値はすべて書き連ねる
                [output_element_core.set(DDC.CITY_MODEL_ATTRIBUTE[cmi][0], DDC.CITY_MODEL_ATTRIBUTE[cmi][1])
                 for cmi in range(len(DDC.CITY_MODEL_ATTRIBUTE))]

                # boundBy設定
                output_element_core\
                    = WM.calc_func_time(self.logger)(self.set_bound_By)(output_element_core,
                                                                        target_plateau_crs,
                                                                        target_coordinates_array)

                # -----------------------------------------------------------------------------------------------------------
                # ジオメトリデータ
                # -----------------------------------------------------------------------------------------------------------
                # 以下属性、ジオメトリ用データ加工
                # すべてメソッドにすること
                feature_string_list, \
                    geometry_string_list\
                    = WM.calc_func_time(self.logger)(self.extract_feature_string_and_geometry_str_to_list)(geometry_distribution_name_list,
                                                                                                           feature_type_string,
                                                                                                           feature_tag_string
                                                                                                           )

                # npy読み込み時点のデータはここ
                # 4点1行の組み合わせに組み替える + xyz座標のみ取得
                # [マルチパッチ]>[4点 ID xyzが4つで16個の要素]
                id_coordinate_dict\
                    = WM.calc_func_time(self.logger)(self.get_coordinates_by_id)(target_coordinates_array)

                # -----------------------------------------------------------------------------------------------------------
                # 属性データ追加用要素作成
                # -----------------------------------------------------------------------------------------------------------
                all_attribute_name_list, \
                    attribute_array_list\
                    = WM.calc_func_time(self.logger)(self.extract_attribute_arrays_from_target_attribute_dataframe)(target_attribute_dataframe,
                                                                                                                    feature_type_string)

                # -----------------------------------------------------------------------------------------------------------
                # 属性の設定 地物ごとのループ
                # -----------------------------------------------------------------------------------------------------------
                for index, dict_items in enumerate(id_coordinate_dict.items()):

                    element_dict, \
                        output_element_tree\
                        = WM.calc_func_time(self.logger)(self.create_feature_element)(output_element_core,
                                                                                      feature_string,
                                                                                      feature_type_string,
                                                                                      target_gml_id_array,
                                                                                      index)

                    # -----------------------------------------------------------------------------------------------------------
                    # 属性追加
                    # -----------------------------------------------------------------------------------------------------------
                    for all_attribute_name_list_index in range(len(all_attribute_name_list)):

                        attribute_split_list = [temp.split(
                            XML_ATTRIBUTE_DELIMITER) for temp in all_attribute_name_list[all_attribute_name_list_index]]

                        if len(attribute_split_list) == 1:

                            # 中でタグに属性追加するか判定
                            attribute_element\
                                = WM.calc_func_time(self.logger)(self.decide_to_add_attribute_to_tag)(attribute_split_list,
                                                                                                      output_element_tree,
                                                                                                      attribute_array_list,
                                                                                                      all_attribute_name_list_index,
                                                                                                      index)

                            # '|'でsplitした結果複数の要素が存在する場合2つ目以降の要素は属性として登録する
                            # 文字列は'='でsplitして最初の要素を属性名、属性値とする

                            # 追加タグの要素数チェック 属性が入っているなら追加
                            if len(attribute_split_list[0]) > 1:

                                # 1つ目以降の要素
                                value_list = attribute_split_list[0][1:]

                                # '='でsplitして属性名と属性値に分ける
                                value_split_list = [value_list[spi].split(
                                    DDC.XML_ATTRIBUTE_VALUE_DELIMITER) for spi in range(len(value_list))]

                                # 追加
                                [attribute_element.set(value_split_list[vi][0], value_split_list[vi][1]) for vi in range(
                                    len(value_split_list))]
                            else:
                                pass

                            # 追加対象タグ名
                            add_target_tag_name = attribute_split_list[0][0]

                        elif attribute_split_list[-2][0] in element_dict:

                            already_element_tree\
                                = WM.calc_func_time(self.logger)(self.append_attribute_to_tag)(element_dict,
                                                                                               attribute_split_list,
                                                                                               attribute_array_list,
                                                                                               all_attribute_name_list_index,
                                                                                               index)

                            # 追加タグの要素数チェック 属性が入っているなら追加
                            if len(attribute_split_list[-1]) > 1:

                                # 1つ目以降の要素
                                value_list = attribute_split_list[-1][1:]

                                # '='でsplitして属性名と属性値に分ける
                                value_split_list = [value_list[spi].split(
                                    DDC.XML_ATTRIBUTE_VALUE_DELIMITER) for spi in range(len(value_list))]

                                # 追加
                                [already_element_tree.set(
                                    value_split_list[vi][0], value_split_list[vi][1]) for vi in range(len(value_split_list))]
                            else:
                                pass

                            # 追加対象タグ名
                            add_target_tag_name = attribute_split_list[-1][0]

                        else:
                            attribute_subelement\
                                = WM.calc_func_time(self.logger)(self.add_attribute_to_tag_in_element_dict)(attribute_split_list,
                                                                                                            attribute_array_list,
                                                                                                            all_attribute_name_list_index,
                                                                                                            index,
                                                                                                            element_dict,
                                                                                                            output_element_tree)

                            # 追加タグの要素数チェック 属性が入っているなら追加
                            if len(attribute_split_list[-1]) > 1:

                                # 1つ目以降の要素
                                value_list = attribute_split_list[-1][1:]

                                # '='でsplitして属性名と属性値に分ける
                                value_split_list = [value_list[spi].split(
                                    DDC.XML_ATTRIBUTE_VALUE_DELIMITER) for spi in range(len(value_list))]

                                # 追加
                                [attribute_subelement.set(
                                    value_split_list[vi][0], value_split_list[vi][1]) for vi in range(len(value_split_list))]
                            else:
                                pass
                            # 追加対象タグ名
                            add_target_tag_name = attribute_split_list[-1][0]

                        # -----------------------------------------------------------------------------------------------------------
                        # 地物のsubelementに対してマルチパッチの座標設定
                        # -----------------------------------------------------------------------------------------------------------
                        # 地物のsubelementに対してマルチパッチの数だけ追加

                        if add_target_tag_name == 'frn:function':

                            feature_element, feature_subelement, element_dict = WM.calc_func_time(
                                self.logger)(NSP.create_element)(feature_string_list, element_dict)

                            # 地物のelement追加
                            output_element_tree.append(feature_element)
                            for gi in range(len(dict_items[1])):
                                # マルチパッチのelement
                                geometry_element, geometry_subelement, element_dict = WM.calc_func_time(
                                    self.logger)(NSP.create_element)(geometry_string_list, element_dict)

                                feature_subelement.append(geometry_element)

                                # ここでマルチパッチごとのループ
                                # マルチパッチごとのタグ追加最後のpostに4点の座標値を結合させた文字列を設定
                                geometry_subelement.text = " ".join(
                                    list(map(str, dict_items[1][gi])))
                        else:
                            pass
                        # -----------------------------------------------------------------------------------------------------------

                        if add_target_tag_name == 'uro:administrator':
                            # -----------------------------------------------------------------------------------------------------------
                            # テーマティックシェープ属性追加 複数のpointを設定できるようにIDを紐づけてpointを取得
                            # -----------------------------------------------------------------------------------------------------------

                            thematic_feature_element, thematic_feature_subelement, element_dict = WM.calc_func_time(
                                self.logger)(NSP.create_element)(DDC.THEMATICSHAPE_FEATURE_LIST, element_dict)
                            thematic_geometry_element, thematic_geometry_subelement, element_dict = WM.calc_func_time(
                                self.logger)(NSP.create_element)(THEMATICSHAPE_GEOMETRY_LIST, element_dict)

                            output_element_tree.append(
                                thematic_feature_element)
                            height_element = ET.Element('uro:heightType')
                            thematic_feature_subelement.append(height_element)
                            height_element.text = '2'
                            height_element.set(
                                'codeSpace', r'../../codelists/ThematicShape_heightType.xml')

                            thematic_feature_subelement.append(
                                thematic_geometry_element)

                            try:
                                link_index = link_dict[int(
                                    target_id_array[index])]
                            except Exception as e:
                                self.logger.error(traceback.format_exc())

                            # 紐付きがなければとばす
                            if len(link_index) == 0:
                                pass
                            else:
                                for temp_index in link_index:

                                    # point用のXML要素とサブ要素を作成し、element_dictに追加
                                    thematic_element, thematic_subelement, element_dict = WM.calc_func_time(
                                        self.logger, False)(NSP.create_element)(DDC.POINT_THEMATICSHAPE_LIST, element_dict)
                                    output_element_tree.append(
                                        thematic_element)

                                    # "uro:heightType"要素を作成し、高さ情報を設定
                                    height_element = ET.Element(
                                        "uro:heightType")
                                    thematic_element.append(height_element)
                                    height_element.text = "2"

                                    # マンホールの座標情報を取得し、point用のXMLのサブ要素のテキストとして設定
                                    try:
                                        thematic_geometry = list(
                                            map(str, mh_cover_coordinates_array[temp_index, 1:4].flatten()))
                                    except Exception:
                                        self.logger.error(
                                            traceback.format_exc())

                                    thematic_subelement.text = " ".join(
                                        thematic_geometry)

                        else:
                            pass
                    # -----------------------------------------------------------------------------------------------------------

                dwh_list, \
                    type_list, \
                    xml_value_list\
                    = WM.calc_func_time(self.logger)(self.xml_element_to_string_and_add_list)(output_element_core,
                                                                                              xml_value_list,
                                                                                              type_list,
                                                                                              dwh_list,
                                                                                              target_unit_code_list,
                                                                                              DDC.TARGET_PREFIX,
                                                                                              target_plateau_crs,
                                                                                              target_unit_code_list_index)

                output_field_set_file\
                    = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                             type_list,
                                                                             xml_value_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
