# MIT License
# 
# Copyright (c) 2025,2026 NTT InfraNet
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
# 中心線を構成する座標の情報をもとに、属性を付加しCityGMLの形式に変換するプロセッサ。
# NoThematicとは中心線の情報を含まないことを意味する。
# ---------------------------------------------------------------------------------------------------------‐

# Python標準ライブラリ
import io
import zipfile
import traceback
import pickle
import base64
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict

from importlib import import_module


# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


pd = import_module("pandas")
np = import_module("numpy")
unary_union = import_module("shapely.ops").unary_union
Polygon = import_module("shapely.geometry").Polygon

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertLineStringCoordinatesToCityGMLNoThematic(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、中心線のないLineStringからCityGML形式のファイルを作成する。
                        ①input: 出力したいデータ項目を持ったFieldSetFile。
                        ②output: 国土基本図郭ごとのCityGMLファイルを持った、1行以上のFieldSetFile。
                      """
        tags = ['Coordinates', 'CityGML', 'gml', 'xml', 'Python']

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

    # coordinates_array(座標配列)のDWH
    CENTER_DWH_NAME = PropertyDescriptor(
        name="Center DWH Name",
        description="入力データの座標配列のDWH名",
        required=False,
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

    # 建物の面情報を示すタグを設定
    FEATURE_TAG_STRING = PropertyDescriptor(
        name="Feature Tag String",
        description="建物の面情報を示すタグ",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
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

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            CENTER_DWH_NAME,
                            GML_ID_DWH_NAME,
                            FEATURE_TAG_STRING,
                            LEVEL2500_UNIT_CODE_STRING,
                            OUTPUT_TARGET_CRS_STRING,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得し、float型に加工する関数
        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            data_definition_delimiter: データ定義ファイルの区切り文字
            center_dwh_name: 中心線のDWH
            gml_id_dwh_name: gml_idのDWH
            feature_tag_string: ジオメトリループ用tag
            level2500_unit_code_string: 出力対象レベル2500国土基本図図郭コード文字列郡
            output_target_crs_string: 出力対象CRS
            output_zip_flag: ZIP圧縮するかどうかのフラグ
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

        # ジオメトリループ用tag
        feature_tag_string = context.getProperty(
            self.FEATURE_TAG_STRING).getValue()

        # 出力対象レベル2500国土基本図図郭コード文字列郡
        level2500_unit_code_string = context.getProperty(
            self.LEVEL2500_UNIT_CODE_STRING).evaluateAttributeExpressions(flowfile).getValue()

        # 出力対象CRS
        output_target_crs_string = context.getProperty(
            self.OUTPUT_TARGET_CRS_STRING).evaluateAttributeExpressions(flowfile).getValue()

        # ZIP圧縮するかどうかのフラグ
        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()


        return data_definition_delimiter, center_dwh_name, gml_id_dwh_name, feature_tag_string, level2500_unit_code_string, output_target_crs_string, output_zip_flag

    def get_flowfile(self, flowfile):
        """
        概要:
            flowfileから、DataDefinitionというattributeのキーから値を取得し、
            (データ定義ファイル)次に、input_field_set_fileをDataFrameに加工する関数

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            data_definition_stream: データ定義ファイル(csv形式)
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
        """

        # FlowFileから"DataDefinition"属性の内容を取得する
        data_definition = flowfile.getAttribute("DataDefinition")

        # data_definitionを、pathとして扱う。
        data_definition_stream = io.StringIO(data_definition)

        # Flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        return data_definition_stream, field_set_file_data_frame

    def create_coordinates_id_array_and_dict_from_coordinates_array(self, field_set_file_data_frame, geometry_dwh_file_name_list):
        """
        概要:
            ジオメトリ配列を取得し、マルチパッチのIDをキーとし、その座標データを値とした辞書に変換する関数。

        引数:
            field_set_file_data_frame: input_field_set_fileをDataFrameに加工したデータ
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

    def extract_coordinates_array_from_field_set_file(self, field_set_file_data_frame, geometry_file_name_list):
        """
        概要:
            field_set_file_data_frameのDwh列で、geometry_file_name_listの0番目(geometryのDwhファイル名)の値の行とValue列の値を抽出し
            base64でデコード、デシリアライズし配列に戻す関数

        引数:
            field_set_file_data_frame: FieldSetFileをDataFrameに加工した物
            geometry_file_name_list: データ定義ファイル内のgeometryのDwhファイル名が格納されているリスト(基本要素は1つ)

        戻り値:
            geometry_value_coordinates_array: 座標配列
        """

        # field_set_file_data_frameから、マルチパッチを抽出。
        # Value列且つ、Dwh列の値がgeometry_file_name_listのインデックスが0番目
        geometry_value_coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_file_name_list[0], "Value"].values[0]))

        return geometry_value_coordinates_array

    def create_linestring_dict_from_field_set_file_data_frame(self, field_set_file_data_frame, center_dwh_name):
        """
        概要:
            field_set_file_data_frameの指定されたDwh列から中心線の座標配列を抽出し、
            IDをキーとした中心線の辞書を作成する

        引数:
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
            center_dwh_name: 中心線のDWH

        戻り値:
            linestring_id_array: 抽出された中心線のIDを格納した numpy 配列
            linestring_dict: 中心線のIDをキーとし、その座標データを値とした辞書
        """

        linestring_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == center_dwh_name, "Value"].values[0]))
        linestring_id_array = np.unique(linestring_array[:, 0])
        linestring_dict = {linestring_id_array[i]: linestring_array[list(np.where(
            linestring_array[:, 0] == linestring_id_array[i])[0]), 1:4] for i in range(len(linestring_id_array))}

        return linestring_id_array, linestring_dict

    def get_target_unit_code_list(self, level2500_unit_code_string, unit_code_delimiter):
        """
        概要:
            レベル2500ユニットコード文字列を指定の区切り文字で分割してリスト化する

        引数:
            level2500_unit_code_string: レベル2500ユニットコードを含む文字列
            unit_code_delimiter: 分割するための区切り文字

        戻り値:
            target_unit_code_list: 分割されたレベル2500ユニットコードのリスト numpy 配列
        """

        target_unit_code_list = level2500_unit_code_string.split(
            unit_code_delimiter)

        return target_unit_code_list

    def get_output_target_crs(self, plateau_crs_dict, output_target_crs_string):
        """
        概要:
            CRS文字列をキーとして、辞書から対応するCRSを取得する

        引数:
            plateau_crs_dict: CRSの辞書、CRS文字列をキーとする
            output_target_crs_string: 出力対象CRS

        戻り値:
            target_plateau_crs: 出力対象CRS
        """

        target_plateau_crs = plateau_crs_dict[output_target_crs_string]

        return target_plateau_crs

    def get_gml_id_array_from_field_set_file_data_frame(self, field_set_file_data_frame, gml_id_dwh_name):
        """
        概要:
            field_set_file_data_frameの指定されたDwh列からgml_idリストを抽出し、numpy配列として返す

        引数:
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
            gml_id_dwh_name: gml_idのDWH

        戻り値:
            gml_id_array: 抽出されたgml_idの配列
        """

        gml_id_list = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == gml_id_dwh_name, "Value"].values[0]))
        gml_id_array = np.array(gml_id_list, dtype=object)

        return gml_id_array

    def output_target_extraction_specifically_feature_bool(self,
                                                           all_attribute_dataframe,
                                                           linestring_id_array,
                                                           coordinates_dict,
                                                           gml_id_array,
                                                           feature_bool
                                                           ):
        """
        概要:
            与えられた条件(feature_bool)に基づいて特定の属性データ (target_attribute_dataframe)とジオメトリデータ(target_coordinates_array)を抽出する

        引数:
            all_attribute_dataframe: 全ての属性データを含むDataFrame
            linestring_id_array: ラインストリングのIDを格納した配列
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
        target_id_array = linestring_id_array[feature_bool].copy()
        target_coordinates_array = np.concatenate(
            [coordinates_dict[target_id_array[i]] for i in range(len(target_id_array))])
        target_gml_id_array = gml_id_array[feature_bool].copy()

        return target_attribute_dataframe, target_id_array, target_coordinates_array, target_gml_id_array

    def extract_feature_string_and_geometry_str_to_list(self,
                                                        geometry_distribution_name_list,
                                                        feature_type_string,
                                                        feature_tag_string
                                                        ):
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
        # [マルチパッチ]>[4点 ID xyzが4つで16個の要素]
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
            feature_type_string: 属性のタイプを示す文字列。この文字列が列名の一部として使用され、それ以降の属性名と値を特定します。

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

        # 属性値が設定されたarrayが要素のlist
        # to_numpy注意 数値が勝手に小数に置き換わらないようにdtype=objectとする
        attribute_array_list = [target_attribute_dataframe[temp_column].to_numpy(
            dtype=object) for temp_column in target_attribute_dataframe.columns.values]

        return all_attribute_name_list, attribute_array_list

    def create_feature_element(self,
                               output_element_core,
                               feature_string,
                               feature_type_string,
                               target_gml_id_array,
                               index
                               ):
        """
        概要:
            target_attribute_dataframe から属性データを抽出し、それぞれの属性に対応する配列（リスト）を取得

        引数:
            output_element_core: 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            feature_string: 地物の要素名を表す文字列
            feature_type_string: 地物の種類を表す文字列
            target_gml_id_array: 地物のgml_idの配列です。各地物には一意のgml_idが設定されている
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

        # gml_idを地物idとして設定
        output_element_tree.set('gml:id', target_gml_id_array[index, 1])

        return element_dict, output_element_tree

    def add_attribute_to_element(self, element_list, element_object):
        
        if len(element_list)>1:

            for element in element_list[1:]:
                # 1つ目以降の要素
                # '='でsplitして属性名と属性値に分ける
                value_split_list = element.split(DDC.XML_ATTRIBUTE_VALUE_DELIMITER)
                
                # 追加
                element_object.set(value_split_list[0],
                                value_split_list[1])
        else:
            pass
        
        return element_object

    def create_element_attribute(self, element_list, element_dict):
        # ---------------------------------------------------------------
        # ネストされた要素を作成 最後の要素を返す←値を追加する用
        # 引数1：要素名List 要素はstr
        # ---------------------------------------------------------------
        root = ET.Element(element_list[0][0])
        root=self.add_attribute_to_element(element_list[0], root)
        element_dict["-".join(element_list[0])] = root
        
        if len(element_list)==1:
            return root, root, element_dict
        else:
            pass

        sub_element = ET.SubElement(root, element_list[1][0])
        sub_element=self.add_attribute_to_element(element_list[1],sub_element)
        element_dict["-".join(element_list[1])] = sub_element

        for fi in range(len(element_list)-2):
            sub_element = ET.SubElement(sub_element, element_list[fi + 2][0])
            sub_element=self.add_attribute_to_element(element_list[fi+2],sub_element)

            element_dict["-".join(element_list[fi+2])] = sub_element

        return root, sub_element, element_dict


    def decide_to_add_attribute_to_tag(self, attribute_split_list, output_element_tree, attribute_array_list, all_attribute_name_list_index, index):
        """
        概要:
            指定された属性情報を使用して新しいXML要素を作成し、出力の要素ツリーに追加する関数。

        引数:
            attribute_split_list: 分割された属性のリスト。
            output_element_tree: 出力のXML要素ツリーに新しい要素を追加するためのルート要素。
            attribute_array_list: 属性の配列リスト。
            all_attribute_name_list_index: 全属性名リストのインデックス。
            index: id_coordinate_dictのインデックス。

        戻り値:
            attribute_element: 新しく作成されたXML要素。
        """

        # attribute_element = ET.Element(attribute_split_list[0][0])
        # output_element_tree.append(attribute_element)
        # attribute_element.text = str(
        #     attribute_array_list[all_attribute_name_list_index][index])


        attribute_element = ET.Element(attribute_split_list[0][0])

        attribute_element\
            =self.add_attribute_to_element(attribute_split_list[0],
                                           attribute_element) 

        attribute_element.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

        output_element_tree.append(attribute_element)

        return attribute_element

    def append_attribute_to_tag(self,
                                element_dict,
                                attribute_split_list,
                                attribute_array_list,
                                all_attribute_name_list_index,
                                index,
                                key_bool):
        """
        概要:
            要素の辞書に対して、指定されたタグに属性を追加する関数

        引数:
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            attribute_split_list: 分割された属性のlist
            attribute_array_list: 属性の配列list
            all_attribute_name_list_index: 全属性名リストのインデックス
            index: id_coordinate_dictのインデックス

        戻り値:
            already_element_tree: 指定されたタグとテキストを持つ新しいXML要素
        """

        # 取り出すタグのインデックス特定
        # 初めてFalseがでるひとつ前のインデックス
        if np.all(key_bool):
            target_index=0
        else:
            target_index=np.min(np.where(np.logical_not(key_bool)))-1
            
        # 設定するタグの作成
        element_list=attribute_split_list[target_index+1:]
        sub_element=element_dict["-".join(attribute_split_list[target_index])]
        
        for fi in range(len(element_list)):
            sub_element = ET.SubElement(sub_element, element_list[fi][0])
            sub_element=self.add_attribute_to_element(element_list[fi],sub_element)

            element_dict["-".join(element_list[fi])] = sub_element
        
        # 地物の属性値を設定
        sub_element.text = str(
            attribute_array_list[all_attribute_name_list_index][index])
        
        return sub_element

    def add_attribute_to_tag_in_element_dict(self,
                                             attribute_split_list,
                                             attribute_array_list,
                                             all_attribute_name_list_index,
                                             index,
                                             element_dict,
                                             output_element_tree):
        """
        概要:
            要素の辞書に特定のタグに属性を追加する関数。

        引数:
            attribute_split_list: 分割された属性のlist
            attribute_array_list: 属性配列のリスト
            all_attribute_name_list_index: 全属性名リストのインデックス
            index: id_coordinate_dictのインデックス
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            output_element_tree: 出力の要素ツリー

        戻り値:
            attribute_subelement: 属性の副要素
        """

        attribute_element,\
        attribute_subelement,\
        element_dict\
            = self.create_element_attribute(attribute_split_list, element_dict)
        output_element_tree.append(attribute_element)
        attribute_subelement.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

        return attribute_subelement

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

    def xml_element_to_string_and_add_list(self,
                                           output_element_core,
                                           xml_value_list,
                                           type_list,
                                           dwh_list,
                                           target_unit_code_list,
                                           target_prefix,
                                           target_plateau_crs,
                                           target_unit_code_list_index):
        """
        概要:
            output_element_core を XML 文字列に変換し、それを指定されたリストに追加する処理を行う関数

        引数:
            output_element_core: 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            xml_value_list: output用に用意した空リスト
            type_list: output用に用意した空リスト
            dwh_list: output用に用意した空リスト
            target_unit_code_list: 分割された地図情報2500レベルの図郭コードのリスト
            target_prefix: 地下埋設物接頭辞(固定値)
            target_plateau_crs: CRS
            target_unit_code_list_index: target_unit_code_list の要素のインデックス

        戻り値:
            dwh_list: XML文字列が追加された後のDWHファイル名のリスト
            type_list: 追加されたXML文字列のデータ型のリスト
            xml_value_list: 追加されたXML文字列のリスト
        """

        # 全階層文字列へ
        xmlstr = ET.tostring(
            output_element_core, short_empty_elements=False, encoding='UTF-8', method='xml')

        # 別の機能でxml読み込み
        xmlstr = minidom.parseString(xmlstr)

        # インデントを指定して成形
        xmlstr = xmlstr.toprettyxml(indent="\t")

        # 改行文字をwindows用に変換
        xmlstr = xmlstr.replace("\n", "\r\n")

        # xmlstr を pickle でシリアライズし、base64 エンコードしてリストに追加する
        xml_value_list.append(xmlstr)

        xml_type = "str"
        type_list.append(xml_type)

        # ファイルを指定して出力(target_unit_code_list の要素 ti、文字列 target_prefix、および文字列 target_plateau_crs を _ で連結し、ファイル名として使う文字列 gml_file_name を作成
        gml_file_name = '_'.join(
            [target_unit_code_list[target_unit_code_list_index], target_prefix, target_plateau_crs])
        dwh_list.append(gml_file_name)

        return dwh_list, type_list, xml_value_list


    def multipatch_to_exterior_polygon(self, triangles):
        """
        マルチパッチ（三角面群）を2Dポリゴンの外周形状に変換する
        - 3D座標の z 値を無視し、XY 平面に射影して処理。
        - 各三角形を 2D Polygon に変換し、unary_union で結合することで
            重なりや隙間を吸収し、外形（輪郭）ポリゴンを抽出。
        - 結果が MultiPolygon の場合は、最も面積の大きいポリゴンを採用。

        引数:
            triangles(numpy.ndarray) : - 形状: (n_triangles, 3, 3)
                                        - 各三角形は3つの頂点 (x, y, z) を持つ。

        戻り値:
        shapely.geometry.Polygon or None
            - 三角面群の統合結果から得られる2Dポリゴンの外周境界（exterior）。
            - 入力データが不正または面として統合できない場合は None を返す。
        """

        triangles_2d = triangles[:, :, :2]

        # 三角形ごとにPolygon化
        polys = [Polygon(tri) for tri in triangles_2d]

        # 全ての三角形を union して外周だけを抽出
        merged = unary_union(polys)

        if merged.geom_type == 'Polygon':
            return Polygon(merged.exterior)

        elif merged.geom_type == 'MultiPolygon':
            largest = max(merged.geoms, key=lambda p: p.area)
            return Polygon(largest.exterior)

        else:
            return None


    def create_feature_rectangle_polygons(self, feature_array, level_mesh_array):
        """
        与えられた地物ポリゴンの外接矩形を基準に、交差するメッシュ矩形群を生成
            - 地物ポリゴンの bounding box を求める
            - 原点座標系に基づき、メッシュ単位で矩形を生成
            - 各矩形は左上 → 右上 → 右下 → 左下の順で座標を保持

        Parameters:
            feature_array (np.ndarray): 地物座標 (N, 2)
            level_mesh_array (np.ndarray): メッシュ単位配列 (M, 2) 単位: m

        Returns:
            np.ndarray: shape=(矩形数, 4, 2) 各矩形の 2D 座標
        """

        min_x = np.min(feature_array[:, 0])
        min_y = np.min(feature_array[:, 1])
        max_x = np.max(feature_array[:, 0])
        max_y = np.max(feature_array[:, 1])

        # 原点座標取得（NSP.get_origin_point_from_coordinates_array 依存）
        area_array = NSP.get_origin_point_from_coordinates_array(
            np.array([[min_x, max_y], [max_x, min_y]]), level_mesh_array)

        temp_origin_array = area_array[0].copy()

        x_number = int(((area_array[1, 0] - area_array[0, 0]) / level_mesh_array[-1, 0])) + 1
        y_number = int(((area_array[0, 1] - area_array[1, 1]) / level_mesh_array[-1, 1])) + 1

        # メッシュ方向ベクトル
        x_mesh_array = np.array([level_mesh_array[-1, 0], 0])
        y_mesh_array = np.array([0, -level_mesh_array[-1, 1]])

        rectangles = []

        for xi in range(x_number):
            for yi in range(y_number):

                # 左上の点
                p1 = temp_origin_array + x_mesh_array * xi + y_mesh_array * yi
                # 右上
                p2 = p1 + x_mesh_array
                # 右下
                p3 = p2 + y_mesh_array
                # 左下
                p4 = p1 + y_mesh_array

                rectangles.append(np.array([p1, p2, p3, p4]))
 
        return np.array(rectangles)


    def is_inside(self, p, edge_start, edge_end, ccw=True):
        """
        点 p が指定した辺の内側にあるか判定（Sutherland–Hodgman 用）

        Parameters:
            p (np.ndarray): 判定対象点 (x, y)
            edge_start (np.ndarray): 辺の始点 (x, y)
            edge_end (np.ndarray): 辺の終点 (x, y)
            ccw (bool): True = 反時計回り（左が内側）、False = 時計回り

        Returns:
            bool: 内側なら True
        """

        # 外積計算で点の相対位置を判定
        cross = (edge_end[0] - edge_start[0]) * (p[1] - edge_start[1]) - \
                (edge_end[1] - edge_start[1]) * (p[0] - edge_start[0])

        return cross >= 0 if ccw else cross <= 0


    def clip_polygon(self, subject_polygon, clip_polygon, clip_ccw=1):
        """
        多角形 subject_polygon を clip_polygon 内部に切り取る（Sutherland–Hodgmanアルゴリズム）

        Parameters:
            subject_polygon (np.ndarray): 切り取られるポリゴン (N, 2)
            clip_polygon (np.ndarray): クリップポリゴン (M, 2)
            clip_ccw (int): 1=反時計回り, 0=時計回り

        Returns:
            np.ndarray: 切り取られたポリゴン頂点 (K, 2)
        """

        ccw = bool(clip_ccw)  # 1 → True（左側が内側）, 0 → False（右側が内側）
        output_list = subject_polygon  # 最初は全体が残っている

        # クリップポリゴンの各辺（順にループ）
        for i in range(len(clip_polygon)):
            input_list = output_list  # 前のループの出力を入力として使用
            output_list = []  # 今回のループの出力初期化

            cp1 = clip_polygon[i]                            # この辺の始点
            cp2 = clip_polygon[(i + 1) % len(clip_polygon)]  # この辺の終点（循環）

            if len(input_list) == 0:
                break  # 切り取り後に点がないなら中断

            s = input_list[-1]  # 始点（初回は最後の点）

            for e in input_list:  # 各辺 s → e を調べる

                line_array=np.array([cp1, cp2])

                unit_array=np.array([[s, e]])

                if self.is_inside(e, cp1, cp2, ccw):  # 終点が内側なら

                    if not self.is_inside(s, cp1, cp2, ccw):
                        # 始点が外で終点が内 → 交点追加
                        output_list.append(NSP.get_intersect_point_array(line_array, unit_array)[0])

                    # 終点（内側）を追加
                    output_list.append(e)

                elif self.is_inside(s, cp1, cp2, ccw):
                    # 始点が内で終点が外 → 交点だけ追加（終点は外なので追加しない）
                    output_list.append(NSP.get_intersect_point_array(line_array, unit_array)[0])

                # 次の辺へ（今の終点が次の始点になる）
                s = e

        return np.array(output_list)


    def calculate_polygon_area(self, coords):
        """
        与えられた多角形の面積を靴紐公式（Shoelace formula）で計算

        Parameters:
            coords(np.ndarray): (N, 2)配列の多角形頂点（閉じている必要はない）

        Returns:
            area(floatI): 計算された面積（正の値）
        """

        x = coords[:, 0]
        y = coords[:, 1]

        # 座標を1つずらして結合することで「隣接する点同士の積」を作る
        x_next = np.roll(x, -1)
        y_next = np.roll(y, -1)

        area = 0.5 * np.abs(np.dot(x, y_next) - np.dot(x_next, y))

        return area


    def transform_polygon_orientation(self, coordinates_array, direction=1):
        """
        多角形の頂点座標を指定された向きに並べ替え、始点と終点を閉じた状態に

        coordinates_array (np.ndarray): (N, 2) の配列
        direction (int): 0=時計回り, 1=反時計回り

        Returns
        coords (np.ndarray): 指定方向に揃えられた多角形座標配列
        """

        # 配列をコピーして編集用にする
        coords = coordinates_array.copy()

        # 元の始点を保持
        start_point = coords[0].copy()

        # 閉じている場合は削除
        closed = np.array_equal(coords[0], coords[-1])
        if closed:
            coords = coords[:-1]

        # 面積計算
        x, y = coords[:, 0], coords[:, 1]
        x_next, y_next = np.roll(x, -1), np.roll(y, -1)
        signed_area = 0.5 * np.sum(x * y_next - x_next * y)

        current_ccw = 1 if signed_area > 0 else 0

        # 方向修正
        if current_ccw != direction:
            coords = coords[::-1]
            # 元の始点を最後に追加して閉じる
            coords = np.vstack([start_point[np.newaxis, :], coords])

        else:
            # 元の始点を最後に追加して閉じる
            coords = np.vstack([coords, start_point[np.newaxis, :]])

        return coords


    def judge_citygml_multipatch(self, feature_array, unit_origin_array, level_mesh_array):
        """
        CityGML形式のMultiPatch地物が、与えられたメッシュ単位内に含まれるか判定
            1. 図郭の境界を取得
            2. MultiPatchの外周を計算
            3. 地物の座標が図郭内に完全に含まれるかを判定
            4. 矩形を作成し、各矩形と地物外周との交差面積を計算
            5. 図郭内の地物面積が最大かどうかで最終判定

        Parameters:
            feature_array (np.ndarray): MultiPatch地物の座標配列 shape=(N, 2)
                                        Nは全頂点数（通常4頂点×三角形数など）
            unit_origin_array (np.ndarray): 図郭の原点座標配列
            level_mesh_array (np.ndarray): メッシュ単位のサイズ配列 shape=(N, 2) 単位:m

        Returns:
            bool: 地物が指定図郭内に含まれる場合 True, そうでなければ False
        """

        # 図郭の最小・最大座標取得（左上・右下）
        unit_min_x, unit_max_x, unit_min_y, unit_max_y = NSP.get_unit_min_max(
            unit_origin_array, level_mesh_array)

        # 図郭原点座標（左上を基準に整数化）
        origin_point = (int(unit_min_x), int(unit_max_y))

        # MultiPatch地物を4頂点×三角形に整形し、外周を取得
        exterior_polygon = self.multipatch_to_exterior_polygon(feature_array.reshape(int(len(feature_array)/4), 4, 2))

        # 外周が存在すれば座標配列に変換、存在しなければ空配列
        if exterior_polygon is not None and not exterior_polygon.is_empty:
            exterior_array = np.array(exterior_polygon.exterior.coords)
        else:
            exterior_array = np.empty((0, 2))  # 空配列でフォールバック

        # 地物の各頂点が図郭内に存在するか判定
        isin_bool = NSP.get_bool_in_rectangle_area(
            unit_min_x, unit_max_x, unit_min_y, unit_max_y, feature_array[:, 0], feature_array[:, 1])

        # 地物が図郭の外に完全にあるかチェック（全てのx,yが境界外）
        bool5 = np.all(feature_array[:, 0] < unit_min_x)  # 左側
        bool6 = np.all(unit_max_x < feature_array[:, 0])  # 右側
        bool7 = np.all(feature_array[:, 1] < unit_min_y)  # 下側
        bool8 = np.all(unit_max_y < feature_array[:, 1])  # 上側

        # 全頂点が図郭内にあれば True
        if np.all(isin_bool):
            return True

        elif np.any([bool5, bool6, bool7, bool8]):
            return False

        else:

            # 外周から外接矩形を生成し、図郭メッシュとの交差面積を算出
            temp_unit_array = self.create_feature_rectangle_polygons(exterior_array, level_mesh_array)

            temp_dict = {}

            for i in range(len(temp_unit_array)):
                coords = temp_unit_array[i]

                # 各矩形の左上座標をキーとして使用
                min_x = int(np.min(coords[:, 0]))
                max_y = int(np.max(coords[:, 1]))
                key = (min_x, max_y)

                oriented_exterior_array = self.transform_polygon_orientation(exterior_array, direction=1)
                oriented_temp_unit_array = self.transform_polygon_orientation(temp_unit_array[i], direction=1)

                # 外周を矩形でクリップ
                value = self.clip_polygon(oriented_exterior_array, oriented_temp_unit_array, clip_ccw=1)

                # 面積が存在しなければ次の矩形へ
                if value is  None or value.size == 0:

                    continue

                # クリップ後の面積計算
                area = self.calculate_polygon_area(value)
                temp_dict[key] = area

            # 原点図郭と面積最大値を比較し、含まれるか判定
            if origin_point in temp_dict:
                max_area = max(temp_dict.values())
                origin_area = temp_dict[origin_point]

                if origin_area >= max_area:
                    return True

                else:
                    return False

            else:
                return False


    def transform(self, context, flowfile):

        try:

            linestring_id_array = None

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            data_definition_delimiter, center_dwh_name, gml_id_dwh_name, feature_tag_string, level2500_unit_code_string, output_target_crs_string, output_zip_flag = \
                WM.calc_func_time(self.logger)(
                    self.get_property)(context, flowfile)

            data_definition_stream, field_set_file_data_frame = WM.calc_func_time(
                self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            geometry_type_list, geometry_dwh_file_name_list, geometry_distribution_name_list, attribute_file_type_list, attribute_dwh_file_name_list, attribute_distribution_name_list, attribute_const_value_list, attribute_data_type_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stream,
                                                                                         data_definition_delimiter=data_definition_delimiter
                                                                                         )
            # -----------------------------------------------------------------------------------------------------------

    # -----------------------------------------------------------------------------------------------------------
    # field_set_dataframeからcoordinates_arrayを抜き出し、coordinates_dictに変換する
    # -----------------------------------------------------------------------------------------------------------
            coordinates_id_array, coordinates_dict = WM.calc_func_time(self.logger)(
                self.create_coordinates_id_array_and_dict_from_coordinates_array)(field_set_file_data_frame, geometry_dwh_file_name_list)

            if center_dwh_name is None or center_dwh_name == "":

                # field_set_file_data_frame からジオメトリ値の配列を抽出
                geometry_value_coordinates_array = pickle.loads(base64.b64decode(
                    field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_file_name_list[0], "Value"].values[0]))

                temp_coordinates_id_array = np.unique(geometry_value_coordinates_array[:,0])

                # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
                temp_coordinate_dict = {
                    temp_coordinates_id_array[i]: geometry_value_coordinates_array[
                        list(np.where(geometry_value_coordinates_array[:, 0] == temp_coordinates_id_array[i])[0]), 1:4
                    ] for i in range(len(temp_coordinates_id_array))
                }

            else:
        # -----------------------------------------------------------------------------------------------------------
        # 【取得】中心線npy取得
        # -----------------------------------------------------------------------------------------------------------
                linestring_id_array, linestring_dict = WM.calc_func_time(self.logger)(
                    self.create_linestring_dict_from_field_set_file_data_frame)(field_set_file_data_frame, center_dwh_name)
        # -----------------------------------------------------------------------------------------------------------

    # -----------------------------------------------------------------------------------------------------------
    # 【取得】データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
    # -----------------------------------------------------------------------------------------------------------
            attribute_feature_id_array, all_attribute_dataframe = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe_datatype)(
                field_set_file_data_frame, attribute_dwh_file_name_list, attribute_distribution_name_list, attribute_const_value_list, attribute_file_type_list, attribute_data_type_list, len(coordinates_id_array), encoding='UTF-8', input_file_type=1)
    # -----------------------------------------------------------------------------------------------------------

            # 空文字をnanに変更
            all_attribute_dataframe = all_attribute_dataframe.replace(
                '', np.nan)

            # -----------------------------------------------------------------------------------------------------------
            # 出力対象図郭取得
            # -----------------------------------------------------------------------------------------------------------
            target_unit_code_list = WM.calc_func_time(self.logger)(
                self.get_target_unit_code_list)(level2500_unit_code_string, DDC.UNIT_CODE_DELIMITER)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 出力対象CRS取得
            # -----------------------------------------------------------------------------------------------------------
            target_plateau_crs = WM.calc_func_time(self.logger)(
                self.get_output_target_crs)(DDC.PLATEAU_CRS_DICT, output_target_crs_string)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # gml_id取得
            # -----------------------------------------------------------------------------------------------------------
            gml_id_array = WM.calc_func_time(self.logger)(
                self.get_gml_id_array_from_field_set_file_data_frame)(field_set_file_data_frame, gml_id_dwh_name)
            # -----------------------------------------------------------------------------------------------------------

            # output用にリストに追加
            dwh_list = []
            type_list = []
            xml_value_list = []

            # 出力対象図郭ごとに処理
            for target_unit_code_list_index in range(len(target_unit_code_list)):

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】出力対象図郭情報取得
                # -----------------------------------------------------------------------------------------------------------
                unit_origin_array, level_mesh_array = WM.calc_func_time(self.logger)(
                    NSP.get_unit_origin)(target_unit_code_list[target_unit_code_list_index])
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # 【抽出】ジオメトリ指定図郭内に存在するかチェック
                # -----------------------------------------------------------------------------------------------------------
                if center_dwh_name is None or center_dwh_name == "":
                    feature_bool = [self.judge_citygml_multipatch(
                        temp_coordinate_dict[temp_coordinates_id_array[i]][:, 0:2], unit_origin_array, level_mesh_array) for i in range(len(temp_coordinates_id_array))]

                else:
                    feature_bool = [WM.calc_func_time(self.logger)(NSP.judge_citygml)(
                        linestring_dict[linestring_id_array[i]][:, :2], unit_origin_array, level_mesh_array) for i in range(len(linestring_id_array))]

                # 出力対象がなければ次へ
                if np.any(feature_bool) == True:
                    pass
                else:
                    continue

                # 出力対象抽出
                target_attribute_dataframe, target_id_array, target_coordinates_array, target_gml_id_array\
                    = WM.calc_func_time(self.logger)(self.output_target_extraction_specifically_feature_bool)(all_attribute_dataframe,
                                                                                                              coordinates_id_array if center_dwh_name is None or center_dwh_name == "" else linestring_id_array,
                                                                                                              coordinates_dict,
                                                                                                              gml_id_array,
                                                                                                              feature_bool
                                                                                                              )

                # -----------------------------------------------------------------------------------------------------------
                try:
                    feature_string = [temp.split(
                        DDC.XML_TAG_DELIMITER) for temp in geometry_distribution_name_list][0][0]
                    feature_type_string = [temp.split(
                        DDC.XML_TAG_DELIMITER) for temp in geometry_distribution_name_list][0][1]
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return FlowFileTransformResult(relationship="failure")

                # -----------------------------------------------------------------------------------------------------------
                # xmlElement
                # -----------------------------------------------------------------------------------------------------------

                try:
                    # 親ノード作成
                    output_element_core = ET.Element(DDC.CORE_STRING)

                    # ここの固定値はすべて書き連ねる
                    [output_element_core.set(DDC.CITY_MODEL_ATTRIBUTE[city_model_index][0], DDC.CITY_MODEL_ATTRIBUTE[city_model_index][1])
                     for city_model_index in range(len(DDC.CITY_MODEL_ATTRIBUTE))]
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return FlowFileTransformResult(relationship="failure")

                # boundBy設定
                output_element_core = WM.calc_func_time(self.logger)(self.set_bound_By)(
                    output_element_core, target_plateau_crs, target_coordinates_array)

                # -----------------------------------------------------------------------------------------------------------
                # ジオメトリデータ
                # -----------------------------------------------------------------------------------------------------------
                # 以下属性、ジオメトリ用データ加工
                # すべてメソッドにすること
                # 引数にはデータ流通基盤の情報をそのまま設定できるように調整する

                feature_string_list, geometry_string_list \
                    = WM.calc_func_time(self.logger)(self.extract_feature_string_and_geometry_str_to_list)(geometry_distribution_name_list,
                                                                                                           feature_type_string,
                                                                                                           feature_tag_string
                                                                                                           )

                # npy読み込み時点のデータはここ
                # 4点1行の組み合わせにreshape + xyz座標のみ取得
                # [マルチパッチ]>[4点 id xyzが4つで16個の要素]

                id_coordinate_dict = (self.get_coordinates_by_id)(
                    target_coordinates_array)
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # 属性データ追加用要素作成 ループの中でdata
                # -----------------------------------------------------------------------------------------------------------
                all_attribute_name_list, attribute_array_list \
                    = WM.calc_func_time(self.logger)(self.extract_attribute_arrays_from_target_attribute_dataframe)(target_attribute_dataframe, feature_type_string)
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # 属性の設定 地物ごとのループ
                # -----------------------------------------------------------------------------------------------------------
                for index, dict_items in enumerate(id_coordinate_dict.items()):
                    element_dict, output_element_tree = WM.calc_func_time(self.logger)(self.create_feature_element)(output_element_core,
                                                                                                                    feature_string,
                                                                                                                    feature_type_string,
                                                                                                                    target_gml_id_array,
                                                                                                                    index)
                    # -----------------------------------------------------------------------------------------------------------
                    # 属性追加 これの内側にジオメトリ追加
                    # -----------------------------------------------------------------------------------------------------------
                    for all_attribute_name_list_index in range(len(all_attribute_name_list)):
                        
                        # タグと属性に分割
                        try:
                            attribute_split_list = [temp.split(
                                DDC.XML_ATTRIBUTE_DELIMITER_FOR_LINESTRING) for temp in all_attribute_name_list[all_attribute_name_list_index]]

                            # XML属性名とXML属性値をキーとする
                            key_list=["-".join(attribute_split) for attribute_split in attribute_split_list]
                            key_bool= np.array([key in element_dict for key in key_list], dtype=np.bool_)
                        
                        except Exception:
                            self.logger.error(traceback.format_exc())
                            return FlowFileTransformResult(relationship="failure")
                        
                        # 最初のタグが設定されていない場合
                        if np.all(key_bool==False) or (key_bool[0] == False):

                            # 中でタグに属性追加するか判定
                            attribute_element\
                                =WM.calc_func_time(self.logger)\
                                                  (self.add_attribute_to_tag_in_element_dict)\
                                                  (attribute_split_list,
                                                   attribute_array_list,
                                                   all_attribute_name_list_index,
                                                   index,
                                                   element_dict,
                                                   output_element_tree)
                                                  
                        # 要素2個以上で初めて出てくる場合
                        else:
                            already_element_tree\
                                =WM.calc_func_time(self.logger)\
                                                  (self.append_attribute_to_tag)\
                                                  (element_dict,
                                                   attribute_split_list,
                                                   attribute_array_list,
                                                   all_attribute_name_list_index,
                                                   index,
                                                   key_bool)

                        # -----------------------------------------------------------------------------------------------------------
                        # 地物のsubelementに対してマルチパッチの座標設定
                        # -----------------------------------------------------------------------------------------------------------

                        # 地物のsubelementに対してマルチパッチの数だけ追加
                        if attribute_split_list[-1][0] == 'frn:function':

                            feature_element, feature_subelement, element_dict = attribute_subelement = WM.calc_func_time(
                                self.logger)(NSP.create_element)(feature_string_list, element_dict)
                            # 地物のelement追加
                            output_element_tree.append(feature_element)

                            for geometry_index in range(len(dict_items[1])):
                                # マルチパッチのelement
                                geometry_element, geometry_subelement, element_dict = attribute_subelement = WM.calc_func_time(
                                    self.logger)(NSP.create_element)(geometry_string_list, element_dict)

                                feature_subelement.append(geometry_element)

                                try:
                                    # ここでマルチパッチごとのループ
                                    # マルチパッチごとのタグ追加最後のpostに4点の座標値を結合させた文字列を設定
                                    geometry_subelement.text = " ".join(
                                        list(map(str, dict_items[1][geometry_index])))

                                except Exception:
                                    self.logger.error(traceback.format_exc())
                                    return FlowFileTransformResult(relationship="failure")

                        else:
                            pass
                        # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------

                dwh_list, type_list, xml_value_list = WM.calc_func_time(self.logger)(self.xml_element_to_string_and_add_list)(output_element_core,
                                                                                                                              xml_value_list,
                                                                                                                              type_list,
                                                                                                                              dwh_list,
                                                                                                                              target_unit_code_list,
                                                                                                                              DDC.TARGET_PREFIX,
                                                                                                                              target_plateau_crs,
                                                                                                                              target_unit_code_list_index)

            output_field_set_file = WM.calc_func_time(self.logger)(
                PBP.set_field_set_file)(dwh_list, type_list, xml_value_list)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
