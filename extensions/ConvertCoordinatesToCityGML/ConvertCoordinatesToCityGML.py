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
# 中心線を構成する座標の情報をもとに、属性を付加しCityGMLの形式に変換するプロセッサ。
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

# 外部ライブラリの動的インポート
from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
np = import_module("numpy")
unary_union = import_module("shapely.ops").unary_union
Polygon = import_module("shapely.geometry").Polygon

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertCoordinatesToCityGML(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、中心線のあるLineStringからCityGML形式のファイルを作成する。処理結果をZIP圧縮することも可能。
                        ①input: 出力したいデータ項目を持ったFieldSetFile。
                        ②output: 国土基本図郭ごとのCityGMLファイルを持った、1行以上のFieldSetFile。
                      """
        tags = ["Coordinates", "CityGML", "gml",
                "xml", "ZIP", "Unpack", "Python"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    # 入力データのGeoNDArrayのDWH名
    CENTER_DWH_NAME = PropertyDescriptor(
        name="Center DWH Name",
        description="入力データのGeoNDArrayのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 入力データのgml_idのDWH名
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

    # results形式データのDWH
    RESULTS_DWH_NAME = PropertyDescriptor(
        name="Index Data DWH Name",
        description="入力データのインデックス更新データのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )


    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            CENTER_DWH_NAME,
                            GML_ID_DWH_NAME,
                            FEATURE_TAG_STRING,
                            LEVEL2500_UNIT_CODE_STRING,
                            OUTPUT_TARGET_CRS_STRING,
                            OUTPUT_ZIP_FLAG,
                            RESULTS_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで入力した値を取得し、float型に加工する関数

        Parameters:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        Returns:
            data_definition_delimiter: データ定義ファイルの区切り文字
            center_dwh_name: 中心線のDWH名
            gml_id_dwh_name: gml_idのDWH名
            feature_tag_string: ジオメトリループ用tag
            level2500_unit_code_string: 出力対象レベル2500国土基本図図郭コード文字列郡
            output_target_crs_string: 出力対象CRS
            output_zip_flag: ZIP圧縮するかどうかのフラグ
            results_dwh_name:results型データのDWH名
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

        # results型データのDWH名
        results_dwh_name = context.getProperty(
            self.RESULTS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return data_definition_delimiter,\
               center_dwh_name, gml_id_dwh_name,\
               feature_tag_string,\
               level2500_unit_code_string,\
               output_target_crs_string,\
               output_zip_flag,\
               results_dwh_name

    def get_flowfile(self, flowfile):
        """
        flowfileから、DataDefinitionというattributeのキーから値を取得し、
        (データ定義ファイル)次に、input_field_set_fileをDataFrameに加工する関数

        Parameters:
            flowfile: プロセッサに入ってくるデータ

        Returns:
            data_definition_stream: データ定義ファイル(csv形式)
            field_set_file_data_frame: DataFrameに加工したFieldSetFile
        """

        # flowfileから"DataDefinition"属性の内容を取得する
        data_definition = flowfile.getAttribute("DataDefinition")

        # data_definitionを、pathとして扱う。
        data_definition_stream = io.StringIO(data_definition)

        # flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        return data_definition_stream, field_set_file_data_frame

    def get_field_set_file_value(self,
                                 field_set_file_data_frame,\
                                 target_dwh_file_name):

        """
        指定したDwh名のValueを取得する関数。

        Parameters:
            field_set_file_data_frame: フィールドセットファイルをDataFrameに加工したデータ
            target_dwh_file_name: 取得対象のDwhファイル名

        Returns:
            result_value:フィールドセットファイルのValueをデコード、デシリアライズした値
        """

        # 取得対象インデックス
        target_index=field_set_file_data_frame["Dwh"] == target_dwh_file_name

        # フィールドセットファイルのValueを、デコード、デシリアライズ
        result_value = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[target_index, "Value"].values[0]))

        return result_value

    def get_start_end_index_by_1d_array(self,target_array):

        """
        与えられたインデックスを使ってTIN（三角形の配列）を生成

        Parameters:
            target_array: Numpy1次元配列

        Returns:
            start_index_array : 始点インデックス
            end_index_array : 終点インデックス

        """

        # 値が1種類しかない場合は始点と終点のインデックスを返す(0と配列の長さ-1)
        if len(np.unique(target_array)) == 1:
            return np.array([0]) ,np.array([len(target_array)-1])

        #始点インデックス取得用配列（例：②[2, 0, 0, 0, 1, 1, 2, 2]）
        roll_array1 = np.roll(target_array,1)

        #終点インデックス取得用配列（例：③[0, 0, 1, 1, 2, 2, 2, 0]）
        roll_array2 = np.roll(target_array,-1)

        #ID列を比較し異なるインデックスリストを取得→始点のインデックス
        #具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
        #　　　　②[2, 0, 0, 0, 1, 1, 2, 2]
        #　　　　→[0, 3, 5]が始点のインデックス
        start_index_array = np.where(np.logical_not((target_array == roll_array1)))[0]

        #ID列を比較し異なるインデックスリストを取得→終点のインデックス
        #具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
        #　　　　③[0, 0, 1, 1, 2, 2, 2, 0]
        #　　　　→[2, 4, 7]が終点のインデックス
        end_index_array = np.where(np.logical_not((target_array == roll_array2)))[0]

        return start_index_array,\
            end_index_array

    def get_coordinates_dict(self,
                             coordinates_array,
                             index_target=0):
        """
        GeoNDArrayを辞書型に変換する関数。

        Parameters:
            coordinates_array: GeoNDArray（id+xyz）

        Returns:
            coordinates_id_array: coordinates_arrayの一意なFID
            coordinates_dict: key:FID,value:座標配列（xyzのみ）
            index_target: GeoNDArrayにおけるFIDの列インデックス
        """

        # FIDの列で始点、終点のインデックスを取得
        si_array,\
        ei_array\
            =self.get_start_end_index_by_1d_array(coordinates_array[:,index_target])

        # FIDをkeyにして設定
        coordinates_dict={coordinates_array[si,index_target]:coordinates_array[si:ei+1,1:4] for si, ei in zip(si_array, ei_array)}

        return coordinates_dict,\
               si_array,\
               ei_array


    def get_target_unit_code_list(self, level2500_unit_code_string, unit_code_delimiter):
        """
        レベル2500ユニットコード文字列を指定の区切り文字で分割してリスト化する

        Parameters:
            level2500_unit_code_string: レベル2500ユニットコードを含む文字列
            unit_code_delimiter: 分割するための区切り文字

        Returns:
            target_unit_code_list: 分割されたレベル2500ユニットコードのリスト numpy 配列
        """

        target_unit_code_list = level2500_unit_code_string.split(
            unit_code_delimiter)

        return target_unit_code_list

    def get_output_target_crs(self, plateau_crs_dict, output_target_crs_string):
        """
        CRS文字列をキーとして、辞書から対応するCRSを取得する

        Parameters:
            plateau_crs_dict: CRSの辞書、CRS文字列をキーとする
            output_target_crs_string: 出力対象CRS

        Returns:
            target_plateau_crs: 出力対象CRS
        """

        target_plateau_crs = plateau_crs_dict[output_target_crs_string]

        return target_plateau_crs


    def output_target_extraction_specifically_feature_bool(self,
                                                           all_attribute_dataframe,
                                                           coordinates_array,
                                                           si_array,
                                                           ei_array,
                                                           feature_bool):
        """
        与えられた条件(feature_bool)に基づいて特定のフィールドデータ (target_attribute_dataframe)とジオメトリデータ(target_coordinates_array)を抽出する

        Parameters:
            all_attribute_dataframe: 全てのフィールドデータを含むDataFrame
            coordinates_dict: ジオメトリの座標情報を持つ辞書
            gml_id_array: gml_idを格納した配列
            si_array:GeoNDArrayの始点のインデックス
            ei_array:GeoNDArrayの終点のインデックス
            feature_bool: 出力対象とするフラグを示すブール型の配列

        Returns:
            target_attribute_dataframe: 出力対象となるフィールドデータのDataFrame
            target_coordinates_array: 出力対象となるジオメトリの座標情報を持つ配列
            target_gml_id_array: 出力対象となるgml_idを格納した配列
        """

        # 出力対象フィールドDataframe抽出
        target_attribute_dataframe\
            =all_attribute_dataframe[feature_bool].reset_index(drop=True)

        # 出力対象GeoNDArrayを取得
        target_coordinates_array\
            = np.concatenate([coordinates_array[si_array[bi]:ei_array[bi]+1]
                              for bi, temp_bool in enumerate(feature_bool) if temp_bool])

        return target_attribute_dataframe,\
               target_coordinates_array

    def extract_feature_string_and_geometry_str_to_list(self,
                                                        geometry_distribution_name_list,
                                                        feature_type_string,
                                                        feature_tag_string
                                                        ):
        """
        geometry_distribution_name_list から特定の地物と座標値の部分を抽出する。
        feature_type_string と feature_tag_string を使って、地物の開始位置と座標値の開始位置を特定する。
        feature_string_list には地物の文字列部分が、geometry_string_list には座標値の文字列部分が格納される。

        Parameters:
            geometry_distribution_name_list: ジオメトリの分布情報を表すリストとなり、各要素は文字列で空白区切りで構造化されている。
            feature_type_string: ジオメトリの種類を示す文字列
            feature_tag_string: ジオメトリのタグを示す文字列

        Returns:
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
        target_coordinates_array から特定の列（xyz座標）を抽出し、 geometry_array として取得
        geometry_arrayをreshape し、各地物ごとに4つの座標セット（id + xyz座標）が16個の要素で表される形のマルチパッチデータ
        マルチパッチデータから地物IDを取得し、各地物IDごとに構成点のxyz座標をマッピングした辞書 id_coordinate_dict を作成

        Parameters:
            target_coordinates_array: 座標情報を含むnumpy配列で、地物ごとの座標データが含まれる

        Returns:
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

        # idのみ
        id_array = multipatch_array2[:, 0]

        # unique_id
        id_unique_array = np.unique(id_array)

        # ここで地物idごとのインデックス取得

        # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
        id_coordinate_dict = {id_unique_array[i]: geometry_array[list(np.where(
            id_array == id_unique_array[i])[0]), :] for i in range(len(id_unique_array))}

        return id_coordinate_dict

    def extract_attribute_arrays_from_target_attribute_dataframe(self, target_attribute_dataframe, feature_type_string):
        """
        target_attribute_dataframe からフィールドデータを抽出し、
        それぞれのフィールドに対応する配列（リスト）を取得

        Parameters:
            target_attribute_dataframe: pandas の DataFrame。フィールドデータが含まれる
            feature_type_string: フィールドのタイプを示す文字列。この文字列が列名の一部として使用され、それ以降の属性名と値を特定します。

        Returns:
            attribute_array_list: 各フィールドに対応するデータを含むリスト
        """

        # target_attribute_dataframeのカラム名を取得
        all_attribute_name_list = target_attribute_dataframe.columns.values

        # 各カラム名を空白 ' ' を区切り文字として分割し、リスト化
        all_attribute_name_list = [temp_list.split(
            ' ') for temp_list in all_attribute_name_list]

        # 各カラム名リストに対して、feature_type_string が最初に出現する位置の次の要素
        # （すなわち、フィールド名とその後の情報）を取得。これにより、各フィールドの名前を抽出する。
        all_attribute_name_list = [
            temp[temp.index(feature_type_string)+1:] for temp in all_attribute_name_list]

        # フィールド値が設定されたarrayが要素のlist
        # to_numpy注意 数値が勝手に小数に置き換わらないようにdtype=objectとする
        attribute_array_list = [target_attribute_dataframe[temp_column].to_numpy(
            dtype=object) for temp_column in target_attribute_dataframe.columns.values]

        return all_attribute_name_list, attribute_array_list

    def create_feature_element(self,
                               output_element_core,
                               feature_string,
                               feature_type_string,
                               gml_id_list):
        """
        ジオメトリのXML要素を作成する

        Parameters:
            output_element_core: 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            feature_string: 地物の要素名を表す文字列
            feature_type_string: 地物の種類を表す文字列
            gml_id_list: 地物のgml_idの配列。各地物には一意のgml_idが設定されている

        Returns:
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
        output_element_tree.set('gml:id', gml_id_list[1])

        return element_dict, output_element_tree

    def add_attribute_to_element(self, element_list, element_object):
        """
        XMLにおける属性名、属性値をタグに設定する。

        Parameters:
            element_list:タグ名のリスト
            element_object:XML要素

        Returns:
            element_object:XML要素
        """

        if len(element_list)>1:

            for element in element_list[1:]:
                # 2つ目からの要素
                # '='でsplitしてXML属性名とXML属性値に分ける
                value_split_list = element.split(DDC.XML_ATTRIBUTE_VALUE_DELIMITER)

                # 追加
                element_object.set(value_split_list[0],
                                   value_split_list[1])

        # 設定するXML属性がないならそのままXML要素を返す
        else:
            pass

        return element_object

    def add_element(self, element_name_list, element, element_dict):

        element = ET.SubElement(element, element_name_list[0])
        element=self.add_attribute_to_element(element_name_list,element)
        element_dict["-".join(element_name_list)] = element

        return element



    def create_element_attribute(self, element_list, element_dict):
        """
        XMLにおける属性名、属性値をタグに設定する。

        Parameters:
            element_list:タグ名のリスト
            element_object:XML要素

        Returns:
            element_object:XML要素
        """
        root = ET.Element(element_list[0][0])
        root=self.add_attribute_to_element(element_list[0], root)
        element_dict["-".join(element_list[0])] = root

        if len(element_list)==1:
            return root, root
        else:
            pass

        # rootに対してsubの要素追加
        sub_element=self.add_element(element_list[1], root, element_dict)

        # 2つ目以降の要素を追加
        for fi in range(len(element_list)-2):
            sub_element=self.add_element(element_list[fi+2], sub_element, element_dict)

        return root, sub_element

    def append_attribute_to_tag(self,
                                element_dict,
                                attribute_split_list,
                                attribute_array_list,
                                all_attribute_name_list_index,
                                index,
                                key_bool):
        """
        要素の辞書に対して、指定されたタグにXML属性を追加する関数

        Parameters:
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            attribute_split_list: 分割されたXML属性のlist
            attribute_array_list: XML属性の配列list
            all_attribute_name_list_index: 全XML属性名リストのインデックス
            index: id_coordinate_dictのインデックス
            key_bool: タグ名ごとに、設定済みかを判定しているbool配列

        """

        # 取り出すタグのインデックス特定
        if np.all(key_bool):
            target_index=0
        else:
            target_index=np.min(np.where(np.logical_not(key_bool)))-1

        # 設定するタグの作成
        element_list=attribute_split_list[target_index+1:]
        sub_element=element_dict["-".join(attribute_split_list[target_index])]

        for fi in range(len(element_list)):
            sub_element=self.add_element(element_list[fi], sub_element, element_dict)

        # 地物の属性値を設定
        sub_element.text = str(
            attribute_array_list[all_attribute_name_list_index][index])

    def add_attribute_to_tag_in_element_dict(self,
                                             attribute_split_list,
                                             attribute_array_list,
                                             all_attribute_name_list_index,
                                             index,
                                             element_dict,
                                             output_element_tree):
        """
        要素の辞書に特定のタグにXML属性を追加する関数。

        Parameters:
            attribute_split_list: 分割されたXML属性のlist
            attribute_array_list: XML属性配列のリスト
            all_attribute_name_list_index: 全XML属性名リストのインデックス
            index: id_coordinate_dictのインデックス
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            output_element_tree: 出力の要素ツリー
        """

        attribute_element,\
        attribute_subelement\
            = self.create_element_attribute(attribute_split_list, element_dict)

        output_element_tree.append(attribute_element)
        attribute_subelement.text = str(
            attribute_array_list[all_attribute_name_list_index][index])


    def add_thematic_shape_attributes(self,
                                      output_element_tree,
                                      element_dict,
                                      linestring_dict,
                                      index,
                                      attribute_split_list,
                                      key_bool,
                                      link_dict,
                                      data_distribution_name):
        """
        テーマティックシェープのXML属性を追加する関数。

        Parameters:
            output_element_tree: 出力の要素ツリー
            element_dict: 各要素の名前をキーとし、その要素のXML要素を値とする辞書
            linestring_dict: IDに対しての中心線の辞書型配列
            target_id_array: 出力対象のIDの配列
            index: id_coordinate_dictのインデックス

        """
        link_index = link_dict[index]

        # 紐付きがなければとばす
        if len(link_index) == 0:
            pass
        else:
            for temp_index in link_index:

                if np.all(key_bool==False) or (key_bool[0] == False):

                    attribute_element,\
                    attribute_subelement\
                        = self.create_element_attribute(attribute_split_list, element_dict)
                    output_element_tree.append(attribute_element)

                    # ポイントの場合はZ値が最も大きい座標を設定する
                    if 'gml:Point' in data_distribution_name:

                        linestring_array=linestring_dict[temp_index]

                        # 1次元配列（1点のみの場合）の場合そのまま設定
                        if linestring_array.ndim==1:
                            thematic_geometry = np.array([list(map(str, linestring_array))]).flatten()
                        else:

                            thematic_geometry = np.array([list(map(str, linestring_array[np.argmax(linestring_array[:,2])]))]).flatten()

                    else:
                        thematic_geometry = np.array(
                            [list(map(str, temp)) for temp in linestring_dict[temp_index]]).flatten()

                    attribute_subelement.text = " ".join(thematic_geometry)

                    # 追加済みにする
                    key_bool[0]=True

                else:

                    if np.all(key_bool):
                        target_index=0
                    else:
                        target_index=np.min(np.where(np.logical_not(key_bool)))-1

                    # 設定するタグの取得 タグ名をKeyに要素を取り出す
                    element_list=attribute_split_list[target_index+1:]
                    sub_element=element_dict["-".join(attribute_split_list[target_index])]

                    for fi in range(len(element_list)):
                        sub_element=self.add_element(element_list[fi], sub_element, element_dict)

                    thematic_geometry = np.array(
                        [list(map(str, temp)) for temp in linestring_dict[temp_index]]).flatten()
                    sub_element.text = " ".join(thematic_geometry)


    def set_bound_By(self, output_element_core, target_plateau_crs, target_coordinates_array):
        """
        指定された情報を使用してboundBy要素を設定し、更新された出力要素を返す関数。

        Parameters:
            output_element_core: 出力要素のコア部分。boundBy要素が追加
            target_plateau_crs: 目標の座標参照系（CRS）を示す文字列。
            target_coordinates_array: 出力対象のGeoNDArray

        Returns:
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
        output_element_core を XML 文字列に変換し、それを指定されたリストに追加する処理を行う関数

        Parameters:
            output_element_core: 出力するXMLのルート要素。この要素の下に新しい地物要素を追加
            xml_value_list: output用に用意した空リスト
            type_list: output用に用意した空リスト
            dwh_list: output用に用意した空リスト
            target_unit_code_list: 分割された地図情報2500レベルの図郭コードのリスト
            target_prefix: 地下埋設物接頭辞(固定値)
            target_plateau_crs: CRS
            target_unit_code_list_index: target_unit_code_list の要素のインデックス

        Returns:
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

        Parameters:
            triangles(numpy.ndarray) : - 形状: (n_triangles, 3, 3)
                                        - 各三角形は3つの頂点 (x, y, z) を持つ。

        Returns:
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

        Parameters:
            coordinates_array (np.ndarray): (N, 2) の配列
            direction (int): 0=時計回り, 1=反時計回り

        Returns
            coords (np.ndarray): 指定方向に揃えられた多角形GeoNDArray
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
            feature_array (np.ndarray): MultiPatch地物のGeoNDArray shape=(N, 2)
                                        Nは全頂点数（通常4頂点×三角形数など）
            unit_origin_array (np.ndarray): 図郭の原点GeoNDArray
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

        # 外周が存在すればGeoNDArrayに変換、存在しなければ空配列
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

    def create_element(self,element_list, element_dict):
        root = ET.Element(element_list[0])
        b = ET.SubElement(root, element_list[1])

        element_dict[element_list[0]] = root
        element_dict[element_list[1]] = b
        for fi in range(len(element_list)-2):
            b = ET.SubElement(b, element_list[fi + 2])
            element_dict[element_list[fi+2]] = b

        return root, b

    def get_data_definition_index_datatype(self,
                                           data_definition_stream,
                                           data_definition_delimiter=DDC.DELIMITER_COMMA,
                                           data_definition_encoding='utf-8'):
        """
        製品データ定義ファイルからジオメトリとフィールド名の情報を取得し、対応するリストを返す
        この関数の目的は製品データ定義ファイルの情報をListにして出力することにある
        たとえジオメトリ型が複数でもファイル名は複数返す必要がある

        Parameters:
            data_definition_stream: 製品データ定義ファイルパス(.csv or .tsv) 文字列
            data_definition_delimiter: 製品データ定義ファイルパスの区切り文字種別 1:タブ[\t](.tsv) それ以外:[,]カンマ(.csv) デフォルト値を0設定して何も指定がなければCSVとして読み込むものとする
            data_definition_encoding: 製品データ定義ファイルのencoding 日本語を含むので基本的に'shift-jis' デフォルト値に設定しておく 文字列

        Returns:
            geometry_type_list: ジオメトリのファイルタイプリスト
            geometry_dwh_file_name_list: ジオメトリのDWHファイル名リスト
            geometry_distribution_name_list: ジオメトリの流通項目名リスト
            attribute_file_type_list:フィールドのファイルタイプリスト
            attribute_dwh_file_name_list: フィールドのDWHファイル名リスト
            attribute_distribution_name_list: フィールドの流通項目名リスト
            attribute_const_value_list: フィールドの固定値リスト
            attribute_data_type_list: フィールドのデータ型リスト
            column_name_list::流通項目名リスト
            file_type_list:ファイルタイプリスト
            dwh_column_name_list:DWHファイル名リスト
        """

        # 製品データ定義ファイルをすべて文字列として読み込み
        # 区切り文字が[\t](タブ)の時
        if data_definition_delimiter == DDC.DELIMITER_TAB:
            data_definition_dataframe = pd.read_csv(
                data_definition_stream, sep='\t', encoding=data_definition_encoding, dtype=str)

        # それ以外の場合、区切り文字をカンマとして読み込む
        else:
            data_definition_dataframe = pd.read_csv(
                data_definition_stream, encoding=data_definition_encoding, dtype=str)

        column_name_list=list(data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME])
        file_type_list=list(data_definition_dataframe[DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME])
        dwh_column_name_list=list(data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME])

        # -----------------------------------------------------------------------------------------------------------
        # ジオメトリ、フィールドのインデックス取得
        # -----------------------------------------------------------------------------------------------------------
        # ファイルタイプの列取得
        file_type_array = data_definition_dataframe[DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME].to_numpy(
        )

        # フィールドのインデックス
        attribute_name_index = (file_type_array == DDC.ATTRIBUTE_FILE_TYPE) | (
            file_type_array == DDC.CONST_ATTRIBUTE_FILE_TYPE)

        # ジオメトリ項目ファイルのインデックス
        # 先頭の文字列が'+'ならジオメトリタイプとする
        geometry_name_index = np.array(
            [file_type_array[i][0] == '+' for i in range(len(file_type_array))], dtype=np.bool_)

        # -----------------------------------------------------------------------------------------------------------
        # ジオメトリ取得対象
        # -----------------------------------------------------------------------------------------------------------
        # ジオメトリファイルタイプList
        geometry_type_list = list(file_type_array[geometry_name_index])

        # ジオメトリDWHファイル名List
        geometry_dwh_file_name_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][geometry_name_index])

        # ジオメトリ流通項目名List
        geometry_distribution_name_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][geometry_name_index])

        # -----------------------------------------------------------------------------------------------------------
        # フィールド取得対象
        # -----------------------------------------------------------------------------------------------------------
        # フィールドファイルタイプリスト
        attribute_file_type_list = list(file_type_array[attribute_name_index])

        # フィールドDWHファイル名List
        attribute_dwh_file_name_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][attribute_name_index])

        # フィールド流通項目名List
        attribute_distribution_name_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][attribute_name_index])

        # フィールド値List
        attribute_const_value_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME][attribute_name_index])

        # フィールドデータ型List
        attribute_data_type_list = list(
            data_definition_dataframe[DDC.DATA_DEFINITION_DATA_TYPE_COLUMN_NAME][attribute_name_index])
        # -----------------------------------------------------------------------------------------------------------

        return geometry_type_list,\
               geometry_dwh_file_name_list,\
               geometry_distribution_name_list,\
               attribute_file_type_list,\
               attribute_dwh_file_name_list,\
               attribute_distribution_name_list,\
               attribute_const_value_list,\
               attribute_data_type_list,\
               column_name_list,\
               file_type_list,\
               dwh_column_name_list

    def get_geometry_attribute_index(self,
                                     column_name_list,
                                     file_type_list):

        """
        データ定義書の主となる地物、テーマティック、フィールドのインデックスを返す

        Parameters:
            column_name_list: データ定義書のDWHファイル名リスト
            file_type_list: データ定義書のファイルタイプリスト
        Returns:
            main_geometry_index:主となる地物のインデックス
            thematic_geometry_index:テーマティックのインデックス
            data_distribution_index_dict:インデックスの辞書型配列
                                         value:データ定義書のフィールドのインデックス
                                         key:フィールドだけのデータフレームのインデックス
        """
        main_geometry_index=None
        thematic_geometry_index=None
        data_distribution_index_dict={}
        attribute_index_count=0
        # 主となる地物とテーマティックのインデックスを判定する
        for di in range(len(file_type_list)):

            # ジオメトリのデータかどうか
            geometry_bool=file_type_list[di][0]=='+'

            # テーマティックかどうか
            thematic_bool='thematic' in column_name_list[di]

            # ジオメトリかつテーマティックが含まれていない場合
            # 主とある地物のインデックス取得
            if geometry_bool and not thematic_bool:
                main_geometry_index=di
                continue
            else:
                pass
            # ジオメトリかつテーマティックが含まれている場合
            # テーマティックのインデックス取得
            if geometry_bool and thematic_bool:
                thematic_geometry_index=di
                continue
            else:
                pass
            data_distribution_index_dict[di]=attribute_index_count
            attribute_index_count=attribute_index_count+1

        return main_geometry_index,\
               thematic_geometry_index,\
               data_distribution_index_dict

    def get_tag_name(self,
                     geometry_distribution_name_list,
                     column_name_list):
        # １つの地物をあらわすタグ名 core:cityObjectMember
        feature_string = [temp.split(
            DDC.XML_TAG_DELIMITER) for temp in geometry_distribution_name_list][0][0]

        # 地物の種類をあらわすタグ名（下水(uro:SewerPipe)など）
        feature_type_string = [temp.split(
            DDC.XML_TAG_DELIMITER) for temp in geometry_distribution_name_list][0][1]

        # DWHの流通項目名を半角スペースを区切り文字として分割し、リスト化
        all_data_distribution_name_list = [temp_list.split(
            ' ') for temp_list in column_name_list]

        # 物の種類をあらわすタグ名以降のタグを取得
        # 地物ごとに設定するタグのこと
        all_data_distribution_name_list = [
            temp[temp.index(feature_type_string)+1:] for temp in all_data_distribution_name_list]

        return feature_string,\
               feature_type_string,\
               all_data_distribution_name_list

    def get_link_dict(self,
                      field_set_file_data_frame,
                      results_dwh_name,
                      coordinates_dict):

        if results_dwh_name is None or results_dwh_name == "":

            results_type_list = [(i,i)for i in range(len(coordinates_dict))]
        else:            
            results_type_list\
                =self.get_field_set_file_value(field_set_file_data_frame,
                                               results_dwh_name)

        # テーマティック設定用辞書型配列作成
        link_dict = {}
        for main_id, sub_id in results_type_list:
            if main_id not in link_dict:
                link_dict[main_id] = []
            link_dict[main_id].append(sub_id)

        return link_dict

    def transform(self, context, flowfile):

        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            data_definition_delimiter, \
            center_dwh_name, \
            gml_id_dwh_name, \
            feature_tag_string, \
            level2500_unit_code_string, \
            output_target_crs_string, \
            output_zip_flag,\
            results_dwh_name\
                = WM.calc_func_time(self.logger)(
                    self.get_property)(context, flowfile)

            data_definition_stream, field_set_file_data_frame = WM.calc_func_time(
                self.logger)(self.get_flowfile)(flowfile)

            _,\
            _,\
            geometry_distribution_name_list,\
            attribute_file_type_list,\
            attribute_dwh_file_name_list,\
            attribute_distribution_name_list,\
            attribute_const_value_list,\
            attribute_data_type_list,\
            column_name_list,\
            file_type_list,\
            dwh_column_name_list\
                = WM.calc_func_time(self.logger)\
                                   (self.get_data_definition_index_datatype)\
                                   (data_definition_stream,
                                    data_definition_delimiter=data_definition_delimiter)

            # -----------------------------------------------------------------------------------------------------------
            # フィールド、取得用インデックス格納
            # -----------------------------------------------------------------------------------------------------------
            main_geometry_index,\
            thematic_geometry_index,\
            data_distribution_index_dict\
                =WM.calc_func_time(self.logger)\
                                  (self.get_geometry_attribute_index)\
                                  (column_name_list,
                                   file_type_list)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 流通項目名から設定するタグ名を取得
            # -----------------------------------------------------------------------------------------------------------
            feature_string,\
            feature_type_string,\
            all_data_distribution_name_list\
                = WM.calc_func_time(self.logger)\
                                   (self.get_tag_name)\
                                   (geometry_distribution_name_list,
                                    column_name_list)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 主となる地物の座標取得
            # -----------------------------------------------------------------------------------------------------------
            # フィールドセットファイルから主となる地物のGeoNDArray取得
            coordinates_array\
                = WM.calc_func_time(self.logger)\
                                    (self.get_field_set_file_value)\
                                    (field_set_file_data_frame,
                                     dwh_column_name_list[main_geometry_index])

            # 主となる地物の辞書型配列取得
            coordinates_dict,\
            coordinates_si_array,\
            coordinates_ei_array\
                = WM.calc_func_time(self.logger)\
                                    (self.get_coordinates_dict)\
                                    (coordinates_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 判定用座標取得
            # -----------------------------------------------------------------------------------------------------------
            if center_dwh_name is None or center_dwh_name == "":

                # 主となる地物を判定用座標とする
                center_coordinates_array=coordinates_array.copy()
                center_coordinates_dict=coordinates_dict.copy()
            else:

                # フィールドセットファイルから中心線のGeoNDArray取得
                center_coordinates_array\
                    = WM.calc_func_time(self.logger)\
                                        (self.get_field_set_file_value)\
                                        (field_set_file_data_frame,
                                        center_dwh_name)

                # 中心線の辞書型配列取得
                center_coordinates_dict,\
                _,\
                _\
                    = WM.calc_func_time(self.logger)\
                                        (self.get_coordinates_dict)\
                                        (center_coordinates_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # テーマティックの座標取得
            # -----------------------------------------------------------------------------------------------------------
            if thematic_geometry_index is None or thematic_geometry_index == "":
                pass
            else:

                # フィールドセットファイルからテーマティックのGeoNDArray取得
                thematic_array\
                    = WM.calc_func_time(self.logger)\
                                       (self.get_field_set_file_value)\
                                       (field_set_file_data_frame,
                                        dwh_column_name_list[thematic_geometry_index])
                
                # テーマティックの辞書型配列取得
                thematic_dict,\
                thematic_si_array,\
                thematic_ei_array\
                    = WM.calc_func_time(self.logger)\
                                       (self.get_coordinates_dict)\
                                       (thematic_array)



            # -----------------------------------------------------------------------------------------------------------
            # データ定義ファイルに指定されたフィールド項目をすべて読み込み一つのDataFrameとする
            # -----------------------------------------------------------------------------------------------------------
            attribute_feature_id_array,\
            all_attribute_dataframe\
                = WM.calc_func_time(self.logger)\
                                   (NSP.create_attribute_dataframe_datatype)\
                                   (field_set_file_data_frame,\
                                    attribute_dwh_file_name_list,\
                                    attribute_distribution_name_list,\
                                    attribute_const_value_list,\
                                    attribute_file_type_list,\
                                    attribute_data_type_list,\
                                    len(coordinates_dict),\
                                    encoding='UTF-8',\
                                    input_file_type=1)
            # -----------------------------------------------------------------------------------------------------------

            # 空文字をnanに変更
            all_attribute_dataframe = all_attribute_dataframe.replace('', np.nan)

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
            gml_id_list\
                = WM.calc_func_time(self.logger)\
                                   (self.get_field_set_file_value)\
                                   (field_set_file_data_frame,
                                    gml_id_dwh_name)

            gml_id_array = np.array(gml_id_list, dtype=object)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # results型が設定されていない場合 連番でresults型作成
            # -----------------------------------------------------------------------------------------------------------
            link_dict\
                = WM.calc_func_time(self.logger)\
                                   (self.get_link_dict)\
                                   (field_set_file_data_frame,
                                    results_dwh_name,
                                    coordinates_dict)
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
                    target_method=self.judge_citygml_multipatch
                else:
                    target_method=NSP.judge_citygml

                feature_bool = [target_method(temp_coordinates_array[:, 0:2],
                                              unit_origin_array,
                                              level_mesh_array)
                                for fid, temp_coordinates_array in center_coordinates_dict.items()]

                # 出力対象がなければ次へ
                if np.any(feature_bool) == True:
                    pass
                else:
                    continue

                # 出力対象抽出
                target_attribute_dataframe,\
                target_coordinates_array\
                    =WM.calc_func_time(self.logger)\
                                      (self.output_target_extraction_specifically_feature_bool)\
                                      (all_attribute_dataframe,
                                       coordinates_array,
                                       coordinates_si_array,
                                       coordinates_ei_array,
                                       feature_bool)

                target_unique_id_array=np.unique(target_coordinates_array[:, 0])
                target_gml_id_array=gml_id_array[feature_bool]

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
                feature_string_list,\
                geometry_string_list \
                    = WM.calc_func_time(self.logger)\
                                       (self.extract_feature_string_and_geometry_str_to_list)\
                                       (geometry_distribution_name_list,
                                        feature_type_string,
                                        feature_tag_string)

                # npy読み込み時点のデータはここ
                # 4点1行の組み合わせにreshape + xyz座標のみ取得
                # [マルチパッチ]>[4点 id xyzが4つで16個の要素]
                id_coordinate_dict\
                      = WM.calc_func_time(self.logger)\
                                         (self.get_coordinates_by_id)\
                                         (target_coordinates_array)
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # フィールドデータ追加用要素作成 ループの中でdata
                # -----------------------------------------------------------------------------------------------------------
                all_attribute_name_list,\
                attribute_array_list\
                    =WM.calc_func_time(self.logger)\
                                      (self.extract_attribute_arrays_from_target_attribute_dataframe)\
                                      (target_attribute_dataframe,\
                                       feature_type_string)
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # フィールドの値をタグに設定 地物ごとのループ
                # -----------------------------------------------------------------------------------------------------------
                for index, dict_items in enumerate(id_coordinate_dict.items()):

                    element_dict,\
                    output_element_tree\
                        = WM.calc_func_time(self.logger)\
                                           (self.create_feature_element)\
                                           (output_element_core,
                                            feature_string,
                                            feature_type_string,
                                            target_gml_id_array[index])
                    # -----------------------------------------------------------------------------------------------------------
                    # XML属性追加 これの内側にジオメトリ追加
                    # -----------------------------------------------------------------------------------------------------------
                    for ci in range(len(column_name_list)):

                        # タグとXML属性に分割
                        try:
                            attribute_split_list = [temp.split(
                                DDC.XML_ATTRIBUTE_DELIMITER_FOR_LINESTRING) for temp in all_data_distribution_name_list[ci]]

                            # XML属性名とXML属性値をキーとする
                            key_list=["-".join(attribute_split) for attribute_split in attribute_split_list]
                            key_bool= np.array([key in element_dict for key in key_list], dtype=np.bool_)

                        except Exception:
                            self.logger.error(traceback.format_exc())
                            return FlowFileTransformResult(relationship="failure")

                        if ci == main_geometry_index:

                            feature_element,\
                            feature_subelement\
                                =self.create_element(feature_string_list,
                                                    element_dict)

                            # 地物のelement追加
                            output_element_tree.append(feature_element)

                            # dict_itemsは要素数2のタプル fidと座標配列（※）が入っている
                            # ※ 形式は[マルチパッチの数]>[x,y,z4点]
                            # マルチパッチの枚数分ループするという意味
                            for geometry_index in range(len(dict_items[1])):
                                # マルチパッチのelement
                                geometry_element,\
                                geometry_subelement\
                                    =self.create_element(geometry_string_list,
                                                    element_dict)

                                feature_subelement.append(geometry_element)

                                # ここでマルチパッチごとのループ
                                # マルチパッチごとのタグ追加最後のpostに4点の座標値を結合させた文字列を設定
                                geometry_subelement.text = " ".join(
                                    list(map(str, dict_items[1][geometry_index])))

                        elif ci == thematic_geometry_index:

                            WM.calc_func_time(self.logger)\
                                                (self.add_thematic_shape_attributes)\
                                                (output_element_tree,
                                                 element_dict,
                                                 thematic_dict,
                                                 target_unique_id_array[index],
                                                 attribute_split_list,
                                                 key_bool,
                                                 link_dict,
                                                 all_data_distribution_name_list[ci])

                        else:

                            # 最初のタグが設定されていない場合
                            if np.all(key_bool==False) or (key_bool[0] == False):
                                WM.calc_func_time(self.logger)\
                                                 (self.add_attribute_to_tag_in_element_dict)\
                                                 (attribute_split_list,
                                                  attribute_array_list,
                                                  data_distribution_index_dict[ci],
                                                  index,
                                                  element_dict,
                                                  output_element_tree)

                            # 2回以上同じクラスが出てくる場合
                            else:
                                WM.calc_func_time(self.logger)\
                                                 (self.append_attribute_to_tag)\
                                                 (element_dict,
                                                  attribute_split_list,
                                                  attribute_array_list,
                                                  data_distribution_index_dict[ci],
                                                  index,
                                                  key_bool)

                dwh_list,\
                type_list,\
                xml_value_list\
                    = WM.calc_func_time(self.logger)\
                                       (self.xml_element_to_string_and_add_list)\
                                       (output_element_core,
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

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
