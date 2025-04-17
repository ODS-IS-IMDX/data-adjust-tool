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
# geometryと必要な属性を3Dtilesに変換する
# 入力データからジオメトリと属性情報を抽出し、指定された座標系に変換してglTF形式で出力。
# どの図郭に存在するか判定させる際に中心点を採用する
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import json

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pyproj = import_module("pyproj")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertFieldSetFileTo3DTilesByUnitThematic(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        入力データからジオメトリと属性情報を抽出し、指定された座標系に変換してglTF形式で出力するプロセッサ。
                        処理結果をZIP圧縮することも可能。
                      """
        tags = ["python", "pandas", "numpy",
                "coordinates", "json", "gltf", "ZIP", "Unpack"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # データ定義ファイルの文字コード
    DATA_DEFINITION_ENCODING = PropertyDescriptor(
        name="Data Definition Encoding",
        description="データ定義ファイルの文字コード",
        default_value="shift-jis",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # 入力元CRS 6677(平面直角) 37654(UTM)など想定
    INPUT_CRS = PropertyDescriptor(
        name="Input CRS",
        description="入力元のCRS(epsgコード)",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # パラメータ計算用CRS 6668 4978
    # ・計算するパラメータはglTFのmatrix（経緯度から計算）と、
    #  tileset.jsonのboundingboxの出力範囲最小最大（経緯度→ラジアンに変換）
    PARAMETER_CRS = PropertyDescriptor(
        name="Parameter CRS",
        description="パラメータ計算用のCRS(epsgコード)",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力単位となる図郭のx座標の大きさ
    X_UNIT = PropertyDescriptor(
        name="X UNIT",
        description="x座標の大きさ",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 出力単位となる図郭のy座標の大きさ
    Y_UNIT = PropertyDescriptor(
        name="Y UNIT",
        description="y座標の大きさ",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 内包、交差判定用座標取得用流通項目名
    # データ定義書の流通項目名列を検索し判定用座標とそのジオメトリタイプを取得する
    # 判定用座標のジオメトリタイプはPointかLineStringとする。
    # それぞれ以下の種類の地物を想定している
    # Point：MHのThematicShape(代表点)、空間IDの中心座標
    # LineString：管路のThematicShape（中心線）
    JUDGE_COORDINATES_DISTRIBUTION_NAME = PropertyDescriptor(
        name="JUDGE COORDINATES DISTRIBUTION NAME",
        description="図郭の内包判定用座標取得用流通項目名",
        default_value="thematic",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # JSONが参照するglTFを格納するディレクトリのパス
    GLTF_DIRECTORY_PATH = PropertyDescriptor(
        name="glTF Directory Path",
        description="tileset.jsonが参照するglTFを格納するディレクトリのパス",
        default_value="data",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
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
                            DATA_DEFINITION_ENCODING,
                            INPUT_CRS,
                            PARAMETER_CRS,
                            X_UNIT,
                            Y_UNIT,
                            JUDGE_COORDINATES_DISTRIBUTION_NAME,
                            GLTF_DIRECTORY_PATH,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # ---------------------------------------------------------------------------------------------
    # 概要   :field_set_fileを3DTiles形式で出力する。
    # ---------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # データ定義ファイルの区切り文字
            data_definition_delimiter\
                = context.getProperty(self.DATA_DEFINITION_DELIMITER).getValue()

            # データ定義ファイルの文字コード
            data_definition_encoding\
                = context.getProperty(self.DATA_DEFINITION_ENCODING).getValue()

            # 入力元CRS
            input_crs\
                = int(context.getProperty(self.INPUT_CRS).evaluateAttributeExpressions(flowfile).getValue())

            # パラメータ計算用CRS
            parameter_crs\
                = int(context.getProperty(self.PARAMETER_CRS).evaluateAttributeExpressions(flowfile).getValue())

            # 出力単位図郭のx座標の大きさ
            x_unit\
                = float(context.getProperty(self.X_UNIT).getValue())

            # 出力単位図郭のy座標の大きさ
            y_unit\
                = float(context.getProperty(self.Y_UNIT).getValue())

            # 内包、交差判定用座標取得用流通項目名
            judge_coordinates_distribution_name\
                = context.getProperty(self.JUDGE_COORDINATES_DISTRIBUTION_NAME).getValue()

            # JSONが参照するglTFを格納するディレクトリのパス
            gltf_directory_path\
                = context.getProperty(self.GLTF_DIRECTORY_PATH).getValue()

            # ZIP圧縮するかどうかのフラグ
            output_zip_flag\
                = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # flowfileから、field_set_data_frame、data_definition_listを取得
            # ---------------------------------------------------------------------------
            target_field_set_file_dataframe, \
                data_definition_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_list)(flowfile)
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

            # ---------------------------------------------------------------------------
            # pyprojオブジェクト作成
            # ---------------------------------------------------------------------------
            transformer_object\
                = WM.calc_func_time(self.logger)(pyproj.Transformer.from_crs)(input_crs,
                                                                              parameter_crs,
                                                                              always_xy=True)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # 後続処理用初期化
            # ---------------------------------------------------------------------------
            # tileset.json用boundingVolume格納用List
            bounding_volume_list = []

            # tileset.json用glTF相対パス格納用List
            uri_list = []

            # mesh配列作成
            mesh_array = np.array([[x_unit, y_unit]])

            # output用のfield_set_fileに格納用のlist
            output_dwh_list = []
            output_value_list = []
            output_type_list = []
            # ---------------------------------------------------------------------------

            # データ定義書ごとに処理
            for data_definition in data_definition_list:

                self.logger.info(data_definition)

                # -----------------------------------------------------------------------------------------------------------
                # 製品データ定義ファイルから以下を取得
                # 出力時ジオメトリ項目ファイル名List
                # 出力時属性項目ファイル名List
                # 入力ファイル元の属性名List
                # ここでジオメトリが複数ある場合どうなるか
                # -----------------------------------------------------------------------------------------------------------
                data_definition_stringio = (io.StringIO(data_definition))

                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    attribute_file_type_list, \
                    attribute_dwh_file_name_list, \
                    attribute_distribution_name_list, \
                    attribute_const_value_list, \
                    attribute_data_type_list\
                    = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stringio,
                                                                                             data_definition_delimiter=data_definition_delimiter,
                                                                                             data_definition_encoding=data_definition_encoding)
                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # 図郭内判定用座標とそのジオメトリタイプを取得
                # 流通項目名からFieldSetFileの対象行取得
                # -----------------------------------------------------------------------------------------------------------
                # glTFにするマルチパッチファイルのDWH名取得（判定用）
                target_index_list = []
                judge_index_list = []

                for i in range(len(geometry_distribution_name_list)):

                    if geometry_distribution_name_list[i] == judge_coordinates_distribution_name:
                        judge_index_list.append(i)
                    else:
                        target_index_list.append(i)

                # 両者とも1件ずつのみなら処理続行
                if (len(target_index_list) == 1) and (len(judge_index_list) == 1):
                    pass
                # それ以外（複数件数or0件）なら終了
                else:
                    continue

                # -----------------------------------------------------------------------------------------------------------
                # glTFに変換する対象の座標取得
                # 座標の取得について辞書型はやめよう
                # 地物の数と属性の数を合わせるために始点と終点のインデックスを使用
                # -----------------------------------------------------------------------------------------------------------
                coordinates_array, \
                    coordinates_id_array, \
                    coordinates_dict\
                    = WM.calc_func_time(self.logger)(NSP.create_coordinates_id_array_and_dict_from_coordinates_array)(target_field_set_file_dataframe,
                                                                                                                      [geometry_dwh_file_name_list[target_index_list[0]]])

                target_start_index_array, \
                    target_end_index_array\
                    = WM.calc_func_time(self.logger)(NCP.get_start_index_and_end_index)(coordinates_array)

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】図郭内判定用npy取得
                # -----------------------------------------------------------------------------------------------------------
                judge_coordinates_array\
                    = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                              target_dwh_list,
                                                                                              geometry_dwh_file_name_list[judge_index_list[0]])

                judge_start_index_array, \
                    judge_end_index_array\
                    = WM.calc_func_time(self.logger)(NCP.get_start_index_and_end_index)(judge_coordinates_array)

                # 判定用座標のジオメトリタイプ
                judge_geometry_type = geometry_type_list[judge_index_list[0]]

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】製品データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
                # -----------------------------------------------------------------------------------------------------------
                attribute_feature_id_array, \
                    all_attribute_dataframe\
                    = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe_datatype)(target_field_set_file_dataframe,
                                                                                              attribute_dwh_file_name_list,
                                                                                              attribute_distribution_name_list,
                                                                                              attribute_const_value_list,
                                                                                              attribute_file_type_list,
                                                                                              attribute_data_type_list,
                                                                                              len(
                                                                                                  coordinates_id_array),
                                                                                              encoding="utf-8",
                                                                                              input_file_type=1)

                # -----------------------------------------------------------------------------------------------------------

                # -----------------------------------------------------------------------------------------------------------
                # ジオメトリの存在範囲から図郭特定（矩形範囲）
                # -----------------------------------------------------------------------------------------------------------
                # 図郭の原点（左上）とそのインデックス（図郭単位で割ったときにどこに位置しているか）を取得
                unit_origin_array, \
                    unit_index_array\
                    = WM.calc_func_time(self.logger)(NSP.calculate_and_convert_unique_origin_indices)(coordinates_array,
                                                                                                      mesh_array)

                # -----------------------------------------------------------------------------------------------------------

                # 出力対象図郭ごとに処理
                # ここから1つのメソッドとする
                for ui in range(len(unit_origin_array)):

                    # -----------------------------------------------------------------------------------------------------------
                    # 【抽出】国土基本図図郭内に存在するlasの座標を抽出 bool配列なので属性も同様に抽出
                    # -----------------------------------------------------------------------------------------------------------
                    # ラインの場合は交差判定を行い図郭内の延長の割合をもって判定する
                    if judge_geometry_type == DDC.LINESTRING_GEOMETRY_TYPE:
                        feature_bool\
                            = WM.calc_func_time(self.logger)(NSP.judge_citygmls)(judge_coordinates_array[:, 1:3],
                                                                                 judge_start_index_array,
                                                                                 judge_end_index_array,
                                                                                 unit_origin_array[ui],
                                                                                 mesh_array,
                                                                                 coordinates_id_array)

                    # ポイントの場合は中心点がどの図郭に存在するかで判定する
                    elif judge_geometry_type == DDC.POINT_GEOMETRY_TYPE:
                        feature_bool\
                            = WM.calc_func_time(self.logger)(NSP.judge_point_in_unit)(judge_coordinates_array[:, 1:3],
                                                                                      unit_origin_array[ui],
                                                                                      mesh_array)
                    else:
                        continue

                    # 出力対象がなければ次へ
                    if np.any(feature_bool) == True:
                        pass
                    else:
                        continue

                    # 出力対象の抽出
                    target_attribute_dataframe, \
                        target_coordinates_array\
                        = WM.calc_func_time(self.logger)(NSP.extract_output_target)(all_attribute_dataframe,
                                                                                    feature_bool,
                                                                                    coordinates_id_array,
                                                                                    coordinates_dict)

                    # 出力することが決まればglTF、tileset.jsonに設定する値を生成する
                    # uri設定用パス格納
                    gltf_file_name, uri_list\
                        = WM.calc_func_time(self.logger)(NSP.append_gltf_uri_string_to_uri_list)(geometry_dwh_file_name_list,
                                                                                                 unit_index_array,
                                                                                                 ui,
                                                                                                 gltf_directory_path,
                                                                                                 uri_list)

                    # 図郭のxy中心点取得（原点とする点のこと）
                    unit_min_x, \
                        unit_max_x, \
                        unit_min_y, \
                        unit_max_y, \
                        target_min_z, \
                        target_max_z, \
                        center_point \
                        = WM.calc_func_time(self.logger)(NSP.get_xy_center_point)(unit_origin_array,
                                                                                  ui,
                                                                                  mesh_array,
                                                                                  target_coordinates_array)

                    # boundingbox範囲ラジアンで生成
                    bounding_volume_list\
                        = WM.calc_func_time(self.logger)(NSP.generate_bounding_box_with_range_of_radians)(transformer_object,
                                                                                                          unit_min_x,
                                                                                                          unit_min_y,
                                                                                                          unit_max_x,
                                                                                                          unit_max_y,
                                                                                                          bounding_volume_list,
                                                                                                          target_min_z,
                                                                                                          target_max_z)

                    center_point_longitude, \
                        center_point_latitude\
                        = transformer_object.transform(center_point[0],
                                                       center_point[1])

                    # 経緯度からWGS84のデカルト座標取得
                    center_point_cartesian3\
                        = WM.calc_func_time(self.logger)(NSP.get_cartesian3_from_degree)(center_point_longitude,
                                                                                         center_point_latitude,
                                                                                         center_point[2],
                                                                                         DDC.ELLIPSOID)

                    # matrix取得
                    # matrix外から設定 デフォルトの値を設定しておくことで経緯度の場合にも対応できる
                    matrix_array\
                        = WM.calc_func_time(self.logger)(NSP.get_matrix_gltf_from_cartesian3)(center_point_cartesian3,
                                                                                              DDC.ELLIPSOID)

                    # 座標平行移動
                    target_coordinates_array\
                        = WM.calc_func_time(self.logger)(NSP.parallel_shift_of_coordinates)(target_coordinates_array,
                                                                                            center_point)

                    # glTF出力用オブジェクト取得 オブジェクト生成処理内でmatrix生成
                    # 座標+原点+属性
                    if judge_geometry_type == DDC.LINESTRING_GEOMETRY_TYPE:
                        target_gltf_object\
                            = WM.calc_func_time(self.logger)(NSP.create_gltf_object)(target_coordinates_array,
                                                                                     target_attribute_dataframe,
                                                                                     list(matrix_array))

                    # ポイントの場合は中心点がどの図郭に存在するかで判定する
                    elif judge_geometry_type == DDC.POINT_GEOMETRY_TYPE:

                        target_gltf_object\
                            = WM.calc_func_time(self.logger)(NSP.create_point_gltf_object)(target_coordinates_array,
                                                                                           target_attribute_dataframe,
                                                                                           list(matrix_array))

                    # 拡張子をつけて、output_dwh_listに格納
                    output_dwh_list.append(gltf_file_name + ".gltf")

                    # gltf_objectの型をoutput_type_listに格納
                    output_type_list.append("str")

                    # glTFを、JSON形式にし、改行文字をwindows用に変換
                    target_gltf_object_json\
                        = WM.calc_func_time(self.logger)(NSP.convert_gltf_to_json_and_format_with_windows_newline)(target_gltf_object)

                    # JSON形式になったglTFをoutput_value_listに格納。
                    output_value_list.append(target_gltf_object_json)

            # 全体の範囲求める
            region_list\
                = WM.calc_func_time(self.logger)(NSP.calculate_bounding_region_from_volume_list)(bounding_volume_list)

            # 各URIとそのboundingVolume設定
            # 範囲追加
            contents_list = []
            for bounding_volume_list_index in range(len(bounding_volume_list)):

                # target_dictを作成
                target_dict\
                    = WM.calc_func_time(self.logger)(NSP.create_target_dict_with_uri_and_bounding_volume)(uri_list,
                                                                                                          bounding_volume_list_index,
                                                                                                          bounding_volume_list)

                contents_list.append(target_dict)

            # 範囲追加
            children_list = []

            # children_dictの作成
            children_dict\
                = WM.calc_func_time(self.logger)(NSP.create_children_dict_with_bounding_volume_and_contents)(region_list,
                                                                                                             contents_list)

            children_list.append(children_dict)

            # JSON用辞書型配列
            tileset_dict\
                = WM.calc_func_time(self.logger)(NSP.create_tileset_dict)(region_list,
                                                                          children_list)

            # tileset.jsonのファイル名を、output_dwh_listに格納
            output_dwh_list.append(DDC.JSON_FILE_NAME)

            # tileset_dictをJSON形式の文字列にし、改行文字をwindows用に変換する。
            tileset_json_string = (json.dumps(tileset_dict, ensure_ascii=False,
                                   indent=4, sort_keys=True, separators=(",", ": "))).replace("\n", "\r\n")

            # tileset_json_stringの型とValueをそれぞれのlistに格納
            output_type_list.append("str")
            output_value_list.append(tileset_json_string)

            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list, output_type_list,
                                                                         output_value_list)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            return FlowFileTransformResult(relationship="failure")
