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
# マルチパッチをシェープファイルとして書き出すプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import traceback
import base64
import io
import pathlib
import os
from pathlib import Path

os.environ["PROJ_LIB"] =r'.\work\python\extensions\PutShapeFileFromMultipatch\1.0.0\osgeo\data\proj'

from importlib import import_module

pd = import_module("pandas")
np = import_module("numpy")
sf = import_module("shapefile")
osr = import_module("osgeo.osr")

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

class PutShapeFileFromMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        マルチパッチを持ったFieldSetFileからシェープファイルに変換し、指定されたディレクトリに出力する。
                        input: マルチパッチを持った1行以上のFieldSetFile
                        output: 入力データのままのデータ（指定されたディレクトリにシェープファイルを出力する）
                      """
        tags = ["ShapeFile", "Multipatch", "Python"]

    # シェープファイルの文字コードを指定
    SHP_ENCODING = PropertyDescriptor(
        name="Shapefile Encoding",
        description="シェープファイルの文字コードを指定",
        default_value="shift-jis",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # データ定義ファイルの区切り文字を指定
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字を指定",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True
    )

    # シェープファイルを格納するフォルダパスを指定
    OUTPUT_SHP_FOLDER_PATH = PropertyDescriptor(
        name="Output Shapefile Folder Path",
        description="シェープファイルを格納するフォルダパス",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # シェープファイル名を設定
    SHP_FILENAME = PropertyDescriptor(
        name="Shapefile Name",
        description="シェープファイル名を設定（拡張子無しで動作）",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [SHP_ENCODING,
                            DATA_DEFINITION_DELIMITER,
                            OUTPUT_SHP_FOLDER_PATH,
                            SHP_FILENAME]

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
            shp_encoding: 出力先のCRS
            data_definition_delimiter: データ定義ファイルの区切り文字(例:Comma)
            output_shp_folder_path: 出力先フォルダ
        """

        # シェープファイルの文字コードを指定
        shp_encoding = context.getProperty(self.SHP_ENCODING).getValue()

        # データ定義ファイルの区切り文字を指定
        data_definition_delimiter = context.getProperty(self.DATA_DEFINITION_DELIMITER).getValue()

        # シェープファイルを格納するフォルダパスを指定
        output_shp_folder_path = context.getProperty(self.OUTPUT_SHP_FOLDER_PATH).evaluateAttributeExpressions(flowfile).getValue()

        shp_filename = context.getProperty(self.SHP_FILENAME).evaluateAttributeExpressions(flowfile).getValue()

        return shp_encoding, data_definition_delimiter, output_shp_folder_path, shp_filename

    def get_flowfile(self, flowfile):
        """
        概要:
            FlowFileからデータ定義とCSV形式のデータを取得する関数

        引数:
            flowfile: プロセッサに渡されるデータ

        戻り値:
            data_definition_stream: データ定義ファイル(csv形式)
            shp_crs: Attribute から取得した crs 情報
            field_set_file_data_frame: データフレームに加工した fieldsetfile
        """

        # データ定義ファイルとCRSをflowfileのattributeから取得
        data_definition = flowfile.getAttribute("DataDefinition")

        # Attribute から crs 情報を取得
        shp_crs = flowfile.getAttribute("crs")

        # 後続処理のためpathとして扱う
        data_definition_stream = io.StringIO(data_definition)

        # Flowfileから、csv形式のfield_set_fileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(io.StringIO(input_field_set_file))

        return data_definition_stream, shp_crs, field_set_file_data_frame

    def create_coordinates_id_array_and_dict_from_coordinates_array(self, field_set_file_data_frame, geometry_dwh_file_name_list):
        """
        概要:
            field_set_file_data_frameのDwh列で、geometry_file_name_listの0番目(geometryのDwhファイル名)の値の行とValue列の値を抽出

        引数:
            field_set_file_data_frame: input_field_set_file をデータフレームに加工したデータ
            geometry_dwh_file_name_list: データ定義ファイルのジオメトリ Dwh ファイル名が格納されたリスト

        戻り値:
            coordinates_id_array: geometry_value_coordinates_arrayのIDだけの配列
            coordinates_dict: マルチパッチのIDをキーとし、その座標データを値とした辞書
        """

        # field_set_file_data_frame からジオメトリ値の配列を抽出
        geometry_value_coordinates_array = pickle.loads(
                base64.b64decode(
                    field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_file_name_list[0], "Value"].values[0]
                )
            )

        # geometry_value_coordinates_array から重複した要素を取り除いた座標IDを取得
        coordinates_id_array = np.unique(geometry_value_coordinates_array[:,0])

        return geometry_value_coordinates_array, coordinates_id_array

    def get_coordinates_for_each_ID(self, geometry_value_coordinates_array, id_index):
        """
        概要:
            ジオメトリ配列からIDごとの座標データを取得し、IDごとにデータを整形する関数

        引数:
            geometry_value_coordinates_array: ジオメトリの座標データを含む配列
            id_index: ジオメトリのIDリスト

        戻り値:
            id_coordinate_dict: IDごとの座標データを格納した辞書
            geometry_number: ジオメトリの数
        """

        # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
        id_coordinate_dict = {
            id_index[i]: geometry_value_coordinates_array[
                list(np.where(geometry_value_coordinates_array[:, 0] == id_index[i])[0]), 1:4
            ] for i in range(len(id_index))
        }

        # 各IDの座標データをリシェイプ
        id_coordinate_dict = {id: array.reshape(int(len(array)/4),4,3)[:,:3,:] for id, array in id_coordinate_dict.items()}

        # ジオメトリの数を計算
        geometry_number = len(np.unique(geometry_value_coordinates_array[:, 0]))

        return id_coordinate_dict, geometry_number

    def generate_shapefile(self, output_shp_file_path, shp_encoding, id_coordinate_dict, attribute_name_list, all_attribute_dataframe):
        """
        概要:
            シェープファイルを生成し、属性とジオメトリを設定する関数

        引数:
            output_shp_file_path: 出力先シェープファイルのパス
            shp_encoding: シェープファイルの文字コード
            id_coordinate_dict: IDごとの座標データを格納した辞書
            attribute_name_list: 属性名のリスト
            all_attribute_dataframe: 属性データを含むデータフレーム

        戻り値:
            基本的になし
        """

        # 出力ファイル生成
        shapefile_writer = sf.Writer(output_shp_file_path, shapeType=sf.MULTIPATCH, encoding=shp_encoding)

        # shapefile.Writerにマルチパッチの座標登録
        [shapefile_writer.multipatch(parts=array, partTypes=[1] * len(array)) for id, array in id_coordinate_dict.items()]

        # geometry以外の属性カラムをセットする geometry は最後の列なので属性の数-1の数だけループして設定
        [shapefile_writer.field(attribute_name_list[i]) for i in range(len(attribute_name_list))]

        # レコードに対してカラム分変数を設定する
        # listに属性をためた後1レコード分呼び出してunpackで展開
        [shapefile_writer.record(*temp_attribute) for temp_attribute in all_attribute_dataframe.values.tolist()]

        # 出力ファイルを閉じる
        shapefile_writer.close()

    def create_prj_file(self, prj_path, prj_encoding, epsg_code=6677):
        """
        概要:
            `.prj` ファイルを生成し、指定された EPSG コードに基づいて地理空間参照システムを定義
            `.prj` ファイルは、シェープファイルに対する座標参照系（CRS）を指定するために使用

        引数:
            prj_path: `.prj` ファイルの出力先パス
            prj_encoding: `.prj` ファイルに書き込む文字エンコーディング
            epsg_code: 使用する EPSG コード

        戻り値:
            なし
        """

        # osgeoのオブジェクト
        p1 = osr.SpatialReference()

        # EPSGコードセット
        p1.ImportFromEPSG(epsg_code)

        # ESRIshpファイル用に変形
        p1.MorphToESRI()

        # ファイルを指定して出力)
        with open(prj_path, "w", encoding = prj_encoding) as f:
            f.write(p1.ExportToWkt())

    def create_cpg_file(self, cpg_path, cpg_encoding):
        """
        概要:
            `.cpg` ファイルを生成し、指定された文字エンコーディングをファイルに書き込む関数
            `.cpg` ファイルは、シェープファイルの文字エンコーディング情報を保存するために使用

        引数:
            cpg_path: `.cpg` ファイルの出力先パス
            cpg_encoding: `.cpg` ファイルに書き込む文字エンコーディング

        戻り値:
            なし
        """

        with open(cpg_path, "w", encoding=cpg_encoding) as f:
            f.write(cpg_encoding)

    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            shp_encoding, data_definition_delimiter, output_shp_folder_path, shp_filename\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから情報を取得
            data_definition_stream, shp_crs, field_set_file_data_frame\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、
            # 入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter=data_definition_delimiter,
                                                                                )

            # field_set_file_data_frameのDwh列で、geometry_file_name_listの0番目(geometryのDwhファイル名)の値の行とValue列の値を抽出
            geometry_value_coordinates_array, id_index\
                = WM.calc_func_time(self.logger)(self.create_coordinates_id_array_and_dict_from_coordinates_array)(field_set_file_data_frame, geometry_file_name_list)

            # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
            id_coordinate_dict, geometry_number\
                = WM.calc_func_time(self.logger)(self.get_coordinates_for_each_ID)(geometry_value_coordinates_array, id_index)

            # データ定義ファイルに指定された属性項目ファイルをすべて読み込み dataframe を作成する
            all_attribute_dataframe = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe)(field_set_file_data_frame,
                                                                                                     dwh_file_name_list,
                                                                                                     attribute_name_list,
                                                                                                     attribute_const_value_list,
                                                                                                     attribute_file_type_list,
                                                                                                     geometry_number,
                                                                                                     input_file_type=1,
                                                                                                     feature_id_column_name='地物ID'
                                                                                                     )

            # --------------------------------------------------------------------------
            # 出力先ディレクトリが存在しない場合、作成する
            # --------------------------------------------------------------------------
            output_dir_path = Path(output_shp_folder_path)

            # ディレクトリが存在しない場合、作成する
            if not output_dir_path.exists():
                output_dir_path.mkdir(parents=True, exist_ok=True)

            try:
                # file_pathに変換
                output_shp_file_path = output_shp_folder_path + "\\" + shp_filename

            except Exception as e:
                self.logger.error(traceback.format_exc())

            # シェープファイルを生成し、属性とジオメトリを設定
            WM.calc_func_time(self.logger)(self.generate_shapefile)(output_shp_file_path,
                                                                    shp_encoding,
                                                                    id_coordinate_dict,
                                                                    attribute_name_list,
                                                                    all_attribute_dataframe)
            # prjファイル出力
            WM.calc_func_time(self.logger)(self.create_prj_file)(pathlib.Path(output_shp_file_path).with_suffix('.prj'), shp_encoding, int(shp_crs))

            WM.calc_func_time(self.logger)(self.create_cpg_file)(pathlib.Path(output_shp_file_path).with_suffix('.cpg'), shp_encoding)

            # .shp を追加する処理
            if not output_shp_file_path.endswith('.shp'):
                output_shp_file_path += '.shp'

            return FlowFileTransformResult(relationship="success",
                                           attributes={"multipatch_shapefile_path": output_shp_file_path})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
