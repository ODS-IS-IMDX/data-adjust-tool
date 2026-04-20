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

# --------------------------------------------------------------------------------------------#-----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】

# バイナリ形式（bytes）のLASファイルまたはFieldSetFile形式の座標・分類データから、地盤高に基づくTIFF画像（RGBA）を生成。

# 1. 入力:
#    - LAS形式のバイナリ点群データ、またはFieldSetFile形式でエンコードされた座標配列およびclassification情報。
# 2. 地盤分類:
#    - 指定された classification コード（例: 2, 11）を「地面」とみなす。
# 3. メッシュ分割:
#    - 指定されたキューブサイズ（例: 0.05 = 5cm）に基づいてメッシュを生成。
# 4. 地盤高の計算:
#    - 各メッシュごとに、指定された分類コード内での最小Z値を基準とし、一定範囲内（ground height）で最大Z値を探索。
# 5. 出力画像:
#    - 地盤高差（cm）に応じて各ピクセルに色を割り当てたRGBA画像（TIFF形式）を生成。
#      - 地盤高が2cm以上20cm未満: 暗い緑（G=128）
#      - 地盤高が20cm以上: 明るい緑（G=255）
# 6. TFWファイル情報:
#    - 出力TIFF画像に対応する空間参照情報を属性としてFlowFileに付与。

# 【出力内容】
# - TIFF形式の地盤高画像（FlowFileのcontents）
# - TFWファイル準拠の空間参照属性（FlowFileのattribute）

# lasファイル用ライブラリ lasの座標やその属性をnumpy配列で扱うことができる以下参考にできる情報
# lasファイルの属性：https://laspy.readthedocs.io/en/latest/intro.html
# classificationのコード：https://desktop.arcgis.com/ja/arcmap/10.5/manage-data/las-dataset/lidar-point-classification.htm

# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.LasPackage as LP
import nifiapi.NifiCustomPackage.WrapperModule as WM


# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
np = import_module("numpy")
laspy = import_module("laspy")
Image = import_module("PIL.Image")


class GenerateGroundHeightImage(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ①input: 座標配列とclassification2行のFieldSetFile、またはバイナリ形式（bytes）の LAS ファイル
                        ②output: 
                            - contents: PIL形式で生成した地盤高画像(TIFF互換)のバイナリデータ
                            - attributes: TFW（World File）形式の画像の空間参照情報文字列
                      """
        tags = ['LAS', 'Python']

    # 座標配列のDWH名
    CENTER_DWH_NAME = PropertyDescriptor(
        name = "Center DWH Name",
        description = "座標配列のDWH名",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 座標配列のDWH名
    CLASSIFICATION_DWH_NAME = PropertyDescriptor(
        name = "Classification DWH Name",
        description = "classification情報のDWH名",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 地面として判別する分類コード
    LAS_CLASSIFICATION_CODES = PropertyDescriptor(
        name = "Las Classification Codes",
        description='''
                        地面として判別する分類コード。
                        点群の classification 属性の中で、このコードに該当する点を「地面」とみなします。
                        複数コードを指定する場合はカンマ区切りで渡してください。（例: 2 や [2, 11]）
                    ''',
        default_value = 2,
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 地面とみなす高さの最大範囲
    GROUND_HEIGHT = PropertyDescriptor(
        name = "Ground Height",
        description = '''
                        地面とみなす高さの最大範囲（単位：メートル）。

                        各メッシュ内で地面とみなされる分類コード（classification）に該当する点群の中で
                        最低点（Z最小）を基準とし、その点からこの値以内のZ値を「地面候補」として扱います。

                        この範囲を超える点は、ノイズや建物等の地上物と見なされ、無視されます。
                        例:0.5 と指定した場合、50cm 以内の高さの点が「地面候補」となります。
                      ''',
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 立方体の一辺(0.05=5cm)
    CUBE_EDGE_LENGTH = PropertyDescriptor(
        name = "Cube Edge Length",
        description = "各メッシュ(＝画像のピクセル)の一辺の長さ(0.05=5cm)",
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [CENTER_DWH_NAME,
                            CLASSIFICATION_DWH_NAME,
                            LAS_CLASSIFICATION_CODES,
                            GROUND_HEIGHT,
                            CUBE_EDGE_LENGTH]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def get_property(self, context, flowfile):
        """
            プロパティで入力した値を取得

        Parameters:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        Returns:
            center_dwh_name: propertyで指定した座標配列のDWH名
            las_classification_codes: 地面として判別する分類コード
            ground_height: 地面とみなす高さの最大範囲（単位：メートル）
            cube_edge_length: 1ピクセル（=メッシュ）の一辺の長さ
        """

        # プロパティで設定した値を取得
        center_dwh_name = context.getProperty(
            self.CENTER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        classification_dwh_name = context.getProperty(
            self.CLASSIFICATION_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        las_classification_codes = context.getProperty(
            self.LAS_CLASSIFICATION_CODES).evaluateAttributeExpressions(flowfile).getValue()

        ground_height = float(context.getProperty(
            self.GROUND_HEIGHT).evaluateAttributeExpressions(flowfile).getValue())

        cube_edge_length = float(context.getProperty(
            self.CUBE_EDGE_LENGTH).evaluateAttributeExpressions(flowfile).getValue())

        return center_dwh_name, classification_dwh_name, las_classification_codes, ground_height, cube_edge_length


    def get_flowfile(self, flowfile):
        """
            flowfileのFieldSetFileをDataFrameに加工

        Parameters:
            flowfile: プロセッサに入ってくるデータ

        Returns:
            field_set_file_dataframe: DataFrameに加工したFieldSetFile
        """

        bytes_flag = False

        try:

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            target_value = pd.read_csv(io.StringIO(input_field_set_file))

        except UnicodeDecodeError:

            # flowfileからlasファイルのバイトデータを取得
            target_value = flowfile.getContentsAsBytes()

            bytes_flag = True

        return target_value, bytes_flag


    def get_coordinates_array_and_classification_from_field_set_file_dataframe(self, field_set_file_dataframe, center_dwh_name, classification_dwh_name):
        """
            FieldSetFileから座標配列とclassificationのValue値を取得

        Parameters:
            field_set_file_data_frame: FieldSetFileをDataFrameに加工したデータ
            center_dwh_name: propertyで指定した座標配列のDWH名
        Returns:
            coordinates_array: 座標配列
            classification: classification(分類)属性
        """

        # field_set_file_dataframe から座標配列とclassificationを抽出
        coordinates_array\
            = LP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, center_dwh_name)

        classification\
            = LP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, classification_dwh_name)

        return coordinates_array, classification


    def write_data_source(self, pil_ground_height_array):
        """
            PIL ImageからTIFF形式のバイナリデータをメモリ上に生成

        Parameters:
            pil_ground_height_array: RGBA形式の画像を表すNumPy配列。メッシュごとの地盤高を可視化した配列

        Returns:
            tiff_bytes: TIFF形式で保存された画像データのバイナリ
        """

        # メッシュIDに地盤高を設定したnumpy配列からImageインスタンス生成
        pil_green_mesh_image = Image.fromarray(pil_ground_height_array)

        # 出力用のバッファをメモリ上に用意
        output_buffer = io.BytesIO()

        # ImageインスタンスをTIFF形式でバッファに保存
        pil_green_mesh_image.save(output_buffer, format="TIFF")

        # TIFFファイルの内容をバイナリデータとして取得
        tiff_bytes = output_buffer.getvalue()

        # バッファを閉じてリソース解放
        output_buffer.close()

        return tiff_bytes


    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:
            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            center_dwh_name, \
            classification_dwh_name, \
            las_classification_codes, \
            ground_height, \
            cube_edge_length\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # flowfileの値を取得しFieldSetFileをDataFrameに加工
            # -----------------------------------------------------------------------------------------------------------
            target_value, \
                bytes_flag\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            if bytes_flag:

                # -----------------------------------------------------------------------------------------------------------
                # bytes形式のlasを読み込み、laspyのオブジェクトに変換
                # -----------------------------------------------------------------------------------------------------------
                las_object\
                    = WM.calc_func_time(self.logger)(LP.read_las)(target_value)

                # -----------------------------------------------------------------------------------------------------------
                # lasオブジェクトから座標配列に変換
                # -----------------------------------------------------------------------------------------------------------
                coordinates_array\
                    = WM.calc_func_time(self.logger)(LP.get_coordinates_from_las_object)(las_object)

                # -----------------------------------------------------------------------------------------------------------
                # classificationを取得
                # -----------------------------------------------------------------------------------------------------------
                index_array = coordinates_array[:, 0]

                classification = list(zip(index_array.copy(), las_object["classification"]))

            else:

                # -----------------------------------------------------------------------------------------------------------
                # 座標配列とclassificationを取得
                # -----------------------------------------------------------------------------------------------------------
                coordinates_array, \
                    classification\
                    = WM.calc_func_time(self.logger)(self.get_coordinates_array_and_classification_from_field_set_file_dataframe)(target_value,
                                                                                                                                    center_dwh_name,
                                                                                                                                    classification_dwh_name)

            # -----------------------------------------------------------------------------------------------------------
            # las内のx座標最小値 las内のy座標最大値 取得
            # -----------------------------------------------------------------------------------------------------------
            xyz_array, \
                min_x, \
                max_y\
                = WM.calc_func_time(self.logger)(LP.get_min_x_max_y)(coordinates_array)

            # -----------------------------------------------------------------------------------------------------------
            # 点群データをメッシュ単位で分類し、分類属性付き構造化配列を作成
            # -----------------------------------------------------------------------------------------------------------
            structured_array, \
                mesh_id_indices_array, \
                xyz_array, \
                = WM.calc_func_time(self.logger)(LP.get_mesh_array)(xyz_array,
                                                                      min_x,
                                                                      max_y,
                                                                      classification,
                                                                      cube_edge_length)

            # -----------------------------------------------------------------------------------------------------------
            # 構造化配列とXYZ座標配列からPIL画像用のRGBA配列を作成
            # -----------------------------------------------------------------------------------------------------------
            pil_ground_height_array\
                = WM.calc_func_time(self.logger)(LP.get_array_for_pil)(xyz_array,
                                                                         structured_array,
                                                                         mesh_id_indices_array,
                                                                         las_classification_codes,
                                                                         ground_height)

            # -----------------------------------------------------------------------------------------------------------
            # PIL ImageからTIFF形式のバイナリデータをメモリ上に生成
            # -----------------------------------------------------------------------------------------------------------
            tiff_bytes\
                = WM.calc_func_time(self.logger)(self.write_data_source)(pil_ground_height_array)

            # -----------------------------------------------------------------------------------------------------------
            # ラスタ画像（TIFF）に対応する TFW（World File） の内容を文字列で生成
            # -----------------------------------------------------------------------------------------------------------
            tfw_dict\
                = WM.calc_func_time(self.logger)(LP.generate_tfw_dict)(min_x, max_y,
                                                                       cube_edge_length)

            return FlowFileTransformResult(relationship="success",
                                           attributes=tfw_dict,
                                           contents=tiff_bytes)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
