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
# GeoDataFrameからシェープファイルを作成し、指定されたディレクトリに出力する。
# 補足
# "Shapefile Name"プロパティに拡張子(.shp)が含まれない場合は、拡張子を自動で追加する。
# 競合するファイル名が存在する場合は"Overwrite Flag"を参照し、上書きするかどうかを決定。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from pathlib import Path
import traceback

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

OVERWRITE = "上書きする"
DO_NOT_OVERWRITE = "上書きしない"


class PutShapeFileFromGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        GeoDataFrameからシェープファイルを作成し、指定されたディレクトリに出力する。
                        ①input: GeoDataFrameかGeoDataFrameを持ったFieldSetFile。
                        ②output: 入力データのままのデータ（指定されたディレクトリにシェープファイルを出力する）
                      """
        tags = ["GeoDataFrame", "Shapefile", "Python"]

    # シェープファイルの文字コード
    SHAPEFILE_ENCODING = PropertyDescriptor(
        name="Shapefile Encoding",
        description="シェープファイルの文字コード",
        default_value="shift-jis",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # シェープファイルを格納するディレクトリ
    OUTPUT_SHAPEFILE_FOLDER_PATH = PropertyDescriptor(
        name="Output Shapefile Folder Path",
        description="シェープファイルを格納するディレクトリ",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # シェープファイル名
    SHAPEFILE_FILENAME = PropertyDescriptor(
        name="Shapefile Name",
        description="シェープファイル名（拡張子の記載がなければ自動で作成する）",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # shapefileの座標参照系（CRS）
    SHAPE_FILE_CRS = PropertyDescriptor(
        name="Shapefile CRS",
        description="シェープファイルのCRS(epsgコード)",
        default_value="${crs}",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 上書きのオプション
    OVERWRITE_FLAG = PropertyDescriptor(
        name="Overwrite Flag",
        description="出力ディレクトリに同名のファイルが既に存在する場合、上書きするかどうか",
        default_value=DO_NOT_OVERWRITE,
        allowable_values=[OVERWRITE, DO_NOT_OVERWRITE],
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [SHAPEFILE_ENCODING,
                            OUTPUT_SHAPEFILE_FOLDER_PATH,
                            SHAPEFILE_FILENAME,
                            SHAPE_FILE_CRS,
                            OVERWRITE_FLAG]

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
            shapefile_encoding: 出力先の文字コード
            output_shapefile_folder_path: 出力先ディレクトリ
            shapefile_filename: 出力ファイル名
            shape_file_crs: シェープファイルのCRS(epsgコード)
            overwrite_flag: 出力ディレクトリに同名のファイルが既に存在する場合、上書きするかどうかのフラグ
        """

        # シェープファイルの文字コード
        shapefile_encoding = context.getProperty(
            self.SHAPEFILE_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

        # シェープファイルを格納するディレクトリ
        output_shapefile_folder_path = context.getProperty(
            self.OUTPUT_SHAPEFILE_FOLDER_PATH).evaluateAttributeExpressions(flowfile).getValue()

        # シェープファイル名
        shapefile_filename = context.getProperty(
            self.SHAPEFILE_FILENAME).evaluateAttributeExpressions(flowfile).getValue()

        # シェープファイルのCRS(epsgコード)
        shape_file_crs = context.getProperty(
            self.SHAPE_FILE_CRS).evaluateAttributeExpressions(flowfile).getValue()

        # 出力ディレクトリに同名のファイルが既に存在する場合、上書きするかどうかのフラグ
        overwrite_flag = context.getProperty(
            self.OVERWRITE_FLAG).getValue()

        return shapefile_encoding, output_shapefile_folder_path, shapefile_filename, shape_file_crs, overwrite_flag

    def convert_target_data_to_shapefile(self, geodataframe, output_shapefile_file_path, shapefile_encoding, shape_file_crs):
        """
        概要:
            読み込んだGeoDataFrameからシェープファイルを作成し、出力する関数

        引数:
            geodataframe: 読み込んだGeoDataFrame
            output_shapefile_file_path: 出力先のファイルパス
            shapefile_encoding: 出力先の文字コード
            shape_file_crs: シェープファイルのCRS(epsgコード)

        戻り値:
            無し
        """

        # CRS(epsgコード)を設定
        geodataframe = geodataframe.to_crs(shape_file_crs)

        # GeoDataFrameをシェープファイル（.shp）として指定されたファイルパスに保存
        geodataframe.to_file(filename=output_shapefile_file_path,
                             driver="ESRI Shapefile", encoding=shapefile_encoding)

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            shapefile_encoding, \
                output_shapefile_folder_path, \
                shapefile_filename, \
                shape_file_crs, \
                overwrite_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # --------------------------------------------------------------------------
            # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得
            # --------------------------------------------------------------------------
            geodataframe = WM.calc_func_time(self.logger, False)(
                PBP.get_value_from_field_Set_file_or_serialized)(flowfile)

            # --------------------------------------------------------------------------
            # 出力先ディレクトリが存在しない場合、作成する
            # --------------------------------------------------------------------------
            output_dir_path = Path(output_shapefile_folder_path)

            # ディレクトリが存在しない場合、作成する
            if not output_dir_path.exists():
                output_dir_path.mkdir(parents=True, exist_ok=True)

            # --------------------------------------------------------------------------
            # 出力先パスの作成
            # --------------------------------------------------------------------------
            try:
                # 指定ファイル名に拡張子が含まれているかの確認(含まれていない場合.shpを追記)
                if not shapefile_filename.lower().endswith(".shp"):
                    shapefile_filename += ".shp"

                # ファイルパスに変換
                output_shapefile_file_path = output_dir_path / shapefile_filename

            except Exception:
                self.logger.error(traceback.format_exc())

            # --------------------------------------------------------------------------
            # ファイルが既に存在している場合の処理
            # --------------------------------------------------------------------------
            if output_shapefile_file_path.exists():

                if overwrite_flag == DO_NOT_OVERWRITE:

                    self.logger.warn(
                        f"{output_shapefile_file_path} は既に存在するため、プロパティOverwrite Flagが'上書きしない'設定では出力できません。")

                    return FlowFileTransformResult(relationship="failure")

            # --------------------------------------------------------------------------
            # 読み込んだGeoDataFrameからシェープファイルを作成し、指定パスに出力
            # --------------------------------------------------------------------------
            WM.calc_func_time(self.logger)(self.convert_target_data_to_shapefile)(geodataframe,
                                                                                  str(
                                                                                      output_shapefile_file_path),
                                                                                  shapefile_encoding,
                                                                                  shape_file_crs)

            return FlowFileTransformResult(relationship="success")

        except Exception:
            return FlowFileTransformResult(relationship="failure")
