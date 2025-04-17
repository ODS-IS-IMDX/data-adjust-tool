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
# GeoJSON形式のデータを読み込み、GeoPandasを使用して
# GeoDataFrameオブジェクトに変換する。変換されたGeoDataFrameは
# シリアライズされ、Nifiのflowfileとして出力される。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
from io import BytesIO
import traceback

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

gpd = import_module("geopandas")
np = import_module("numpy")


class ConvertGeoJSONToGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        GeoJSONファイルをGeoDataFrameに変換する。
                        ①input: GeoJSONファイル。
                        ②output: 入力データのGeoJSONが持っていたデータをそのまま持ったGeoDataFrame。
                      '''
        tags = ['GeoJSON', 'GeoDataFrame', 'Python']

    # GeoJSONのエンコーディングを示すプロパティ値
    GEOJSON_FILE_ENCODE = PropertyDescriptor(
        name='GeoJSON File Encode',
        description='GeoJSONの文字コード',
        default_value='shift-jis',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # GeoJSONの座標参照系（CRS）を示すプロパティ値
    GEOJSON_FILE_CRS = PropertyDescriptor(
        name='GeoJSON File CRS',
        description='GeoJSONのCRS(epsgコード)',
        default_value='6677',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [GEOJSON_FILE_ENCODE,
                            GEOJSON_FILE_CRS]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def get_property(self, context):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: processorの設定値が格納されているデータ

        戻り値:
            geojson_file_encode: GeoJSONのエンコーディングを示すプロパティの値
            geojson_file_crs: GeoJSONの座標参照系（CRS）を示すプロパティの値
        """
        # プロパティの取得
        geojson_file_encode = context.getProperty(
            self.GEOJSON_FILE_ENCODE).getValue()
        geojson_file_crs = context.getProperty(
            self.GEOJSON_FILE_CRS).getValue()

        # 上記のプロパティの値を確認するログ
        self.logger.info(
            f'[Property], geojson_file_encode: {geojson_file_encode}, geojson_file_crs: {geojson_file_crs}')

        return geojson_file_encode, geojson_file_crs

    def Convert_shape_file_to_geodataframe(self, flowfile, geojson_file_encode, geojson_file_crs):
        """
        概要:
            ZIPファイルからShapefileを読み込み、GeoDataFrameを作成する関数

        引数:
            flowfile: flowfileオブジェクトからバイトデータを取得するための引数
            geojson_file_encode: GeoJSONのエンコーディングを示すプロパティの値
            geojson_file_crs: GeoJSONの座標参照系（CRS）を示すプロパティの値

        戻り値:
            geojson_dataframe: GeoJSONを読み込んで作成したGeoDataFrameオブジェクト
        """

        # flowfileからバイトデータを取得
        geojson_bytes = flowfile.getContentsAsBytes()

        # バイトデータをBytesIOストリームに変換
        geojson_stream = BytesIO(geojson_bytes)

        # GeoPandasを使用してGeoJSONを読み込み、geojson_dataframeを作成
        geojson_dataframe = gpd.read_file(geojson_stream,
                                          driver='GeoJSON',
                                          crs=geojson_file_crs,
                                          encoding=geojson_file_encode
                                          )

        return geojson_dataframe

    def add_index_column(self, geojson_dataframe):
        """
        概要:
            GeoDataFrameにインデックス列'Findex'を追加する関数

        引数:
            geojson_dataframe: インデックス列を追加するGeoDataFrameオブジェクト

        戻り値:
            geojson_dataframe: インデックス列を追加した後のGeoDataFrameオブジェクト
        """

        # インデックス列'Findex'を追加
        index_list = np.arange(0, len(geojson_dataframe))

        geojson_dataframe['Findex'] = index_list

        return geojson_dataframe

    def serialize_geodataframe(self, geojson_dataframe):
        """
        概要:
            GeoDataFrameをpickle形式でシリアライズする関数

        引数:
            geojson_dataframe: pickle形式でシリアライズするGeoDataFrameオブジェクト

        戻り値:
            geojson_dataframe: pickle形式でシリアライズしたバイトデータ
        """

        # GeoDataFrameをpickle形式でシリアライズ
        serialize_dataframe = pickle.dumps(geojson_dataframe)

        return serialize_dataframe

    def transform(self, context, flowfile):
        try:
            # プロパティの取得
            geojson_file_encode, geojson_file_crs = WM.calc_func_time(
                self.logger)(self.get_property)(context)

            # ZIPファイルからGeoDataFrameを作成
            geojson_dataframe = WM.calc_func_time(self.logger)(
                self.Convert_shape_file_to_geodataframe)(flowfile, geojson_file_encode, geojson_file_crs)

            # GeoDataFrameにインデックス列'Findex'を追加
            geojson_dataframe = WM.calc_func_time(self.logger)(
                self.add_index_column)(geojson_dataframe)

            # GeoDataFrameをpickle形式でシリアライズ
            serialize_dataframe = WM.calc_func_time(self.logger)(
                self.serialize_geodataframe)(geojson_dataframe)

            return FlowFileTransformResult(relationship='success', attributes={'encode': geojson_file_encode, 'crs': geojson_file_crs}, contents=serialize_dataframe)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship='failure')
