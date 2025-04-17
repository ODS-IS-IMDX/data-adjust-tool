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
# shapefileとしてZIPファイルを受け取り、GeoDataFrameに変換して処理を行うプロセッサ。
# プロパティとしてshapefileのエンコーディングと座標参照系（CRS）を指定し、
# 変換後の結果をpickle形式でシリアライズして出力する。
# 処理が成功した場合はsuccessリレーションに結果を送信、
# エラーが発生した場合はfailureリレーションにエラーを送信する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
from io import BytesIO
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
translate = import_module("shapely.affinity").translate
scale = import_module("shapely.affinity").scale

# 定数を定義
MILLIMETER = "ミリメートル(mm)"
CENTIMETER = "センチメートル(cm)"
DECIMETER = "デシメートル(dm)"
METER = "メートル(m)"
KILOMETER = "キロメートル(km)"
DEGREE = "経緯度"


class ConvertShapeFileToGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        シェープファイルをGeoDataFrameに変換する。
                        ①input: シェープファイルを圧縮したZIPファイル。
                        ②output: シェープファイルのデータを、そのまま持ったGeoDataFrame。
                      '''
        tags = ['GeoDataFrame', 'ShapeFile', 'ZIP', 'Python']

    # shapefileのエンコーディングを示すプロパティの値
    SHAPE_FILE_ENCODE = PropertyDescriptor(
        name='Shapefile Encode',
        description='シェープファイルの文字コード',
        default_value='shift-jis',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # shapefileの座標参照系（CRS）を示すプロパティの値
    SHAPE_FILE_CRS = PropertyDescriptor(
        name='Shapefile CRS',
        description='シェープファイルのCRS(epsgコード)',
        default_value='6677',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # x座標の平行移動値
    X_OFFSET = PropertyDescriptor(
        name='Latitude(X) Offset',
        description='緯度(x座標)の平行移動値',
        expression_language_scope=ExpressionLanguageScope.NONE,
        default_value="0",
        sensitive=False,
        required=False
    )

    # y座標の平行移動値
    Y_OFFSET = PropertyDescriptor(
        name='Longitude(Y) Offset',
        description='経度(y座標)の平行移動値',
        expression_language_scope=ExpressionLanguageScope.NONE,
        default_value="0",
        sensitive=False,
        required=False
    )

    # 座標データの単位(ミリ、センチ、メートル、キロ、経緯度)
    SPECIFY_UNIT = PropertyDescriptor(
        name='Specify Unit',
        description='座標データの単位(ミリ、センチ、メートル、キロ、経緯度)',
        expression_language_scope=ExpressionLanguageScope.NONE,
        allowable_values=[MILLIMETER, CENTIMETER,
                          DECIMETER, METER, KILOMETER, DEGREE],
        sensitive=False,
        required=False
    )

    property_descriptors = [SHAPE_FILE_ENCODE,
                            SHAPE_FILE_CRS,
                            X_OFFSET,
                            Y_OFFSET,
                            SPECIFY_UNIT]

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
            shape_file_encode: shapefileのエンコーディングを示すプロパティの値
            shape_file_crs: shapefileの座標参照系（CRS）を示すプロパティの値
            x_offset: x座標の平行移動値
            y_offset: y座標の平行移動値
            specify_unit: 座標データの単位
        """

        # プロパティの取得
        shape_file_encode = context.getProperty(
            self.SHAPE_FILE_ENCODE).getValue()
        shape_file_crs = context.getProperty(self.SHAPE_FILE_CRS).getValue()
        x_offset = context.getProperty(self.X_OFFSET).getValue()
        y_offset = context.getProperty(self.Y_OFFSET).getValue()
        specify_unit = context.getProperty(self.SPECIFY_UNIT).getValue()

        # 上記のプロパティの値を確認するログ
        self.logger.info(
            f'[Property], Shape File Encode: {shape_file_encode}, shape_file_crs: {shape_file_crs}')

        return shape_file_encode, \
            shape_file_crs, \
            x_offset, \
            y_offset, \
            specify_unit

    def create_geodataframe_from_zip(self, flowfile, shape_file_encode, shape_file_crs):
        """
        概要:
            ZIPファイルからshapefileを読み込み、GeoDataFrameを作成する関数

        引数:
            flowfile: flowfileオブジェクトからバイトデータを取得するための引数
            shape_file_encode: shapefileのエンコーディングを示すプロパティの値
            shape_file_crs: shapefileの座標参照系（CRS）を示すプロパティの値
        戻り値:
            shape_dataframe: shapefileを読み込んで作成したGeoDataFrameオブジェクト
        """

        # flowfileからZIPファイルのバイトデータを取得
        shape_zip_bytes = flowfile.getContentsAsBytes()

        # バイトデータをBytesIOストリームに変換
        shape_zip_stream = BytesIO(shape_zip_bytes)

        # GeoPandasを使用してshapefileを読み込み、shape_dataframeを作成
        shape_dataframe = gpd.read_file(shape_zip_stream,
                                        driver='ESRI Shapefile',
                                        crs=shape_file_crs,
                                        encoding=shape_file_encode
                                        )

        return shape_dataframe

    def translate_and_scale_geometry(self, shape_dataframe, x_offset, y_offset, specify_unit):
        """
        概要:
            ジオメトリに対し、平行移動とスケール処理を行う。
        引数:
            shape_dataframe: shapefileを読み込んで作成したGeoDataFrameオブジェクト
            x_offset: x座標の平行移動値
            y_offset: y座標の平行移動値
            specify_unit: 座標データの単位
        戻り値:
            changed_dataframe: 平行移動とスケールが適用された新しいGeoDataFrameオブジェクト
        """

        # GeoDataFrameのコピーを作成
        changed_dataframe = shape_dataframe.copy()

        # x_offsetがプロパティで入力された場合は、x軸に対し平行移動
        if x_offset not in (None, ""):
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: translate(geom, xoff=float(x_offset), yoff=0))

        # y_offsetがプロパティで入力された場合は、y軸に対し平行移動
        if y_offset not in (None, ""):
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: translate(geom, xoff=0, yoff=float(y_offset)))

        # ミリメートルから、0.001倍しメートルに変換
        if specify_unit == MILLIMETER:
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: scale(geom, xfact=0.001, yfact=0.001, origin=(0, 0)))

        # センチメートルから、0.01倍しメートルに変換
        if specify_unit == CENTIMETER:
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: scale(geom, xfact=0.01, yfact=0.01, origin=(0, 0)))

        # デシメートルから、0.1倍しメートルに変換
        if specify_unit == DECIMETER:
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: scale(geom, xfact=0.1, yfact=0.1, origin=(0, 0)))

        # キロメートルから、1000倍しメートルに変換
        if specify_unit == KILOMETER:
            changed_dataframe["geometry"] = changed_dataframe["geometry"].apply(
                lambda geom: scale(geom, xfact=1000, yfact=1000, origin=(0, 0)))

        return changed_dataframe

    def serialize_geodataframe(self, shape_dataframe):
        """
        概要:
            GeoDataFrameをpickle形式でシリアライズする関数

        引数:
            shape_dataframe: pickle形式でシリアライズするGeoDataFrameオブジェクト

        戻り値:
            serialize_dataframe: pickle形式でシリアライズしたバイトデータ
        """

        # GeoDataFrameをpickle形式でシリアライズ
        serialize_dataframe = pickle.dumps(shape_dataframe)

        return serialize_dataframe

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:
            # プロパティの取得
            shape_file_encode, \
                shape_file_crs, \
                x_offset, \
                y_offset, \
                specify_unit\
                = WM.calc_func_time(self.logger)(self.get_property)(context)

            # ZIPファイルからGeoDataFrameを作成
            shape_dataframe = WM.calc_func_time(self.logger)(
                self.create_geodataframe_from_zip)(flowfile, shape_file_encode, shape_file_crs)

            # GeoDataFrameのジオメトリに対し、平行移動とスケール処理を行う。
            changed_dataframe = WM.calc_func_time(self.logger)(self.translate_and_scale_geometry)(
                shape_dataframe, x_offset, y_offset, specify_unit)

            # GeoDataFrameをpickle形式でシリアライズ
            serialize_dataframe = WM.calc_func_time(self.logger)(
                self.serialize_geodataframe)(changed_dataframe)

            # 処理結果をFlowFileTransformResultオブジェクトとして返す
            return FlowFileTransformResult(relationship='success',
                                           attributes={'encode': shape_file_encode,
                                                       'crs': shape_file_crs},
                                           contents=serialize_dataframe)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship='failure')
