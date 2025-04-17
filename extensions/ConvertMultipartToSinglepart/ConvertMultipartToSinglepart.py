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
# DataFrame の geometry 列にマルチパートが含まれる場合、シングルパートへ変換を行うプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import base64
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")


class ConvertMultipartToSinglepart(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        マルチジオメトリを単一ジオメトリに変換する。
                        ①input: GeoDataFrameか、GeoDataFrameを持った1行のFieldSetFile。
                        ②output: 入力データと同じ形式のデータ。
                      """
        tag = ['GeoDataFrame', 'Python']

    property_descriptors = []

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def deserialize_geodataframe(self, flowfile):
        """
        概要:
            FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            geodataframe: バイトデータから還元された geodataframe
        """

        # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得する
        serialize_dataframe = flowfile.getContentsAsBytes()

        # バイトデータからGeoDataFrameを復元する
        geodataframe = pickle.loads(serialize_dataframe)

        return geodataframe

    def serialize_geodataframe(self, geodataframe):
        """
        概要:
            GeoDataFrameをpickle形式でシリアライズする関数

        引数:
            geodataframe: pickle形式でシリアライズするGeoDataFrameオブジェクト

        戻り値:
            serialize_dataframe: pickle形式でシリアライズしたバイトデータ
        """

        # GeoDataFrameをpickle形式でシリアライズ
        serialize_dataframe = pickle.dumps(geodataframe)

        return serialize_dataframe

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            try:

                # flowfileから、csv形式のFieldSetFileを取得。
                input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

                # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
                field_set_file_data_frame = pd.read_csv(
                    io.StringIO(input_field_set_file))

                target_dwh = field_set_file_data_frame.loc[0, "Dwh"]

                target_type = field_set_file_data_frame.loc[0, "Type"]

                geodataframe = pickle.loads(base64.b64decode(
                    field_set_file_data_frame.loc[0, "Value"]))

                field_set_flag = True

            except UnicodeDecodeError:

                # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元
                geodataframe = WM.calc_func_time(self.logger)(
                    self.deserialize_geodataframe)(flowfile)

                field_set_flag = False

            try:

                # マルチジオメトリをシングルジオメトリに変換
                if geodataframe.geometry.geom_type.isin(['MultiLineString', 'MultiPoint', 'MultiPolygon']).any():

                    exploded_gdf = geodataframe.explode(index_parts=False)

                    # 元の情報を保持したままジオメトリを更新
                    geodataframe = exploded_gdf.reset_index(drop=True)

                else:
                    pass

            except Exception:
                self.logger.error(traceback.format_exc())

            if field_set_flag == True:

                output_dwh_list = [target_dwh]

                output_type_list = [target_type]

                output_geodataframe_list = [geodataframe]

                output_data = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                     output_type_list,
                                                                                     output_geodataframe_list)

            else:

                # GeoDataFrame を pickle 形式でシリアライズ
                output_data = WM.calc_func_time(self.logger)(
                    self.serialize_geodataframe)(geodataframe)

            return FlowFileTransformResult(relationship="success", contents=output_data)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
