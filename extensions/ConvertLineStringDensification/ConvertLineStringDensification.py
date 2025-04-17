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
# LineStringを高密度化するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
LineString = import_module("shapely.geometry").LineString


class ConvertLineStringDensification(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LineStringの高密度化を行う。
                        ①input: LineStringジオメトリ1行のFieldSetFile。
                        ②output: LineStringジオメトリ1行のFieldSetFile。
                      """
        tags = ['Geometry', 'Python']

    # 高密度化した際の区切りの最長距離
    INTERVAL = PropertyDescriptor(
        name="Interval",
        description="高密度化した際の区切りの最長距離",
        required=True,
        default_value="5",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [INTERVAL]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        概要
            プロパティで入力した値を取得する関数
        引数
            context: プロセッサの設定値が格納されているデータ

        戻り値
            interval: 高密度化した際の区切りの最長距離
        """

        interval = int(context.getProperty(self.INTERVAL).getValue())

        return interval

    def densify_line(self, line, interval):
        """
        概要
            LineStringを高密度化させる関数
        引数
            line: geometry1行あたりに格納されたLineString
            interval: 高密度化した際の区切りの最長距離

        戻り値
            densified_line: 高密度化したLineString
        """

        # 座標のnumpy配列
        coords_array = np.array(line.coords)

        # LineStringの基となる座標のリスト
        new_coords_list = []

        # 各セグメント（連続する2点の間）に対して処理を行う
        for i in range(len(coords_array) - 1):
            start_point = coords_array[i]
            end_point = coords_array[i + 1]

            # セグメントの長さを計算（X, Y 座標に基づく）
            segment_length = np.linalg.norm(end_point[:2] - start_point[:2])

            # セグメントの長さがゼロの場合（同じ点が連続している場合）、スキップ
            if segment_length == 0:
                new_coords_list.append(tuple(start_point))
                continue
            if segment_length <= interval:
                new_coords_list.append(tuple(start_point))
                continue

            # セグメント内で高密度化する点の間隔を計算
            num_points = int(np.ceil(segment_length / interval))
            for j in range(num_points):
                fraction = j / num_points
                new_point = start_point + fraction * (end_point - start_point)
                new_coords_list.append(tuple(new_point))

        # 最後の点を追加（重複を避けるため）
        if new_coords_list[-1] != tuple(coords_array[-1]):
            new_coords_list.append(tuple(coords_array[-1]))

        # 新しい LineString を作成し、元の Z 座標を保持
        densified_line = LineString(new_coords_list)

        return densified_line

    def densify_geodataframe(self, geometry_value_list, interval):
        """
        概要
            高密度化する関数
        引数
            geometry_value_list: FieldSetFileのValue列に格納されたgeometryのリスト
            interval: 高密度化した際の区切りの最長距離

        戻り値
            densified_geometries_list: 高密度化を行ったgeometryのリスト
        """

        # 高密度化されたジオメトリを格納するリスト
        densified_geometries_list = []

        # ジオメトリを1行ずつ処理
        for line in geometry_value_list:

            # 高密度化処理を行う
            densified_line = WM.calc_func_time(self.logger, False)(
                self.densify_line)(line, interval)

            # 高密度化されたLineStringを格納
            densified_geometries_list.append(densified_line)

        return densified_geometries_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 高密度化する間隔 (メートル)
            interval = WM.calc_func_time(self.logger)(
                self.get_property)(context)

            # LineStringをintervalにそぐうように高密度化
            densified_geometries_list = WM.calc_func_time(self.logger)(
                self.densify_geodataframe)(geometry_value_list, interval)

            # set_field_set_fileのためリストに変換
            geometry_dwh_list = [geometry_dwh]
            geometry_type_list = [geometry_type]
            densified_geometries_value_list = [densified_geometries_list]

            # 出力用FieldSetFileに格納
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(geometry_dwh_list,
                                                                                           geometry_type_list,
                                                                                           densified_geometries_value_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
