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

# Python標準ライブラリ
import traceback
from collections import defaultdict

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
ConvexHull = import_module("scipy.spatial").ConvexHull
pyproj = import_module("pyproj")

# 定数

# 配列のZ値を取得するためのindex
Z_INDEX = 3

class ConvertCoordinatesToBottomPolygonArray(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列を底面ポリゴンに変換する。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 底面ポリゴンの座標配列を持った1行のFieldSetFile。
                      """
        tags = ["CSV", "DataFrame", "Python"]

    # 出力マルチパッチのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 入力CRS
    FROM_CRS = PropertyDescriptor(
        name="From CRS",
        description="入力座標のCRS",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 変換CRS
    TO_CRS = PropertyDescriptor(
        name="To CRS",
        description="経緯度にするためのCRS",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            FROM_CRS,
                            TO_CRS]

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
            output_dwh_name: 出力FieldSetFileのDWH名
            from_crs: 入力座標配列のCRS
            to_crs: 経緯度にするためのCRS
        """
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        from_crs = context.getProperty(
            self.FROM_CRS).evaluateAttributeExpressions(flowfile).getValue()

        to_crs = context.getProperty(
            self.TO_CRS).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name, from_crs, to_crs

    # 経度を指定してXIDを取得する関数
    def calculate_x_index(self, longtitude, max_tile=2**26):
        return np.floor(max_tile * ((longtitude + 180) / 360))

    # 緯度を指定してYIDを取得する関数
    def calculate_y_index(self, latitude, max_tile=2**26):
        # degree→radian
        latitude = latitude * np.pi / 180
        return np.floor(max_tile * (1 - np.log(np.tan(latitude) + (1 / np.cos(latitude))) / np.pi) / 2)

    def create_mesh_id_dict_from_coordinates(self, x_id_array, y_id_array, coordinates_array):
        # x,yのメッシュidを結合 xy平面メッシュidのlistを生成
        mesh_id_list = [str(x_id_array[i]) + '@' + str(y_id_array[i]) for i in range(len(x_id_array))]

        # xy平面メッシュidをKey、座標配列
        mesh_id_default_dict = defaultdict(list)
        [mesh_id_default_dict[mesh_id_list[mi]].append(coordinates_array[mi]) for mi in range(len(mesh_id_list))]

        return mesh_id_default_dict

    #辞書型配列のValueに設定されたListから指定列の最小値インデックスを持つ要素を取り出す
    #引数1:default_dict or dictionary Keyにxyメッシュid('x@y' str)を想定 Valueに座標配列(numpy.array, np.float64)が要素のList
    #引数2:最小値取得対象列インデックス int
    #戻り値:最小値を持つ座標配列 2次元 np.float64 [構成点]>[id + xyz]
    def get_smallest_in_mesh(self, default_dict, column_index):
        
        #結果格納用List
        result_list = []

        #KeyごとにValueを取得しその中で指定列の最小値を持つデータのみ抽出
        for temp_key, temp_value in default_dict.items():
            #List→numpy.arrayへ
            temp_value_array = np.array(temp_value)

            #最小値の配列を抽出
            result_list.append(temp_value_array[np.argmin(temp_value_array[:,column_index])])

        #結果をnumpy.array
        return np.array(result_list)

    #頂点群から頂点群をすべて囲む凸多角形ポリゴン生成
    #構成点は頂点群から選択され反時計回りになっている
    #引数1:頂点群 2次元配列 [頂点]>[xyz座標]
    #戻り値1:凸多角形ポリゴンの座標 2次元配列 [頂点]>[xyz座標]
    def create_convex_polygon(self, points_array):
        #xy座標で凸多角形ポリゴンのインデックス取得
        hull = ConvexHull(points_array[:,1:3])

        #元のxyz座標を凸多角形ポリゴンのインデックスで指定
        hull_points_array = points_array[hull.vertices]

        # 1枚の底面ポリゴンの為、0列目にid0を追加
        hull_points_array = np.insert(hull_points_array, 0, 0, axis=1)

        return hull_points_array


    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            output_dwh_name, \
                from_crs, \
                to_crs\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # FieldSetFileの1行目の値を取得
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                coordinates_array\
                = PBP.get_dataframe_and_value_from_field_set_file(flowfile)

            # 空間IDのメッシュ取得のために経緯度に変換
            transformer_object = pyproj.Transformer.from_crs(from_crs, to_crs, always_xy=True)

            # 平直から経緯度へ
            longtitude_array, latitude_array = transformer_object.transform(coordinates_array[:,1], coordinates_array[:,2])

            # voxel化
            x_id_array = self.calculate_x_index(longtitude_array)
            y_id_array = self.calculate_y_index(latitude_array)

            mesh_id_default_dict =self.create_mesh_id_dict_from_coordinates(x_id_array, 
                                                                            y_id_array, 
                                                                            coordinates_array)

            # xyメッシュごとの最下層取得
            bottom_array = self.get_smallest_in_mesh(mesh_id_default_dict, Z_INDEX)

            # convexhullで外周取得
            bottom_array = self.create_convex_polygon(bottom_array[:,:4])

            # 底面ポリゴンをフィールドセットファイルに加工
            output_field_set_file = (PBP.set_field_set_file)(
                [output_dwh_name], [target_type], [bottom_array])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
