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
# Coordinatesのz座標に土被りの値と管の幅を考慮した値を格納するプロセッサ。
# --------------------------------------------------------------------------------------------

# 標準ライブラリ
from io import StringIO
import base64
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")


class ConvertCoordinatesZByEarthCovering(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        土被りの値に基づき、座標配列のZ座標を減算する。
                        ①input: 座標配列を持ったFieldSetFile。FieldSetFileには幅と土被りの行を持たせても良い。
                        ②output: 座標配列を持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Python']

    # 土被りの深さ
    EARTH_COVERING = PropertyDescriptor(
        name="Earth Covering",
        description="土被りの深さ",
        required=False,
        default_value="0",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 土被りのDWH
    EARTH_COVERING_DWH_NAME = PropertyDescriptor(
        name="Earth Covering DWH Name",
        description="入力データの土被りのDWH名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 幅のDWH名を指定する。
    WIDTH_DWH_NAME = PropertyDescriptor(
        name="Width DWH Name",
        description="入力データの幅のDWH名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [EARTH_COVERING,
                            EARTH_COVERING_DWH_NAME,
                            WIDTH_DWH_NAME
                            ]

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
            earth_covering: 土被りの深さ
            earth_covering_dwh_name: 土被りのDWH名
            width_dwh_name: 幅のDWH名
        """

        earth_covering = float(context.getProperty(
            self.EARTH_COVERING).getValue())

        earth_covering_dwh_name = context.getProperty(
            self.EARTH_COVERING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        width_dwh_name = context.getProperty(
            self.WIDTH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        if earth_covering != 0 and earth_covering_dwh_name is not None:
            raise ValueError(
                "Earth Covering, Earth Covering DWH Nameはどちらか片方のみを入力してください。")

        return earth_covering, earth_covering_dwh_name, width_dwh_name

    def get_width_list_and_coordinates_array(self, flowfile, earth_covering_dwh_name, width_dwh_name):
        """
        概要:
            field_set_fileのValue列の値を、デコード、デシリアライズしwidth_list, coordinates_arrayにする。

        引数:
            flowfile: プロセッサに渡されるデータ
            earth_covering_dwh_name: 入力データの土被りのDWH名
            width_dwh_name: 入力データの幅のDWH名
        戻り値:
            earth_covering_list: DWHから習得した土被りのリスト
            width_list: 幅のリスト
            coordinates_array: 座標配列
            coordinates_dwh_name: 座標の配列のDWH名
        """

        # flowfileのデータをNifiのプロパティから取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # flowfileのデータを、pathとして認識させる。
        field_set_file_stringio = StringIO(input_field_set_file)

        # データフレームに変換。
        field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

        # データフレームからValue列、Dwh列をリスト化。
        value_list = field_set_file_data_frame["Value"].tolist()
        dwh_list = field_set_file_data_frame["Dwh"].tolist()

        # 1行の場合の処理
        if len(dwh_list) == 1:

            field_set_file_dataframe, \
                coordinates_dwh_name, \
                geometry_type, \
                coordinates_array\
                = WM.calc_func_time(self.logger, False)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            earth_covering_list = []
            width_list = []

        # 複数行の場合の処理
        else:
            # リストからプロパティで入力した、幅のDWH名のインデックスを抽出
            earth_covering_dwh_name_index = None
            if earth_covering_dwh_name != "" and earth_covering_dwh_name is not None:
                earth_covering_dwh_name_index = dwh_list.index(
                    earth_covering_dwh_name)
                earth_covering_list = pickle.loads(base64.b64decode(
                    value_list[earth_covering_dwh_name_index]))
            else:
                earth_covering_list = []

            # リストからプロパティで入力した、土被りのDWH名のインデックスを抽出
            width_dwh_name_index = None
            if width_dwh_name != "" and width_dwh_name is not None:
                width_dwh_name_index = dwh_list.index(width_dwh_name)
                width_list = pickle.loads(base64.b64decode(
                    value_list[width_dwh_name_index]))
            else:
                width_list = []

            # dwh_listの中には幅,深さ,座標の3つの要素が含まれている。値が同じものが含まれている場合は要素数が2つになってしまうので矯正が必要。
            # 幅,深さのDWH名はプロパティで指定したが、geometry(座標の値)は指定されていない。そのためここでgeometryのインデックスを取得。
            geometry_index = None
            for index in range(len(dwh_list)):
                if index != width_dwh_name_index and index != earth_covering_dwh_name_index:
                    geometry_index = index

            # Value_listの各要素を該当のインデックスを用いて行を指定し、
            # デコード、デシリアライズし、listまたはarrayに変換する。
            coordinates_array = pickle.loads(
                base64.b64decode(value_list[geometry_index]))

            # coordinatesのDwh名を取得
            coordinates_dwh_name = dwh_list[geometry_index]

        return earth_covering_list, width_list, coordinates_array, coordinates_dwh_name

    def calculate_earth_covering_into_z_value(self, earth_covering, earth_covering_list, coordinates_list, width_list):
        """
        概要:
            土被りの値をz座標に格納する関数

        引数:
            earth_covering: プロパティで入力した土被りの値
            earth_covering_list: DWHから入手した土被りのリスト
            coordinates_list: 座標のリスト
            width_list: 幅のリスト

        戻り値:
            coordinates_with_earth_covering_array: 土被りの値が計算された後の座標配列
        """

        # 座標のリストを DataFrame に変換
        coordinates_df = pd.DataFrame(coordinates_list, columns=[
                                      'findex', 'x', 'y', 'z'])

        # 土被りを属性から取得した場合
        if earth_covering_list != []:

            # パターン1　土被り,幅のリストがどちらもあるとき
            if width_list != []:
                # 幅のリストを DataFrame に変換
                width_df = pd.DataFrame(
                    width_list, columns=['findex', 'width'])

                # 土被りのリストを DataFrame に変換
                earth_covering_df = pd.DataFrame(earth_covering_list, columns=[
                                                 'findex', 'earth_covering'])

                # 幅と土被りのDataFrameをIDでマージ
                attribute_df = earth_covering_df.join(
                    width_df.set_index('findex'), on='findex', how='left')

                # 幅と座標の DataFrame を ID でマージ
                merged_df = coordinates_df.join(
                    attribute_df.set_index('findex'), on='findex', how='left')

                # Z 座標を計算
                merged_df['z'] = merged_df.apply(
                    lambda row: row['z'] - (float(row['earth_covering']) +
                                            float(float(row['width']) / 2)),
                    axis=1
                )

            # パターン2　土被りのリストはあるが、幅のリストはないとき
            else:
                # 土被りのリストを DataFrame に変換
                earth_covering_df = pd.DataFrame(earth_covering_list, columns=[
                                                 'findex', 'earth_covering'])

                # 幅と座標の DataFrame を ID でマージ
                merged_df = coordinates_df.join(
                    earth_covering_df.set_index('findex'), on='findex', how='left')

                # Z 座標を計算
                merged_df['z'] = merged_df.apply(
                    lambda row: row['z'] - float(row['earth_covering']),
                    axis=1
                )

        # 土被りを規定値で設定した場合
        else:

            # パターン3　幅のリストがあるとき
            if width_list != []:
                # 幅のリストを DataFrame に変換
                width_df = pd.DataFrame(
                    width_list, columns=['findex', 'width'])

                # 幅と座標の DataFrame を ID でマージ
                merged_df = coordinates_df.join(
                    width_df.set_index('findex'), on='findex', how='left')

                # Z 座標を計算
                merged_df['z'] = merged_df.apply(
                    lambda row: row['z'] -
                    (earth_covering + float(float(row['width']) / 2)),
                    axis=1
                )

            # パターン4　幅のリストがないとき
            else:

                # 名称を統一
                merged_df = coordinates_df

                # Z 座標を計算
                merged_df['z'] = merged_df.apply(
                    lambda row: row['z'] - earth_covering,
                    axis=1
                )

        # 結果をリストに戻す
        new_coordinates_list = merged_df[[
            'findex', 'x', 'y', 'z']].values.tolist()

        coordinates_with_earth_covering_array = np.array(new_coordinates_list)

        return coordinates_with_earth_covering_array

    def transform(self, context, flowfile):
        try:

            # プロパティの値を取得
            earth_covering, \
                earth_covering_dwh_name, \
                width_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            # field_set_fileのValue列の値を、デコード、デシリアライズする。。
            earth_covering_list, \
                width_list, \
                coordinates_array, \
                coordinates_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_width_list_and_coordinates_array)(flowfile,
                                                                                            earth_covering_dwh_name,
                                                                                            width_dwh_name)

            # 座標を配列からリストに変換
            coordinates_list = list(coordinates_array)

            # 土被りの値をz座標に格納する
            coordinates_with_earth_covering_array\
                = WM.calc_func_time(self.logger)(self.calculate_earth_covering_into_z_value)(earth_covering,
                                                                                             earth_covering_list,
                                                                                             coordinates_list,
                                                                                             width_list)

            # FieldSetFile用に各要素をリスト化
            dwh_list = [coordinates_dwh_name]
            type_list = ["geometry"]
            value_list = [coordinates_with_earth_covering_array]

            # FieldSetFileに加工
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                         type_list,
                                                                         value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
