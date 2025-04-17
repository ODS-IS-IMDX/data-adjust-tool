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
# 共通のIDを所持しているgeometryを1つにまとめた"マルチパッチ"を作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")

CREATE = "作成する"
NOT_CREATE = "作成しない"
ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertCoordinatesToMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列をマルチパッチに変換する。処理結果をZIP圧縮することも可能。
                        ①input: 座標配列と、深さか幅の少なくともどちらかを持った2行もしくは3行のFieldSetFile。
                        ②output: マルチパッチの座標配列を持った1行のFieldSetFile
                      """
        tags = ["Coordinates", "Multipatch", "ZIP", "Unpack", "Python"]

    # 角柱の入口にマルチパッチを作成するかのフラグを設定する。
    START_MULTIPATCH_FLAG = PropertyDescriptor(
        name="Start Multipatch Flag",
        description="角柱の入口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 角柱の出口にマルチパッチを作成するかのフラグを設定する。
    END_MULTIPATCH_FLAG = PropertyDescriptor(
        name="End Multipatch Flag",
        description="角柱の出口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 幅のDWH名を指定する。
    WIDTH_DWH_NAME = PropertyDescriptor(
        name="Width DWH Name",
        description="入力データの幅のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 深さのDWH名を指定する。
    DEPTH_DWH_NAME = PropertyDescriptor(
        name="Depth DWH Name",
        description="入力データの深さのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
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

    property_descriptors = [START_MULTIPATCH_FLAG,
                            END_MULTIPATCH_FLAG,
                            WIDTH_DWH_NAME,
                            DEPTH_DWH_NAME,
                            OUTPUT_DWH_NAME,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        pass

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
            start_multipatch_flag: 角柱の入口にマルチパッチを作成するかのフラグ
            end_multipatch_flag: 角柱の出口にマルチパッチを作成するかのフラグ
            width_dwh_name: 幅のDWHファイル名
            depth_dwh_name: 深さのDWHファイル名
            output_dwh_name: output_field_set_file用のgeometryのDWHファイル名
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """

        # 以下5つをNifiのプロパティから取得
        # 角柱の入口にマルチパッチを作成するかのフラグ
        start_multipatch_flag = context.getProperty(
            self.START_MULTIPATCH_FLAG).getValue()

        if start_multipatch_flag == CREATE:
            start_multipatch_flag = 1
        else:
            start_multipatch_flag = 0

        # 角柱の出口にマルチパッチを作成するかのフラグ
        end_multipatch_flag = context.getProperty(
            self.END_MULTIPATCH_FLAG).getValue()

        if end_multipatch_flag == CREATE:
            end_multipatch_flag = 1
        else:
            end_multipatch_flag = 0

        # 幅のDWHファイル名
        width_dwh_name = context.getProperty(
            self.WIDTH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 深さのDWHファイル名
        depth_dwh_name = context.getProperty(
            self.DEPTH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # output_field_set_file用のgeometryのDWHファイル名
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # ZIP圧縮するかどうかのフラグ
        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return start_multipatch_flag, end_multipatch_flag, width_dwh_name, depth_dwh_name, output_dwh_name, output_zip_flag

    def get_depth_width_list_and_coordinates_array(self, flowfile, width_dwh_name, depth_dwh_name):
        """
        概要:
            field_set_fileのValue列の値を、デコード、デシリアライズしdepth_list, width_list, coordinates_arrayにする。

        引数:
            flowfile: プロセッサに入ってくるデータ
            width_dwh_name: 幅のDWHファイル名
            depth_dwh_name: 深さのDWHファイル名

        戻り値:
            depth_list: 地物IDと深さの値が入ったlist
            width_list: 地物IDと幅の値が入ったlist
            coordinates_array: 座標配列
        """

        # flowfileのデータをNifiのプロパティから取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # flowfileのデータを、パスとして認識させる。
        field_set_file_stringio = io.StringIO(input_field_set_file)

        # DataFrameに変換。
        field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

        # DataFrameからValue列、Dwh列をリスト化。
        value_list = field_set_file_data_frame["Value"].tolist()
        dwh_list = field_set_file_data_frame["Dwh"].tolist()

        # リストからプロパティで入力した、幅,深さのDWH名のインデックスを抽出。
        width_dwh_name_index = dwh_list.index(width_dwh_name)
        depth_dwh_name_index = dwh_list.index(depth_dwh_name)

        # dwh_listの中には幅,深さ,座標の3つの要素が含まれている。値が同じものが含まれている場合は要素数が2つになってしまうので矯正が必要。
        # 幅,深さのDWH名はプロパティで指定したが、geometry(座標の値)は指定されていない。そのためここでgeometryのインデックスを取得。
        geometry_index = None
        for index in range(len(dwh_list)):
            if index != width_dwh_name_index and index != depth_dwh_name_index:
                geometry_index = index

        # Value_listの各要素を該当のインデックスを用いて行を指定し、
        # デコード、デシリアライズし、listまたはarrayに変換する。
        depth_list = pickle.loads(base64.b64decode(
            value_list[depth_dwh_name_index]))
        width_list = pickle.loads(base64.b64decode(
            value_list[width_dwh_name_index]))
        coordinates_array = pickle.loads(
            base64.b64decode(value_list[geometry_index]))

        return depth_list, width_list, coordinates_array

    def check_nan_values(self, array):
        """
        概要:
            np.nanが存在するかを確認する関数。存在する場合はErrorを返す。
            ここでインデックスと属性の数が揃っているかを確認する。（インデックスにIDを用いる）

        引数:
            array: width or depthの配列が入る。

        戻り値:
            基本的になし。引数のarrayにnanがあった場合ValueErrorを返す。
        """

        # arrayの各要素を見て、nanかどうかを判定する。nanの時その要素に対してTrueが付与される。
        nan_target_bool_array = np.isnan(array)

        # nan_target_bool_arrayに1つでもnanが含まれていればraise ValueErrorが処理される。
        if np.any(nan_target_bool_array):
            raise ValueError("座標配列にnanが含まれているため当処理を行えません。")

    def is_LineStringZ_valid(self, coordinates_array):
        """
        概要:
            LineStringZ（2点を用いて直線の情報となったもの）として成り立っているかを確認する関数。
            ➡coordinates_arrayの要素数は{ID, x成分, y成分, z成分}の4つ。0番目がIDとなる。
            IDが等しい1つものをgeometry1つとし合体したものをマルチパッチと呼ぶが、
            まず線を構成するためには点が2つ必要なので、共通のIDの数が2以上であることを確認する。

        引数:
            coordinates_array: 座標配列

        戻り値:
            id_unique_array: 出現順に直したIDの配列

        """

        # uniqueなIDを取得する。
        # id_unique_array   : 各点のIDを取得（このとき内部でソートされる）
        # id_indexes_array  : 元の配列で各点（のID）が初めて出てくる番号
        # id_counts_array   : 共通のIDの個数（点のIDが共通のもので線を結ぶため）
        id_unique_array, \
            id_indexes_array, \
            id_counts_array\
            = np.unique(coordinates_array[:, 0], return_index=True, return_counts=True)

        # 元のnumpy配列の出現順に戻す。
        id_unique_array = id_unique_array[id_indexes_array.argsort()]
        id_counts_array = id_counts_array[id_counts_array.argsort()]

        # 構成点が1つしかない地物が存在するかチェック
        linestring_judge_bool_array = id_counts_array == 1

        if np.any(linestring_judge_bool_array):
            # 構成点が1つしかない地物が存在した場合、エラーを返す。
            raise ValueError("点に対しては当処理を行えません。線に変換してください。")
        else:
            # 構成点がすべて2つ以上存在する場合はid_unique_arrayを返す。
            return id_unique_array

    def convert_np_float64_from_list(self, list):
        """
        概要:
            listをfloat型の配列に変換する関数

        引数:
            list: 加工するデータ

        戻り値:
            array: float64型の配列

        """

        # listをfloat型の配列に変換
        array = np.array(list, dtype=np.float64)

        # nanがあればValueErrorを返す
        WM.calc_func_time(self.logger, False)(self.check_nan_values)(array)

        # なければそのままarrayを返す
        return array

    def get_coordinates_by_id(self, id_unique_array, coordinates_array):
        """
        概要:
            geometryのID1つごとのインデックス取得

        引数:
            id_unique_array: ユニークな地物ID
            coordinates_array: 座標配列

        戻り値:
            id_coordinate_dict: IDごとの構成点座標

        """

        # coordinates_arrayには{ID, x成分, y成分, z成分}の4つの要素があるので、IDを除いた座標の列を指定して取得する。
        id_coordinate_dict = {id_unique_array[i]: coordinates_array[list(np.where(
            coordinates_array[:, 0] == id_unique_array[i])[0]), 1:4] for i in range(len(id_unique_array))}

        return id_coordinate_dict

    def generate_multi_patch(self, id_unique_array, id_coordinate_dict, width_array, depth_array, start_multipatch_flag, end_multipatch_flag):
        """
        概要:
            マルチパッチを生成する関数

        引数:
            id_unique_array: ユニークな地物ID
            id_coordinate_dict: IDごとの構成点座標
            width_array: 幅のIDと値の入った配列
            depth_array: 深さのIDと値の入った配列
            start_multipatch_flag: 角柱の入口にマルチパッチを作成するかのフラグ
            end_multipatch_flag: 角柱の出口にマルチパッチを作成するかのフラグ

        戻り値:
            multi_patch_list: マルチパッチ化の結果格納用リスト

        """

        # 結果格納用リスト
        multi_patch_list = []

        # 入力shpファイルのジオメトリ数だけマルチパッチ生成
        # geometryのIDでマルチパッチを作成
        for i in range(len(id_unique_array)):

            # geometryのIDが共通するものでマルチパッチを生成する。
            temporary_multipatch_array = (NCP.get_multipatch_array)(id_coordinate_dict[id_unique_array[i]],
                                                                    width_array[i, 1],
                                                                    depth_array[i, 1],
                                                                    int(start_multipatch_flag),
                                                                    int(end_multipatch_flag))

            # ポリゴンを閉じるために先頭の点を4点目に設定
            multipatch_array = np.concatenate([temporary_multipatch_array, temporary_multipatch_array[:, 0:1, :]], axis=1).reshape(
                (len(temporary_multipatch_array)*4, 6))

            # マルチパッチジオメトリID生成
            multipatch_geometry_id_array = np.repeat(np.arange(
                0, len(temporary_multipatch_array)), 4).reshape(len(multipatch_array), 1)

            # 地物ID生成
            feature_id_array = np.array(
                len(multipatch_array) * [id_unique_array[i]]).reshape(len(multipatch_array), 1)

            # ID+xyz+multi_IDの形に結合
            multipatch_array = np.concatenate(
                [feature_id_array, multipatch_array, multipatch_geometry_id_array], axis=1)

            multi_patch_list.append(multipatch_array)

        return multi_patch_list

    def create_field_set_file_from_multi_patch(self, multi_patch_list, output_dwh_name):
        """
        概要:
            output用のfield_set_fileを作る関数

        引数:
            multi_patch_list: マルチパッチ化の結果格納用リスト
            output_dwh_name: プロパティで入力したDWHファイル名

        戻り値:
            output_field_set_file: 次のプロセッサに送るためのCSV形式のデータ

        """

        # マルチパッチを結合
        coordinates_array = np.concatenate(multi_patch_list, 0)

        # output_field_set_fileのvalueの型
        geometry_type = "geometry"

        # 出力FieldSetFile仕様にリスト化
        output_dwh_name_list = [output_dwh_name]
        geometry_type_list = [geometry_type]
        coordinates_array_list = [coordinates_array]

        # マルチパッチをフィールドセットファイルに加工
        output_field_set_file = (PBP.set_field_set_file)(
            output_dwh_name_list, geometry_type_list, coordinates_array_list)

        return output_field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            # Nifiのプロパティから入力したものを呼び出す。
            start_multipatch_flag, \
                end_multipatch_flag, \
                width_dwh_name, \
                depth_dwh_name, \
                output_dwh_name, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileのValue列をbase64でデコード、pickleでデシリアライズする。
            depth_list, \
                width_list, \
                coordinates_array\
                = WM.calc_func_time(self.logger)(self.get_depth_width_list_and_coordinates_array)(flowfile, width_dwh_name, depth_dwh_name)

            # LineStringZ（2点を用いて直線となったもの）として成り立っているかを確認する
            # uniqueなIDは元のnumpy配列の出現順に直しておく
            id_unique_array = WM.calc_func_time(self.logger)(
                self.is_LineStringZ_valid)(coordinates_array)

            # マルチパッチを作成するために、幅,深さをfloat型に変換
            width_array = WM.calc_func_time(self.logger)(
                self.convert_np_float64_from_list)(width_list)
            depth_array = WM.calc_func_time(self.logger)(
                self.convert_np_float64_from_list)(depth_list)

            # IDが紐づくジオメトリだけ抽出。IDの絞り込み。
            id_unique_array, width_array = WM.calc_func_time(self.logger)(
                NSP.get_target_array)(id_unique_array, width_array)
            id_unique_array, depth_array = WM.calc_func_time(self.logger)(
                NSP.get_target_array)(id_unique_array, depth_array)

            # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
            # geometryのID1つごとのインデックス取得（共通のIDを持つものをグループ化）
            id_coordinate_dict = WM.calc_func_time(self.logger)(
                self.get_coordinates_by_id)(id_unique_array, coordinates_array)

            # geometryのID1つごとにマルチパッチを生成する。
            multi_patch_list = WM.calc_func_time(self.logger)(self.generate_multi_patch)(id_unique_array,
                                                                                         id_coordinate_dict,
                                                                                         width_array,
                                                                                         depth_array,
                                                                                         start_multipatch_flag,
                                                                                         end_multipatch_flag
                                                                                         )

            # FieldSetFileに変換する。
            output_field_set_file = WM.calc_func_time(self.logger)(
                self.create_field_set_file_from_multi_patch)(multi_patch_list, output_dwh_name)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZip圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
