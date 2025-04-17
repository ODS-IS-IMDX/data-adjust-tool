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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 複数のGeoDataFrameもしくはDataframeを、縦結合か横結合かを指定して結合する。
# 縦結合の場合は各DWH名をfilename列として追加する。
# ※横結合の場合は2行のFieldSetFileのみ対応。
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
gpd = import_module("geopandas")


class JoinGeoDataFrame(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        複数のGeoDataFrameもしくはDataframeを、縦結合か横結合かを指定して結合する。
                        縦結合の場合は各DWH名をfilename列として追加する。
                        ※横結合の場合は2行のFieldSetFileのみ対応。
                        ①input: 結合したいGeoDataFrameで構成された2行以上のFieldSetFile。
                        ②output: シリアライズされたGeoDataFrame。
                      """
        tags = ['GeoDataFrame', 'Python']

    # 結合方法
    JOIN_METHOD = PropertyDescriptor(
        name="Join Method",
        description="結合方法",
        allowable_values=["縦結合(外部結合)", "縦結合(内部結合)",
                          "横結合(内部結合)", "横結合(外部結合)", "横結合(左結合)", "横結合(右結合)"],
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 結合される側のGeoDataFrameのDWH名
    MAIN_DWH_NAME = PropertyDescriptor(
        name="Main DWH Name",
        description="結合される側のGeoDataFrameのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 横結合の際の結合先のGeoDataFrameのDWH名(同じカラム名は想定しない)
    HORIZONTAL_JOIN_TARGET_COLUMN = PropertyDescriptor(
        name="Horizontal Join Target Column",
        description="結合先のGeoDataFrameのDWH名",
        default_value="""
下記を参考にして修正すること

==========ここから==========
column1, column2, column3
==========ここまで==========

結合する側のDataFrameで追加したいカラム名をカンマ区切りで記述。
※横結合の場合にのみ記述。
""",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    BASE_KEY_COLUMN_NAME = PropertyDescriptor(
        name="Base Key Column Name",
        description="結合される側のキーとなる列のカラム名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    TARGET_KEY_COLUMN_NAME = PropertyDescriptor(
        name="Target Key Column Name",
        description="結合する側のキーとなる列のカラム名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )
    property_descriptors = [JOIN_METHOD,
                            MAIN_DWH_NAME,
                            HORIZONTAL_JOIN_TARGET_COLUMN,
                            BASE_KEY_COLUMN_NAME,
                            TARGET_KEY_COLUMN_NAME]

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
            join_method: 出力先の文字コード
            base_key_column_name: 出力先フォルダ
            main_dwh_name: 出力ファイル名
        """

        join_method = context.getProperty(self.JOIN_METHOD).getValue()

        main_dwh_name = context.getProperty(
            self.MAIN_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        join_target_column = context.getProperty(
            self.HORIZONTAL_JOIN_TARGET_COLUMN).evaluateAttributeExpressions(flowfile).getValue()

        base_key_column_name = context.getProperty(
            self.BASE_KEY_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        target_key_column_name = context.getProperty(
            self.TARGET_KEY_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return join_method, main_dwh_name, join_target_column, base_key_column_name, target_key_column_name

    def get_flowfile(self, flowfile):
        """
        概要:
            flowfileからCSV形式のデータを読み込み、Dwh列でグループ化する

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            grouped_list: Dwh列"/"でグループ化されたデータフレームのリスト
        """

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_dataframe = pd.read_csv(
            io.StringIO(input_field_set_file))

        return field_set_file_dataframe

    def create_use_column_name_list(self, join_target_column, target_key_column_name, property_join_method):
        """
        概要:
            プロパティで設定した使用カラム名の取得とfilenameの追加(縦結合の場合)関数

        引数:
            join_target_column: 横結合で使用する使用カラム名
            target_key_column_name: 結合する側のキーとなる列のカラム名
            property_join_method: 結合方法

        戻り値:
            use_column_name_list: 結合する際に使用するカラム名
        """

        # ","でsplitしたのちcol.strip()で前後に余計な空白があった際、空白を除去しカラム名リストに
        use_column_name_list = [col.strip()
                                for col in join_target_column.split(',')]

        # カラム名リストにキーが入っていなかった際にキーを追加(後の処理で使用カラムリスト変数に入っていない列は削除されるため)
        if target_key_column_name not in use_column_name_list:
            use_column_name_list.append(target_key_column_name)

        # 縦結合の場合はfilenameが必要　後続処理でuse_column_name_list以外のものをドロップするためここで追加
        if property_join_method in ["縦結合(外部結合)", "縦結合(内部結合)"]:
            use_column_name_list.append("filename")

        return use_column_name_list

    def vertical_join(self, main_geodataframe, actual_join_method, geodataframes_list):
        """
        概要:
            縦結合用関数

        引数:
            main_geodataframe: 結合される側のGeoDataFrame
            actual_join_method: 結合処理を行う際、joinに指定する結合方法
            geodataframes_list: 結合する側のGeoDataFrameが格納されたリスト

        戻り値:
            link_geodataframe: 結合後のGeoDataFrame
        """

        # main_geodataframeとgeodataframes_list内のGeoDataFrameを結合
        for geodataframe in geodataframes_list:

            main_geodataframe = pd.concat(
                [main_geodataframe, geodataframe], join=actual_join_method, ignore_index=True)

        link_geodataframe = main_geodataframe

        return link_geodataframe

    def horizontal_join(self, actual_join_method, main_geodataframe, base_key_column_name, target_key_column_name, geodataframes_list):
        """
        概要:
            横結合用関数

        引数:
            actual_join_method: 結合処理を行う際、howに指定する結合方法
            main_geodataframe: 結合される側のGeoDataFrame
            base_key_column_name: 結合される側のキー
            geodataframes_list: 結合する側のGeoDataFrameが格納されたリスト

        戻り値:
            link_geodataframe: 結合後のGeoDataFrame
        """

        # main_geodataframeとgeodataframes_list内のGeoDataFrameを結合
        for geodataframe in geodataframes_list:

            # base_key_column_nameプロパティで指定したキーを基に横結合
            main_geodataframe = main_geodataframe.merge(
                geodataframe, how=actual_join_method, left_on=base_key_column_name, right_on=target_key_column_name)

        link_geodataframe = main_geodataframe

        return link_geodataframe

    def convert_data_types(self, link_geodataframe, type_dict):
        """
        概要:
            列名をキーにした型情報を持つ辞書を用いて型変換を行う関数。

        引数:
            link_geodataframe: 変換対象のGeoDataFrame。
            type_dict: 列名をキーにした型情報を持つ辞書。

        戻り値:
            GeoDataFrame: 型変換後のGeoDataFrame。
        """

        # 辞書を用いて型変換を行う
        for column, dtype in type_dict.items():

            if dtype == 'object':
                # NoneとNaNを統一してからobject型に変換(NaN→Noneに変更)
                link_geodataframe[column] = link_geodataframe[column].apply(
                    lambda x: None if pd.isna(x) else x)

                link_geodataframe[column] = link_geodataframe[column].astype(
                    object)

            elif dtype == 'int64':
                link_geodataframe[column] = link_geodataframe[column].fillna(
                    0).astype("int64")

            elif dtype == 'float64':
                link_geodataframe[column] = link_geodataframe[column].astype(
                    "float64")

            elif dtype == 'geometry':  # GeoDataFrame特有の型
                link_geodataframe[column] = gpd.GeoSeries(
                    link_geodataframe[column])

            elif dtype == "str":
                link_geodataframe[column] = link_geodataframe[column].astype(
                    str)

        return link_geodataframe

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            property_join_method, \
                main_dwh_name, \
                join_target_column, \
                base_key_column_name, \
                target_key_column_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileを取得
            # --------------------------------------------------------------------------
            field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # field_set_file_data_frameの行数を取得
            field_set_file_dataframe_rows = field_set_file_dataframe.shape[0]

            # 横結合は2行のFieldSetFileのみ対応なのでそれ以上の行数でエラー
            if field_set_file_dataframe_rows > 2:
                if property_join_method in ["横結合(内部結合)", "横結合(外部結合)", "横結合(左結合)", "横結合(右結合)"]:
                    self.logger.error("エラー: 横結合選択は2行のFieldSetFileにのみ対応しています。")
                    return FlowFileTransformResult(relationship="failure")

            # --------------------------------------------------------------------------
            # 結合する側の使用カラム名リストを取得(横結合で使用)
            # --------------------------------------------------------------------------
            if property_join_method in ["横結合(内部結合)", "横結合(外部結合)", "横結合(左結合)", "横結合(右結合)"]:
                use_column_name_list\
                    = WM.calc_func_time(self.logger)(self.create_use_column_name_list)(
                        join_target_column, target_key_column_name, property_join_method)

            dwh_list = field_set_file_dataframe["Dwh"].to_list()
            value_list = field_set_file_dataframe["Value"].to_list()

            # メインのGeoDataFrame取得に使用
            main_dwh_index = dwh_list.index(main_dwh_name)

            # 結合する側のGeoDataFrame格納リストと結合後の型変換用辞書
            geodataframes_list = []
            type_dict = {}

            # 各GeoDataFrameを処理し、型情報を辞書に保存
            for i, value in enumerate(value_list):
                geodata_frame = pickle.loads(base64.b64decode(value))

                # メインのGeoDataFrame取得
                if i == main_dwh_index:
                    main_geodataframe = geodata_frame

                    # メインのGeoDataFrameにもfilename列追加
                    if property_join_method in ["縦結合(外部結合)", "縦結合(内部結合)"]:
                        main_geodataframe["filename"] = dwh_list[i]

                    continue

                # 横結合の際はfilename必要ない
                if property_join_method in ["縦結合(外部結合)", "縦結合(内部結合)"]:
                    geodata_frame["filename"] = dwh_list[i]

                if property_join_method in ["横結合(内部結合)", "横結合(外部結合)", "横結合(左結合)", "横結合(右結合)"]:

                    # use_column_list に含まれるカラムだけを選択
                    selected_columns = [
                        col for col in geodata_frame.columns if col in use_column_name_list]
                    geodata_frame = geodata_frame[selected_columns]

                    # 結合する側はgeometryが必要ないのでここでドロップ
                    if 'geometry' in geodata_frame.columns:
                        geodata_frame = geodata_frame.drop(columns='geometry')

                # 各列のカラム名をkey、データ型をvalueとして辞書に格納
                for column in geodata_frame.columns:
                    type_dict[column] = str(geodata_frame[column].dtype)

                geodataframes_list.append(geodata_frame)

            # --------------------------------------------------------------------------
            # 縦結合処理用の処理方法取得
            # --------------------------------------------------------------------------
            if property_join_method in ["縦結合(外部結合)", "縦結合(内部結合)"]:

                if property_join_method == "縦結合(外部結合)":
                    actual_join_method = "outer"

                if property_join_method == "縦結合(内部結合)":
                    actual_join_method = "inner"

                # --------------------------------------------------------------------------
                # 縦結合処理
                # --------------------------------------------------------------------------
                link_geodataframe\
                    = WM.calc_func_time(self.logger)(self.vertical_join)(main_geodataframe, actual_join_method, geodataframes_list)

            elif property_join_method in ["横結合(内部結合)", "横結合(外部結合)", "横結合(左結合)", "横結合(右結合)"]:

                # 同じカラム名のリストを作成
                duplicate_columns = []

                for geodataframe in geodataframes_list:

                    # GeoDataFrameのカラムとuse_column_name_listを比較
                    for col in geodataframe.columns:
                        if col in main_geodataframe.columns:
                            # キーは同じでよい
                            if col == base_key_column_name:
                                continue
                            duplicate_columns.append(col)

                    # 同じカラム名が見つかった場合にエラーメッセージを出力
                    if duplicate_columns:
                        self.logger.error(
                            "エラー: targetとbaseに同じカラム名が存在: " + ", ".join(duplicate_columns))
                        return FlowFileTransformResult(relationship="failure")

                    # --------------------------------------------------------------------------
                    # 結合される側とする側のキーの型が異なる場合に一致させる(基本的には同じものを想定)
                    # --------------------------------------------------------------------------
                    # key_column_nameで指定したキーを基に横結合する前に型を合わせる
                    main_dtype = main_geodataframe[base_key_column_name].dtype

                    # GeoDataFrameのキー列のデータ型
                    geodataframe_dtype = geodataframe[target_key_column_name].dtype

                    # 型が異なっていれば、メインのGeoDataFrameの型に合わせる
                    if main_dtype != geodataframe_dtype:
                        geodataframe[target_key_column_name] = geodataframe[target_key_column_name].astype(
                            main_dtype)

                # --------------------------------------------------------------------------
                # 横結合処理用の処理方法取得
                # --------------------------------------------------------------------------
                if property_join_method == "横結合(内部結合)":
                    actual_join_method = "inner"

                if property_join_method == "横結合(外部結合)":
                    actual_join_method = "outer"

                if property_join_method == "横結合(左結合)":
                    actual_join_method = "left"

                if property_join_method == "横結合(右結合)":
                    actual_join_method = "right"

                # --------------------------------------------------------------------------
                # 横結合処理
                # --------------------------------------------------------------------------

                link_geodataframe\
                    = WM.calc_func_time(self.logger)(self.horizontal_join)(actual_join_method, main_geodataframe, base_key_column_name, target_key_column_name, geodataframes_list)

            # --------------------------------------------------------------------------
            # 結合後のGeoDataFrame-DataFrameが空だった場合エラー出力
            # --------------------------------------------------------------------------
            if link_geodataframe.shape[0] == 0:
                self.logger.error("エラー: 結合結果が空でした。結合方法を再度確認してください。")
                return FlowFileTransformResult(relationship="failure")

            # --------------------------------------------------------------------------
            # 結合方法によっては結合によってカラムが消える可能性があるため、型変換処理前にカラムを調整
            # --------------------------------------------------------------------------
            link_gdf_columns = link_geodataframe.columns.tolist()
            # type_dictからlink_geodataframeのカラムに存在しないキーを削除
            type_dict = {key: value for key,
                         value in type_dict.items() if key in link_gdf_columns}

            # --------------------------------------------------------------------------
            # 型変換処理
            # --------------------------------------------------------------------------
            link_geodataframe\
                = WM.calc_func_time(self.logger)(self.convert_data_types)(link_geodataframe, type_dict)

            return FlowFileTransformResult(relationship="success",
                                           contents=pickle.dumps(link_geodataframe))

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
