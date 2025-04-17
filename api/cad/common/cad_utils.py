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

# Python標準ライブラリ
import io
import base64
import pickle
import re
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")
np = import_module("numpy")
Point = getattr(import_module("shapely.geometry"), "Point")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


def encode_value(text):
    """
    オブジェクトをシリアライズしてbase64エンコードする

    :param text: エンコードするオブジェクト
    :type text: Any
    :return: base64エンコードされた文字列
    :rtype: bytes
    """
    return base64.b64encode(pickle.dumps(text)).decode("utf-8")


def decode_value(base64_string):
    """
    base64エンコードされた文字列をデコードし、デシリアライズする

    :param base64_string: base64エンコードされた文字列
    :type base64_string: str
    :return: デコードおよびデシリアライズされたオブジェクト
    :rtype: str
    """
    return pickle.loads(base64.b64decode(base64_string))


def get_number_from_string(s: str, num_type: type = float):
    """
    文字列から指定された型の数値を取得する関数。
    数字に変換できない場合は ValueError を発生させる。

    :param s: 文字列
    :type s: str
    :param num_type: 変換後の型
    :type num_type: type

    :return: 変換された数値
    :rtype: Any

    :raises ValueError: 数字に変換できない場合に発生するエラー
    """
    try:
        # 指定された型に変換
        return num_type(s)
    except (ValueError, TypeError):
        # 数字に変換できない場合、ValueErrorを発生させる
        raise ValueError(f"'{s}' を {num_type.__name__} に変換できませんでした")


def field_set_file_to_dataframe(field_set_file, is_decode=True):
    """
    FieldSetFileをDataFrameに変換する

    :param field_set_file: FieldSetFile
    :type field_set_file: bytes
    :param is_decode: Value列のデシリアライズの実施の有無 True:実施 False:実施しない
    :type is_decode: bool

    :return: 変換されたDataFrame
    :rtype: Pandas.DataFrame
    """

    def validate_pickle_loads(byte_data):
        """
        バイトデータがpickle.loadsによるデシリアライズ処理が可能かを確認し
        可能な場合はデシリアライズした値を返す。

        :param byte_data: デシリアライズ処理対象データ
        :type byte_data: bytes

        :return: デシリアライズ処理後のインプットデータ。デシリアライズできなかった場合は入力データをそのまま返す。
        :rtype: Any
        """
        try:
            # デシリアライズを実施
            deserialized_data = pickle.loads(byte_data)
            return deserialized_data
        except (pickle.UnpicklingError, EOFError, TypeError, MemoryError) as e:
            # デシリアライズできなかった場合、入力値をそのまま返す
            return byte_data

    try:
        # FieldSetFileをデコード
        field_set_file_decode = field_set_file.decode("utf-8")

        # デコードしたFieldSetFileをpathとして扱う
        field_set_file_stringio = io.StringIO(field_set_file_decode)

        # DataFrameに加工
        dataframe = pd.read_csv(field_set_file_stringio)

        if is_decode:
            for i in range(len(dataframe)):
                # Value列の値をデシリアライズ
                deserialized_value = decode_value(dataframe.loc[i, "Value"])

                # Value値がtuple型かつ要素数が2かつ右辺がバイトデータの場合
                if isinstance(deserialized_value[0], tuple) and len(deserialized_value[0]) == 2 and isinstance(
                        deserialized_value[0][1], bytes):
                    # タプルの右辺がpickle.loadsに対応するバイナリデータの場合、デシリアライズを行い再格納
                    deserialized_value = [tuple([value[0], validate_pickle_loads(value[1])]) for value in
                                          deserialized_value]

                # デシリアライズ処理後のValue値を再格納
                dataframe.loc[i, "Value"] = deserialized_value

        return dataframe

    except Exception as e:
        raise Exception(f"field_set_file_to_dataframe_Exception: {str(e)}")


def field_set_file_to_geodataframe(
    field_set_file,
    crs=None,
    geometry_name=None,
    attr_name=None,
    feature_id_column_name="FID",
    output_dataframe=False,
    no_geometry_flg=False,
):
    """
    FieldSetFileのジオメトリ情報からGeoDataFrameを生成する

    :param field_set_file: ジオメトリ情報を持つFieldSetFile
    :type field_set_file:  bytes
    :param crs: 生成されるGeoDataFrameに設定するCRS
    :type crs: str
    :param geometry_name: 操作対象のジオメトリのデータ名 (デフォルトはNone、指定されない場合は全てのジオメトリ)
    :ype geometry_name: str
    :param attr_name: 属性情報に基づき、Trueのジオメトリのみを取得 (デフォルトはNone、指定されない場合は全て取得)
    :type attr_name: str
    :param feature_id_column_name: 地物IDの文字列を指定。 (デフォルトは"FID")
    :type feature_id_column_name: str
    :param output_dataframe: 出力形式のフラグ。Falseの場合GeoDataFrame、Trueの場合DataFrameの形式で出力。（デフォルトはFalse）
    :type output_dataframe: bool
    :param no_geometry_flg: 入力データにジオメトリ情報が含まれていないことを示すフラグ。（デフォルトはFalse）
    :type no_geometry_flg: bool

    :return: GeoDataFrame
    :rtype: Pandas.GeoDataFrame
    """
    try:
        # FieldSetFileからDataFrameを作成
        df = field_set_file_to_dataframe(field_set_file)

        # '/' で分割し、前半をカテゴリ、後半をキーとして新しいカラムに追加
        df["category"] = df["Dwh"].str.split("/").str[0]
        df["subkey"] = df["Dwh"].str.split("/").str[1]

        # category列をlayer属性として設定
        df["layer"] = df["category"]

        # Typeがresultsのものは除外
        df = df[df["Type"] != "results"]

        # 全カテゴリのデータを格納するリスト
        all_properties = []

        # geometry_nameが指定されている場合、そのカテゴリ内でループ
        if geometry_name:
            categories = [geometry_name]
        else:
            # geometry_nameがNoneの場合、全てのカテゴリを処理対象とする
            categories = df["category"].unique()

        # 各カテゴリごとに処理
        for category in categories:
            attributes = {}  # 属性情報格納先

            # 出力形式ごとに処理を分岐
            if output_dataframe:
                # 出力がDataFrameの場合
                # 対象のDataFrameを取得
                target_df = df[df["category"] == category].reset_index(drop=True)

                # 他の属性情報を辞書に格納
                for subkey in target_df["subkey"].unique():
                    attributes[subkey] = target_df[target_df["subkey"] == subkey][
                        "Value"
                    ].values[0]

                data_num = len(
                    target_df.loc[0, "Value"]
                )  # 先頭レコードの値の数->レイヤごとのレコード数

                # 各ジオメトリのデータを保存するDataFrameを構築
                data = {"layer": [category] * data_num}  # 同じlayer値を設定

            else:
                # 出力がGeoDataFrameの場合
                # 対象のDataFrameを取得
                target_df = df[(df["category"] == category)]
                geometry_df = target_df[target_df["Type"] == "geometry"]

                # ジオメトリデータが無い場合は例外を発生させる
                if geometry_df.empty and not no_geometry_flg:
                    raise ValueError(
                        f"カテゴリ '{category}' に対応するジオメトリが存在しません。"
                    )

                # ジオメトリリストの取得
                geometry = geometry_df["Value"].values[0]

                # ジオメトリ情報がGeoNdarrayの場合、ジオメトリの基本的な構造（地物オブジェクトの配列）に変換する
                if isinstance(geometry, np.ndarray) and geometry.ndim == 2:
                    # GeoNdarrayからGeoDataFrameを生成し、GeoDataFrameのジオメトリデータでジオメトリ情報を上書きする
                    gdf_from_geo_ndarray = geo_ndarray_to_geodataframe(geometry)
                    geometry = gdf_from_geo_ndarray.geometry.tolist()

                data_num = len(geometry)  # ジオメトリの数を取得->レイヤごとのレコード数

                # 他の属性情報を辞書に格納
                df_except_geometry = target_df[target_df["Type"] != "geometry"]
                for subkey in df_except_geometry["subkey"]:
                    attributes[subkey] = target_df[target_df["subkey"] == subkey][
                        "Value"
                    ].values[0]

                # 各ジオメトリのデータを保存するDataFrameを構築
                data = {
                    "geometry": geometry,  # ジオメトリを直接設定
                    "layer": [category] * data_num,  # 同じlayer値を設定
                }

            # ジオメトリ情報、レイヤ情報以外の属性データの処理を行う
            if feature_id_column_name in attributes:
                # FID情報が存在する場合
                if (
                    attr_name
                    and attr_name.strip() != ""
                    and target_df[target_df["Dwh"] == f"{category}/{attr_name}"].empty
                ):
                    # 属性が指定されいている場合でその属性の行が存在しなければ
                    raise ValueError(f"{category}/{attr_name} が存在しません。")

                # fidxとfid_valueを取り出し、それぞれの値で配列を作成
                if feature_id_column_name == "FID":
                    fid_values, fidxs = zip(*attributes[feature_id_column_name])
                else:
                    fidxs, fid_values = zip(*attributes[feature_id_column_name])
                fid_values = np.array(fid_values)
                fidxs = np.array(fidxs)

                # 各属性をDataFrameに追加
                for att_key, att_val in attributes.items():
                    if att_key == feature_id_column_name:
                        # 地物IDの列を作成
                        data[att_key] = fid_values
                        data["FIndex"] = fidxs
                    else:
                        if isinstance(att_val[0], tuple):
                            # fidxに基づく属性マッピングを効率化
                            # att_valからfidxとその対応する値を抽出して、辞書形式に変換
                            att_val_dict = {fidx: val for fidx, val in att_val}
                            # fidxsに基づきatt_val_dictから対応する値を取得
                            att_new_val = [att_val_dict.get(fidx, np.nan) for fidx in fidxs]
                            data[att_key] = att_new_val
                        elif isinstance(att_val, list) and isinstance(att_val[0], list):
                            # リストの0番目のリストから値を取得して一括で設定
                            data[att_key] = att_val[0]
                        else:
                            # リスト全体から値を取得して一括で設定
                            data[att_key] = att_val

                # 現カテゴリのデータをDataFrameとして作成しリストに追加
                current_properties = pd.DataFrame(data)
                all_properties.append(current_properties)
            else:
                if attr_name and attr_name.strip() != "":
                    # FIDの行が存在せず、属性が指定されいている場合
                    raise ValueError(f"{category}/FID が存在しません。")
                else:
                    # attributesをDataFrameとして一括で設定
                    attributes_df = pd.DataFrame(attributes)
                    attributes_df = attributes_df.reindex(range(data_num)).fillna(
                        np.nan
                    )

                    # geometryおよびcategoryのデータと結合
                    properties_df = pd.DataFrame(data).join(attributes_df)
                    all_properties.append(properties_df)

        # リストに格納された全カテゴリのデータを結合し、nanをNoneに差し替え
        df_properties = pd.concat(all_properties, ignore_index=True).replace(
            np.nan, None
        )

        # 属性が指定されている場合
        if attr_name and attr_name.strip() != "":
            # 属性がTrueのもののみを抽出
            df_properties = df_properties[df_properties[attr_name]]

        # GeoDataFrameのgeometry列を指定して作成
        if output_dataframe is False:
            if crs is None:
                geodataframe = gpd.GeoDataFrame(
                    df_properties, geometry="geometry", crs=None
                )
            else:
                geodataframe = gpd.GeoDataFrame(
                    df_properties, geometry="geometry", crs=f"EPSG:{crs}"
                )
        else:
            geodataframe = df_properties

        return geodataframe

    except Exception as e:
        raise Exception(f"Error converting FieldSetFile to GeoDataFrame: {str(e)}")


def dataframe_to_field_set_file(df):
    """
    DataFrameをFieldSetFileに変換する

    :param df: DataFrame
    :type df: pandas.DataFrame

    :return: FieldSetFile
    :rtype: str
    """
    if len(df) <= 0:
        return pd.DataFrame()

    new_df = df.copy()

    dwh_list = []
    field_value_list = []
    field_type_list = []
    for index, row in new_df.iterrows():
        dwh_list.append(row["Dwh"])
        field_type_list.append(row["Type"])
        row_value = encode_value(row["Value"])

        field_value_list.append(row_value)

    return NSP.get_field_set_file(field_type_list, field_value_list, dwh_list)


def geo_ndarray_to_ndarray_dict(geo_ndarray):
    """
    numpy配列のGeoNdarrayから、IDをKeyとする辞書構造に変換

    :param geo_ndarray: ndarray形式のジオメトリ情報
    :type geo_ndarray: np.ndarray
    
    :return: IDをKeyとする辞書構造のジオメトリ情報
    :rtype: dict
    """
    ndarray_dict = {}

    # ジオメトリIDを取得
    unique_ids = np.unique(geo_ndarray[:, 0])

    for geom_id in unique_ids:
        # 同じIDの座標を取得
        geom_points = geo_ndarray[geo_ndarray[:, 0] == geom_id][
            :, 1:
        ]  # IDを除いた座標部分を取得

        # 形式を変換
        if geom_points.shape[1] == 2:  # 2次元（Zがない場合）
            geom_array = np.array([np.array([x, y]) for x, y in geom_points])
        elif geom_points.shape[1] == 3:  # 3次元（Zがある場合）
            geom_array = np.array([np.array([x, y, z]) for x, y, z in geom_points])

        # 辞書に追加
        ndarray_dict[int(geom_id)] = geom_array

    return ndarray_dict


def geo_ndarray_to_geodataframe(geo_ndarray):
    """
    numpy配列のGeoNdarrayからGeoDataFrameに変換

    :param geo_ndarray: ndarray形式のジオメトリ情報
    :type geo_ndarray: np.ndarray

    :return: GeoDataFrame
    :rtype: pandas.GeoDataFrame
    """
    # GeoNdarrayを辞書型に変換する
    ndarray_dict = geo_ndarray_to_ndarray_dict(geo_ndarray)

    # Shapelyオブジェクトのリストを作成（Point, Polygon, LineStringを判断して追加）
    geometries = []
    for _id, shape in ndarray_dict.items():
        if len(shape) == 1:
            # Pointオブジェクトとして追加
            geometries.append(Point(shape[0]))
        elif len(shape) > 2 and np.array_equal(shape[0], shape[-1]):
            # Polygonオブジェクトとして追加
            geometries.append(Polygon(shape))
        else:
            # LineStringオブジェクトとして追加
            geometries.append(LineString(shape))

    # GeoDataFrameを作成（Shapelyオブジェクトのリストを使用）
    return gpd.GeoDataFrame(geometry=geometries)


def add_index_column(geodataframe, findex_type="float"):
    """
    GeoDataFrameにインデックス列'Findex'を追加する関数

    :param geodataframe: GeoDataFrame
    :type geodataframe: pandas.GeoDataFrame
    :param findex_type: Findex値の型（デフォルトはfloat）
    :type findex_type: float
    
    :return: GeoDataFrame
    :rtype: pandas.GeoDataFrame
    """
    # インデックス列'Findex'を追加
    index_list = np.arange(0, len(geodataframe), dtype=findex_type)

    geodataframe["Findex"] = index_list

    return geodataframe


def geodataframe_to_fieldsetfile(
    geodf, attribute_name_list, fid_column_name="", layer_column="layer"
):
    """
    GeoDataFrameをFieldSetFileに変換する。
    レイヤごと、ジオメトリタイプごとで処理する。

    :param geodf: GeoDataFrame
    :type geodf: pandas.GeoDataFrame
    :param attribute_name_list: Dwh名リスト
    :type attribute_name_list: list
    :param layer_column: 使用するレイヤの列名（デフォルトは"layer"）
    :type layer_column: str
    :param fid_column_name: 使用するFIDの列名（デフォルトは空）
    :type fid_column_name: str

    :return: FieldSetFile
    :rtype: str
    """
    try:
        # レイヤ列の存在を確認し、存在しない場合はエラー
        if layer_column not in geodf.columns:
            raise ValueError(f"指定されたレイヤ列 '{layer_column}' が存在しません。")

        # レイヤごとのGeoDataFrameを取得
        unique_layer_list = geodf[layer_column].unique()

        # FieldSetFile生成用データ格納先
        field_dwh_list = []  # Dwh列
        field_type_list = []  # Type列
        field_value_list = []  # Value列

        # 1. フィルタ前の各列のデータ型を保持
        dtypes_before = geodf.dtypes

        for layer_name in unique_layer_list:
            # 対象レイヤ名のGeoDataFrameを抽出
            geodataframe_by_layer = geodf[
                geodf[layer_column] == layer_name
            ].reset_index(drop=True)

            # 対象レイヤのジオメトリタイプリストを取得
            geom_type_list = geodataframe_by_layer.geometry.geom_type.unique()

            for geom_type in geom_type_list:
                # 対象ジオメトリタイプのGeoDataFrameを抽出
                gdf_by_geom_type = geodataframe_by_layer[
                    geodataframe_by_layer.geometry.geom_type == geom_type
                ].reset_index(drop=True)

                # 値のないものはNoneとする
                for field_name in attribute_name_list:
                    if field_name == "geometry":
                        continue
                    gdf_by_geom_type[field_name] = gdf_by_geom_type[field_name].apply(
                        lambda x: x if not pd.isna(x) else None
                    )

                # 3. フィルタ後に型を再設定する
                for column in gdf_by_geom_type.columns:
                    gdf_by_geom_type[column] = gdf_by_geom_type[column].astype(
                        dtypes_before[column]
                    )

                # GeoDataFrameにFindex列を追加
                gdf_by_geom_type = add_index_column(gdf_by_geom_type)

                # ジオメトリリストとFieldSetFileのType列の値を取得
                field_value_list_by_layer, field_type_list_by_layer = (
                    NSP.write_field_file(gdf_by_geom_type, attribute_name_list)
                )

                # [] 内の内容を含む角括弧を削除する処理
                field_type_list_by_layer = [
                    re.sub(r"\[.*?\]", "", str(dtype))
                    for dtype in field_type_list_by_layer
                ]

                # FID値を取得
                base64_geometry_items_list, fid_count = NSP.write_feature_file(
                    gdf_by_geom_type, fid_column_name
                )

                # Dwh名にレイヤー名を追加
                field_dwh_list_by_layer = [
                    f"{layer_name}/{dwh_file_name}"
                    for dwh_file_name in attribute_name_list
                ]
                # レイヤごとの情報にFID情報を追加する
                field_dwh_list_by_layer.append(f"{layer_name}/FID")
                field_value_list_by_layer.append(base64_geometry_items_list)
                field_type_list_by_layer.append("string")

                # FieldSetFile生成用データに、レイヤごとの情報を格納
                field_dwh_list.extend(field_dwh_list_by_layer)
                field_type_list.extend(field_type_list_by_layer)
                field_value_list.extend(field_value_list_by_layer)

        # FieldSetFileを生成
        field_set_file = NSP.get_field_set_file(
            field_type_list, field_value_list, field_dwh_list
        )

        return field_set_file

    except Exception as e:
        raise Exception(f"[ERROR convert_geodataframe_to_fieldsetfile]: {repr(e)}")
