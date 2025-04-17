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

from importlib import import_module

# 外部ライブラリの動的インポート
strtobool = getattr(import_module("distutils.util"), "strtobool")
np = import_module("numpy")
gpd = import_module("geopandas")
CRS = getattr(import_module("pyproj"), "CRS")
CRSError = getattr(import_module("pyproj.exceptions"), "CRSError")

import cad.common.cad_utils as CU
import cad.common.cad_spatial as CS  # 空間演算処理共通化
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class SpatialOperationsLogic:

    # Operation Typeの定義
    OPERATION_INTERSECTS = "Intersects"
    OPERATION_CONTAINS = "Contains"
    OPERATION_WITHIN = "Within"
    OPERATION_TOUCHES = "Touches"
    OPERATION_CROSSES = "Crosses"
    OPERATION_OVERLAPS = "Overlaps"
    OPERATION_COVERS = "Covers"
    OPERATION_COVERED_BY = "Covered_by"
    OPERATION_EQUALS = "Equals"
    OPERATION_DISTANCE = "Distance"
    OPERATION_ISCIRCLE = "IsCircle"
    OPERATION_TOUCHESENDPOINT = "TouchesEndpoint"
    OPERATION_CASCADETOUCHES = "CascadeTouches"
    OPERATION_MOSTINTERSECTS = "MostIntersects"

    # Post Operation Typeの定義
    POST_OPERATION_NONE = "None"
    POST_OPERATION_UNION = "Union"
    POST_OPERATION_INTERSECTION = "Intersection"
    POST_OPERATION_DIFFERENCE = "Difference"
    POST_OPERATION_SYMMETRICAL_DIFFERENCE = "SymmetricalDifference"
    POST_OPERATION_CLIP = "Clip"

    def convert_from_dataframe_to_geodataframe(self, dataframe, all_params):
        """
        DataFrameをGeoDataFrameに変換する。

        :param dataframe: FieldSetFileの内容を含むDataFrame。
        :type dataframe: pandas.DataFrame
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: DataFrameから生成されたGeoDataFrame。
        :rtype: tuple(geopandas.GeoDataFrame, geopandas.GeoDataFrame)

        :raises Exception: 処理エラーが発生した場合に発生する。
        """

        def create_geodataframe(dataframe, geometry_name, attr_name, crs):
            """
            GeoDataFrameを生成する。

            :param dataframe: 変換対象のDataFrame。
            :type dataframe: pandas.DataFrame
            :param geometry_name: 対象のジオメトリデータ名。
            :type geometry_name: str
            :param attr_name: 属性情報。
            :type attr_name: str
            :param crs: 座標参照系(CRS)。
            :type crs: str

            :return: DataFrameから生成されたGeoDataFrame。
            :rtype: geopandas.GeoDataFrame

            :raises ValueError: プロパティで指定されたデータがFieldSetFile内に存在しない場合に発生する。
            :raises Exception: 処理エラーが発生した場合に発生する。
            """
            try:
                # 処理対象のジオメトリデータが存在するかどうかをチェック
                if (dataframe["Dwh"] == f"{geometry_name}/geometry").any():
                    geometry = dataframe.loc[
                        dataframe["Dwh"] == f"{geometry_name}/geometry", "Value"
                    ].values[0]
                else:
                    raise ValueError(f"{geometry_name}/geometry does not exist.")

                if not attr_name or attr_name.strip() == "":
                    # フィルタリング属性の指定が無い場合、処理対象のFIDデータが存在するかどうかをチェック
                    if (dataframe["Dwh"] == f"{geometry_name}/FID").any():
                        fids = dataframe.loc[
                            dataframe["Dwh"] == f"{geometry_name}/FID", "Value"
                        ].values[0]
                    else:
                        raise ValueError(f"{geometry_name}/FID does not exist.")
                    geodataframe = gpd.GeoDataFrame(
                        {"FID": fids}, geometry=geometry, crs=crs
                    )
                else:
                    # フィルタリング属性の指定がある場合、フィルタリング属性データのTrue、Falseの値を用いてGeoDataFrameを作成
                    properties = []
                    selected_geometries = []

                    # 処理対象のフィルタリング属性データが存在するかどうかをチェックして取得
                    if (dataframe["Dwh"] == f"{geometry_name}/{attr_name}").any():
                        fid_tf_list = dataframe.loc[
                            dataframe["Dwh"] == f"{geometry_name}/{attr_name}", "Value"
                        ].values[0]
                    else:
                        raise ValueError(f"{geometry_name}/{attr_name} does not exist.")

                    # Trueの値のジオメトリのみを抽出
                    for idx, (fid, tf) in enumerate(fid_tf_list):
                        if tf:
                            properties.append({"FID": (str(float(fid)), float(fid))})
                            selected_geometries.append(geometry[idx])

                    geodataframe = gpd.GeoDataFrame(
                        properties, geometry=selected_geometries, crs=crs
                    )

                return geodataframe

            except Exception:
                raise Exception(f"[ERROR create_geodataframe]: {str(e)}")

        try:
            # メイン側のGeoDataFrameを取得
            main_geodf = create_geodataframe(
                dataframe,
                all_params["main_geom_name"],
                all_params["main_attr_name"],
                all_params["crs"],
            )

            # 演算タイプがIsCircleの場合はメインのGeoDataFrameのみ取得する
            if all_params["operation_type"] == self.OPERATION_ISCIRCLE:
                return main_geodf

            # サブ側のGeoDataFrameを取得
            sub_geodf = create_geodataframe(
                dataframe,
                all_params["sub_geom_name"],
                all_params["sub_attr_name"],
                all_params["crs"],
            )

            return main_geodf, sub_geodf

        except Exception as e:
            raise Exception(f"[ERROR convert_from_dataframe_to_geodataframe]: {str(e)}")

    def make_output_format(self, datas, all_params):
        """
        指定された出力形式(Output Format)に対応した処理結果を作成する

        :param datas: 空間演算処理結果のデータ。
        :type datas: list[tuple(float)]
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: 出力形式に整形したデータ。
        :rtype: list[tuple(float)]

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            if all_params["output_format"] == "pairs":
                # nanを許容しない
                result = [
                    (idx, val)
                    for idx, val in datas
                    if (
                        isinstance(val, float)
                        and not np.isnan(val)  # 値が数値の場合
                        or (
                            isinstance(val, tuple)
                            and all(not np.isnan(v) for v in val)  # 値がタプルの場合
                        )
                    )
                ]
            elif all_params["output_format"] == "list":
                temp_dict = dict()
                # pairs形式のデータをlist形式へ変換
                for key, value in datas:
                    if key in temp_dict:
                        temp_dict[key].append(value)
                    else:
                        temp_dict[key] = [value]

                pairs_to_list = [(key, value) for key, value in temp_dict.items()]

                # 出力形式がlistの場合、Include Empty Resultsプロパティの値（空の値を許容するかのフラグ）で分岐
                if all_params["include_empty_results"]:
                    # nanを許容（nanはNoneに変換）
                    result = [
                        (
                            sub_list
                            if not (
                                isinstance(sub_list[1][0], float)
                                and np.isnan(sub_list[1][0])
                            )
                            else (sub_list[0], None)
                        )
                        for sub_list in pairs_to_list
                    ]
                else:
                    # nanを許容しない
                    result = [
                        sub_list
                        for sub_list in pairs_to_list
                        if not (
                            isinstance(sub_list[1][0], float)
                            and np.isnan(sub_list[1][0])
                        )
                    ]
            else:
                raise

            return result
        except Exception as e:
            raise Exception(f"[ERROR make_output_format]: {str(e)}")

    def process_results(self, result, all_params):
        """
        結果データを整形後、シリアライズしてbase64でエンコードする。

        :param result: 空間演算処理結果のデータ。
        :type result: list[tuple(float)]
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: シリアライズしてbase64でエンコードしたデータ
        :rtype: str

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            # IsCircleとCascadeTouches以外について、出力形式に応じてデータをフォーマット
            format_result = result
            if (
                not all_params["operation_type"] == self.OPERATION_ISCIRCLE
                and not all_params["operation_type"] == self.OPERATION_CASCADETOUCHES
            ):
                format_result = self.make_output_format(result, all_params)

            # フォーマットされたデータをFieldSetFile用に変換
            base64_serialized_results = CU.encode_value(format_result)

            return base64_serialized_results
        except Exception as e:
            raise Exception(f"[ERROR process_results]: {str(e)}")

    def format_field_set_file_data(self, results, all_params):
        """
        FieldSetFile用にデータを生成する。

        :param results: 演算結果のリスト。
        :type results: list[tuple(float)]
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: FieldSetFileに格納するType列の値のリスト、Value列の値のリスト、Dwh列の値のリスト
        :rtype: tuple(list[str],list[str],list[str])

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            if all_params["operation_type"] == self.OPERATION_TOUCHESENDPOINT:
                # 演算タイプがTouchesEndpointの場合
                # 共通処理関数を使用して結果を処理(2行のみ)
                field_value_list = [
                    (
                        self.process_results(result, all_params)
                        if idx == 0 or idx == 1
                        else CU.encode_value(result)
                    )
                    for idx, result in enumerate(results)
                ]

                field_type_list = ["results", "results", "geometry", "point"]
                field_dwh_list = [
                    f"{all_params['main_geom_name']}/{all_params['suffix']}",
                    f"{all_params['sub_geom_name']}/{all_params['suffix']}",
                    f"{all_params['main_geom_name']}/new_geometry",
                    f"{all_params['sub_geom_name']}/{all_params['suffix']}_Cross",
                ]
            elif all_params["operation_type"] == self.OPERATION_CASCADETOUCHES:
                # 演算タイプがCascadeTouchesの場合
                # 結果をシリアライズしてbase64でエンコード
                field_value_list = [
                    self.process_results(result, all_params) for result in results
                ]
                # FieldSetFileの中身を準備
                field_type_list = ["results", "float64", "float64"]
                field_dwh_list = [
                    f'{all_params["main_geom_name"]}/{all_params["suffix"]}',
                    f'{all_params["main_geom_name"]}/条数',
                    f'{all_params["main_geom_name"]}/段数',
                ]
            else:
                # 演算タイプがTouchesEndpoint,CascadeTouches以外の場合
                field_type_list = ["results"]
                base64_serialized_fid_list = self.process_results(results, all_params)
                field_value_list = [base64_serialized_fid_list]
                field_dwh_list = [
                    f"{all_params['main_geom_name']}/{all_params['suffix']}"
                ]

            return field_type_list, field_value_list, field_dwh_list

        except Exception as e:
            raise Exception(f"[ERROR format_field_set_file_data]: {str(e)}")

    def format_field_set_file_data_for_post_operation(
        self, geo_list, fid_list, results_list, all_params
    ):
        """
        FieldSetFile用にデータを生成する(Post Operation)。

        :param geo_list: 演算結果のgeometryリスト。
        :type geo_list: list[shapely.geometry.base.BaseGeometry]
        :param fid_list: 演算結果のfidリスト。
        :type fid_list: list[tuple(str, float)]
        :param results_list: 演算結果のresults。
        :type results_list: list[tuple(float, tuple(float))]
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: FieldSetFile。
        :rtype: tuple(list[str], list[str], list[str])

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            # ジオメトリ情報を取得し、FieldSetFile作成用に加工
            geometry_dwh = f"{all_params['post_geometry_name']}/geometry"
            geometry_type = "geometry"
            geometry_value = CU.encode_value(geo_list)

            # FID情報をFieldSetFile作成用に加工
            fid_dwh = f"{all_params['post_geometry_name']}/FID"
            fid_type = "string"
            fid_value = CU.encode_value(fid_list)

            # 結果に対応するソースジオメトリのFIDリストをFieldSetFile作成用に加工
            results_dwh = f"{all_params['post_geometry_name']}/{all_params['suffix']}"
            results_type = "results"
            results_value = CU.encode_value(results_list)

            # FieldSetFile作成用リストに格納
            field_dwh_list = [geometry_dwh, fid_dwh, results_dwh]
            field_type_list = [geometry_type, fid_type, results_type]
            field_value_list = [geometry_value, fid_value, results_value]

            return field_type_list, field_value_list, field_dwh_list

        except Exception as e:
            raise Exception(
                f"[ERROR format_field_set_file_data_for_post_operation]: {str(e)}"
            )

    # Operation Typeに応じた関数をマッピング
    operation_map = {
        OPERATION_INTERSECTS: CS.perform_intersects,
        OPERATION_CONTAINS: CS.perform_contains,
        OPERATION_WITHIN: CS.perform_within,
        OPERATION_TOUCHES: CS.perform_touches,
        OPERATION_CROSSES: CS.perform_crosses,
        OPERATION_OVERLAPS: CS.perform_overlaps,
        OPERATION_COVERS: CS.perform_covers,
        OPERATION_COVERED_BY: CS.perform_covered_by,
        OPERATION_EQUALS: CS.perform_equals,
        OPERATION_DISTANCE: CS.perform_distance,
        OPERATION_ISCIRCLE: CS.perform_is_circle,
        OPERATION_TOUCHESENDPOINT: CS.perform_touches_endpoint,
        OPERATION_CASCADETOUCHES: CS.perform_cascade_touches,
        OPERATION_MOSTINTERSECTS: CS.perform_most_intersects,
    }

    # Post Operation Typeに応じた関数をマッピング
    post_operation_map = {
        POST_OPERATION_UNION: CS.perform_union,
        POST_OPERATION_INTERSECTION: CS.perform_intersection,
        POST_OPERATION_DIFFERENCE: CS.perform_difference,
        POST_OPERATION_SYMMETRICAL_DIFFERENCE: CS.perform_symmetrical_difference,
        POST_OPERATION_CLIP: CS.perform_clip,
    }

    def get_params(self, attribute, properties):
        """
        属性の値とプロパティの値を取得し辞書にして返す

        :param attribute: 属性。
        :type attribute: dict
        :param properties: プロパティ。
        :type attribute: dict

        :return: 属性の値とプロパティの値を辞書にしたもの。
        :rtype: dict

        :raises CRSError: 指定されたCRS値が無効な値である場合に発生する。
        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            all_params = {}

            crs_value = (
                properties["CRS"]
                or attribute.get("crs", "")
                or attribute.get("CRS", "")
            )
            all_params["crs"] = CRS.from_epsg(
                CU.get_number_from_string(crs_value, int)
            ).srs  # CRS有効チェック
            all_params["operation_type"] = properties["OPERATION_TYPE"]
            all_params["main_geom_name"] = properties["MAIN_GEOMETRY_NAME"]
            all_params["main_attr_name"] = properties["MAIN_ATTRIBUTE_NAME"]
            all_params["sub_geom_name"] = properties["SUB_GEOMETRY_NAME"]
            all_params["sub_attr_name"] = properties["SUB_ATTRIBUTE_NAME"]
            all_params["tolerance"] = CU.get_number_from_string(properties["TOLERANCE"])
            all_params["include_empty_results"] = strtobool(
                properties["INCLUDE_EMPTY_RESULTS"]
            )
            all_params["output_format"] = properties["OUTPUT_FORMAT"]
            all_params["suffix"] = properties["SUFFIX"]
            all_params["distance"] = CU.get_number_from_string(properties["DISTANCE"])
            all_params["is_not"] = strtobool(properties["IS_NOT"])

            all_params["post_operation_type"] = properties["POST_OPERATION_TYPE"]
            all_params["post_geometry_name"] = properties["POST_GEOMETRY_NAME"]

            return all_params

        except CRSError:
            # CRS値が無効な値の場合
            raise Exception(f"[get_params]: Invalid CRS Value ({crs_value}).")

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def main_proc(self, dataframe, all_params):
        """
        メインプロセス。

        :param dataframe: インプットデータから作成したDataFrame。
        :type dataframe: pandas.DataFrame
        :param all_params: プロパティと属性情報。
        :type all_params: dict

        :return: FieldSetFile。
        :rtype: str

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            # 演算タイプに応じた関数を取得
            operation_function = self.operation_map.get(all_params["operation_type"])

            # 演算タイプがIsCircleの場合
            if all_params["operation_type"] == self.OPERATION_ISCIRCLE:
                # 演算タイプがIsCircleの場合
                # 操作対象GeoDataFrameを取得
                geodf = self.convert_from_dataframe_to_geodataframe(
                    dataframe, all_params
                )
                args = (geodf, all_params["tolerance"])
                # 空間演算処理
                results = operation_function(*args)

                # FieldSetFile用データ作成
                field_type_list = []
                field_value_list = []
                field_dwh_list = []

                # 共通処理関数を使用して結果を処理
                base64_serialized_circle_results = CU.encode_value(results)

                field_type_list.append("bool")
                field_value_list.append(base64_serialized_circle_results)
                field_dwh_list.append(
                    all_params["main_geom_name"] + "/" + all_params["suffix"]
                )

                if all_params["is_not"]:
                    # Is NotプロパティがTrueの場合、結果を反転させる
                    not_circle_results = [(i, not val) for i, val in results]

                    # 共通処理関数を使用して結果を処理
                    base64_serialized_not_circle_results = CU.encode_value(
                        not_circle_results
                    )

                    field_type_list.append("bool")
                    field_value_list.append(base64_serialized_not_circle_results)
                    field_dwh_list.append(
                        all_params["main_geom_name"] + "/Not_" + all_params["suffix"]
                    )
            else:
                # 演算タイプがIsCircle以外の場合
                if not all_params["sub_geom_name"]:
                    raise Exception(
                        f"Not found sub geometry name: {all_params['sub_geom_name']}"
                    )

                # メイン側GeoDataFrameとサブ側GeoDataFrameを取得
                main_geodf, sub_geodf = self.convert_from_dataframe_to_geodataframe(
                    dataframe, all_params
                )

                # 引数作成
                if all_params["operation_type"] == self.OPERATION_DISTANCE:
                    args = (main_geodf, sub_geodf, all_params["distance"])
                elif (
                    all_params["operation_type"] == self.OPERATION_INTERSECTS
                    or all_params["operation_type"] == self.OPERATION_TOUCHESENDPOINT
                    or all_params["operation_type"] == self.OPERATION_CASCADETOUCHES
                    or all_params["operation_type"] == self.OPERATION_MOSTINTERSECTS
                ):
                    args = (main_geodf, sub_geodf, all_params["tolerance"])
                else:
                    args = (main_geodf, sub_geodf)

                # 演算タイプごとに空間演算処理（ジオメトリ判定）を行い、FieldSetFile用データを生成
                if all_params["operation_type"] == self.OPERATION_DISTANCE:
                    distance_results, target_geometry = operation_function(*args)
                    # Post Operation用
                    results = [
                        (main_idx, distance[0])
                        for main_idx, distance in distance_results
                        if isinstance(distance, tuple)
                    ]

                    # メイン側のジオメトリデータを、空間演算処理（ジオメトリ判定）後のジオメトリデータで更新
                    main_geodf.geometry = target_geometry

                    # FieldSetFile用データを生成
                    field_type_list, field_value_list, field_dwh_list = (
                        self.format_field_set_file_data(
                            distance_results,
                            all_params,
                        )
                    )
                elif all_params["operation_type"] == self.OPERATION_TOUCHESENDPOINT:
                    fid_list_main_sub, fid_list_sub_main, new_geom_list, point_list = (
                        operation_function(*args)
                    )

                    # Post Operation用
                    results = fid_list_main_sub

                    # FieldSetFile用データを生成
                    field_type_list, field_value_list, field_dwh_list = (
                        self.format_field_set_file_data(
                            [
                                fid_list_main_sub,
                                fid_list_sub_main,
                                new_geom_list,
                                point_list,
                            ],
                            all_params,
                        )
                    )
                elif all_params["operation_type"] == self.OPERATION_CASCADETOUCHES:
                    flattened_touched_pairs, rows_results, steps_results = (
                        operation_function(*args)
                    )
                    # Post Operation用
                    results = flattened_touched_pairs

                    # FieldSetFile用データを生成
                    field_type_list, field_value_list, field_dwh_list = (
                        self.format_field_set_file_data(
                            [flattened_touched_pairs, rows_results, steps_results],
                            all_params,
                        )
                    )
                else:
                    # 空間演算処理（ジオメトリ判定）
                    results = operation_function(*args)

                    # FieldSetFile用データを生成
                    field_type_list, field_value_list, field_dwh_list = (
                        self.format_field_set_file_data(results, all_params)
                    )

                # Post Operation Typeが設定されているとき
                if not all_params["post_operation_type"] == self.POST_OPERATION_NONE:
                    post_operation_function = self.post_operation_map.get(
                        all_params["post_operation_type"]
                    )

                    # 空間演算処理（ジオメトリ生成）
                    geo_list, fid_list, pairs_list = post_operation_function(
                        main_geodf, sub_geodf, results
                    )

                    # FieldSetFile作成準備
                    st_field_type_list, st_field_value_list, st_field_dwh_list = (
                        self.format_field_set_file_data_for_post_operation(
                            geo_list, fid_list, pairs_list, all_params
                        )
                    )

                    # 空間演算処理（ジオメトリ判定）と結果を統合
                    field_type_list.extend(st_field_type_list)
                    field_value_list.extend(st_field_value_list)
                    field_dwh_list.extend(st_field_dwh_list)

            # FieldSetFileを生成
            return NSP.get_field_set_file(
                field_type_list, field_value_list, field_dwh_list
            )

        except Exception as e:
            raise Exception(f"[ERROR main_proc]: {str(e)}")

    def __call__(self, input_field_set_file, attribute, properties):
        """
        指定された空間演算を実行し、その結果をFieldSetFileとして返す。

        :param input_field_set_file: バイナリ形式のFieldSetFile。
        :type input_field_set_file: bytes
        :param attribute: 属性。
        :type attribute: dict
        :param properties: プロパティ。
        :type properties: dict

        :return: 出力用のFieldSetFileと属性情報
        :rtype: tuple(str, dict)

        :raises Exception: 処理エラーが発生した場合に発生する。
        """
        try:
            # 属性の値とプロパティの値を取得し辞書にして返す
            all_params = self.get_params(attribute, properties)

            # FieldSetFileからDataFrameを生成
            dataframe = CU.field_set_file_to_dataframe(input_field_set_file)

            field_set_file = self.main_proc(dataframe, all_params)

            return field_set_file, attribute

        except Exception as e:
            raise Exception(f"[ERROR __call__]: {str(e)}")
