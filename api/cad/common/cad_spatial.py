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
from collections import defaultdict
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")
sjoin = getattr(import_module("geopandas.tools"), "sjoin")
unary_union = getattr(import_module("shapely.ops"), "unary_union")
Point = getattr(import_module("shapely.geometry"), "Point")
LineString = getattr(import_module("shapely.geometry"), "LineString")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
MultiPoint = getattr(import_module("shapely.geometry"), "MultiPoint")


def process_spatial_operations(
    main_geo_df,
    sub_geo_df,
    predicate,
):
    """
    空間演算処理を行い、関連するジオメトリのペアリング情報を生成する。

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param predicate: 演算タイプ
    :type predicate: str

    :return: 関連するメインジオメトリとサブジオメトリのペアリング情報を表すFIDリスト
    :rtype: list[tuple[float, float]]
    """
    # 空間結合により指定された演算タイプで関連するジオメトリを取得
    joined_gdf = sjoin(main_geo_df, sub_geo_df, how="left", predicate=predicate)

    # ペアリング情報を生成
    pairs_results = joined_gdf.apply(
        lambda row: (
            (row["FID_left"][1], row["FID_right"][1])
            if row["FID_right"] is not np.nan
            else (row["FID_left"][1], row["FID_right"])
        ),
        axis=1,
    ).tolist()

    pairs_results = sorted(pairs_results, key=lambda x: (x[0], x[1]))

    return pairs_results


def perform_intersects(main_geo_df, sub_geo_df, tolerance):
    """
    Intersects 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param tolerance: 探索許容範囲
    :type tolerance: float

    :return: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    if tolerance < 0:
        raise ValueError(f"Unsupported tolerance: {tolerance}")

    # toleranceが正の数の場合
    if tolerance != 0:
        main_geo_df.geometry = main_geo_df.geometry.apply(lambda x: x.buffer(tolerance))

    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "intersects")

    return pairs_results


def perform_contains(main_geo_df, sub_geo_df):
    """
    Contains 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "contains")

    return pairs_results


def perform_within(main_geo_df, sub_geo_df):
    """
    Within 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "within")

    return pairs_results


def perform_touches(main_geo_df, sub_geo_df):
    """
    Touches 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "touches")

    return pairs_results


def perform_crosses(main_geo_df, sub_geo_df):
    """
    Crosses 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "crosses")

    return pairs_results


def perform_overlaps(main_geo_df, sub_geo_df):
    """
    Overlaps 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "overlaps")

    return pairs_results


def perform_covers(main_geo_df, sub_geo_df):
    """
    Covers 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "covers")

    return pairs_results


def perform_covered_by(main_geo_df, sub_geo_df):
    """
    CoveredBy 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "covered_by")

    return pairs_results


def perform_equals(main_geo_df, sub_geo_df):
    """
    Equals 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    pairs_results = []  # equals結果格納先

    # メインとなるGeoDataFrameのレコード数分ループ
    for idx, row in main_geo_df.iterrows():

        # サブとなるGeoDataFrameの中でメインと同一のジオメトリのFIDを取得
        equals_res = sub_geo_df["FID"][
            sub_geo_df.geometry.apply(lambda geom: geom.equals(row.geometry))
        ].tolist()
        equals_res = equals_res if equals_res else [np.nan]

        # メインFindexとサブFindexからペアリング情報を生成
        pairs = [
            (
                (float(row["FID"][1]), float(sub_fid[1]))
                if sub_fid is not np.nan
                else (float(row["FID"][1]), np.nan)
            )
            for sub_fid in equals_res
        ]
        pairs_results = pairs_results + pairs

    return pairs_results


def perform_distance(main_geo_df, sub_geo_df, distance):
    """
    Distance 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param distance: 探索範囲値
    :type distance: float

    :return: 演算結果のペアリング情報とジオメトリ間の距離情報、探索範囲値に応じて変更したジオメトリデータ
    :rtype: tuple[list[float, tuple(float, float)], geopandas.GeoSeries]
    """
    # 探索範囲値に応じてメインジオメトリを操作
    if distance == 0:
        # 探索範囲値が0の場合、元のジオメトリを探索基準として取得
        target_main_geoseries = main_geo_df.geometry
    elif distance > 0:
        # 探索範囲値が0より大きい場合、探索範囲値分バッファを広げた状態を探索基準として取得
        target_main_geoseries = main_geo_df.geometry.buffer(distance)
    else:
        # サブジオメトリ全てを包括するポリゴンを生成
        bounding_box = sub_geo_df.total_bounds
        expanded_geometry = Polygon([
            (bounding_box[0], bounding_box[1]),  # 最小x, 最小y
            (bounding_box[2], bounding_box[1]),  # 最大x, 最小y
            (bounding_box[2], bounding_box[3]),  # 最大x, 最大y
            (bounding_box[0], bounding_box[3]),  # 最小x, 最大y
        ])
        # 探索範囲値が負の数のとき、サブジオメトリ全てを包括するジオメトリを探索基準として取得
        target_main_geoseries = main_geo_df['geometry'].apply(lambda x: expanded_geometry)

    pairs_results_with_distance = []  # 処理結果格納先

    for idx, main_geom in target_main_geoseries.items():

        target_sub_geo_df = sub_geo_df[
            sub_geo_df.geometry.apply(
                lambda geom: geom.intersects(main_geom)
            )
        ]

        if target_sub_geo_df.empty:
            # 探索範囲内にサブジオメトリが存在しない場合nanを格納する
            pair_nan = (float(main_geo_df.loc[idx, 'FID'][1]), np.nan)
            pairs_results_with_distance.append(pair_nan)
        else:
            # サブFindexと、メインジオメトリとサブジオメトリの距離を取得
            distance_results = target_sub_geo_df.apply(
                lambda sub_row: (
                    float(sub_row["FID"][1]),
                    sub_row["geometry"].distance(main_geo_df.loc[idx, 'geometry']),
                ),
                axis=1,
            )

            # メインFindexと距離情報を結合する
            distance_pairs = [
                (float(main_geo_df.loc[idx, 'FID'][1]), dist_and_sub_fidx)
                for dist_and_sub_fidx in distance_results.tolist()
            ]
            pairs_results_with_distance = pairs_results_with_distance + distance_pairs

    return pairs_results_with_distance, target_main_geoseries


def perform_is_circle(gdf, tolerance):
    """
    IsCircle 操作を実行する関数

    :param gdf: 操作対象のGeoDataFrame
    :type gdf: geopandas.GeoDataFrame
    :param tolerance: 円相似度の許容誤差
    :type tolerance: float

    :return: Findexと円判定結果のリスト
    :rtype: list[tuple[float, bool]]
    """

    def is_circle(geometry, tol) -> bool:
        """
        ジオメトリが円に近いかどうかを判定する関数

        :param geometry: ジオメトリのコレクション（ジオメトリタイプはLineStringまたはPolygonを想定）
        :type geometry: geopandas.GeoSeries
        :param tol: 円とみなす許容誤差（0に近いほど厳密）
        :type tol: float

        :return: 円判定結果（TrueまたはFalse）
        :rtype: bool
        """
        # ジオメトリタイプチェック
        if "LineString" == geometry.geom_type:
            # ジオメトリがLineStringかつ、多角形を形成しない形状の場合は円のチェックを行わない
            if len(geometry.coords) < 4 or geometry.coords[0] != geometry.coords[-1]:
                return False
            # ラインをポリゴンに変換
            polygon = Polygon(geometry.coords)
        elif "Polygon" == geometry.geom_type:
            polygon = geometry
        else:
            return False

        # 重心を中心点とし、ポリゴンの円周から算出される半径を用いて外接円を生成
        circumscribed_circle = Point(polygon.centroid).buffer(
            polygon.length / (2 * np.pi)
        )
        # 生成した円と注記ジオメトリとの相似度を計算
        similarity = (
            polygon.symmetric_difference(circumscribed_circle).area / polygon.area
        )

        # 円の相似度が許容誤差を下回る場合、注記ジオメトリの形状を円とみなし、判定結果としてTrueを返す
        return similarity < tol

    if tolerance < 0:
        raise ValueError(f"Unsupported tolerance: {tolerance}")

    # 各ジオメトリの形状が円かどうかを判定
    circle_results = gdf.apply(
        lambda row: (
            float(row["FID"][1]),
            is_circle(row["geometry"], tolerance),
        ),
        axis=1,
    ).tolist()

    return circle_results


def perform_touches_endpoint(main_geo_df, sub_geo_df, tolerance):
    """
    TouchesEndpoint の処理を実行する

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param tolerance: 許容探索範囲値
    :type tolerance: float

    :return: 以下４つの処理結果を返す。
        1. メイン側を軸としたペアリング情報（pairs_results_side_main）
        2. サブ側を軸としたペアリング情報（pairs_results_side_sub）
        3. サブジオメトリとの交点の座標が追加されたメインジオメトリリスト（new_main_geometries）
        4. 交差判定された側のサブジオメトリの端点のリスト（sorted(sub_point_list, key=lambda x: x[0])）
    :rtype: tuple[
        list[tuple[float, float]],
        list[tuple[float, float]],
        list[shapely.geometry.LineString],
        list[tuple[float, shapely.geometry.Point]]
    ]
    """

    def insert_point_into_line(line, point):
        """
        LineString上の適切な場所にPointの座標を挿入

        :param line: 2点以上で構成されるLineString
        :type line: shapely.geometry.LineString
        :param point: Point(対象のLineString上のPointを想定)
        :type point: shapely.geometry.Point

        :return: Pointの座標が挿入されたLineString
        :rtype: shapely.geometry.LineString
        """
        # 元のLineStringの座標のリスト
        coords = list(line.coords)

        # 無限大の値を初期値とする
        min_distance = float("inf")

        # Pointの座標の挿入位置
        insert_position = None

        # 2点で構成される線分に分割して最も近い場所に対象のPointの座標を挿入
        for idx in range(len(coords) - 1):
            # 2点で構成される線分
            segment = LineString([coords[idx], coords[idx + 1]])

            # 対象のPointから最も近いセグメント上のPoint
            point_on_segment = segment.interpolate(segment.project(point))

            # 対象のPointから最も近いセグメント上のPointから、対象のPointまでの距離を計算
            distance_from_point = point.distance(point_on_segment)

            # 最短距離を持つセグメントを特定
            if distance_from_point < min_distance:
                min_distance = distance_from_point
                insert_position = idx + 1  # 次の点の前に挿入

        # Pointの座標
        point_coord = list(point.coords)[0]

        if point_coord not in coords:
            # 対象のLineStringにその座標と同じ座標がなければ
            # 最も近いセグメントの位置に新しい点を挿入
            coords.insert(insert_position, point_coord)

        # 新しいLineStringを作成
        return LineString(coords)

    # サブジオメトリがLineStringではない場合は処理をしない(端点が不明である為)
    if sub_geo_df.iloc[0].geometry.geom_type != "LineString":
        raise ValueError("Geometry type is not LineString.")

    # toleranceが負の値の場合は終了
    if tolerance < 0:
        raise ValueError(f"Unsupported tolerance: {tolerance}")

    # 交差判定された側のサブジオメトリの端点のリスト(サブジオメトリ1つに対し、複数のメインが紐づく可能性がある)
    sub_point_list = []

    # サブジオメトリとの交点の座標が追加されたメインジオメトリ
    new_main_geometries = []

    # メインジオメトリとサブジオメトリの端点の交点を取得して処理を行う
    for main_geom in main_geo_df.geometry:
        for sub_geom, sub_fid in zip(sub_geo_df.geometry, sub_geo_df.FID):
            for coord_idx in [0, -1]:
                # サブジオメトリの端点を取得
                end_point = Point(sub_geom.coords[coord_idx])

                # サブジオメトリの端点にバッファを入れて交差を確認
                buffer_end_point = end_point.buffer(tolerance)
                is_intersect = main_geom.intersects(buffer_end_point)

                if is_intersect:
                    # 交差判定されたサブジオメトリの端点をリストに追加
                    sub_point_list.append((sub_fid[1], end_point))

                    # メインジオメトリに交点を追加
                    main_geom = insert_point_into_line(main_geom, end_point)

        # 交点追加後のメインジオメトリをリストに追加
        new_main_geometries.append(main_geom)

    # サブ側のLineStringのジオメトリの端点をMultiPointにし、バッファを入れてMultiPolygonにする
    sub_geo_df.geometry = [
        MultiPoint([Point(geom.coords[0]), Point(geom.coords[-1])]).buffer(tolerance)
        for geom in sub_geo_df.geometry
    ]

    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results_side_main = process_spatial_operations(
        main_geo_df, sub_geo_df, "intersects"
    )
    pairs_results_side_sub = process_spatial_operations(
        sub_geo_df, main_geo_df, "intersects"
    )

    return (
        pairs_results_side_main,
        pairs_results_side_sub,
        new_main_geometries,
        sorted(sub_point_list, key=lambda x: x[0]),
    )


def perform_cascade_touches(main_geo_df, sub_geo_df, tolerance):
    """
    CascadeTouches 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param tolerance: 許容探索範囲値
    :type tolerance: float

    :return: 以下3つの処理結果を返す。
        - flattened_touched_pairs: 各メインジオメトリに対するサブジオメトリのFIDのリスト。１対１のデータ構造。
        - rows_pairs: 各メインジオメトリに対する条数のリスト
        - steps_pairs: 各メインジオメトリに対する段数のリスト
    :rtype: tuple[list[tuple[float, float]], list[tuple[float, float|None]], list[tuple[float, float|None]]]
    """

    def cascade_touches(main_gdf, sub_gdf, buffer):
        """
        メインジオメトリに対するサブジオメトリの接触を確認しFindexを紐づける

        :param main_gdf: メインジオメトリのGeoDataFrame
        :type main_gdf: geopandas.GeoDataFrame
        :param sub_gdf: サブジオメトリのGeoDataFrame
        :type sub_gdf: geopandas.GeoDataFrame
        :param buffer: バッファの距離
        :type buffer: float

        :return: 以下２つの処理結果を返す。
             - touched_pairs: 各メインジオメトリに対するサブジオメトリのFIDリスト。段ごとの区切りを持つ１対多のデータ構造。
             - flattened_touched_pairs: 各メインジオメトリに対するサブジオメトリのFIDのリスト。１対１のデータ構造。
        :rtype: tuple[list[float, list[float]], list[tuple[float, float]]]
        """
        # 各ジオメトリにバッファを追加
        main_gdf["geometry"] = main_gdf["geometry"].buffer(buffer)
        sub_gdf["geometry"] = sub_gdf["geometry"].buffer(buffer)

        # メインジオメトリ別の接触するサブジオメトリのFIDのリスト
        touched_pairs = []  # メインジオメトリ別段別
        flattened_touched_pairs = []  # メインジオメトリ別

        # メインジオメトリごとにループ処理
        for idx, main_geom in enumerate(main_gdf.geometry):
            # ループ中のメインジオメトリのFindex
            main_fidx = float(main_gdf["FID"].values[idx][1])

            # FIDのリストを初期化
            record_touches = []  # 段別のFIDのリスト
            flattened_touched = []  # FIDのリスト

            # 現在のメインジオメトリと交差してるサブジオメトリを抽出
            current_intersects_gdf = sub_geo_df[sub_gdf.intersects(main_geom)]

            # 接触が確認されていないサブジオメトリ
            unchecked_gdf = sub_gdf

            while not current_intersects_gdf.empty:
                # サブのGeoDataFrameから接触未確認のデータのみ抽出
                unchecked_gdf = unchecked_gdf[
                    ~unchecked_gdf["FID"].isin(current_intersects_gdf["FID"])
                ].dropna()

                # 直近で交差判定されたサブジオメトリのFIDをリスト形式で取得
                intersects_fid_fsf = [
                    float(fid[1]) for fid in current_intersects_gdf["FID"].values
                ]

                # 接触判定Findexリストに追加
                record_touches.append(intersects_fid_fsf)
                flattened_touched = flattened_touched + intersects_fid_fsf

                # 直近で交差判定されたサブジオメトリを1つにまとめる
                unary_geo = unary_union(current_intersects_gdf.geometry)

                # 接触が確認されていないサブジオメトリのうち、直近で接触が確認されたものと交差しているもの(1つ上の段)を抽出
                current_intersects_gdf = unchecked_gdf[
                    unchecked_gdf.intersects(unary_geo)
                ]

            # 各メインジオメトリに対する接触結果を保存
            if len(flattened_touched) > 0:
                touched_pairs.append((main_fidx, record_touches))
                for fid in flattened_touched:
                    flattened_touched_pairs.append((main_fidx, fid))

        return touched_pairs, flattened_touched_pairs

    if tolerance < 0:
        raise ValueError(f"Unsupported tolerance: {tolerance}")

    # メインジオメトリと接触するサブジオメトリのリストを作成
    touched_pairs_list, flattened_touched_pairs_list = cascade_touches(
        main_geo_df, sub_geo_df, tolerance
    )

    # 段数
    steps = [(fid, float(len(fids))) for fid, fids in touched_pairs_list]

    # 条数
    rows = [(fid, float(max([len(step) for step in record]))) for fid, record in touched_pairs_list]

    # メイン側にFID情報がある場合、Findexをリスト化
    if "FID" not in main_geo_df.columns:
        raise ValueError("column `FID` does not exist.")
    main_findex_list = [float(fidx) for _, fidx in main_geo_df["FID"].tolist()]

    # 条数と段数にnanの値を追加し、Findexデータと結合する
    rows_dict = dict(rows)
    rows_results = [rows_dict.get(fidx, None) for fidx in main_findex_list]
    rows_pairs = list(zip(main_findex_list, rows_results))
    steps_dict = dict(steps)
    steps_results = [steps_dict.get(fidx, None) for fidx in main_findex_list]
    steps_pairs = list(zip(main_findex_list, steps_results))

    return flattened_touched_pairs_list, rows_pairs, steps_pairs


def perform_most_intersects(main_geo_df, sub_geo_df, tolerance):
    """
    Most Intersects 操作を実行する関数

    :param main_geo_df: メインとなるGeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブとなるGeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param tolerance: 探索許容範囲
    :type tolerance: float

    :return pairs_results: 演算結果のペアリング情報
    :rtype: list[tuple[float, float]]
    """
    def calculate_intersection_area(m_geom, s_geom):
        """
        ジオメトリ同士の交差面積を算出する。

        :param m_geom: メイン側ジオメトリ
        :type m_geom: shapely.geometry.base.BaseGeometry
        :param s_geom: サブ側ジオメトリ
        :type s_geom: shapely.geometry.base.BaseGeometry

        :return: 交差面積（長さ）
        :rtype: float
        """
        intersection_area = m_geom.intersection(s_geom)
        if isinstance(intersection_area, Polygon):
            # 交差面積がポリゴンの場合
            return intersection_area.area
        elif isinstance(intersection_area, LineString):
            # 交差面積がラインの場合
            return intersection_area.length
        else:
            # 交差面積がポリゴン、ライン以外の場合
            return 0

    if tolerance < 0:
        raise ValueError(f"Unsupported tolerance: {tolerance}")

    # toleranceの値が0より大きい場合
    if tolerance != 0:
        main_geo_df.geometry = main_geo_df.geometry.apply(lambda x: x.buffer(tolerance))

    # メインとサブのGeoDataFrameを空間結合し、ペアリング情報を取得
    pairs_results = process_spatial_operations(main_geo_df, sub_geo_df, "intersects")

    # メイン側Findexの値を基準にサブ側Findexをまとめる
    grouped_pairing_dict = defaultdict(list)
    for key, value in pairs_results:
        grouped_pairing_dict[key].append(value)
    grouped_pairing_list = [(key, values) for key, values in grouped_pairing_dict.items()]

    most_intersects_results = []  # 最も交差部分の多いペアの格納先

    for pair in grouped_pairing_list:
        main_fidx, sub_fidx_list = pair
        if len(sub_fidx_list) == 1:
            # 同じメインに紐づくサブが単一の場合
            most_intersects_results.append((main_fidx, sub_fidx_list[0]))
            continue

        # 同じメインに紐づくサブが複数の場合
        main_geom = main_geo_df[main_geo_df["FID"].apply(lambda x: x[1] == main_fidx)].geometry.tolist()[0]
        sub_geoms = sub_geo_df[sub_geo_df["FID"].apply(lambda x: x[1] in sub_fidx_list)].geometry.tolist()

        # メインとサブの交差面積（長さ）をリスト形式で取得
        intersection_area_list = [calculate_intersection_area(main_geom, sub_geoms[i]) for i in range(len(sub_fidx_list))]
        # 交差面積（長さ）が最大の値を持つインデックスを取得
        max_area_indices = [i for i, value in enumerate(intersection_area_list) if value == max(intersection_area_list)]

        # 最大交差面積（長さ）を持つサブが複数か単一かで処理を分岐
        if len(max_area_indices) == 1:
            # 単一の場合は最大面積（長さ）を持つサブ側のFindexを取得
            sub_findex = sub_fidx_list[max_area_indices[0]]
        else:
            # 複数の場合、最大面積（長さ）を持つFindexとジオメトリデータを取得
            max_sub_info = [(sub_fidx_list[i], sub_geoms[i]) for i in max_area_indices]
            # 最も座標の小さいものを抽出（座標が同じ場合はFindexの小さい方が抽出される）
            smallest_sub_info = min(max_sub_info,
                                    key=lambda geom: min(geom[1].coords, key=lambda coord: (coord[0], coord[1])))
            # 最も座標の小さいものの0番目（Findex）を取得
            sub_findex = smallest_sub_info[0]

        # 最も交差部分の多いサブ側Findexとメイン側Findexを正式なペアとする
        most_intersects_results.append((pair[0], sub_findex))

    return most_intersects_results


def create_spatial_transformation_results(gdf):
    """
    空間演算ジオメトリ生成処理にて生成されたGeoDataFrameから、ジオメトリ・FID・ペアリング情報を抽出する。

    :param gdf: 空間演算ジオメトリ生成処理にて生成されたGeoDataFrame
    :type gdf: geopandas.GeoDataFrame

    :return: ジオメトリリスト, FIDリスト,ペアリング情報
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, tuple[float, float]]]
    ]
    """
    # ジオメトリデータのリストを作成
    geom_list = gdf.geometry.tolist()
    # 新しいFIDデータを作成
    fid_list = [(f"{float(idx)}", float(idx)) for idx in gdf.index]
    # 生成されたジオメトリのFindexとジオメトリ判定のペアリング情報のペアリングを行う
    pairs_list = [
        (float(idx), value)
        for idx, row in gdf.iterrows()
        for value in (
            row["results"] if isinstance(row["results"], list) else [row["results"]]
        )
    ]

    return geom_list, fid_list, pairs_list


def perform_union(main_geo_df, sub_geo_df, results):
    """
    Union 操作を実行する関数。
    ジオメトリのペアリング情報を基に、ジオメトリの結合を行う。

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param results: results型の空間演算結果。タプルのリスト。e.g. [(0, 1), (0, 2), (2, 3)]
    :type results: list[tuple[float, float]]

    :return: 以下３つの処理結果を返す。
         - Union操作により生成されたジオメトリリスト
         - Union操作により生成されたジオメトリのFIDリスト
         - Union操作により生成されたジオメトリとジオメトリ判定のペアリング情報とのペアリング
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, float]]
    ]
    """
    union_geometry_list = []  # Union処理結果ジオメトリ格納先
    union_pairs_list = []  # Union元ペアリング情報格納先

    # メイン側Findexごとにペアリング情報をまとめる
    unique_main_findex_list = list({res[0] for res in results})
    results_by_main_fidx = {
        fidx: [t[1] for t in results if t[0] == fidx]
        for fidx in unique_main_findex_list
    }

    # メインFindexの数分ループ
    for main_fidx, sub_indices in results_by_main_fidx.items():
        # サブFindexがnanの場合コンティニュー
        if np.nan in sub_indices:
            continue

        # GeoDataFrameから処理対象（メイン・サブ）データを抽出する
        target_main_geo_df = main_geo_df[
            main_geo_df["FID"].apply(lambda x: x[1] == main_fidx)
        ].reset_index(drop=True)
        target_sub_geo_df = sub_geo_df[
            sub_geo_df["FID"].apply(lambda fid: fid[1] in sub_indices)
        ].reset_index(drop=True)

        # メイン側GeoDataFrameとサブ側GeoDataFrameを結合
        gdf_combined = pd.concat(
            [target_main_geo_df, target_sub_geo_df], ignore_index=True
        )

        # Union処理を行う
        union_results = gdf_combined.unary_union

        # 結合結果がある場合、結合結果ジオメトリと結合元ペアリング情報をリストに格納
        if not union_results.is_empty:
            union_geometry_list.append(union_results)
            result = [(main_fidx, sub_fidx) for sub_fidx in sub_indices]
            union_pairs_list.append(result)

    # Union結果をGeoDataFrame化
    union_gdf = gpd.GeoDataFrame(
        {"results": union_pairs_list}, geometry=union_geometry_list
    )

    # GeoDataFrameから、演算後のジオメトリデータ・FIDデータ、演算前後のFIDのペアリングデータを取得
    geometry_list, fid_list, pairs_list = create_spatial_transformation_results(
        union_gdf
    )

    return geometry_list, fid_list, pairs_list


def perform_intersection(main_geo_df, sub_geo_df, results):
    """
    Intersection 操作を実行する関数。
    ジオメトリのペアリング情報を基に、共通部分のジオメトリ化を行う。

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param results: results型の空間演算結果。タプルのリスト。e.g. [(0, 1), (0, 2), (2, 3)]
    :type results: list[tuple[float, float]]

    :return: 以下３つの処理結果を返す。
         - Intersection操作により生成されたジオメトリリスト
         - Intersection操作により生成されたジオメトリのFIDリスト
         - Intersection操作により生成されたジオメトリとジオメトリ判定のペアリング情報とのペアリング
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, float]]
    ]
    """
    inter_geometries = []  # Intersection処理結果ジオメトリ格納先
    inter_pairs = []  # Intersection元ペアリング情報格納先

    # ペアリング情報の数分ループ
    for res in results:
        # サブFindexがnanの場合コンティニュー
        if res[1] is np.nan:
            continue

        # GeoDataFrameから処理対象（メイン・サブ）データを抽出する
        target_main_geo_df = main_geo_df[
            main_geo_df["FID"].apply(lambda x: x[1] == res[0])
        ].reset_index(drop=True)
        target_sub_geo_df = sub_geo_df[
            sub_geo_df["FID"].apply(lambda x: x[1] == res[1])
        ].reset_index(drop=True)

        # Intersection処理を行う
        intersection_results = target_main_geo_df.geometry.intersection(
            target_sub_geo_df.geometry
        )

        # 接点がある場合、接点ジオメトリと接点生成元ペアリング情報をリストに格納
        if not intersection_results.empty:
            inter_geometries.append(intersection_results.tolist()[0])
            inter_pairs.append(res)

    # メインFindexをリスト化
    main_findex_list = [fid[0] for fid in inter_pairs]
    # ペアリングごとのIntersection操作結果をGeoDataFrame化
    inter_gdf_by_pairs = gpd.GeoDataFrame(
        {"src_results": inter_pairs, "main_findex": main_findex_list},
        geometry=inter_geometries,
    )

    # メイン側Findexの値でグルーピング
    grouped_gdf = inter_gdf_by_pairs.groupby("main_findex")
    # グルーピングしたGeoDataFrameからジオメトリデータとペアリング情報を取得
    inter_geometry_list = [row.geometry.unary_union for _idx, row in grouped_gdf]
    inter_pairs_list = [row["src_results"].tolist() for _idx, row in grouped_gdf]

    # ジオメトリデータとペアリング情報からGeoDataFrameを作成
    intersection_gdf = gpd.GeoDataFrame(
        {"results": inter_pairs_list},
        geometry=inter_geometry_list,
    )

    # GeoDataFrameから、演算後のジオメトリデータ・FIDデータ、演算前後のFIDのペアリングデータを取得
    geometry_list, fid_list, pairs_list = create_spatial_transformation_results(
        intersection_gdf
    )

    return geometry_list, fid_list, pairs_list


def perform_difference(main_geo_df, sub_geo_df, results):
    """
    Difference 操作を実行する関数。
    ジオメトリのペアリング情報を基に、１対１で差分を抽出し、差分のジオメトリ化を行う。

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param results: results型の空間演算結果。タプルのリスト。e.g. [(0, 1), (0, 2), (2, 3)]
    :type results: list[tuple[float, float]]

    :return: 以下３つの処理結果を返す。
         - Difference操作により生成されたジオメトリリスト
         - Difference操作により生成されたジオメトリのFIDリスト
         - Difference操作により生成されたジオメトリとジオメトリ判定のペアリング情報とのペアリング
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, float]]
    ]
    """
    diff_geometry_list = []  # Difference処理結果ジオメトリ格納先
    diff_pairs_list = []  # Difference元ペアリング情報格納先

    # ペアリング情報の数分ループ
    for res in results:
        # サブFindexがnanの場合コンティニュー
        if res[1] is np.nan:
            continue

        # GeoDataFrameから処理対象（メイン・サブ）のｈデータを抽出する
        target_main_geom = main_geo_df[
            main_geo_df["FID"].apply(lambda x: x[1] == res[0])
        ].reset_index(drop=True).geometry[0]
        target_sub_geom = sub_geo_df[
            sub_geo_df["FID"].apply(lambda x: x[1] == res[1])
        ].reset_index(drop=True).geometry[0]

        # Difference処理を行う
        difference_results = target_main_geom.difference(
            target_sub_geom
        )

        # 差分がある場合、差分ジオメトリと差分生成元ペアリング情報をリストに格納
        if not difference_results.is_empty:
            diff_geometry_list.append(difference_results)
            diff_pairs_list.append(res)

    # ジオメトリデータとペアリング情報からGeoDataFrameを作成
    difference_gdf = gpd.GeoDataFrame(
        {"results": diff_pairs_list},
        geometry=diff_geometry_list,
    )

    # GeoDataFrameから、演算後のジオメトリデータ・FIDデータ、演算前後のFIDのペアリングデータを取得
    geometry_list, fid_list, pairs_list = create_spatial_transformation_results(
        difference_gdf
    )

    return geometry_list, fid_list, pairs_list


def perform_symmetrical_difference(main_geo_df, sub_geo_df, results):
    """
    SymmetricalDifference 操作を実行する関数。
    ジオメトリのペアリング情報を基に、１対多で差分を抽出し、差分のジオメトリ化を行う。

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param results: results型の空間演算結果。タプルのリスト。e.g. [(0, 1), (0, 2), (2, 3)]
    :type results: list[tuple[float, float]]

    :return: 以下３つの処理結果を返す。
         - SymmetricalDifference操作により生成されたジオメトリリスト
         - SymmetricalDifference操作により生成されたジオメトリのFIDリスト
         - SymmetricalDifference操作により生成されたジオメトリとジオメトリ判定のペアリング情報とのペアリング
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, float]]
    ]
    """
    sym_diff_geometry_list = []
    sym_diff_pairs_list = []

    # メイン側Findexごとにペアリング情報をまとめる
    unique_main_findex_list = list({res[0] for res in results})
    results_by_main_fidx = {
        fidx: [t[1] for t in results if t[0] == fidx]
        for fidx in unique_main_findex_list
    }

    # メインFindexの数分ループ
    for main_fidx, sub_indices in results_by_main_fidx.items():
        # サブFindexがnanの場合コンティニュー
        if np.nan in sub_indices:
            continue

        # GeoDataFrameから処理対象（メイン・サブ）データを抽出する
        target_main_geom = main_geo_df[
            main_geo_df["FID"].apply(lambda x: x[1] == main_fidx)
        ].reset_index(drop=True)
        target_sub_geom = sub_geo_df[
            sub_geo_df["FID"].apply(lambda fid: fid[1] in sub_indices)
        ].reset_index(drop=True)

        # SymmetricalDifference処理を実行
        sym_diff_results = target_main_geom.difference(
            target_sub_geom.unary_union
        )[0]

        # 差分がある場合、差分ジオメトリと差分生成元ペアリング情報をリストに格納
        if not sym_diff_results.is_empty:
            sym_diff_geometry_list.append(sym_diff_results)
            result = [(main_fidx, sub_fidx) for sub_fidx in sub_indices]
            sym_diff_pairs_list.append(result)

    # SymmetricalDifference処理結果をGeoDataFrame化
    sym_diff_gdf = gpd.GeoDataFrame(
        {"results": sym_diff_pairs_list},
        geometry=sym_diff_geometry_list,
    )

    # GeoDataFrameから、演算後のジオメトリデータ・FIDデータ、演算前後のFIDのペアリングデータを取得
    geometry_list, fid_list, pairs_list = create_spatial_transformation_results(
        sym_diff_gdf
    )

    return geometry_list, fid_list, pairs_list


def perform_clip(main_geo_df, sub_geo_df, results):
    """
    Clip 操作を実行する関数。
    ジオメトリのペアリング情報を基に、ジオメトリのクリッピングを行う。

    :param main_geo_df: メイン側GeoDataFrame
    :type main_geo_df: geopandas.GeoDataFrame
    :param sub_geo_df: サブ側GeoDataFrame
    :type sub_geo_df: geopandas.GeoDataFrame
    :param results: results型の空間演算結果。タプルのリスト。e.g. [(0, 1), (0, 2), (2, 3)]
    :type results: list[tuple[float, float]]

    :return: 以下３つの処理結果を返す。
         - Clip操作により生成されたジオメトリリスト
         - Clip操作により生成されたジオメトリのFIDリスト
         - Clip操作により生成されたジオメトリとジオメトリ判定のペアリング情報とのペアリング
    :rtype: tuple[
        list[shapely.geometry.base.BaseGeometry],
        list[tuple[str, float]],
        list[tuple[float, float]]
    ]
    """
    clip_geometry_list = []  # Clip処理結果ジオメトリ格納先
    clip_pairs_list = []  # Clip元ペアリング情報格納先

    # ペアリング情報の数分ループ
    for res in results:
        # サブFindexがnanの場合コンティニュー
        if res[1] is np.nan:
            continue

        # GeoDataFrameから処理対象（メイン・サブ）データを抽出する
        target_main_geo_df = main_geo_df[
            main_geo_df["FID"].apply(lambda x: x[1] == res[0])
        ].reset_index(drop=True)
        target_sub_geo_df = sub_geo_df[
            sub_geo_df["FID"].apply(lambda x: x[1] == res[1])
        ].reset_index(drop=True)

        # Clip処理を行う
        clip_results = target_main_geo_df.clip(target_sub_geo_df)

        # 差分がある場合、差分ジオメトリと差分生成元ペアリング情報をリストに格納
        if not clip_results.empty:
            clip_geometry_list.append(clip_results.geometry.tolist()[0])
            clip_pairs_list.append(res)

    # ジオメトリデータとペアリング情報からGeoDataFrameを作成
    clip_gdf = gpd.GeoDataFrame(
        {"results": clip_pairs_list},
        geometry=clip_geometry_list,
    )

    # GeoDataFrameから、演算後のジオメトリデータ・FIDデータ、演算前後のFIDのペアリングデータを取得
    geometry_list, fid_list, pairs_list = create_spatial_transformation_results(
        clip_gdf
    )

    return geometry_list, fid_list, pairs_list
