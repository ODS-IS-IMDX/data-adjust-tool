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

# --------------------------------------------------------------------------------------------
# 【モジュール概要】
# NifiComplicationPackage.py を変更しないように、
# ExtendedOutsideTIN 用に必要な3関数だけを独立させた拡張モジュール。
#
# 方針:
# - 共通の基盤ロジック(create_TIN / judge_inclusion / execute_affine_transformation / correct_z_value)
#   は既存の NifiComplicationPackage.py をそのまま利用する
# - TIN外(-1)の点に対する「最近傍境界TIN割り当て」だけ、このモジュールで追加する
# - ConvertCoordinatesVectorAffineExtendedOutsideTIN.py からは本モジュールを呼ぶ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from importlib import import_module

# 既存の共通パッケージをそのまま利用する
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP

# 外部ライブラリの動的インポート
np = import_module("numpy")
Delaunay = import_module("scipy.spatial").Delaunay
cKDTree = import_module("scipy.spatial").cKDTree

# --------------------------------------------------------------------------------------------
# 既存 NifiComplicationPackage.py 側の定数を利用する
# --------------------------------------------------------------------------------------------
__coordinate_plane_xy__ = NCP.__coordinate_plane_xy__
__coordinate_plane_xz__ = NCP.__coordinate_plane_xz__

__create_TIN_result_index_target_tin__ = NCP.__create_TIN_result_index_target_tin__
__create_TIN_result_index_standard_tin__ = NCP.__create_TIN_result_index_standard_tin__

__affine_result_index_target_feature__ = NCP.__affine_result_index_target_feature__
__affine_result_index_target_tin__ = NCP.__affine_result_index_target_tin__

__point_coordinate_index_x__ = NCP.__point_coordinate_index_x__
__point_coordinate_index_y__ = 2
__point_coordinate_index_z__ = NCP.__point_coordinate_index_z__


def fill_outside_points_with_nearest_boundary_tin(
    tin_index_array,
    correction_target_tin_array,
    position_standard_tin_array,
    position_standard_gcp_array,
    correction_target_feature_array,
    coordinate_plane,
    logger,
    max_distance=None,
):
    """
    ----------------------------------------------------------------
    【概要】
    TIN外(-1)の構成点にもTINインデックスを割り当て、外挿アフィン可能にする。

    【方針】
    - Delaunayの境界TIN(外周三角形)だけを候補にする
    - 境界TINの重心をKDTree化し、外側点に最寄り境界TINを割り当てる
    - max_distance を指定した場合、遠すぎる点は -1 のまま残す

    【引数】
    ① tin_index_array:
       judge_inclusionの戻り値を想定。shape=(N, 2) で [point_id, tin_index]

    ② correction_target_tin_array:
       補正対象側TIN。shape=(T, 3, D)

    ③ position_standard_tin_array:
       位置基準側TIN。
       現状この関数では直接使わないが、元実装との整合性維持のため引数として残す

    ④ position_standard_gcp_array:
       create_TINと同じ基準GCP配列。境界TIN抽出用にDelaunayを組むために使う

    ⑤ correction_target_feature_array:
       補正対象地物(点群)

    ⑥ coordinate_plane:
       __coordinate_plane_xy__ or __coordinate_plane_xz__

    ⑦ logger:
       ロガー

    ⑧ max_distance:
       これを超える場合は割り当てず -1 のまま残す(任意)

    【戻り値】
    更新済み tin_index_array
    ----------------------------------------------------------------
    """

    # ------------------------------------------------------------
    # この引数は現状未使用だが、元の変更後実装との整合性のため受けておく
    # 将来、TIN対応関係の検証を入れたくなったときの拡張余地にもなる
    # ------------------------------------------------------------
    _ = position_standard_tin_array

    # ------------------------------------------------------------
    # 1) 対象平面の座標インデックスを決める
    #    ここで「どの列を2次元平面として扱うか」を決める
    # ------------------------------------------------------------
    if coordinate_plane == __coordinate_plane_xy__:
        tin_xy_idx = (__point_coordinate_index_x__, __point_coordinate_index_y__)
        feat_xy_idx = (__point_coordinate_index_x__, __point_coordinate_index_y__)
        delaunay_cols = [__point_coordinate_index_x__, __point_coordinate_index_y__]

    elif coordinate_plane == __coordinate_plane_xz__:
        tin_xy_idx = (__point_coordinate_index_x__, __point_coordinate_index_z__)
        feat_xy_idx = (__point_coordinate_index_x__, __point_coordinate_index_z__)
        delaunay_cols = [__point_coordinate_index_x__, __point_coordinate_index_z__]

    else:
        raise ValueError("coordinate_plane is invalid")

    # ------------------------------------------------------------
    # 2) -1(=TIN外) の点を抽出する
    #    judge_inclusion で所属TINが見つからなかった点だけを救済対象にする
    # ------------------------------------------------------------
    outside_mask = (tin_index_array[:, 1] == -1)

    outside_count = int(np.sum(outside_mask))
    if outside_count == 0:
        logger.info("TIN外の点は0件。割り当て処理は不要。")
        return tin_index_array

    logger.info(f"TIN外の点が{outside_count}件あるため、最近傍境界TINへ割り当てる。")

    # ------------------------------------------------------------
    # 3) 境界TIN(外周三角形)を抽出する
    #    Delaunay.neighbors のどれかが -1 なら、その三角形は外周に接している
    # ------------------------------------------------------------
    delaunay_obj = Delaunay(position_standard_gcp_array[:, delaunay_cols])
    neighbors = delaunay_obj.neighbors

    boundary_tri_mask = np.any(neighbors == -1, axis=1)
    boundary_tri_indices = np.where(boundary_tri_mask)[0]

    # ------------------------------------------------------------
    # 念のための保険
    # 通常ここが0件になることはかなり考えにくいが、
    # 万一のときは全TINを候補にして処理継続する
    # ------------------------------------------------------------
    if len(boundary_tri_indices) == 0:
        logger.warning("境界TINが見つからない。全TINから最近傍を取る。")
        boundary_tri_indices = np.arange(len(correction_target_tin_array))

    # ------------------------------------------------------------
    # 4) 境界TINの重心を計算し、KDTreeを作る
    #    補正対象地物も補正対象TINも同じ座標系にいるため、
    #    距離計算は correction_target_tin_array を基準に行う
    # ------------------------------------------------------------
    boundary_tin = correction_target_tin_array[boundary_tri_indices][:, :, [tin_xy_idx[0], tin_xy_idx[1]]]
    boundary_centroids = np.mean(boundary_tin, axis=1)

    tree = cKDTree(boundary_centroids)

    # ------------------------------------------------------------
    # 5) 外側点をKDTreeで最近傍境界TINへ割り当てる
    #    nn_idx は boundary_tri_indices 上の添字なので、実TIN index に戻す
    # ------------------------------------------------------------
    outside_points = correction_target_feature_array[outside_mask][:, [feat_xy_idx[0], feat_xy_idx[1]]]

    distances, nn_idx = tree.query(outside_points, k=1)
    assigned_tin_indices = boundary_tri_indices[nn_idx]

    # ------------------------------------------------------------
    # max_distance を指定した場合だけ、遠すぎる点は除外する
    # ------------------------------------------------------------
    if max_distance is not None:
        too_far_mask = distances > max_distance
        assigned_tin_indices = assigned_tin_indices.copy()
        assigned_tin_indices[too_far_mask] = -1

        too_far_count = int(np.sum(too_far_mask))
        if too_far_count > 0:
            logger.warning(
                f"max_distance={max_distance} を超える外側点が{too_far_count}件あり、割り当てず-1のまま残した。"
            )

    # ------------------------------------------------------------
    # 6) 元の tin_index_array に反映する
    # ------------------------------------------------------------
    result_tin_index_array = tin_index_array.copy()
    result_tin_index_array[outside_mask, 1] = assigned_tin_indices

    logger.info("TIN外点のTIN割り当てが完了。")
    return result_tin_index_array


def calculate_geometry_coordinate_by_vector_affine_2D(
    correction_target_gcp_array,
    position_standard_gcp_array,
    correction_target_feature_array,
    split_unit_number,
    logger,
):
    """
    ----------------------------------------------------------------
    【概要】
    ExtendedOutsideTIN 用の 2D ベクトルアフィン処理。

    【ポイント】
    既存 NifiComplicationPackage.py の 2D 処理と流れは同じだが、
    judge_inclusion の結果が -1 になった点にも、
    最近傍境界TINを割り当てて外挿アフィン可能にしている。

    【戻り値】
    補正後の補正対象地物
    ----------------------------------------------------------------
    """

    # -------------------------------------------------------
    # 1) TINの生成
    # -------------------------------------------------------
    create_TIN_result = NCP.create_TIN(
        correction_target_gcp_array,
        position_standard_gcp_array,
    )

    correction_target_tin_array = create_TIN_result[__create_TIN_result_index_target_tin__]
    position_standard_tin_array = create_TIN_result[__create_TIN_result_index_standard_tin__]

    # -------------------------------------------------------
    # 2) 内包判定
    #    まずは既存ロジックで普通に所属TINを判定する
    # -------------------------------------------------------
    tin_index_array = NCP.judge_inclusion(
        correction_target_tin_array,
        position_standard_tin_array,
        correction_target_feature_array,
        split_unit_number,
        logger,
    )

    # -------------------------------------------------------
    # 3) TIN外(-1) の点に対して、最近傍の境界TINを割り当てる
    #    これが ExtendedOutsideTIN の本体
    # -------------------------------------------------------
    tin_index_array = fill_outside_points_with_nearest_boundary_tin(
        tin_index_array=tin_index_array,
        correction_target_tin_array=correction_target_tin_array,
        position_standard_tin_array=position_standard_tin_array,
        position_standard_gcp_array=position_standard_gcp_array,
        correction_target_feature_array=correction_target_feature_array,
        coordinate_plane=__coordinate_plane_xy__,
        logger=logger,
        max_distance=None,
    )

    # -------------------------------------------------------
    # 4) アフィン変換(xy座標)
    #    計算自体は既存ロジックをそのまま利用する
    # -------------------------------------------------------
    affine_result = NCP.execute_affine_transformation(
        correction_target_tin_array,
        position_standard_tin_array,
        correction_target_feature_array,
        split_unit_number,
        tin_index_array,
        __coordinate_plane_xy__,
    )

    correction_target_feature_array = affine_result[__affine_result_index_target_feature__]

    return correction_target_feature_array


def calculate_geometry_coordinate_by_vector_affine_3D(
    correction_target_gcp_array,
    position_standard_gcp_array,
    correction_target_feature_array,
    split_unit_number,
    logger,
):
    """
    ----------------------------------------------------------------
    【概要】
    ExtendedOutsideTIN 用の 3D ベクトルアフィン処理。

    【ポイント】
    - xy平面でTINを使ったアフィン変換を行う
    - TIN外(-1)点にも最近傍境界TINを割り当てる
    - その後、既存ロジックの z値補正を実施する

    【戻り値】
    補正後の補正対象地物
    ----------------------------------------------------------------
    """

    # -------------------------------------------------------
    # 1) TINの生成
    # -------------------------------------------------------
    create_TIN_result = NCP.create_TIN(
        correction_target_gcp_array,
        position_standard_gcp_array,
    )

    correction_target_tin_array = create_TIN_result[__create_TIN_result_index_target_tin__]
    position_standard_tin_array = create_TIN_result[__create_TIN_result_index_standard_tin__]

    # -------------------------------------------------------
    # 2) 内包判定
    # -------------------------------------------------------
    tin_index_array = NCP.judge_inclusion(
        correction_target_tin_array,
        position_standard_tin_array,
        correction_target_feature_array,
        split_unit_number,
        logger,
    )

    # -------------------------------------------------------
    # 3) TIN外(-1) の点に対して、最近傍の境界TINを割り当てる
    #    3Dでも、元の変更後ロジックに合わせて xy 平面で判定する
    # -------------------------------------------------------
    tin_index_array = fill_outside_points_with_nearest_boundary_tin(
        tin_index_array=tin_index_array,
        correction_target_tin_array=correction_target_tin_array,
        position_standard_tin_array=position_standard_tin_array,
        position_standard_gcp_array=position_standard_gcp_array,
        correction_target_feature_array=correction_target_feature_array,
        coordinate_plane=__coordinate_plane_xy__,
        logger=logger,
        max_distance=None,
    )

    # -------------------------------------------------------
    # 4) アフィン変換(xy座標)
    # -------------------------------------------------------
    affine_result = NCP.execute_affine_transformation(
        correction_target_tin_array,
        position_standard_tin_array,
        correction_target_feature_array,
        split_unit_number,
        tin_index_array,
        __coordinate_plane_xy__,
    )

    correction_target_feature_array = affine_result[__affine_result_index_target_feature__]
    correction_target_tin_array = affine_result[__affine_result_index_target_tin__]

    # -------------------------------------------------------
    # 5) z値の補正
    #    ここも既存のロジックをそのまま使う
    # -------------------------------------------------------
    correction_target_feature_array = NCP.correct_z_value(
        correction_target_tin_array,
        position_standard_tin_array,
        correction_target_feature_array,
        tin_index_array,
    )

    return correction_target_feature_array