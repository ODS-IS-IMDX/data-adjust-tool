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

from __future__ import annotations

import pickle
from dataclasses import dataclass
from importlib import import_module
from typing import Dict, List, Optional, Sequence, Tuple

np = import_module("numpy")
pandas = import_module("pandas")
cv2 = import_module("cv2")
ndi = import_module("scipy.ndimage")
skeletonize = import_module("skimage.morphology").skeletonize

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic

PointYX = Tuple[int, int]   # (y, x)
PointXY = Tuple[int, int]   # (x, y)
PixelLine = List            # [[(x1, y1), (x2, y2)], ...]


@dataclass
class VectorizeConfig:
    """
    vectorize_white_lines 相当の設定値をまとめたデータクラス。

    単位:
    - threshold: [pixel value]
    - open_kernel_size, close_kernel_size, junction_dilate_radius: [px]
    - min_component_area: [px^2]
    - min_spur_length_px, max_gap_px, max_lateral_offset_px, simplify_epsilon: [px]
    - spur_length_width_factor, max_width_ratio: [ratio]
    - max_endpoint_angle_deg: [deg]
    - smooth_window: [points]
    """

    threshold: int = 200
    open_kernel_size: int = 3
    close_kernel_size: int = 3
    min_component_area: int = 8
    junction_dilate_radius: int = 2
    min_spur_length_px: float = 6.0
    spur_length_width_factor: float = 1.2
    max_gap_px: float = 10.0
    max_endpoint_angle_deg: float = 25.0
    max_lateral_offset_px: float = 3.0
    max_width_ratio: float = 1.8
    smooth_window: int = 5
    simplify_epsilon: float = 1.5
    min_polyline_length_px: float = 0.0


@dataclass
class Node:
    """
    グラフ上のノード情報。

    kind:
        - "junction": 分岐点 / 交点
        - "endpoint": 端点
    """

    node_id: int
    kind: str
    anchor_yx: PointYX


@dataclass
class Segment:
    """
    ノード間の中心線セグメント情報。

    points_yx は (y, x) 順の点列である。
    """

    seg_id: int
    start_node_id: Optional[int]
    end_node_id: Optional[int]
    points_yx: "np.ndarray"
    mean_width_px: float
    active: bool = True


@dataclass
class LeafEndpoint:
    """
    再接続候補となる leaf endpoint 情報。
    """

    node_id: int
    seg_id: int
    at_start: bool
    point_yx: "np.ndarray"
    outward_dir_yx: "np.ndarray"
    width_px: float


class ConvertLineFromImageToVectorLogic(FlexibleRasterVectorLogic):
    """
    画像の白線マスクから中心線ネットワークを抽出し、
    互換出力として line の配列へ変換して返すロジッククラス。

    この版では、旧ロジックの内部処理は使わず、
    vectorize_white_lines と同じ考え方で以下の流れを採用する。

    1. グレースケール化 + 2値化
    2. open / close による前処理
    3. distance transform による局所線幅推定
    4. skeletonize による 1px 中心線化
    5. endpoint / junction の抽出と junction クラスタ統合
    6. node 間セグメント抽出
    7. 短い spur の削除
    8. endpoint の自然な再接続
    9. 平滑化 + RDP簡略化
    10. polyline を line の配列に展開して返す

    データI/Oは既存契約を維持する。
    - 入力:
        - bytes: pickle 化された画像 ndarray
        - pandas.Series: content 列に pickle 化画像 bytes
    - 出力:
        - bytes: pickle 化された line 配列
        - pandas.Series: content 列へ pickle 化 line 配列を書き戻す
    """

    N8 = [
        (-1, -1), (-1, 0), (-1, 1),
        (0, -1),           (0, 1),
        (1, -1),  (1, 0),  (1, 1),
    ]

    def __init__(self):
        """
        コンストラクタ。

        このクラスは状態を持たず、
        各呼び出しで与えられた入力画像とプロパティに従って処理する。
        """
        pass

    def input_check(self, byte_data, attribute):
        """
        入力データと FlowFile 属性の最低限の妥当性を検証する。

        Parameters
        ----------
        byte_data : bytes | pandas.Series
            入力データ。
        attribute : dict
            FlowFile 属性。ColorSpace を含む必要がある。

        Raises
        ------
        Exception
            入力データや属性が不足している場合。
        """
        if byte_data is None:
            raise Exception("入力データが設定されていません")

        if not attribute or attribute.get("ColorSpace") is None:
            raise Exception("attributeにColorSpaceが設定されていません")

    def _make_ellipse_kernel(self, radius: int) -> "np.ndarray":
        """
        半径 [px] から楕円カーネルを作成する。

        Parameters
        ----------
        radius : int
            半径 [px]。

        Returns
        -------
        numpy.ndarray
            OpenCV の morphology で使うカーネル。
        """
        radius = max(0, int(radius))
        size = radius * 2 + 1
        return cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (size, size))

    def _in_bounds(self, shape: Tuple[int, int], y: int, x: int) -> bool:
        """
        座標が画像範囲内かを判定する。

        Parameters
        ----------
        shape : tuple[int, int]
            画像 shape = (height, width)
        y : int
            行番号。
        x : int
            列番号。

        Returns
        -------
        bool
            範囲内なら True。
        """
        h, w = shape
        return 0 <= y < h and 0 <= x < w

    def _iter_neighbors_8(self, shape: Tuple[int, int], y: int, x: int) -> List[PointYX]:
        """
        指定座標の 8 近傍座標を返す。

        Parameters
        ----------
        shape : tuple[int, int]
            画像 shape = (height, width)
        y : int
            行番号。
        x : int
            列番号。

        Returns
        -------
        list[tuple[int, int]]
            範囲内の 8 近傍座標。
        """
        result: List[PointYX] = []
        for dy, dx in self.N8:
            ny = y + dy
            nx = x + dx
            if self._in_bounds(shape, ny, nx):
                result.append((ny, nx))
        return result

    def _ensure_gray_image(self, image: "np.ndarray") -> "np.ndarray":
        """
        入力画像をグレースケール 2 次元配列へ正規化する。

        Parameters
        ----------
        image : numpy.ndarray
            入力画像。

        Returns
        -------
        numpy.ndarray
            uint8 のグレースケール画像。

        Raises
        ------
        ValueError
            サポート外の shape / ndim の場合。
        """
        if image is None:
            raise ValueError("画像が取得できませんでした")

        arr = np.asarray(image)

        if arr.ndim == 2:
            gray = arr
        elif arr.ndim == 3:
            if arr.shape[2] == 1:
                gray = arr[:, :, 0]
            elif arr.shape[2] >= 3:
                gray = cv2.cvtColor(arr[:, :, :3].astype(np.uint8), cv2.COLOR_BGR2GRAY)
            else:
                raise ValueError(f"サポート外の画像 shape です: {arr.shape}")
        else:
            raise ValueError(f"サポート外の画像 ndim です: {arr.ndim}")

        if gray.dtype != np.uint8:
            gray = np.clip(gray, 0, 255).astype(np.uint8)

        return gray

    def _preprocess_mask(self, gray: "np.ndarray", cfg: VectorizeConfig) -> "np.ndarray":
        """
        白線を前景とする bool マスクを生成する。

        手順:
        1. 閾値で2値化
        2. open で微小な白ノイズを除去
        3. close で小さな途切れを補う
        4. 面積が小さすぎる連結成分を削除

        Parameters
        ----------
        gray : numpy.ndarray
            グレースケール画像。
        cfg : VectorizeConfig
            設定値。

        Returns
        -------
        numpy.ndarray
            前景マスク(bool)。
        """
        _, bin_img = cv2.threshold(gray, int(cfg.threshold), 255, cv2.THRESH_BINARY)

        open_kernel = self._make_ellipse_kernel(int(cfg.open_kernel_size) // 2)
        opened = cv2.morphologyEx(bin_img, cv2.MORPH_OPEN, open_kernel)

        close_kernel = self._make_ellipse_kernel(int(cfg.close_kernel_size) // 2)
        closed = cv2.morphologyEx(opened, cv2.MORPH_CLOSE, close_kernel)

        num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(
            (closed > 0).astype(np.uint8),
            connectivity=8,
        )

        cleaned = np.zeros_like(closed, dtype=np.uint8)
        for label_id in range(1, num_labels):
            area = int(stats[label_id, cv2.CC_STAT_AREA])
            if area >= int(cfg.min_component_area):
                cleaned[labels == label_id] = 255

        return cleaned > 0

    def _estimate_width_map(self, mask: "np.ndarray") -> "np.ndarray":
        """
        distance transform を使って局所線幅を推定する。

        線の中心に近い画素ほど大きな値になり、
        その 2 倍を局所線幅 [px] とみなす。

        Parameters
        ----------
        mask : numpy.ndarray
            前景マスク(bool)。

        Returns
        -------
        numpy.ndarray
            推定線幅マップ [px]。
        """
        dist = ndi.distance_transform_edt(mask)
        width_map = dist * 2.0
        return width_map.astype(np.float32)

    def _skeletonize_mask(self, mask: "np.ndarray") -> "np.ndarray":
        """
        前景マスクを 1px 幅の中心線へ変換する。

        Parameters
        ----------
        mask : numpy.ndarray
            前景マスク(bool)。

        Returns
        -------
        numpy.ndarray
            スケルトン(bool)。
        """
        skel = skeletonize(mask)
        return skel.astype(bool)

    def _compute_skeleton_degree(self, skel: "np.ndarray") -> "np.ndarray":
        """
        スケルトン各画素の 8 近傍接続数を計算する。

        Parameters
        ----------
        skel : numpy.ndarray
            スケルトン(bool)。

        Returns
        -------
        numpy.ndarray
            接続数マップ。
        """
        kernel = np.array(
            [
                [1, 1, 1],
                [1, 0, 1],
                [1, 1, 1],
            ],
            dtype=np.uint8,
        )
        degree = ndi.convolve(skel.astype(np.uint8), kernel, mode="constant", cval=0)
        return degree.astype(np.int32)

    def _nearest_point_to_centroid(self, coords_yx: "np.ndarray") -> PointYX:
        """
        座標群の重心に最も近い点を代表点として返す。

        Parameters
        ----------
        coords_yx : numpy.ndarray
            shape = (N, 2) の座標群。

        Returns
        -------
        tuple[int, int]
            代表点 (y, x)。
        """
        centroid = coords_yx.mean(axis=0)
        diffs = coords_yx.astype(np.float64) - centroid
        dist2 = (diffs ** 2).sum(axis=1)
        idx = int(np.argmin(dist2))
        y, x = coords_yx[idx]
        return int(y), int(x)

    def _build_nodes_and_node_map(
        self,
        skel: "np.ndarray",
        degree_map: "np.ndarray",
        cfg: VectorizeConfig,
    ) -> Tuple[Dict[int, Node], "np.ndarray"]:
        """
        endpoint と junction を検出し、
        ノード辞書とノードラベルマップを作る。

        手順:
        1. degree == 1 を endpoint とみなす
        2. degree >= 3 を junction 候補とみなす
        3. junction 候補は少し膨張させてクラスタ統合する
        4. endpoint は 1 画素 1 ノードとして扱う
        5. 各ノード領域に node_id を付与する

        Parameters
        ----------
        skel : numpy.ndarray
            スケルトン(bool)。
        degree_map : numpy.ndarray
            接続数マップ。
        cfg : VectorizeConfig
            設定値。

        Returns
        -------
        tuple
            (nodes, node_label_map)
        """
        h, w = skel.shape
        node_label_map = np.zeros((h, w), dtype=np.int32)
        nodes: Dict[int, Node] = {}
        next_node_id = 1

        raw_endpoints = skel & (degree_map == 1)
        raw_junctions = skel & (degree_map >= 3)

        if raw_junctions.any():
            kernel = self._make_ellipse_kernel(int(cfg.junction_dilate_radius))
            dilated = cv2.dilate((raw_junctions.astype(np.uint8) * 255), kernel)
            dilated_bool = dilated > 0

            num_labels, labels = cv2.connectedComponents(dilated_bool.astype(np.uint8), connectivity=8)

            for label_id in range(1, num_labels):
                region = labels == label_id
                region_on_skel = region & skel
                if not region_on_skel.any():
                    continue

                coords = np.argwhere(region_on_skel)
                anchor_yx = self._nearest_point_to_centroid(coords)

                node_id = next_node_id
                next_node_id += 1

                node_label_map[region_on_skel] = node_id
                nodes[node_id] = Node(node_id=node_id, kind="junction", anchor_yx=anchor_yx)

        endpoint_coords = np.argwhere(raw_endpoints & (node_label_map == 0))
        for y, x in endpoint_coords:
            node_id = next_node_id
            next_node_id += 1

            node_label_map[int(y), int(x)] = node_id
            nodes[node_id] = Node(node_id=node_id, kind="endpoint", anchor_yx=(int(y), int(x)))

        return nodes, node_label_map

    def _order_component_pixels(self, component_mask: "np.ndarray") -> "np.ndarray":
        """
        分岐のない 1 本の細線成分を順序付き点列へ並べ替える。

        node を除去した後の線成分は基本的に 1 本の鎖になるので、
        端点から隣接をたどれば順序付き点列を作れる。

        Parameters
        ----------
        component_mask : numpy.ndarray
            bool 型の成分マスク。

        Returns
        -------
        numpy.ndarray
            shape = (N, 2) の (y, x) 順点列。
        """
        coords = np.argwhere(component_mask)
        if len(coords) == 0:
            return np.empty((0, 2), dtype=np.int32)

        coord_set = {tuple(v) for v in coords.tolist()}
        shape = component_mask.shape

        degree_in_component: Dict[PointYX, int] = {}
        for y, x in coord_set:
            count = 0
            for ny, nx in self._iter_neighbors_8(shape, y, x):
                if (ny, nx) in coord_set:
                    count += 1
            degree_in_component[(y, x)] = count

        endpoints = [p for p, deg in degree_in_component.items() if deg <= 1]
        start = endpoints[0] if endpoints else next(iter(coord_set))

        ordered: List[PointYX] = []
        visited: set[PointYX] = set()

        prev: Optional[PointYX] = None
        current: PointYX = start

        while True:
            ordered.append(current)
            visited.add(current)

            candidates: List[PointYX] = []
            cy, cx = current

            for ny, nx in self._iter_neighbors_8(shape, cy, cx):
                nb = (ny, nx)
                if nb in coord_set and nb != prev and nb not in visited:
                    candidates.append(nb)

            if not candidates:
                break

            if len(candidates) == 1 or prev is None:
                nxt = candidates[0]
            else:
                current_vec = np.array(
                    [current[0] - prev[0], current[1] - prev[1]],
                    dtype=np.float64,
                )

                best_score = -1e18
                nxt = candidates[0]

                for cand in candidates:
                    cand_vec = np.array(
                        [cand[0] - current[0], cand[1] - current[1]],
                        dtype=np.float64,
                    )
                    score = float(np.dot(self._normalize_vector(current_vec), self._normalize_vector(cand_vec)))
                    if score > best_score:
                        best_score = score
                        nxt = cand

            prev = current
            current = nxt

            if len(ordered) > len(coord_set) + 5:
                break

        if len(ordered) < len(coord_set):
            for p in coord_set:
                if p not in visited:
                    ordered.append(p)

        return np.array(ordered, dtype=np.int32)

    def _adjacent_node_ids(self, node_label_map: "np.ndarray", y: int, x: int) -> List[int]:
        """
        指定画素の周囲に接している node_id 一覧を返す。

        自身と 8 近傍を調べる。

        Parameters
        ----------
        node_label_map : numpy.ndarray
            各画素の node_id マップ。
        y : int
            行番号。
        x : int
            列番号。

        Returns
        -------
        list[int]
            接している node_id の一覧。
        """
        ids: set[int] = set()

        own_id = int(node_label_map[y, x])
        if own_id > 0:
            ids.add(own_id)

        shape = node_label_map.shape
        for ny, nx in self._iter_neighbors_8(shape, y, x):
            nid = int(node_label_map[ny, nx])
            if nid > 0:
                ids.add(nid)

        return sorted(ids)

    def _choose_best_node_for_endpoint(
        self,
        candidate_ids: Sequence[int],
        endpoint_yx: PointYX,
        nodes: Dict[int, Node],
    ) -> Optional[int]:
        """
        component 端点に隣接する node 候補が複数あるとき、
        endpoint に最も近い anchor を持つ node を選ぶ。

        Parameters
        ----------
        candidate_ids : Sequence[int]
            候補 node_id 群。
        endpoint_yx : tuple[int, int]
            component 側の端点。
        nodes : dict[int, Node]
            ノード辞書。

        Returns
        -------
        Optional[int]
            選択された node_id。
        """
        if not candidate_ids:
            return None

        ey, ex = endpoint_yx
        best_id = None
        best_dist2 = float("inf")

        for nid in candidate_ids:
            ay, ax = nodes[nid].anchor_yx
            dy = ay - ey
            dx = ax - ex
            dist2 = dy * dy + dx * dx
            if dist2 < best_dist2:
                best_dist2 = dist2
                best_id = nid

        return best_id

    def _extract_segments(
        self,
        skel: "np.ndarray",
        node_label_map: "np.ndarray",
        nodes: Dict[int, Node],
        width_map: "np.ndarray",
    ) -> Dict[int, Segment]:
        """
        node を除いたスケルトンから線分セグメントを抽出する。

        考え方:
        - node 画素は「交点や端点」
        - それ以外のスケルトン画素は「node 間をつなぐ線の途中」
        - そのため、node を除去した残りを連結成分に分けると、
          基本的には 1 component = 1 segment になる

        Parameters
        ----------
        skel : numpy.ndarray
            スケルトン(bool)。
        node_label_map : numpy.ndarray
            node ラベルマップ。
        nodes : dict[int, Node]
            ノード辞書。
        width_map : numpy.ndarray
            推定線幅マップ。

        Returns
        -------
        dict[int, Segment]
            セグメント辞書。
        """
        line_mask = skel & (node_label_map == 0)
        num_labels, labels = cv2.connectedComponents(line_mask.astype(np.uint8), connectivity=8)

        segments: Dict[int, Segment] = {}
        next_seg_id = 1

        for label_id in range(1, num_labels):
            component_mask = labels == label_id
            coords = np.argwhere(component_mask)
            if len(coords) == 0:
                continue

            ordered = self._order_component_pixels(component_mask)
            if len(ordered) == 0:
                continue

            start_pixel = tuple(int(v) for v in ordered[0])
            end_pixel = tuple(int(v) for v in ordered[-1])

            start_candidates = self._adjacent_node_ids(node_label_map, start_pixel[0], start_pixel[1])
            end_candidates = self._adjacent_node_ids(node_label_map, end_pixel[0], end_pixel[1])

            start_node_id = self._choose_best_node_for_endpoint(start_candidates, start_pixel, nodes)
            end_node_id = self._choose_best_node_for_endpoint(end_candidates, end_pixel, nodes)

            points_list: List["np.ndarray"] = []

            if start_node_id is not None:
                points_list.append(np.array(nodes[start_node_id].anchor_yx, dtype=np.int32).reshape(1, 2))

            points_list.append(ordered.astype(np.int32))

            if end_node_id is not None:
                end_anchor = np.array(nodes[end_node_id].anchor_yx, dtype=np.int32).reshape(1, 2)
                if not np.array_equal(points_list[-1][-1], end_anchor[0]):
                    points_list.append(end_anchor)

            points_yx = np.vstack(points_list)

            component_widths = width_map[component_mask]
            mean_width = float(component_widths.mean()) if len(component_widths) > 0 else 1.0

            segments[next_seg_id] = Segment(
                seg_id=next_seg_id,
                start_node_id=start_node_id,
                end_node_id=end_node_id,
                points_yx=points_yx,
                mean_width_px=mean_width,
                active=True,
            )
            next_seg_id += 1

        return segments

    def _compute_node_degree_from_segments(self, segments: Dict[int, Segment]) -> Dict[int, int]:
        """
        active な segment だけを見て、各 node の次数を計算する。

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。

        Returns
        -------
        dict[int, int]
            node_id -> degree
        """
        degree: Dict[int, int] = {}

        for seg in segments.values():
            if not seg.active:
                continue

            if seg.start_node_id is not None:
                degree[seg.start_node_id] = degree.get(seg.start_node_id, 0) + 1

            if seg.end_node_id is not None:
                degree[seg.end_node_id] = degree.get(seg.end_node_id, 0) + 1

        return degree

    def _polyline_length(self, points_yx: "np.ndarray") -> float:
        """
        点列長を計算する。

        Parameters
        ----------
        points_yx : numpy.ndarray
            shape = (N, 2) の点列。

        Returns
        -------
        float
            長さ [px]。
        """
        if len(points_yx) < 2:
            return 0.0

        diffs = np.diff(points_yx.astype(np.float64), axis=0)
        lens = np.sqrt((diffs ** 2).sum(axis=1))
        return float(lens.sum())

    def _prune_spurs(
        self,
        segments: Dict[int, Segment],
        nodes: Dict[int, Node],
        cfg: VectorizeConfig,
    ) -> None:
        """
        短い spur を削除する。

        判定条件:
        - 端点を含む leaf segment である
        - 長さが短い
        - 線幅に対して見ても短い

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。
        nodes : dict[int, Node]
            ノード辞書。
        cfg : VectorizeConfig
            設定値。
        """
        changed = True

        while changed:
            changed = False
            node_degree = self._compute_node_degree_from_segments(segments)

            for seg in segments.values():
                if not seg.active:
                    continue

                seg_len = self._polyline_length(seg.points_yx)
                threshold = max(float(cfg.min_spur_length_px), float(cfg.spur_length_width_factor) * seg.mean_width_px)

                start_is_leaf_endpoint = False
                end_is_leaf_endpoint = False

                if seg.start_node_id is not None:
                    start_node = nodes.get(seg.start_node_id)
                    if start_node is not None:
                        start_is_leaf_endpoint = (
                            start_node.kind == "endpoint"
                            and node_degree.get(seg.start_node_id, 0) == 1
                        )

                if seg.end_node_id is not None:
                    end_node = nodes.get(seg.end_node_id)
                    if end_node is not None:
                        end_is_leaf_endpoint = (
                            end_node.kind == "endpoint"
                            and node_degree.get(seg.end_node_id, 0) == 1
                        )

                if seg_len < threshold and (start_is_leaf_endpoint or end_is_leaf_endpoint):
                    seg.active = False
                    changed = True

    def _normalize_vector(self, vec: "np.ndarray") -> "np.ndarray":
        """
        ベクトルを正規化する。

        Parameters
        ----------
        vec : numpy.ndarray
            対象ベクトル。

        Returns
        -------
        numpy.ndarray
            正規化ベクトル。
        """
        norm = float(np.linalg.norm(vec))
        if norm == 0.0:
            return vec.astype(np.float64)
        return vec.astype(np.float64) / norm

    def _angle_deg_between(self, v1: "np.ndarray", v2: "np.ndarray") -> float:
        """
        2 ベクトルのなす角を度数で返す。

        Parameters
        ----------
        v1 : numpy.ndarray
            ベクトル 1。
        v2 : numpy.ndarray
            ベクトル 2。

        Returns
        -------
        float
            角度 [deg]。
        """
        u1 = self._normalize_vector(v1)
        u2 = self._normalize_vector(v2)
        dot = float(np.clip(np.dot(u1, u2), -1.0, 1.0))
        return float(np.degrees(np.arccos(dot)))

    def _lateral_offset(
        self,
        base_point: "np.ndarray",
        direction: "np.ndarray",
        target_point: "np.ndarray",
    ) -> float:
        """
        方向ベクトルに対する target の横ずれ量を計算する。

        Parameters
        ----------
        base_point : numpy.ndarray
            基準点。
        direction : numpy.ndarray
            進行方向ベクトル。
        target_point : numpy.ndarray
            対象点。

        Returns
        -------
        float
            横ずれ量 [px]。
        """
        u = self._normalize_vector(direction)
        d = target_point.astype(np.float64) - base_point.astype(np.float64)

        if np.linalg.norm(u) == 0.0:
            return 1e9

        proj = np.dot(d, u)
        perp = d - proj * u
        return float(np.linalg.norm(perp))

    def _get_segment_outward_direction(self, seg: Segment, at_start: bool) -> "np.ndarray":
        """
        segment 端点から「外向き」に伸びる方向ベクトルを返す。

        例えば点列が
            P0 -> P1 -> P2 -> ...
        で、P0 側の endpoint を見る場合、
        線の内側方向は P1 - P0 だが、
        endpoint 再接続で欲しいのは「その先に延長される方向」なので
        外向きベクトルは P0 - P1 になる。

        Parameters
        ----------
        seg : Segment
            対象セグメント。
        at_start : bool
            True なら先頭側、False なら末尾側。

        Returns
        -------
        numpy.ndarray
            shape = (2,) の (y, x) ベクトル。
        """
        pts = seg.points_yx.astype(np.float64)

        if len(pts) < 2:
            return np.array([0.0, 0.0], dtype=np.float64)

        if at_start:
            return pts[0] - pts[1]

        return pts[-1] - pts[-2]

    def _collect_leaf_endpoints(
        self,
        segments: Dict[int, Segment],
        nodes: Dict[int, Node],
    ) -> List[LeafEndpoint]:
        """
        active segment から leaf endpoint 候補を集める。

        条件:
        - node 次数が 1
        - その node が segment の端点として存在する

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。
        nodes : dict[int, Node]
            ノード辞書。

        Returns
        -------
        list[LeafEndpoint]
            再接続候補 endpoint 一覧。
        """
        node_degree = self._compute_node_degree_from_segments(segments)
        result: List[LeafEndpoint] = []

        for seg in segments.values():
            if not seg.active:
                continue

            if seg.start_node_id is not None and node_degree.get(seg.start_node_id, 0) == 1:
                result.append(
                    LeafEndpoint(
                        node_id=seg.start_node_id,
                        seg_id=seg.seg_id,
                        at_start=True,
                        point_yx=seg.points_yx[0].astype(np.float64),
                        outward_dir_yx=self._get_segment_outward_direction(seg, at_start=True),
                        width_px=seg.mean_width_px,
                    )
                )

            if seg.end_node_id is not None and node_degree.get(seg.end_node_id, 0) == 1:
                result.append(
                    LeafEndpoint(
                        node_id=seg.end_node_id,
                        seg_id=seg.seg_id,
                        at_start=False,
                        point_yx=seg.points_yx[-1].astype(np.float64),
                        outward_dir_yx=self._get_segment_outward_direction(seg, at_start=False),
                        width_px=seg.mean_width_px,
                    )
                )

        return result

    def _score_endpoint_pair(
        self,
        ep1: LeafEndpoint,
        ep2: LeafEndpoint,
        cfg: VectorizeConfig,
    ) -> Optional[float]:
        """
        2 つの endpoint を再接続してよいかを判定し、
        よければスコアを返す。

        条件:
        - 同じ segment 同士ではない
        - 距離が近い
        - 相手が外向き延長方向にいる
        - 横ずれが小さい
        - 線幅差が大きすぎない

        Parameters
        ----------
        ep1 : LeafEndpoint
            endpoint 1。
        ep2 : LeafEndpoint
            endpoint 2。
        cfg : VectorizeConfig
            設定値。

        Returns
        -------
        Optional[float]
            接続可ならスコア、不可なら None。
        """
        if ep1.seg_id == ep2.seg_id:
            return None

        p1 = ep1.point_yx
        p2 = ep2.point_yx

        gap = float(np.linalg.norm(p2 - p1))
        if gap > float(cfg.max_gap_px):
            return None

        width_ratio = max(ep1.width_px, ep2.width_px) / max(1e-6, min(ep1.width_px, ep2.width_px))
        if width_ratio > float(cfg.max_width_ratio):
            return None

        v12 = p2 - p1
        v21 = p1 - p2

        angle1 = self._angle_deg_between(ep1.outward_dir_yx, v12)
        angle2 = self._angle_deg_between(ep2.outward_dir_yx, v21)

        if angle1 > float(cfg.max_endpoint_angle_deg):
            return None
        if angle2 > float(cfg.max_endpoint_angle_deg):
            return None

        lat1 = self._lateral_offset(p1, ep1.outward_dir_yx, p2)
        lat2 = self._lateral_offset(p2, ep2.outward_dir_yx, p1)

        if lat1 > float(cfg.max_lateral_offset_px):
            return None
        if lat2 > float(cfg.max_lateral_offset_px):
            return None

        score = gap + 0.15 * (angle1 + angle2) + 0.5 * (lat1 + lat2)
        return float(score)

    def _orient_segment_to_end_at_node(self, seg: Segment, connect_node_id: int) -> "np.ndarray":
        """
        connect_node_id 側で終わる向きに segment 点列をそろえる。

        Returns
        -------
        numpy.ndarray
            other_end -> ... -> connect_node の順の点列。
        """
        if seg.end_node_id == connect_node_id:
            return seg.points_yx.copy()

        if seg.start_node_id == connect_node_id:
            return seg.points_yx[::-1].copy()

        raise ValueError(f"segment {seg.seg_id} は node {connect_node_id} を端点に持っていません")

    def _orient_segment_to_start_at_node(self, seg: Segment, connect_node_id: int) -> "np.ndarray":
        """
        connect_node_id 側で始まる向きに segment 点列をそろえる。

        Returns
        -------
        numpy.ndarray
            connect_node -> ... -> other_end の順の点列。
        """
        if seg.start_node_id == connect_node_id:
            return seg.points_yx.copy()

        if seg.end_node_id == connect_node_id:
            return seg.points_yx[::-1].copy()

        raise ValueError(f"segment {seg.seg_id} は node {connect_node_id} を端点に持っていません")

    def _other_end_node_id(self, seg: Segment, connect_node_id: int) -> Optional[int]:
        """
        segment のうち、connect_node_id ではない反対側 node_id を返す。

        Parameters
        ----------
        seg : Segment
            対象セグメント。
        connect_node_id : int
            接続側 node_id。

        Returns
        -------
        Optional[int]
            反対側 node_id。
        """
        if seg.start_node_id == connect_node_id:
            return seg.end_node_id
        if seg.end_node_id == connect_node_id:
            return seg.start_node_id
        return None

    def _concat_polylines(self, poly1: "np.ndarray", poly2: "np.ndarray") -> "np.ndarray":
        """
        2 本の点列を連結する。

        Parameters
        ----------
        poly1 : numpy.ndarray
            前半点列。
        poly2 : numpy.ndarray
            後半点列。

        Returns
        -------
        numpy.ndarray
            連結後点列。
        """
        if len(poly1) == 0:
            return poly2.copy()
        if len(poly2) == 0:
            return poly1.copy()
        if np.array_equal(poly1[-1], poly2[0]):
            return np.vstack([poly1, poly2[1:]])
        return np.vstack([poly1, poly2])

    def _merge_two_segments(
        self,
        seg1: Segment,
        connect_node1_id: int,
        seg2: Segment,
        connect_node2_id: int,
        new_seg_id: int,
    ) -> Segment:
        """
        2 本の segment を endpoint-to-endpoint で接続し、1 本にまとめる。

        形としては
            other1 -> ... -> connect1 -> connect2 -> ... -> other2
        となる。

        中間の connect1 と connect2 の間は直線でつなぐ。

        Parameters
        ----------
        seg1 : Segment
            前半セグメント。
        connect_node1_id : int
            seg1 側の接続 node。
        seg2 : Segment
            後半セグメント。
        connect_node2_id : int
            seg2 側の接続 node。
        new_seg_id : int
            新しい segment ID。

        Returns
        -------
        Segment
            結合済みセグメント。
        """
        poly1 = self._orient_segment_to_end_at_node(seg1, connect_node1_id)
        poly2 = self._orient_segment_to_start_at_node(seg2, connect_node2_id)

        connector = np.vstack([poly1[-1], poly2[0]])

        merged = self._concat_polylines(poly1, connector)
        merged = self._concat_polylines(merged, poly2)

        other1 = self._other_end_node_id(seg1, connect_node1_id)
        other2 = self._other_end_node_id(seg2, connect_node2_id)

        mean_width = float((seg1.mean_width_px + seg2.mean_width_px) / 2.0)

        return Segment(
            seg_id=new_seg_id,
            start_node_id=other1,
            end_node_id=other2,
            points_yx=merged,
            mean_width_px=mean_width,
            active=True,
        )

    def _reconnect_leaf_endpoints(
        self,
        segments: Dict[int, Segment],
        nodes: Dict[int, Node],
        cfg: VectorizeConfig,
    ) -> None:
        """
        人間の目には 1 本に見えやすい leaf endpoint 同士を再接続する。

        やり方:
        1. leaf endpoint 候補を集める
        2. ペアごとに接続スコアを計算する
        3. スコアの小さい順に、衝突しない組だけ採用する
        4. 対応する 2 本の segment を 1 本へ結合する
        5. 安定するまで繰り返す

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。
        nodes : dict[int, Node]
            ノード辞書。
        cfg : VectorizeConfig
            設定値。
        """
        next_seg_id = max(segments.keys(), default=0) + 1

        changed = True
        while changed:
            changed = False

            leafs = self._collect_leaf_endpoints(segments, nodes)
            if len(leafs) < 2:
                break

            candidates: List[Tuple[float, int, int]] = []

            for i in range(len(leafs)):
                for j in range(i + 1, len(leafs)):
                    score = self._score_endpoint_pair(leafs[i], leafs[j], cfg)
                    if score is not None:
                        candidates.append((score, i, j))

            if not candidates:
                break

            candidates.sort(key=lambda v: v[0])

            used_nodes: set[int] = set()
            used_segments: set[int] = set()
            merges: List[Tuple[LeafEndpoint, LeafEndpoint]] = []

            for _, i, j in candidates:
                ep1 = leafs[i]
                ep2 = leafs[j]

                if ep1.node_id in used_nodes or ep2.node_id in used_nodes:
                    continue
                if ep1.seg_id in used_segments or ep2.seg_id in used_segments:
                    continue
                if ep1.seg_id == ep2.seg_id:
                    continue

                merges.append((ep1, ep2))
                used_nodes.add(ep1.node_id)
                used_nodes.add(ep2.node_id)
                used_segments.add(ep1.seg_id)
                used_segments.add(ep2.seg_id)

            if not merges:
                break

            for ep1, ep2 in merges:
                seg1 = segments.get(ep1.seg_id)
                seg2 = segments.get(ep2.seg_id)

                if seg1 is None or seg2 is None:
                    continue
                if not seg1.active or not seg2.active:
                    continue

                merged = self._merge_two_segments(
                    seg1=seg1,
                    connect_node1_id=ep1.node_id,
                    seg2=seg2,
                    connect_node2_id=ep2.node_id,
                    new_seg_id=next_seg_id,
                )
                next_seg_id += 1

                seg1.active = False
                seg2.active = False
                segments[merged.seg_id] = merged
                changed = True

    def _smooth_polyline(self, points_yx: "np.ndarray", window: int) -> "np.ndarray":
        """
        ポリラインを軽く平滑化する。

        端点は固定し、中間点だけ移動平均でなめらかにする。

        Parameters
        ----------
        points_yx : numpy.ndarray
            shape = (N, 2) の点列。
        window : int
            移動平均窓サイズ [points]。

        Returns
        -------
        numpy.ndarray
            平滑化後の点列。
        """
        if len(points_yx) < 3:
            return points_yx.copy()

        window = max(3, int(window))
        if window % 2 == 0:
            window += 1

        half = window // 2
        pts = points_yx.astype(np.float64).copy()
        out = pts.copy()

        for i in range(1, len(pts) - 1):
            left = max(0, i - half)
            right = min(len(pts), i + half + 1)
            out[i] = pts[left:right].mean(axis=0)

        out[0] = pts[0]
        out[-1] = pts[-1]

        return np.rint(out).astype(np.int32)

    def _point_line_distance(
        self,
        point: "np.ndarray",
        start: "np.ndarray",
        end: "np.ndarray",
    ) -> float:
        """
        点と線分(start-end)の距離を返す。

        RDP 簡略化で使う。

        Parameters
        ----------
        point : numpy.ndarray
            対象点。
        start : numpy.ndarray
            線分始点。
        end : numpy.ndarray
            線分終点。

        Returns
        -------
        float
            距離 [px]。
        """
        p = point.astype(np.float64)
        a = start.astype(np.float64)
        b = end.astype(np.float64)

        ab = b - a
        ab_norm2 = float(np.dot(ab, ab))

        if ab_norm2 == 0.0:
            return float(np.linalg.norm(p - a))

        t = float(np.dot(p - a, ab) / ab_norm2)
        t = max(0.0, min(1.0, t))
        proj = a + t * ab
        return float(np.linalg.norm(p - proj))

    def _rdp_simplify(self, points_yx: "np.ndarray", epsilon: float) -> "np.ndarray":
        """
        Ramer-Douglas-Peucker 法で点列を簡略化する。

        Parameters
        ----------
        points_yx : numpy.ndarray
            shape = (N, 2) の点列。
        epsilon : float
            許容誤差 [px]。

        Returns
        -------
        numpy.ndarray
            簡略化後の点列。
        """
        if len(points_yx) < 3:
            return points_yx.copy()

        start = points_yx[0]
        end = points_yx[-1]
        max_dist = -1.0
        split_index = -1

        for i in range(1, len(points_yx) - 1):
            dist = self._point_line_distance(points_yx[i], start, end)
            if dist > max_dist:
                max_dist = dist
                split_index = i

        if max_dist > epsilon:
            left = self._rdp_simplify(points_yx[: split_index + 1], epsilon)
            right = self._rdp_simplify(points_yx[split_index:], epsilon)
            return np.vstack([left[:-1], right])

        return np.vstack([start, end]).astype(np.int32)

    def _postprocess_segments(self, segments: Dict[int, Segment], cfg: VectorizeConfig) -> None:
        """
        セグメントへ平滑化と簡略化を適用する。

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。
        cfg : VectorizeConfig
            設定値。
        """
        for seg in segments.values():
            if not seg.active:
                continue

            pts = seg.points_yx
            if len(pts) < 2:
                continue

            smoothed = self._smooth_polyline(pts, int(cfg.smooth_window))
            simplified = self._rdp_simplify(smoothed, float(cfg.simplify_epsilon))

            if len(simplified) < 2:
                simplified = pts

            seg.points_yx = simplified.astype(np.int32)

    def _filter_short_polylines(self, segments: Dict[int, Segment], cfg: VectorizeConfig) -> None:
        """
        polyline 全体の長さがしきい値以下の segment を無効化する。

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。
        cfg : VectorizeConfig
            設定値。
        """
        threshold = float(cfg.min_polyline_length_px)

        if threshold <= 0.0:
            return

        for seg in segments.values():
            if not seg.active:
                continue

            seg_len = self._polyline_length(seg.points_yx)
            if seg_len <= threshold:
                seg.active = False

    def _convert_segments_to_pixel_lines(self, segments: Dict[int, Segment]) -> List[PixelLine]:
        """
        active な polyline セグメント群を line 配列へ展開する。

        出力形式は互換のため
            [[(x1, y1), (x2, y2)], ...]
        とする。

        Parameters
        ----------
        segments : dict[int, Segment]
            セグメント辞書。

        Returns
        -------
        list
            line 配列。
        """
        pixel_lines: List[PixelLine] = []

        for seg in segments.values():
            if not seg.active:
                continue

            pts = seg.points_yx.astype(np.int32)
            if len(pts) < 2:
                continue

            for i in range(len(pts) - 1):
                y1, x1 = pts[i]
                y2, x2 = pts[i + 1]

                if int(y1) == int(y2) and int(x1) == int(x2):
                    continue

                pixel_lines.append(
                    [
                        (int(x1), int(y1)),
                        (int(x2), int(y2)),
                    ]
                )

        return pixel_lines

    def _build_config(self, properties: dict) -> VectorizeConfig:
        """
        NiFi プロパティ辞書から VectorizeConfig を生成する。

        Parameters
        ----------
        properties : dict
            NiFi プロパティ辞書。

        Returns
        -------
        VectorizeConfig
            構築した設定値。

        Raises
        ------
        ValueError
            プロパティ値が不正な場合。
        """
        try:
            cfg = VectorizeConfig(
                threshold=int(properties.get("threshold", 200)),
                open_kernel_size=int(properties.get("open_kernel_size", 3)),
                close_kernel_size=int(properties.get("close_kernel_size", 3)),
                min_component_area=int(properties.get("min_component_area", 8)),
                junction_dilate_radius=int(properties.get("junction_dilate_radius", 2)),
                min_spur_length_px=float(properties.get("min_spur_length_px", 6.0)),
                spur_length_width_factor=float(properties.get("spur_length_width_factor", 1.2)),
                max_gap_px=float(properties.get("max_gap_px", 10.0)),
                max_endpoint_angle_deg=float(properties.get("max_endpoint_angle_deg", 25.0)),
                max_lateral_offset_px=float(properties.get("max_lateral_offset_px", 3.0)),
                max_width_ratio=float(properties.get("max_width_ratio", 1.8)),
                smooth_window=int(properties.get("smooth_window", 5)),
                simplify_epsilon=float(properties.get("simplify_epsilon", 1.5)),
                min_polyline_length_px=float(properties.get("min_polyline_length_px", 0.0)),
            )
        except Exception as e:
            raise ValueError(f"プロパティの取得時にエラーが発生しました: {e}")

        if not (0 <= int(cfg.threshold) <= 255):
            raise ValueError("threshold は 0〜255 の範囲で指定する必要があります")
        if int(cfg.open_kernel_size) <= 0:
            raise ValueError("open_kernel_size は 1 以上である必要があります")
        if int(cfg.close_kernel_size) <= 0:
            raise ValueError("close_kernel_size は 1 以上である必要があります")
        if int(cfg.min_component_area) < 0:
            raise ValueError("min_component_area は 0 以上である必要があります")
        if int(cfg.junction_dilate_radius) < 0:
            raise ValueError("junction_dilate_radius は 0 以上である必要があります")
        if float(cfg.min_spur_length_px) < 0.0:
            raise ValueError("min_spur_length_px は 0 以上である必要があります")
        if float(cfg.spur_length_width_factor) < 0.0:
            raise ValueError("spur_length_width_factor は 0 以上である必要があります")
        if float(cfg.max_gap_px) < 0.0:
            raise ValueError("max_gap_px は 0 以上である必要があります")
        if not (0.0 <= float(cfg.max_endpoint_angle_deg) <= 180.0):
            raise ValueError("max_endpoint_angle_deg は 0〜180 の範囲で指定する必要があります")
        if float(cfg.max_lateral_offset_px) < 0.0:
            raise ValueError("max_lateral_offset_px は 0 以上である必要があります")
        if float(cfg.max_width_ratio) < 1.0:
            raise ValueError("max_width_ratio は 1.0 以上である必要があります")
        if int(cfg.smooth_window) <= 0:
            raise ValueError("smooth_window は 1 以上である必要があります")
        if float(cfg.simplify_epsilon) < 0.0:
            raise ValueError("simplify_epsilon は 0 以上である必要があります")
        if float(cfg.min_polyline_length_px) < 0.0:
            raise ValueError("min_polyline_length_px は 0 以上である必要があります")
        
        return cfg

    def _vectorize_to_pixel_lines(self, image: "np.ndarray", cfg: VectorizeConfig) -> List[PixelLine]:
        """
        画像 1 枚を line 配列へ変換する本体処理。

        Parameters
        ----------
        image : numpy.ndarray
            入力画像。
        cfg : VectorizeConfig
            設定値。

        Returns
        -------
        list
            [[(x1, y1), (x2, y2)], ...] 形式の line 配列。
        """
        gray = self._ensure_gray_image(image)
        mask = self._preprocess_mask(gray, cfg)

        if not mask.any():
            return []

        width_map = self._estimate_width_map(mask)
        skel = self._skeletonize_mask(mask)

        if not skel.any():
            return []

        degree_map = self._compute_skeleton_degree(skel)
        nodes, node_label_map = self._build_nodes_and_node_map(skel, degree_map, cfg)
        segments = self._extract_segments(skel, node_label_map, nodes, width_map)

        if not segments:
            return []

        self._prune_spurs(segments, nodes, cfg)
        self._reconnect_leaf_endpoints(segments, nodes, cfg)
        self._postprocess_segments(segments, cfg)
        self._filter_short_polylines(segments, cfg)

        return self._convert_segments_to_pixel_lines(segments)

    def __call__(self, byte_data, attribute, properties):
        """
        入力画像から中心線ネットワークを抽出し、line 配列として返す。

        Parameters
        ----------
        byte_data : bytes | pandas.Series
            bytes の場合は pickle 化画像 ndarray、
            Series の場合は content 列に画像 bytes が入る想定。
        attribute : dict
            FlowFile 属性。ColorSpace は BINARY または GRAYSCALE を想定する。
        properties : dict
            NiFi プロパティ辞書。
            vectorize_white_lines 相当の設定値のみを受け付ける。

        Returns
        -------
        tuple
            (new_byte_data, attribute)
        """
        self.input_check(byte_data, attribute)

        color_space = attribute["ColorSpace"]
        if color_space not in ("BINARY", "GRAYSCALE"):
            raise Exception("BINARY, GRAYSCALE以外のColorSpaceが設定されています")

        if type(byte_data) is pandas.core.series.Series:
            if "content" not in byte_data:
                raise ValueError("FieldSetFile入力ですが content 列が存在しません")
            image = pickle.loads(byte_data["content"])
            is_series = True
        else:
            image = pickle.loads(byte_data)
            is_series = False

        cfg = self._build_config(properties)
        pixel_lines = self._vectorize_to_pixel_lines(image=image, cfg=cfg)

        if is_series:
            byte_data["content"] = pickle.dumps(pixel_lines)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(pixel_lines)

        return new_byte_data, attribute