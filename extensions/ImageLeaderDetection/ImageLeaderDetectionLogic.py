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
import pickle
import math
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")
DBSCAN = import_module("sklearn.cluster").DBSCAN

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic
from raster_to_vector.common.processor_results import ProcessorResults


class ImageLeaderDetectionLogic(BaseRasterVectorLogic):
    def __init__(self):
        pass

    def input_check(self, byte_data, attribute):
        """
        入力データと属性の妥当性を確認します。

        Parameters
        ----------
        byte_data : bytes
            入力される画像データのバイト列です。
        attribute : dict
            画像の属性情報を含む辞書です。ColorSpace 属性が含まれている必要があります。

        Raises
        ------
        Exception
            byte_data が None の場合や ColorSpace が設定されていない場合に例外を発生させます。
        """
        if byte_data is None:
            raise Exception('入力データが設定されていません')
        if not attribute:
            raise Exception('attributeが設定されていません')

    def __call__(self, byte_data, attribute, properties):
        """
        入力データの円検出結果と旗上げ線の座標(カウント対象)から管路の条数と段数をカウント結果(条数と段数)をリストで返します。
        管路を検出できなかった場合は空リストを返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の円検出結果と旗上げ線の座標を持つFieldSetFile
        attribute : dict
            line_coordsキーに、旗上げ線の両端の座標（バイト列を文字列化した形式）が設定されている必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - search_range_factor : string
                指定された旗上げ線に対応する管路の集合を探索する範囲の調整値が設定されます。

            - cluster_epsilon : string
                管路の集合をクラスタリングする調整値が設定されます。
                
            - count_columns_threshold : string
                管路の条数算出処理の端数を切り上げる閾値が設定されます。

            - line_coords_suffix : string
                管旗上げ線座標のFieldSetFileの項目名が指定されます。

            - pipe_columns_suffix : string
                管路の条数のFieldSetFileの項目名が指定されます。

            - pipe_rows_suffix : string
                管路の段数のFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            - new_byte_data : float
                管路のカウント結果を条数と段数としてそれぞれfloat型でProcessorResultsに設定して返します。
                
            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------            
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入力データ
        circles = pickle.loads(byte_data)

        # 旗上げ線の項目名取得
        line_coords_suffix = properties.get('line_coords_suffix', 'line_coords')
        pipe_columns_suffix = properties.get('pipe_columns_suffix', '条数')
        pipe_rows_suffix = properties.get('pipe_rows_suffix', '段数')

        # 旗上げ線座標
        try:
            leader_coord = eval(attribute[line_coords_suffix])
            leader_coord = pickle.loads(leader_coord)
        except Exception as e:
            raise Exception(f'line_coordsの旗上げ線の座標に無効な値が設定されています: {str(e)}')
        for xy in leader_coord:
            for num in xy:
                if not isinstance(num, int) or num < 0:
                    raise ValueError("line_coordsの旗上げ線の座標は0以上である必要があります")
        
        # 管路の集合を探索する範囲の調整値を取得
        try:
            search_range_factor = float(properties.get('search_range_factor', 1.5))
        except Exception as e:
            raise Exception(f'search_range_factorに無効な値が設定されています: {str(e)}')
        if search_range_factor < 0:
            raise ValueError("search_range_factorは0以上である必要があります")

        # 管路の集合をクラスタリングする調整値を取得
        try:
            cluster_epsilon = int(properties.get('cluster_epsilon', 0))
        except Exception as e:
            raise Exception(f'cluster_epsilonに無効な値が設定されています: {str(e)}')
        if cluster_epsilon < 0:
            raise ValueError("cluster_epsilonは0以上である必要があります")

        # 管路の条数カウント時に管路が交互に埋設されている状態を考慮する閾値
        try:
            count_columns_threshold = float(properties.get('count_columns_threshold', 0.5))
        except Exception as e:
            raise Exception(f'count_columns_thresholdに無効な値が設定されています: {str(e)}')
        if not (0.0 <= count_columns_threshold < 1.0):
            raise ValueError("count_columns_thresholdは0.0以上1.0未満である必要があります")

        num_columns = 0
        num_rows = 0
        if 0 < len(circles):
            # 旗上げ線付近をROIとし、旗上げ線座標をROI座標系に変換
            roi, leader_coord_roi = self.make_roi(leader_coord, circles, search_range_factor)

            # 検出済みの円検出結果をROI内のものに絞り(同一中心座標の円も絞る)、ROI座標系に変換
            circles_roi = self.select_circle(circles, roi)

            if 0 < len(circles_roi):
                # ROI内の管路をクラスタリングして決定
                circles_select = self.cluster_circles(circles_roi, leader_coord_roi, cluster_epsilon)

                if 0 < len(circles_select):
                    # 旗上げ線が水平になるように円の座標を回転
                    circles_rotate = self.rotate_coord(leader_coord_roi, circles_select)

                    # 管路の条数/段数カウント
                    num_columns, num_rows = self.count_circles(circles_rotate, count_columns_threshold)

        # 管路のカウント結果を応答
        new_byte_data = ProcessorResults()
        new_byte_data.add_result(column_name=pipe_columns_suffix, col_type='float64', value=np.float64(num_columns))
        new_byte_data.add_result(column_name=pipe_rows_suffix, col_type='float64', value=np.float64(num_rows))

        return new_byte_data, attribute


    def make_roi(self, leader_coord, circles, search_range_factor):
        """
        旗上げ線の座標から管路を探索する処理対象の範囲(ROI)を決定する関数
        旗上げ線に直交する上方向と下方向にそれぞれ旗上げ線のsearch_range_factor倍の長さの範囲をROIとする

        :param leader_coord: 旗上げ線の座標 [(始点x,始点y),(終点x,終点y)]
        :param search_range_factor: 旗上げ線に対応する管路の集合を探索する範囲の調整値
        :return: ROI座標 [(右上x, 右上y),(左下x, 左下y)]、ROI座標系の旗上げ線座標 [(始点x,始点y),(終点x,終点y)]
        """
        if search_range_factor == 0:
            # search_range_factorが0の場合は、画像全体をROIとする
            margin = 5
            roi_x1 = 0
            roi_y1 = 0
            roi_x2 = max(circle[0][0] + circle[1] for circle in circles) + margin
            roi_y2 = max(circle[0][1] + circle[1] for circle in circles) + margin
        else:
            # 始点と終点の座標
            point_start = np.array([leader_coord[0][0], leader_coord[0][1]])
            point_end = np.array([leader_coord[1][0], leader_coord[1][1]])
            # 旗上げ線のベクトルを計算
            vector = point_end - point_start
            # 旗上げ線の中点（交点）を計算
            midpoint = point_start + vector / 2
            # 旗上げ線に直交するベクトルを計算（90度回転）
            perpendicular_vector = np.array([-vector[1], vector[0]])
            # ベクトルの正規化（単位ベクトル化）
            perpendicular_unit_vector = perpendicular_vector / np.linalg.norm(perpendicular_vector)
            # 旗上げ線の長さを計算
            length = np.linalg.norm(vector)
            # 交点から旗上げ線の長さの1.5倍の距離にある点を計算
            point_plus = midpoint + perpendicular_unit_vector * length * search_range_factor
            point_minus = midpoint - perpendicular_unit_vector * length * search_range_factor

            # ROI決定時は念のため旗上げ線を長く計算する
            margin = 100
            unit_vector = vector / np.linalg.norm(vector)
            point_start_new = point_start - unit_vector * margin
            point_end_new = point_end + unit_vector * margin

            # ROI座標決定
            coord = np.vstack([point_start_new, point_end_new, point_plus, point_minus])
            roi_x1 = int(np.min(coord[:, 0]))
            roi_y1 = int(np.min(coord[:, 1]))
            roi_x2 = int(np.max(coord[:, 0]))
            roi_y2 = int(np.max(coord[:, 1]))

            # ROIが画像をはみ出す場合の調整
            if roi_x1 < 0: roi_x1 = 0
            if roi_y1 < 0: roi_y1 = 0
            if roi_x2 < 0: roi_x2 = 0
            if roi_y2 < 0: roi_y2 = 0

        # ROI
        roi = [(roi_x1, roi_y1), (roi_x2, roi_y2)]
        # 旗上げ線座標をROI系座標に変換
        leader_coord_roi = [(leader_coord[0][0]-roi_x1, leader_coord[0][1]-roi_y1), (leader_coord[1][0]-roi_x1, leader_coord[1][1]-roi_y1)]

        return roi, leader_coord_roi


    def select_circle(self, circles, roi):
        """
        検出された円のリストを処理対象の範囲(ROI)に存在するものだけに絞り込む関数

        :param circles: 検出済の円のリスト [[(中心x,中心y),半径],[(中心x,中心y),半径],・・・]
        :param roi: 処理対象の範囲 [(右上x, 右上y),(左下x, 左下y)]
        :return: ROI範囲内に絞られた円のリスト [[(中心x,中心y),半径],[(中心x,中心y),半径],・・・]
        """
        x1, y1 = roi[0]
        x2, y2 = roi[1]
        # ROIに含まれる円を選択
        circles_in_rectangle = [
            circle for circle in circles
            if x1 <= circle[0][0] <= x2 and y1 <= circle[0][1] <= y2
        ]

        # 同一の中心座標を有する円が複数検出されている可能性があるため１つに絞る
        select_circle = self.remove_duplicate_circles(circles_in_rectangle)

        # 中心座標をROIの相対座標に変換
        circles_roi = [[(center[0]-x1, center[1]-y1), radius] for center, radius in select_circle]

        return circles_roi


    def remove_duplicate_circles(self, circles):
        """
        同一中心座標を持つ円を一つに絞り込む関数

        :param circles: 検出された円のリスト [[(中心x, 中心y), 半径], ...]
        :param tolerance: 中心座標が同一とみなす許容値（ピクセル）
        :return: 絞り込まれた円のリスト [[(中心x, 中心y), 半径], ...]
        """
        # NumPy配列に変換
        circles_array = np.array(circles, dtype=object)
        centers = np.array([circle[0] for circle in circles_array])
        radii = np.array([circle[1] for circle in circles_array])

        # 全体の半径の平均を計算
        mean_radius = np.mean(radii)

        filtered_circles = []
        used_indices = set()

        for i, (center, radius) in enumerate(circles):
            if i in used_indices:
                continue

            # 同一中心座標を持つ円のインデックスを取得
            distances = np.linalg.norm(centers - np.array(center), axis=1)
            similar_indices = np.where(distances <= mean_radius/2)[0]  # 中心座標のズレが平均半径の半分以内なら同一円のグループとみなす

            # 同一中心座標を持つ円の中から、平均半径に最も近い円を選択
            similar_radii = radii[similar_indices]
            best_index = similar_indices[np.argmin(np.abs(similar_radii - mean_radius))]
            filtered_circles.append(circles[best_index])

            used_indices.update(similar_indices)

        return filtered_circles


    def cluster_circles(self, circles, leader_coord, cluster_epsilon):
        """
        円のリストをクラスタリングし旗上げ線に最も近いクラスタの円の情報を管路と判定する関数

        :param circles: クラスタリング対象の円のリスト [[(中心x,中心y),半径],[(中心x,中心y),半径],・・・]
        :param leader_coord: 旗上げ線の座標 [(始点x,始点y),(終点x,終点y)]
        :param cluster_epsilon: 2つのポイントが同じクラスタに属するための最大距離 小さすぎると本来同一クラスタでポイントが別のクラスタ属するようになる
        :return: 旗上げ線に最も近いクラスタの円のリスト(=管路と判定した情報) [[(中心x,中心y),半径],[(中心x,中心y),半径],・・・]
        """
        if cluster_epsilon == 0:
            # 検出されたすべての円を管路の集合と見なす
            return circles

        # 円の中心座標をクラスタリング
        centers = np.array([circle[0] for circle in circles])
        dbscan = DBSCAN(eps=cluster_epsilon, min_samples=1)
        cluster_labels = dbscan.fit_predict(centers)

        # 旗上げ線の中点、長さを算出
        point_s = np.array([leader_coord[0][0], leader_coord[0][1]])
        point_e = np.array([leader_coord[1][0], leader_coord[1][1]])
        target_coord = (point_s + point_e) // 2                 # 旗上げ線の中点
        length = np.linalg.norm(point_e - point_s).astype(int)  # 旗上げ線の長さ

        # 旗上げ線の中点に最も近いクラスタを決定
        distances = np.linalg.norm(centers - target_coord, axis=1)    # 旗上げ線の中点と全ての円の中心座標との距離を算出
        nearest_circle_index  = np.argmin(distances)                  # 旗上げ線の中点に最も近い円のインデックスを取得
        distances_nearest = distances[nearest_circle_index]           # 旗上げ線の中点に最も近い距離を取得
        if circles[nearest_circle_index][1]*4 <= distances_nearest:
            # 円2個分(半径x4)以上離れていたら対象なしとし、空リストを応答(旗上げ線から離れ過ぎなら対応する管路ではないと判断)
            return []
        nearest_cluster_label = cluster_labels[nearest_circle_index]  # 旗上げ線の中点に最も近いクラスタのラベルを取得
        # 旗上げ線の中点に最も近いクラスタの円情報をリストに取得
        nearest_cluster_circles = [
            circles[i] for i, label in enumerate(cluster_labels)
            if label == nearest_cluster_label
        ]

        return nearest_cluster_circles


    def rotate_coord(self, leader_coord, circles):
        """
        旗上げ線が水平となるように円の座標を回転させる関数

        :param base_line: 旗上げ線の座標 [(始点x, 始点y),(終点x,終点y)]
        :param circles: 管路と判定された円のリスト [[(中心x, 中心y), 半径], ...]
        :return: 回転後の座標に更新した円のリスト [[(中心x, 中心y), 半径], ...]
        """
        def convert_rotate_coord(x, y, matrix):
            """
            回転後の座標を返す関数
            """
            point = np.array([[x], [y], [1]])
            rotate_point = np.dot(matrix, point).astype(int)
            return rotate_point[0][0], rotate_point[1][0]

        # 回転行列作成
        x1, y1, x2, y2 = leader_coord[0][0], leader_coord[0][1], leader_coord[1][0], leader_coord[1][1]
        angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
        center = ((x2 + x1) // 2, (y2 + y1) // 2)
        rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)

        # 回転後のleader_coordのy座標を算出
        rotate_line_x1, rotate_line_y1 = convert_rotate_coord(x1, y1, rotation_matrix)

        # 回転後の円の中心座標を算出
        rotate_circles = []
        for (x, y), r in circles:
            rotate_circle_x, rotate_circle_y = convert_rotate_coord(x, y, rotation_matrix)
            rotate_circles.append([(rotate_circle_x, rotate_circle_y), r])

        # 回転後の円群の重心を算出
        circles_array = np.array(rotate_circles, dtype=object)
        rotate_centers = np.array([circle[0] for circle in circles_array])
        rotate_centroid = np.mean(rotate_centers, axis=0)
        rotate_centroid = tuple(rotate_centroid.astype(int))

        if rotate_line_y1 < rotate_centroid[1]:
            # 回転した結果base_lineが円群の重心よりも上にあればさらに180度回転
            rotation_matrix = cv2.getRotationMatrix2D(center, angle+180, 1.0)
            rotate_circles = []
            for (x, y), r in circles:
                rotate_circle_x, rotate_circle_y = convert_rotate_coord(x, y, rotation_matrix)
                rotate_circles.append([(rotate_circle_x, rotate_circle_y), r])

        return rotate_circles


    def count_circles(self, circles, count_columns_threshold):
        """
        管路と判定された円をカウントする関数
        条数(列数)は円の欠落もカウントした全行数すべて参照した最大値とする。
        段数(行数)は半径以上のズレが無い限り同一の行とみなす。

        :param circles: 管路と判定された円のリスト [[(中心x, 中心y), 半径], ...]
        :param count_columns_threshold: 管路の条数算出処理の端数を切り上げる閾値
        :return: 管路の条数、段数
        """
        # 例）縦方向：段数、横方向：条数
        #
        # 　｜１２３４            　｜１２３４５６７        　｜１２３４５
        # －＋－－－－            －＋－－－－－－－        －＋－－－－－
        # ２｜〇　〇〇            ２｜　〇　〇　　〇        ２｜ 〇　　〇
        # １｜〇〇〇　            １｜〇　〇　〇〇　        １｜〇〇〇〇
        # 条数：4、段数：2        条数：7、段数：2          条数：5※、段数：2
        #                                                 ※条数は4.5程度であるが切り上げて5とする。切り上げる小数点部はcolumns_thresholdの設定による。

        # 中心座標のみを抽出
        centers = [circle[0] for circle in circles]
        # x座標とy座標のリストを作成
        x_coords = [center[0] for center in centers]
        y_coords = [center[1] for center in centers]
        # 平均半径
        avg_radius = math.ceil(sum(circle[1] for circle in circles) / len(circles))

        # y座標でソートし、各行の円を特定
        sorted_centers = sorted(centers, key=lambda c: c[1])
        rows = []
        current_row = [sorted_centers[0]]
        for center in sorted_centers[1:]:
            if abs(center[1] - current_row[0][1]) < avg_radius:
                # y方向ズレが半径未満なら同一段数とみなす
                current_row.append(center)
            else:
                rows.append(current_row)
                current_row = [center]
        rows.append(current_row)
        # 段数を決定
        num_rows = len(rows)

        # 条数を計算
        min_x = min(x_coords)
        max_x = max(x_coords)
        x_diff = max_x - min_x
        # 中心座標の最小x座標と最大x座標の差にいくつ円が入るかを推定
        estimated_columns = (x_diff / (avg_radius * 2)) + 1  # 中心座標で計算しており、両端の半径x2分(円１つ分)が加味されていないので最後に+1する

        # 段毎x方向のズレを考慮して、条数を決定
        if estimated_columns - math.floor(estimated_columns) < count_columns_threshold:
            # estimated_columnsの小数部がrounding_threshold未満なら切り捨て
            num_columns =  math.floor(estimated_columns)
        else:
            # estimated_columnsの小数部がrounding_threshold以上なら切り上げ
            num_columns =  math.ceil(estimated_columns)

        return num_columns, num_rows
