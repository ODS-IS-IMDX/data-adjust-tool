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
import os
from importlib import import_module

# 外部ライブラリの動的インポート
cv2 = import_module("cv2")
np = import_module("numpy")
pandas = import_module("pandas")
Image = import_module('PIL.Image')
DBSCAN = import_module("sklearn.cluster").DBSCAN

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic

class ImageMergeLogic(FlexibleRasterVectorLogic):
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

        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def __call__(self, byte_data, attribute, properties):
        """
        分割された画像を結合し、座標を結合後の画像サイズに適用します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - color_space: string
                結合後の画像のカラースペースが設定されます。

            - offset_coordinate_x: string
                使用するオフセットX座標のFieldSetFileの項目名が指定されます。

            - offset_coordinate_y: string
                使用するオフセットY座標のFieldSetFileの項目名が指定されます。

            - circle_cluster_epsilon: int
                分割時の重なり領域で複数検出した円の中心座標から同一の円とみなす距離(px)が設定されます。

            - line_cluster_epsilon: int
                分割時の重なり領域で複数検出した線分の始点、終点、中点から同一の線分とみなす距離(px)が設定されます。
            
            - fsf_image_src: string
                結合する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_circle_coordinate: string
                円の座標が格納されているFieldSetFileの項目名が指定されます。

            - fsf_line_coordinate: string
                線分の座標が格納されているFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                結合された画像と座標のバイト列です。

            dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入出力項目取得
        offset_coordinate_x = properties.get('offset_coordinate_x', 'offset_coordinate_x')
        offset_coordinate_y = properties.get('offset_coordinate_y', 'offset_coordinate_y')
        fsf_image_src = properties.get('fsf_image_src', 'content')
        new_color_space = properties.get('color_space', 'RGB')
        # デフォルト値は空白
        fsf_circle_coordinate = properties.get('fsf_circle_coordinate', '')
        fsf_line_coordinate = properties.get('fsf_line_coordinate', '')

        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            # オフセットX座標
            try:
                offset_x_list = byte_data[offset_coordinate_x]
            except:
                raise ValueError(f'offset_coordinate_xに無効な値が設定されています: {properties.get("offset_coordinate_x")}')
            # オフセットY座標
            try:
                offset_y_list = byte_data[offset_coordinate_y]
            except:
                raise ValueError(f'offset_coordinate_yに無効な値が設定されています: {properties.get("offset_coordinate_y")}')
            # 分割画像
            try:
                split_image_list = byte_data[fsf_image_src]
                split_image_list = [pickle.loads(split_image) for split_image in split_image_list]
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
            # 円情報
            if fsf_circle_coordinate != '':
                try:
                    circle_coordinate_list = byte_data[fsf_circle_coordinate]
                    circle_coordinate_list = [pickle.loads(circle_coordinate) for circle_coordinate in circle_coordinate_list]
                except:
                    raise ValueError(f'fsf_circle_coordinateに無効な値が設定されています: {properties.get("fsf_circle_coordinate")}')
            # 線分情報
            if fsf_line_coordinate != '':
                try:
                    line_coordinate_list = byte_data[fsf_line_coordinate]
                    line_coordinate_list = [pickle.loads(line_coordinate) for line_coordinate in line_coordinate_list]
                except:
                    raise ValueError(f'fsf_line_coordinateに無効な値が設定されています: {properties.get("fsf_line_coordinate")}')      
        else:
            raise Exception('入力データはFieldSetFile形式である必要があります')
        
        # 円座標の処理
        if fsf_circle_coordinate != '':
            try:
                circle_cluster_epsilon = int(properties.get('circle_cluster_epsilon', 0))
            except:
                raise ValueError(f'circle_cluster_epsilonに無効な値が設定されています: {properties.get("circle_cluster_epsilon")}')
            if circle_cluster_epsilon < 0:
                raise ValueError(f'circle_cluster_epsilonは0以上の値を入力してください: {properties.get("circle_cluster_epsilon")}')
            # 円の座標をクラスタリング
            circle_averages_coordinate_list = self.circle_process(circle_coordinate_list, offset_x_list, offset_y_list, circle_cluster_epsilon)
            byte_data[fsf_circle_coordinate] = pickle.dumps(circle_averages_coordinate_list)

        # 線分座標の処理
        if fsf_line_coordinate != '':
            try:
                line_cluster_epsilon = int(properties.get('line_cluster_epsilon', 0))
            except:
                raise ValueError(f'line_cluster_epsilonに無効な値が設定されています: {properties.get("line_cluster_epsilon")}')
            if line_cluster_epsilon < 0:
                raise ValueError(f'line_cluster_epsilonは0以上の値を入力してください: {properties.get("line_cluster_epsilon")}')
            # 線分の座標をクラスタリング
            line_averages_coordinate_list = self.line_process(line_coordinate_list, offset_x_list, offset_y_list, line_cluster_epsilon)
            byte_data[fsf_line_coordinate] = pickle.dumps(line_averages_coordinate_list)

        # 属性から値を取得
        height_division_num = int(attribute['height_division_num'])
        width_division_num = int(attribute['width_division_num'])
        overlap_region = int(attribute['overlap_region'])

        # リストを2次元配列に変換
        split_image_list = np.array(split_image_list, dtype=object).reshape(height_division_num, width_division_num)
        merge_image_list = []
        for row in range(height_division_num):
            for column in range(width_division_num):
                split_image = split_image_list[row][column]
                # 全体画像の右下
                if column == (width_division_num - 1) and row == (height_division_num - 1):
                    width = split_image.shape[1]
                    height = split_image.shape[0]
                # 全体画像の右側
                elif column == (width_division_num - 1):
                    width = split_image.shape[1]
                    height = split_image.shape[0] - overlap_region
                # 全体画像の下側
                elif row == (height_division_num - 1):
                    width = split_image.shape[1] - overlap_region
                    height = split_image.shape[0]
                else:
                    width = split_image.shape[1] - overlap_region
                    height = split_image.shape[0] - overlap_region
                
                # overlap_regionを削除
                merge_image_list.append(split_image[0:height,0:width])

        # 行ごとに横に結合
        row_image = [np.hstack(merge_image_list[i * width_division_num:(i + 1) * width_division_num]) for i in range(height_division_num)]
        # 横に結合した行を縦に結合
        merged_image = np.vstack(row_image)

        byte_data['content'] = pickle.dumps(merged_image)
        byte_data['color_space'] = new_color_space
        byte_data['Findex'] = 0
        new_byte_data = byte_data

        # 結合後の画像のカラースペースを設定
        attribute['ColorSpace'] = new_color_space

        return new_byte_data, attribute
    
    def remove_duplicates_process(self, merged_coordinate_list):
        seen = set()
        unique_list = []
        # 重複を削除しつつ順序を維持
        for item in merged_coordinate_list:
            if item not in seen:
                unique_list.append(item)
                seen.add(item)

        return np.array(unique_list)
    
    def dbscan_process(self, unique_item, eps):
        dbscan = DBSCAN(eps=eps, min_samples=1)
        labels = dbscan.fit_predict(unique_item)
        group_averages = []
        unique_labels = set(labels)
        # ラベルが-1の場合、それはノイズなので無視
        for label in unique_labels:
            if label != -1:
                group_points = unique_item[labels == label]
                # グループ内の平均を計算
                group_average = np.mean(group_points, axis=0)
                group_averages.append(group_average.astype(int))
        
        return np.array(group_averages)
    
    def circle_process(self, circle_coordinate_list, offset_x_list, offset_y_list, eps):
        # それぞれの円の座標を結合後の座標に適用する
        circle_merged_coordinate_list = []
        for i, circle_coordinate in enumerate(circle_coordinate_list):
            for circle in circle_coordinate:
                x, y = circle[0]
                radius = circle[1]
                circle_x = offset_x_list[i] + x
                circle_y = offset_y_list[i] + y
                circle_merged_coordinate_list.append((circle_x, circle_y, radius))
        # 円の重複をなくす
        unique_circle = self.remove_duplicates_process(circle_merged_coordinate_list)

        # 近い円座標をクラスタリング
        circle_averages = self.dbscan_process(unique_circle, eps)
        # 元の形に変形
        circle_averages = [[(x[0], x[1]), x[2]]for x in circle_averages]

        return circle_averages
    
    def line_process(self, line_coordinate_list, offset_x_list, offset_y_list, eps):
        # それぞれの線分の座標を結合後の座標に適用する
        line_merged_coordinate_list = []
        line_list_tmp = []
        for i, line_coordinate in enumerate(line_coordinate_list):
            for line in line_coordinate:
                for line_point in line:
                    # 線分の太さ情報は使わない
                    if isinstance(line_point, tuple):
                        line_list_tmp.append(line_point)

                x1 = line_list_tmp[0][0] + offset_x_list[i]
                y1 = line_list_tmp[0][1] + offset_y_list[i]
                x2 = line_list_tmp[1][0] + offset_x_list[i]
                y2 = line_list_tmp[1][1] + offset_y_list[i]
                mid_point_x = (x1 + x2) // 2
                mid_point_y = (y1 + y2) // 2
                line_merged_coordinate_list.append((x1, y1, x2, y2, mid_point_x, mid_point_y))
                line_list_tmp = []
        # 線分の重複をなくす
        unique_line = self.remove_duplicates_process(line_merged_coordinate_list)

        # 近い線分座標をクラスタリング
        line_averages = self.dbscan_process(unique_line, eps)
        # 元の形に変形
        line_averages = [[(x[0], x[1]), (x[2], x[3])] for x in line_averages]

        return line_averages