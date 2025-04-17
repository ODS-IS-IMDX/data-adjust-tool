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
from importlib import import_module

# 外部ライブラリの動的インポート
cv2 = import_module("cv2")
np = import_module("numpy")
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageConnectedRegionDetectionLogic(FlexibleRasterVectorLogic):
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
        連結領域を検出し、座標データを返します。

        Parameters
        ----------
        byte_data: 
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute: 
            画像データに関連する属性情報の辞書です。ColorSpace が BINARY である必要があります。
        properties: 
            プロパティ設定を含む辞書です。
            - min_area: int
                検出する連結領域の最小面積(縦pixel x 横pixel)が設定されます。

            - max_area: int
                検出する連結領域の最大面積(縦pixel x 横pixel)が設定されます。

            - sort_largest: bool
                応答する連結領域座標の順番を矩形面積の大きい順か小さい順かが設定されます。

            - limit_count: int
                応答する連結領域座標の数が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_detection_data: string
                検出した連結領域座標を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                検出された連結領域座標のバイト列です。
                
            - attribute : dict
                更新された属性情報です。元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入出力項目取得
        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_detection_data = properties.get('fsf_detection_data', 'content')

        # 入出力情報データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        # 画像の色空間チェック
        color_space = attribute['ColorSpace']
        if color_space != 'BINARY':
            raise Exception('BINARY以外のColorSpaceが設定されています')

        # プロパティ値取得
        # 検出する連結領域の最小面積
        try:
            self.min_area = int(properties.get('min_area', 0))
        except:
            raise ValueError(f'無効なmin_areaが選択されています: {properties.get("min_area")}')
        if self.min_area < 0:
            raise Exception(f'min_areaは0以上の整数を設定する必要があります: {properties.get("min_area")}')
        # 検出する連結領域の最大面積
        try:
            self.max_area = int(properties.get('max_area', -1))
        except:
            raise ValueError(f'無効なmax_areaが選択されています: {properties.get("max_area")}')
        if self.max_area < -1:
            raise Exception(f'max_areaは-1以上の整数を設定する必要があります: {properties.get("max_area")}')
        if self.max_area != -1:
            if self.max_area <= self.min_area:
                raise Exception(f'min_areaとmax_areaの大小関係はmin_area < max_areaである必要があります: min_area={properties.get("min_area")}, max_area={properties.get("max_area")}')
        # 応答する連結領域座標の順番
        sort_largest = properties.get('sort_largest', 'true')  # リスト選択式ではあるがチェック処理を実装しておく
        if sort_largest.lower() == 'true':
            self.sort_largest = True
        elif sort_largest.lower() == 'false':
            self.sort_largest = False
        else:
            raise Exception(f'sort_largestはTrueまたはFalseである必要があります: {properties.get("sort_largest")}')
        # 応答する連結領域座標の数
        try:
            self.limit_count = int(properties.get('limit_count', -1))
        except:
            raise ValueError(f'無効なlimit_countが選択されています: {properties.get("limit_count")}')
        if self.limit_count < -1:
            raise Exception(f'limit_countは-1以上の整数を設定する必要があります: {properties.get("limit_count")}')

        # 連結領域取得（≒物体検出/ラベリング）
        num_labels, labels, stats, centroids = cv2.connectedComponentsWithStats(image)
        # 連結領域のフィルタリング
        num_labels, labels, stats, centroids = self.filter_components(num_labels, labels, stats, centroids)

        # 応答形式に変換
        transformed_list = self.convert_coord(stats)

        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_detection_data] = pickle.dumps(transformed_list)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(transformed_list)

        return new_byte_data, attribute

    def filter_components(self, num_labels, labels, stats, centroids):
        '''
        検出した連結領域情報を以下の２つの観点でフィルタリングする
          ・面積が指定範囲内
          ・面積大きい順にＮ個 or 小さい順位にＮ個
        '''
        valid_components = []  # 条件を満たすラベルを保存(label, area, stats, centroids)
        
        for label in range(1, num_labels):  # labels=0は背景なのでスキップ
            area = stats[label, cv2.CC_STAT_AREA]
            if self.is_area_within_range(area):
                valid_components.append((label, area, stats[label], centroids[label]))
        
        # 面積でソートし、必要に応じて上位N個に絞り込む
        if valid_components:
            valid_components.sort(key=lambda x: x[1], reverse=self.sort_largest)
            if self.limit_count != -1:
                valid_components = valid_components[:self.limit_count]
        
        # 有効なラベルのリストを作成
        valid_labels = [comp[0] for comp in valid_components]
        
        # labelsの更新（ラベル番号を振り直す）
        new_labels = np.zeros_like(labels)
        for new_label, old_label in enumerate(valid_labels, start=1):
            new_labels[labels == old_label] = new_label
        
        # 統計情報とcentroidsの更新
        filtered_stats = [stats[0]]  # 背景の情報
        filtered_centroids = [centroids[0]]  # 背景の情報
        
        for comp in valid_components:
            filtered_stats.append(comp[2])
            filtered_centroids.append(comp[3])
        
        # numpy arrayに変換
        filtered_stats = np.array(filtered_stats)
        filtered_centroids = np.array(filtered_centroids)
        
        # フィルタリング後のラベル数（背景を含む）
        new_num_labels = len(valid_labels) + 1
        
        return new_num_labels, new_labels, filtered_stats, filtered_centroids

    def is_area_within_range(self, area):
        '''
        検出した輪郭の面積が指定範囲内かを判定する
        '''
        if self.max_area == -1:
            # 上限なし
            if self.min_area <= area:
                return True
        else:
            # 範囲指定
            if self.min_area <= area <= self.max_area:
                return True

    def convert_coord(self, stats):
        '''
        検出した輪郭が連結領域の形状に近いかを判定する
        '''
        converted_coord = []
        # インデックス0は背景なのでスキップ
        for label in range(1, len(stats)):
            # 左上座標
            left = stats[label, cv2.CC_STAT_LEFT]
            top = stats[label, cv2.CC_STAT_TOP]
            
            # 幅と高さを使って右下座標を計算
            right = left + stats[label, cv2.CC_STAT_WIDTH]
            bottom = top + stats[label, cv2.CC_STAT_HEIGHT]
            
            converted_coord.append([(left, top), (right, bottom)])
    
        return converted_coord
