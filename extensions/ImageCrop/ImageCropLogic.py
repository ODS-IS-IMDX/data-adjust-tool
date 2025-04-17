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
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageCropLogic(FlexibleRasterVectorLogic):
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

        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def __call__(self, byte_data, attribute, properties):
        """
        指定された座標で画像を切り出し、切り出した画像データを返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - fsf_imgae_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_imgae_dst: string
                処理した画像を出力するFieldSetFileの項目名が指定されます。

            - fsf_crop_coords: string
                切り出し座標が設定されたFieldSetFileの項目名が指定されます。

            - crop_coord_start: string
                切り出し座標の矩形左上座標が設定されます。

            - crop_coord_end: string
                切り出し座標の矩形右下座標が設定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                切り出された画像データのバイト列です。
                
            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入出力項目取得
        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_image_dst = properties.get('fsf_image_dst', 'content')
        fsf_crop_coords = properties.get('fsf_crop_coords', 'crop_coords')
        crop_coord_start = properties.get('crop_coord_start', '')
        crop_coord_end = properties.get('crop_coord_end', '')

        # 切り出し元画像データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image_src = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image_src = pickle.loads(byte_data)

        # 切り出し座標データ取得
        if crop_coord_start != '' and crop_coord_end != '':
            # プロパティ値の切り出し座標を使用（座標変換とチェック）
            try:
                crop_coords = [self.str_to_coord(crop_coord_start), self.str_to_coord(crop_coord_end)]
            except:
                raise ValueError(f'crop_coord_startまたはcrop_coord_endに無効な値が設定されています: \
                                crop_coord_start=({properties.get("crop_coord_start")}), crop_coord_start=({properties.get("crop_coord_start")})')
        elif type(byte_data) is pandas.core.series.Series:
            # FSFの切り出し座標を使用
            try:
                crop_coords = pickle.loads(byte_data[fsf_crop_coords])
            except:
                raise ValueError(f'fsf_crop_coordsに無効な値が設定されています: {properties.get("fsf_crop_coords")}')
        else:
            raise ValueError(f'crop_coord_startまたはcrop_coord_endに無効な値が設定されています: \
                             crop_coord_start=({properties.get("crop_coord_start")}), crop_coord_start=({properties.get("crop_coord_start")})')

        # 切り出し座標正規化とチェック
        crop_coords = self.normalize_coordinates(crop_coords)

        # 画像切り出し
        image_cropped_list = []
        for (x1, y1), (x2, y2) in crop_coords:
            image_cropped_list.append(image_src[y1:y2, x1:x2])

        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            if len(image_cropped_list) == 1:
                # 切り出し座標が1つのみの場合は、fsf_image_dstに指定された項目に出力
                byte_data[fsf_image_dst] = pickle.dumps(image_cropped_list[0])
            else:
                # 切り出し座標が2つ以上の場合は、fsf_image_dstに'_0'のように連番を付与した項目に出力
                for i, image_cropped in enumerate(image_cropped_list):
                    byte_data[fsf_image_dst + f'_{i}'] = pickle.dumps(image_cropped)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(image_cropped_list[0])

        return new_byte_data, attribute


    def str_to_coord(self, coord_str):
        '''
        文字列を座標のタプルに変換
        '''
        x, y = coord_str.split(',')
        return (int(x), int(y))


    def normalize_coordinates(self, coords):
        '''
        座標の正規化と形式チェック
        '''
        # 1層の場合、2層のネストに変換
        if isinstance(coords[0], tuple):
            # すべての要素がタプルで、各タプルが2つの要素を持つことを確認
            if all(isinstance(p, tuple) and len(p) == 2 for p in coords):
                # [(x1,y1),(x2,y2)] → [[(x1,y1),(x2,y2)]] にlistをネストする
                return [coords]
            raise ValueError("指定された切り出し座標のフォーマットが[(x1,y1),(x2,y2)]形式ではありません。")

        # 2層のネストの場合、形式を検証
        if all(isinstance(group, list) and len(group) == 2 and
                all(isinstance(p, tuple) and len(p) == 2 for p in group) 
                for group in coords):
            # [[(x1,y1),(x2,y2)],・・・]の形式なのでチェックOK
            return coords
        else:
            raise ValueError("指定された切り出し座標のフォーマットが[[(x1,y1),(x2,y2)],・・・]形式ではありません。")
