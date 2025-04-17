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


class ImageProcessorLogic(FlexibleRasterVectorLogic):
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
        指定された処理方法に基づいて、画像の加工を行います。

        **入力データの形式:**
        
        - FieldSetFileのそれぞれの項目に前景または背景画像が格納されています。
        - プロパティから前景画像のFieldSetFileの項目名、背景画像のFieldSetFileの項目名を指定します。
        - 両方の画像はNumPy配列形式で渡され、色空間はRGBまたはRGBAを想定しています。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB RGBA である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - processing_method: string
                適用する処理方法が設定されます。

            - mask_threshold: int
                マスキング処理のしきい値（0～255）が設定されます。

            - alpha_threshold: int
                透過処理に使用するアルファしきい値（0～255）が設定されます。

            - alpha: float
                ブレンディングや透過処理に使用するアルファ値（0.0～1.0）が設定されます。

            - blend_mode: string
                ブレンディング処理のモードが設定されます。
                
            - fsf_foreground: string
                処理する前景画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_background: string
                処理する後景画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_image_dst: string
                処理した画像を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                画像の加工が行われた結果画像のバイト列です。

            dict
                更新された属性情報を含む辞書です。ColorSpace は 処理結果に応じてGRAYSCALE、RGB、RGBAのいずれかに設定されます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入出力項目取得
        fsf_foreground = properties.get('fsf_foreground', 'foreground')
        fsf_background = properties.get('fsf_background', 'background')
        fsf_image_dst = properties.get('fsf_image_dst', 'content')

        if type(byte_data) is pandas.core.series.Series:
            try:
                foreground = pickle.loads(byte_data[fsf_foreground])
                background = pickle.loads(byte_data[fsf_background])
            except:
                raise ValueError("fsf_foreground または fsf_backgroundに無効な値が設定されています。")
        else:
            img_dict = pickle.loads(byte_data)
            try:
                foreground = img_dict.get('foreground', None)
                background = img_dict.get('background', None)
            except Exception:
                raise ValueError("データ形式が不正です。辞書形式で 'foreground' または 'background' のキーが含まれている必要があります。")

        if background is None:
            raise ValueError("背景画像 (background) が設定されていません。")

        method = properties.get('processing_method', 'Masking')
        if method == 'Masking':
            mask = foreground if foreground is not None else np.zeros(background.shape[:2], dtype=np.uint8)
            mask = cv2.resize(mask, (background.shape[1], background.shape[0]))
            if len(mask.shape) == 3:
                mask = cv2.cvtColor(mask, cv2.COLOR_RGB2GRAY)
                
            try:    
                mask_threshold = int(properties.get('mask_threshold', 127))
            except:
                raise ValueError(f"mask_thresholdに無効な値が設定されています。: {properties.get('mask_threshold', 127)}")
            
            if not 0 <= mask_threshold <= 255:
                    raise ValueError(f"mask_thresholdは0以上255以下の値を入力してください。: {mask_threshold}")
            
            _, mask = cv2.threshold(mask, mask_threshold, 255, cv2.THRESH_BINARY)
            processed_image = cv2.bitwise_and(background, background, mask=mask)

        elif method == 'Transparency':
            if foreground is None:
                processed_image = background
            else:
                foreground = cv2.resize(foreground, (background.shape[1], background.shape[0]))
                try:
                    if background.shape[2] == 3:
                        background = cv2.cvtColor(background, cv2.COLOR_RGB2RGBA)
                    if foreground.shape[2] == 3:
                        foreground = cv2.cvtColor(foreground, cv2.COLOR_RGB2RGBA)
                except:
                    raise ValueError("前景画像(foreground)と背景画像(background)はRGBである必要があります。")

                # アルファ値を取得する
                alpha = foreground[:, :, 3] / 255.0

                # アルファ値がしきい値以上の部分を不透明として設定
                try:
                    alpha_threshold = int(properties.get('alpha_threshold', 127))
                except:
                    raise ValueError(f"alpha_thresholdに無効な値が設定されています。: {properties.get('alpha_threshold', 127)}")
                
                if not 0 <= alpha_threshold <= 255:
                    raise ValueError(f"alpha_thresholdは0以上255以下の値を入力してください。: {alpha_threshold}")

                alpha[alpha >= (alpha_threshold / 255.0)] = 1.0 # 0 ~ 255を0 ~ 1.0に正規化
                alpha[alpha < (alpha_threshold / 255.0)] = 0

                # 画像と次元を合わせる
                alpha = alpha[:, :, np.newaxis]
                
                # 新しいアルファチャンネルを適用して画像を更新
                processed_image = background * (1.0 - alpha) + foreground * alpha
                processed_image[:, :, 3] = np.maximum(background[:, :, 3], foreground[:, :, 3])
                processed_image = processed_image.astype(np.uint8)

        elif method == 'Blending':
            if foreground is None:
                processed_image = background
            else:
                foreground = cv2.resize(foreground, (background.shape[1], background.shape[0]))
                blend_mode = properties.get('blend_mode', 'weighted_average')

                try:
                    alpha = float(properties.get('alpha', 0.5))
                except:
                    raise ValueError(f"alphaに無効な値が設定されています。: {properties.get('alpha', 127)}")
                
                if not 0.0 <= alpha <= 1.0:
                    raise ValueError(f"alphaは0以上1.0以下の値を入力してください。: {alpha}")
                
                if blend_mode == 'weighted_average':
                    processed_image = cv2.addWeighted(background, 1 - alpha, foreground, alpha, 0)
                elif blend_mode == 'maximum':
                    processed_image = np.maximum(background, foreground)
                elif blend_mode == 'minimum':
                    processed_image = np.minimum(background, foreground)
                else:
                    raise ValueError('無効なブレンドモードです')

        else:
            raise ValueError('無効な処理方法が選択されています')

        new_color_space = 'GRAYSCALE' if len(processed_image.shape) == 2 else ('RGBA' if processed_image.shape[2] == 4 else 'RGB')

        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_image_dst] = pickle.dumps(processed_image)
            byte_data['color_space'] = new_color_space
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(processed_image)
            
        attribute['ColorSpace'] = new_color_space

        return new_byte_data, attribute