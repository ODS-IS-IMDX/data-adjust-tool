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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class ImageShapeCorrection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageShapeCorrection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像の形状補正を行うプロセッサクラス。

    **処理概要:**

    - 入力データを受け取り、指定された方法で形状補正を行います。
    - 形状補正は、Rotation（回転）、Scaling（拡大縮小）、Translation（移動）、Shearing（せん断）を選択できます。
    - 変換結果はバイト列形式で返されます。

    **注意事項:**

    - カラースペースは、RGB、GRAYSCALE、BINARYのいずれかである必要があります。異なる場合はエラーとなります。


    タグ
    --------------------------------
    * image processing
    * shape correction
    * rotation
    * scaling
    * translation
    * shearing

    プロパティ
    --------------------------------
    **Transformation Method**

    **必須入力  リスト入力**

    - 形状補正の方法を指定します。

      #. Rotation: 回転を行います。
      #. Scaling: 拡大縮小を行います。
      #. Translation: 移動を行います。
      #. Shearing: せん断を行います。

    - 初期値は Rotationです。

    **Rotation Angle**

    **任意入力  直接入力**

    - 回転時の回転角度を指定します。（単位：度）
    - 画像は指定された角度で回転する。正の値は反時計回り、負の値は時計回りで回転します。

        + 45：反時計回りに45度回転
        + -90：時計回りに90度回転
        + 360：1周回転（元の位置）
        + 400：反時計回りに400度（360度+40度）

    - 初期値は 0 です。

    **Rotation Scale**

    **任意入力  直接入力**

    - 回転時の拡大縮小スケールを指定します。
    - 画像は回転と同時に指定されたスケールで拡大縮小します。
    - 初期値は 1.0 です。

    **Rotation Center**

    **任意入力  直接入力**

    - 回転時の回転中心座標を指定します。
    - 画像は指定された座標を中心に回転します。
    - 初期値は 0,0 です。

    **Scale X**

    **任意入力  直接入力**

    - 拡大縮小の横方向のスケールを指定します。
    - 画像は指定されたスケールで横方向に拡大縮小します。
    - 初期値は 1.0 です。

    **Scale Y**

    **任意入力  直接入力**

    - 拡大縮小の縦方向のスケールを指定します。
    - 画像は指定されたスケールで縦方向に拡大縮小します。
    - 初期値は 1.0 です。

    **Interpolation Method**

    **任意入力  リスト入力**

    - 拡大縮小における補間方法を指定します。
    
      #. LINEAR: 4つの近傍ピクセルの値を線形に補間します。バランスの取れた方法です（バイリニア補間）。
      #. NEAREST: 最も近いピクセルの値をそのまま使用します。計算が速いですが、画像がギザギザになることがあります（最近傍補間）。
      #. CUBIC: 16個の近傍ピクセルを使用して補間します。計算は遅いですが、より滑らかな画像が得られます（バイキュービック補間。
      #. AREA: ピクセル領域の関係を利用し、特に画像を縮小する際に有効で、モアレを避けることができます（リサンプリング）。
      #. LANCZOS4: 64個の近傍ピクセルを使用し、高品質な補間が可能です（Lanczos法の補間）。

    - 初期値はLINEARです。

    **Translation X**

    **任意入力  直接入力**

    - 移動させる横方向の移動量を指定します。
    - 画像は指定されたピクセル分で横方向に移動します。
    - 初期値は 0 です。

    **Translation Y**

    **任意入力  直接入力**

    - 移動させる縦方向の移動量を指定します。
    - 画像は指定されたピクセル分で縦方向に移動します。
    - 初期値は 0 です。

    **Shear X**

    **任意入力  直接入力**

    - せん断させる横方向の強さを指定します。
    - 画像は指定された強さで左上を原点として横方向に引き延ばされます。
    - 初期値は 0.0 です。

    **Shear Y**

    **任意入力  直接入力**

    - せん断させる縦方向の強さを指定します。
    - 画像は指定された強さで左上を原点として縦方向に引き延ばされます。
    - 初期値は 0.0 です。

    リレーションシップ
    --------------------------------
    * success: 形状補正処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGB、GRAYSCALE、BINARYである必要があります。

    Writes Attributes
    --------------------------------
    * なし

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB、BINARY、GRAYSCALE形式)である必要があります。
    
    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Image Shape Correction: Rotation, Scaling, Translation, and Shearing'

    transformation_method = PropertyDescriptor(
        name="Transformation Method",
        description="形状補正の方法を指定します。\
                        Rotation: 回転を行います。\
                        Scaling: 拡大縮小を行います。\
                        Translation: 移動を行います。\
                        Shearing: せん断を行います。\
                    初期値は Rotationです。",
        allowable_values=["Rotation", "Scaling", "Translation", "Shearing"],
        required=True,
        default_value="Rotation",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    angle = PropertyDescriptor(
        name="Rotation Angle",
        description="回転時の回転角度を指定します。\
                    画像は指定された角度で反時計回りに回転します。\
                    初期値は 0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0",
        sensitive=False
    )

    scale = PropertyDescriptor(
        name="Rotation Scale",
        description="回転時の拡大縮小スケールを指定します。\
                    画像は回転と同時に指定されたスケールで拡大縮小します。\
                    初期値は 1.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    center = PropertyDescriptor(
        name = 'Rotation Center',
        description="回転時の回転中心座標を指定します。\
                    画像は指定された座標を中心に回転します。\
                    初期値は 0,0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0,0",
        sensitive=False
    )

    scale_x = PropertyDescriptor(
        name="Scale X",
        description="拡大縮小の横方向のスケールを指定します。\
                    画像は指定されたスケールで横方向に拡大縮小します。\
                    初期値は 1.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    scale_y = PropertyDescriptor(
        name="Scale Y",
        description="拡大縮小の縦方向のスケールを指定します。\
                    画像は指定されたスケールで縦方向に拡大縮小します。\
                    初期値は 1.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    interpolation = PropertyDescriptor(
        name="Interpolation Method",
        description="拡大縮小における補間方法を指定します。\
                        LINEAR: 4つの近傍ピクセルの値を線形に補間します。バランスの取れた方法です（バイリニア補間）。\
                        NEAREST: 最も近いピクセルの値をそのまま使用します。計算が速いですが、画像がギザギザになることがあります（最近傍補間）。\
                        CUBIC: 16個の近傍ピクセルを使用して補間します。計算は遅いですが、より滑らかな画像が得られます（バイキュービック補間）。\
                        AREA: ピクセル領域の関係を利用し、特に画像を縮小する際に有効で、モアレを避けることができます（リサンプリング）。\
                        LANCZOS4: 64個の近傍ピクセルを使用し、高品質な補間が可能です（Lanczos法の補間）。\
                    初期値はLINEARです。",
        allowable_values=["LINEAR", "NEAREST", "CUBIC", "AREA", "LANCZOS4"],
        required=False,
        default_value="LINEAR",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    tx = PropertyDescriptor(
        name="Translation X",
        description="移動させる横方向の移動量を指定します。\
                    画像は指定されたピクセル分で横方向に移動します。\
                    初期値は 0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0",
        sensitive=False
    )

    ty = PropertyDescriptor(
        name = "Translation Y",
        description = "移動させる縦方向の移動量を指定します。\
                    画像は指定されたピクセル分で縦方向に移動します。\
                    初期値は 0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required = False,
        default_value = "0",
        sensitive=False
    )

    shear_x = PropertyDescriptor(
        name="Shear X",
        description="せん断させる横方向の強さを指定します。\
                    画像は指定された強さで左上を原点として横方向に引き延ばされます。\
                    初期値は 0.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0.0",
        sensitive=False
    )

    shear_y = PropertyDescriptor(
        name="Shear Y",
        description="せん断させる縦方向の強さを指定します。\
                    画像は指定された強さで左上を原点として縦方向に引き延ばされます。\
                    初期値は 0.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0.0",
        sensitive=False
    )

    property_descriptors = [
        transformation_method,
        angle, scale, center,
        scale_x, scale_y, interpolation,
        tx, ty,
        shear_x, shear_y
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageShapeCorrectionLogic.ImageShapeCorrectionLogic')
    def __init__(self, **kwargs):
        pass
