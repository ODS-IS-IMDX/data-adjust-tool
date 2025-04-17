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


class ImageSharpening(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageSharpening

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像のシャープ化を行うプロセッサクラスです。

    このプロセッサは、入力データに対して指定された方法でシャープ化処理を適用し、画像を加工します。

    **処理概要:**

    - 入力データと属性の妥当性を確認します。
    - 指定された方法（ラプラシアンフィルタまたはアンシャープマスク法）でシャープ化を行います。
    - シャープ化された画像データをバイト列形式で返します。

    **注意事項:**

    - カラースペースは RGBである必要があります。異なる場合はエラーとなります。
    - カーネルサイズは 3,3のような形式で指定する必要があります。
    - カーネルサイズが int,int 形式でない場合や、要素が2つでない場合、または要素が偶数の場合には例外を発生させます。

    タグ
    --------------------------------
    * image processing
    * sharpening
    * laplacian filter
    * unsharp mask

    プロパティ
    --------------------------------
    **Sharpening Method**

    **必須入力  リスト入力**

    - シャープ化の方法を指定します。

    #. Laplacian: ラプラシアンフィルタを使用してシャープ化します。
        エッジを強調しますがノイズも強調されやすいです。輪郭抽出や二値化などの前処理としてエッジを強調したい場合に有効です。
    #. UnsharpMask: アンシャープマスク法でシャープ化します。
        より自然なシャープ化を行いノイズの強調を抑制します。文字や線の鮮明化や画質改善などの有効です。

    - 初期値はLaplacianです。

    **Kernel Size**

    **必須入力  直接入力**

    - ラプラシアンフィルタで使用するカーネルサイズを指定します。
    - プロパティ（Sharpening Method）でLaplacianを指定した場合のみ有効です。
    - 実用的な範囲は 3,3 ～ 7,7 程度で、以下の効果があります。

        + 3,3：局所的な効果、処理が高速
        + 5,5：より広い範囲の平均化、細部が若干失われる
        + 7,7：さらに広い範囲の平均化、細部がより失われる

    - 初期値は3,3です。

    **Custom Filter**

    **任意入力  直接入力**

    - ラプラシアンフィルタで使用するカスタムフィルタを指定します。
    - プロパティ（Sharpening Method）でLaplacianを指定した場合のみ有効です。
    - カンマ区切りの整数値で指定します。
    - 初期値は 0,-1,0,-1,5,-1,0,-1,0（ラプラシアンフィルタ）です。

    - Custom Filterの指定例：
    - カーネルサイズ3,3のフィルタ（デフォルト値）

        + プロパティ指定値:0,-1,0,-1,5,-1,0,-1,0

        実際に使用されるカーネル（カーネルサイズに展開）:

        0,-1, 0

        -1, 5,-1   ⇒中央の5の値を大きくするとシャープ化の強度が強くなります。

        0,-1, 0

    - 標準的な強度のフィルタ（カーネルサイズ5,5の場合）

        + プロパティ指定値:0,0,-1,0,0,0,-1,2,-1,0,-1,2,5,2,-1,0,-1,2,-1,0,0,0,-1,0,0

        実際に使用されるカーネル（カーネルサイズに展開）:

        0, 0,-1, 0, 0

        0,-1, 2,-1, 0

        -1, 2, 5, 2,-1

        0,-1, 2,-1, 0

        0, 0,-1, 0, 0

    **Unsharp Strength**

    **任意入力  直接入力**

    - アンシャープマスク法の強度を指定します。
    - プロパティ（Sharpening Method）でUnsharpMaskを指定した場合のみ有効です。
    - 実用的な範囲は 0.8～2.0程度で、以下の効果があります。

        + 0.8：線を少し強調
        + 1.5：線をはっきりと強調、適度なコントラスト向上
        + 2.0：線を大きく強調、高いコントラスト

    - 初期値は 1.5 です。

    **Blur Sigma**

    **任意入力  直接入力**

    - アンシャープマスク法のガウシアンブラーのシグマ値を指定します。
    - プロパティ（Sharpening Method）でUnsharpMaskを指定した場合のみ有効です。
    - 実用的な範囲は 0.5～2.0程度で、以下の効果があります。

        + 0.5：細かい線の保持、シャープな輪郭を維持
        + 1.0：バランスの取れた線の強調
        + 2.0：ノイズ抑制、滑らかな線の強調

    - 初期値は 1.0 です。

    リレーションシップ
    --------------------------------
    * success: シャープ化処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGBである必要があります。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageSharpening'

    kernel_size = PropertyDescriptor(
        name="Kernel Size",
        description="ラプラシアンフィルタで使用するカーネルサイズを指定します。\
                    プロパティ（Sharpening Method）でLaplacianを指定した場合のみ有効です。\
                    3,3のような形式で指定する必要があります。\
                    実用的な範囲は 3,3 ～ 7,7 程度です。\
                    3,3だと局所的な効果で7,7だとさらに広い範囲の平均化がされ細部がより失われます。\
                    初期値は3,3です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="3,3",
        sensitive=False
    )

    custom_filter = PropertyDescriptor(
        name="Custom Filter",
        description="ラプラシアンフィルタで使用するカスタムフィルタを指定します。\
                    プロパティ（Sharpening Method）でLaplacianを指定した場合のみ有効です。\
                    カンマ区切りの整数値で指定します。\
                    詳細はリファレンスを参照してください。\
                    初期値は 0,-1,0,-1,5,-1,0,-1,0（ラプラシアンフィルタ）です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0,-1,0,-1,5,-1,0,-1,0",
        sensitive=False
    )

    sharpening_method = PropertyDescriptor(
        name="Sharpening Method",
        description="シャープ化の方法を指定します。\
                        Laplacian: ラプラシアンフィルタを使用してシャープ化します。\
                        エッジを強調しますがノイズも強調されやすいです。輪郭抽出や二値化などの前処理としてエッジを強調したい場合に有効です。\
                        UnsharpMask: アンシャープマスク法でシャープ化します。\
                        より自然なシャープ化を行いノイズの強調を抑制します。文字や線の鮮明化や画質改善などの有効です。\
                    初期値はLaplacianです。",
        allowable_values=["Laplacian", "UnsharpMask"],
        required=True,
        default_value="Laplacian",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    unsharp_strength = PropertyDescriptor(
        name="Unsharp Mask Strength",
        description="アンシャープマスク法の強度を指定します。\
                    プロパティ（Sharpening Method）でUnsharpMaskを指定した場合のみ有効です。\
                    実用的な範囲は 0.8～2.0程度です\
                    0.8だと線を少し強調し、2.0だと線を大きく強調し、高いコントラストとなります。\
                    初期値は 1.5 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.5",
        sensitive=False
    )

    blur_sigma = PropertyDescriptor(
        name="Blur Sigma",
        description="アンシャープマスク法のガウシアンブラーのシグマ値を指定します。\
                    プロパティ（Sharpening Method）でUnsharpMaskを指定した場合のみ有効です。\
                    実用的な範囲は 0.5～2.0程度です\
                    0.5だと細かい線の保持、シャープな輪郭の維持ができ、2.0だとノイズ抑制、滑らかな線の強調ができます。\
                    初期値は 1.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    property_descriptors = [kernel_size, custom_filter, sharpening_method, unsharp_strength, blur_sigma]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageSharpeningLogic.ImageSharpeningLogic')
    def __init__(self, **kwargs):
        pass
