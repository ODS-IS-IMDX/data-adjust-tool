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


class ImageSmoothing(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageSmoothing

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像の平滑化を行うプロセッサクラスです。

    このプロセッサは、入力データに対してガウシアンブラーを適用し、画像を平滑化します。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 画像をロードし、指定されたカーネルサイズとシグマでガウシアンブラーを適用します。
    - 平滑化された画像データをバイト列形式で返します。

    **注意事項:**

    - カラースペースは RGBである必要があります。異なる場合はエラーとなります。
    - カーネルサイズは 5,5のような形式で指定する必要があります。
    - カーネルサイズが int,int形式でない場合や、要素が2つでない場合、または要素が偶数の場合には例外が発生します。

    タグ
    --------------------------------
    * image processing
    * smoothing
    * gaussian blur

    プロパティ
    --------------------------------
    **Kernel Size**

    **必須入力  直接入力**

    - ガウシアン平滑化で使用するカーネルサイズを奇数で指定します。
    - 実用的な範囲は 3,3 ～ 7,7 程度で、以下の効果があります。

        + 3,3：微細なノイズ除去、高速処理が必要な場合
        + 5,5：一般的なノイズ除去
        + 7,7：広い範囲のノイズ除去

    - 初期値は 5,5です。
    
    **Sigma**

    **必須入力  直接入力**

    - ガウシアン平滑化の標準偏差（シグマ）を設定します。
    - 実用的な範囲は 0.5～3.0程度で、以下の効果があります。

        + 1.0：図面スキャン時の微細なゴミ除去
        + 2.0：手書き図面のノイズ除去、スキャン図面の汚れ除去
        + 3.0：画像の細部を平滑化により抑制し、主要な構造を保持

    - なお、0の場合、シグマはカーネルサイズから自動的に計算されます。
    - カーネルサイズが大きくなると、より広い範囲の平滑化が必要となるため、シグマも適切に大きくする必要があり、比較的良好な結果が得られる値を自動的に使用します。
    - 初期値は1.0です。

    リレーションシップ
    --------------------------------
    * success: 平滑化処理が正常に完了した場合
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
        description = 'ImageSmoothing'

    kernel_size = PropertyDescriptor(
        name="Kernel Size",
        description="ガウシアン平滑化で使用するカーネルサイズを奇数で指定します。\
                    実用的な範囲は 3,3 ～ 7,7 程度です。\
                    3,3だと微細なノイズ除去で、7,7だと広い範囲のノイズ除去に使用できます。\
                    初期値は 5,5です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="5,5",
        sensitive=False
    )

    sigma = PropertyDescriptor(
        name="Sigma",
        description="ガウシアン平滑化の標準偏差（シグマ）を設定します。\
                    実用的な範囲は 0.5～3.0程度です。\
                    1.0だと図面スキャン時の微細なゴミ除去、3.0だと画像の細部を平滑化により抑制できます。\
                    なお、0の場合、シグマはカーネルサイズから自動的に計算されます。\
                    初期値は1.0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="1.0",
        sensitive=False
    )
    property_descriptors = [kernel_size, sigma]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageSmoothingLogic.ImageSmoothingLogic')
    def __init__(self, **kwargs):
        pass
