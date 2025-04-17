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

from raster_to_vector.common.base_processor import BaseProcessor

class CalculateAngleDirectionSymbol(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    CalculateAngleDirectionSymbol

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    入力された方位シンボルから真北(真上)まで時計回りに回転させるために必要な角度を返すプロセッサクラスです。

    このプロセッサは、入力データ(輪郭データ)に対してひし形(四角形)を近似し、その対角線の長い方を軸に真北までに必要な回転角を計算します。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 輪郭データをロードし、その座標からひし形(四角形)を近似します。
    - ひし形の対角線を取得し、その対角線の長い方を方位シンボルの軸線とします。
    - ひし形の対角線の交点を取得し、その交点から軸線の始点、終点で長いほうを方位シンボルの北向きとします。
    - 真北までに必要な回転角を計算して値をバイト列形式で返します。

    **注意事項:**

    - 現在、プロパティで設定する項目はありません。
    - 円の中に矢印や、三角形のタイプの方位シンボルは角度を計算することはできません。
    - 上記タイプの方位シンボルの場合、エラーまたは処理できても正確な回転角を計算することはできません。

    タグ
    --------------------------------
    * image processing
    * direction symbol
    * angle calculation

    プロパティ
    --------------------------------
    このプロセッサには特定のプロパティはありません。

    リレーションシップ
    --------------------------------
    * success: 角度計算処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: カラースペース。

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
    * 輪郭座標のバイトデータ

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'CalculateAngleDirectionSymbol'

    property_descriptors = []

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('CalculateAngleDirectionSymbolLogic.CalculateAngleDirectionSymbolLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
