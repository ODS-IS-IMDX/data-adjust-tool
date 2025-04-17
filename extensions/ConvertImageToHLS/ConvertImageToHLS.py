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


class ConvertImageToHLS(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ConvertImageToHLS

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像データを指定されたカラースペース（RGB、HSV、BGR）からHLS色空間に変換するプロセッサクラスです。

    **処理概要:**

    - 入力データを読み込み、現在のカラースペースを確認します。
    - 画像データをHLS色空間に変換します。HSV色空間からHLSへの変換は特別な処理が必要です。
    - 変換された画像データをバイト列形式で返します。

    **注意事項:**

    - カラースペースはRGB、HSV、BGRである必要があります。異なる場合はエラーとなります。

    タグ
    --------------------------------
    * image processing
    * color space conversion
    * HLS

    プロパティ
    --------------------------------
    このプロセッサには特定のプロパティはありません。

    リレーションシップ
    --------------------------------
    * success: HLS変換処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGB、HSV、BGRである必要があります。

    Writes Attributes
    --------------------------------
    * ColorSpace: 処理が正常に完了した場合にHLSに設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB、HSV、BGR 形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ConvertImageToHLS'

    @BaseProcessor.set_logic_class('ConvertImageToHLSLogic.ConvertImageToHLSLogic')
    def __init__(self, **kwargs):
        pass
