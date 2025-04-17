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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 読み込まれたGeotiffからワールドファイル情報を取得する
# --------------------------------------------------------------------------------------------
from raster_to_vector.common.base_processor import BaseProcessor


class ExtractGeoTiffInfo(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ExtractGeoTiffInfo

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像データからワールドファイル情報を取得するプロセッサクラスです。

    このプロセッサは、入力画像のワールドファイル情報を取得します。

    **処理概要:**

    - 入力データを読み込み、ワールドファイル情報の有無を確認します。
    - ワールドファイル情報を取得し、attributesへ設定します。
    - 後続フローへ画像データをそのまま出力します。

    **注意事項:**

    - 入力画像はワールドファイル情報を所持している必要があります。

    タグ
    --------------------------------
    * image processing
    * world file information extracting

    プロパティ
    --------------------------------
    * なし

    リレーションシップ
    --------------------------------
    * success: ワールドファイル情報取得処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * filename: 入力した画像データ名。
    * ColorSpace: 入力画像のカラースペース。特に指定はありません。

    Writes Attributes
    --------------------------------
    * UnitXPerPixel: X方向のピクセルサイズ
    * UnitYPerPixel: Y方向のピクセルサイズ
    * UpperLeftX: xの回転パラメータ
    * UpperLeftY: yの回転パラメータ
    * PixelToCoordinateX: 左上ピクセル中心のX座標
    * PixelToCoordinateY: 左上ピクセル中心のY座標
    
    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式のGeoTiff画像

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """読み込まれたGeotiffからワールドファイル情報を取得する"""
        tags = ["python","numpy", "pandas", "rasterio", "Coordinates"]

    @BaseProcessor.set_logic_class('ExtractGeoTiffInfoLogic.ExtractGeoTiffInfoLogic')
    def __init__(self, **kwargs):
        pass
