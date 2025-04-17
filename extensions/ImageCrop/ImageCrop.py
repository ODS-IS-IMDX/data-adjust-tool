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


class ImageCrop(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageCrop

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    指定された座標で画像を切り出すプロセッサクラスです。

    **処理概要:**

    - 指定された座標で画像を切り出します。座標はリスト型で複数指定可能です。
    - 切り出された画像データをバイト列形式で返します。

    **注意事項:**

    - 切り出し座標の指定には、下記の2通り存在します。
    - 両方指定された場合は、後者のcrop_coord_start/crop_coord_endに指定した座標が優先されます。

    #. 矩形領域を検出するImageConnectedRegionDetectionプロセッサの結果をプロパティ(FSF Crop Coords)に指定する方法。
        矩形領域の座標が複数設定されている場合は複数の画像に切り出されます。
    #. 切り出し座標をプロパティ(crop_coord_start/crop_coord_end)に直接指定する方法。
    
    タグ
    --------------------------------
    * image processing
    * crop

    プロパティ
    --------------------------------

    **FSF Image Src**

    **必須入力  直接入力**

    - 切り出し元となる入力画像が設定されたFSFの項目を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Image Dst**

    **必須入力  直接入力**

    - 切り出した画像を設定するFSFの項目を指定します。
    - FSF Crop Coordsで指定したFSF項目に複数の切り出し座標が指定された場合は、切り出し画像は本項目に'_0'のように通番を付与した項目名に出力します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    **FSF Crop Coords**

    **任意入力  直接入力**

    - 切り出し座標が設定されたFieldSetFileの項目名を指定します。
    - ImageConnectedRegionDetectionプロセッサの出力結果（平面図位置の特定結果など）が設定されたFieldSetFileの項目名を指定します。
    - FSF Crop Coordsでは、[[(x1,y1),(x2,y2)],・・・]形式で複数の切り出し座標に対応しています。
    - ただし、Crop Coord Start / Crop Coord Endで切り出し座標が設定されている場合は、本プロパティの指定は無視されます。
    - 初期値はcrop_coordsです。

    **Crop Coord Start**

    **任意入力  直接入力**

    - 切り出したい座標の矩形左上座標を指定します。
    - 括弧は不要でx座標とy座標をカンマ区切りで指定します。
    - 指定した座標が画像範囲外の場合は範囲内に補正されて切り出しが行われます。
    - 方位シンボルの位置など、あらかじめ切り出したい座標が判明している場合は、本プロパティを使用します。
    - 指定形式: x1,y1
    - 初期値は""(空白)です。

    **Crop Coord End**

    **任意入力  直接入力**

    - 切り出したい座標の矩形右下座標を指定します。
    - 括弧は不要でx座標とy座標をカンマ区切りで指定します。
    - 指定した座標が画像範囲外の場合は範囲内に補正されて切り出しが行われます。
    - 方位シンボルの位置など、あらかじめ切り出したい座標が判明している場合は、本プロパティを使用します。
    - 指定形式: x2,y2
    - 初期値は""(空白)です。

    リレーションシップ
    --------------------------------
    * success: 画像処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(色空間の指定なし)である必要があります。

    System Resource Considerations
    --------------------------------
    * なし
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Image Crop'

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="切り出し元となる入力画像が設定されたFSFの項目を指定します。\
            初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_image_dst = PropertyDescriptor(
        name="FSF Image Dst",
        description="切り出した画像を設定するFSFの項目を指定します。\
            初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_crop_coords = PropertyDescriptor(
        name="FSF Crop Coords",
        description="切り出し座標が設定されたFieldSetFileの項目名を指定します。\
            ImageConnectedRegionDetection等の領域検出結果が出力されたFieldSetFileの項目名を指定します。\
            ただし、Crop Coord Start/Crop Coord Endで切り出し座標が設定されている場合は、FSF Crop Coordsの指定は無視されます。\
            初期値はcrop_coordsです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="crop_coords",
        sensitive=False
    )

    crop_coord_start = PropertyDescriptor(
        name="Crop Coord Start",
        description="切り出し座標の矩形左上座標を指定します。\
            括弧は不要でx座標とy座標をカンマ区切りで指定します。\
            初期値は""(空白)です。\
            e.g) 50,100",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="",
        sensitive=False
    )

    crop_coord_end = PropertyDescriptor(
        name="Crop Coord End",
        description="切り出し座標の矩形右下座標を指定します。\
            括弧は不要でx座標とy座標をカンマ区切りで指定します。\
            初期値は""(空白)です。\
            e.g) 200,400",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="",
        sensitive=False
    )

    property_descriptors = [
        fsf_image_src,
        fsf_image_dst,
        fsf_crop_coords,
        crop_coord_start,
        crop_coord_end
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageCropLogic.ImageCropLogic')
    def __init__(self, **kwargs):
        pass
