# MIT License
#
# Copyright (c) 2026 NTT InfraNet
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

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor

class ConvertLasToImage(BaseProcessor):
    """
    LAS→OpenCVイメージを生成し、ジオ情報は FlowFile 属性に保持する。
    出力は PNG（RGB/Intensity）または .npy（DEM）のバイト列である。
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'Create OpenCV image (DEM/RGB/Intensity) from LAS; geo metadata in attributes.'
        )
        tags = ['las', 'image', 'opencv', 'dem', 'intensity', 'rgb']

    INPUT_PATH = PropertyDescriptor(
        name='Input Path',
        description='入力 LAS/LAZ ファイルのフルパス（例: ${absolute.path}${filename}）',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True,
    )

    RASTER_PRODUCT = PropertyDescriptor(
        name='Raster Product',
        description='出力種別：標高（DEM） / カラー画像（RGB） / 反射強度（Intensity）から選択。',
        allowable_values=['標高（DEM）', 'カラー画像（RGB）', '反射強度（Intensity）'],
        default_value='標高（DEM）',
        required=True,
    )

    RES_MM = PropertyDescriptor(
        name='Resolution (mm/px)',
        description='ラスタ解像度（1ピクセルあたりのサイズを mm 単位で指定）。値が小さいほど高解像度になる。',
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value='50',
        required=True,
    )

    ALPHA_MM = PropertyDescriptor(
        name='Alpha (mm)',
        description='高さしきい値（mm単位）。低頻度ノイズの除去やマスク判定の基準として利用する。',
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value='50',
        required=True,
    )

    # LAS 読み込み時の最大点数（チャンク単位）。メモリと速度のトレードオフ調整用。
    CHUNK = PropertyDescriptor(
        name='Chunk Size',
        description='1チャンクあたりの最大点数。大きくすると高速だがメモリ使用量が増える。',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='5000000',
        required=False,
    )

    property_descriptors = [INPUT_PATH, RASTER_PRODUCT, RES_MM, ALPHA_MM, CHUNK]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertLasToImageLogic.ConvertLasToImageLogic')
    def __init__(self, **kwargs):
        pass
