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


class ConvertImageToRaster(BaseProcessor):
    """
    LasToImage の FlowFile（DEM .npy または PNG 画像）と、そのジオメタデータ属性をもとに GeoTIFF を生成する。
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'LasToImage で出力された DEM／PNG と、transform.gdal / crs.epsg などの属性を用いて '
            'GeoTIFF を生成して出力する。'
        )
        tags = ['geotiff', 'image', 'opencv', 'raster', 'dem', 'rgb', 'intensity']

    # GeoTIFF を出力するルートディレクトリ
    # 例: /data/raster や ${raster.root.dir}
    OUTPUT_PATH = PropertyDescriptor(
        name='OutputPath',
        description='GeoTIFF 出力先ディレクトリ',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 出力ファイル名のベース（拡張子を除いた部分）
    # LasToImage の product.kind / res.mm / alpha.mm などに応じて
    # "_DEM_50mm" のようなサフィックスをロジック側で付与する想定。
    BASENAME = PropertyDescriptor(
        name='Basename',
        description='出力 GeoTIFF のファイル名ベース（拡張子除く）。product.kind / res.mm / alpha.mm に応じてサフィックスを自動付与する。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 出力 EPSG（未指定なら LasToImage が付与した EPSG 属性をそのまま使用）
    OUT_EPSG = PropertyDescriptor(
        name='OutputEPSG',
        description='空欄の場合は LasToImage の FlowFile 属性（例: crs.epsg）を踏襲。値を指定するとその EPSG で GeoTIFF を出力する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        required=False,
    )

    # GeoTIFF の圧縮方式
    COMPRESSION = PropertyDescriptor(
        name='Compression',
        description='GeoTIFF の圧縮方式。DEFLATE / LZW / ZSTD / NONE から選択する。',
        allowable_values=['DEFLATE', 'LZW', 'ZSTD', 'NONE'],
        default_value='DEFLATE',
        required=False,
    )

    property_descriptors = [OUTPUT_PATH, BASENAME, OUT_EPSG, COMPRESSION]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertImageToRasterLogic.ConvertImageToRasterLogic')
    def __init__(self, **kwargs):
        pass
