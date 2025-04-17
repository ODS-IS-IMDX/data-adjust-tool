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
# 【プロセッサ概要】boundedByの更新
#  複数のCityGMLのboundedByを取得、すべてを内包する値でboundedByを更新
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")
etree = import_module("lxml.etree")

# boundedByのタグ名
BOUNDED_BY_TAG_NAME = 'gml:boundedBy'
ENVELOPE_TAG_NAME = 'gml:Envelope'

# lowerのタグ名
LOWER_TAG_NAME = 'gml:lowerCorner'
# upperのタグ名
UPPER_TAG_NAME = 'gml:upperCorner'


CITYGML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'
CITYGML_ENCODING = 'UTF-8'


class UpdateBoundedByAllCityGML(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """複数のCityGMLのboundedByを取得、すべてを内包する値でboundedByを更新"""
        tags = ["python", "pandas", "CityGML", "xml"]

    property_descriptors = []

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_xml_string_list(self, target_value_list):
        # --------------------------------------------------------------------------
        # Value列からxmlの文字列を取得し,リストに格納する
        # --------------------------------------------------------------------------
        xml_string_list = [pickle.loads(base64.b64decode(
            target_value_list[i])) for i in range(len(target_value_list))]
        return xml_string_list

    def get_boundedby_text(self, xml_string_list):
        # --------------------------------------------------------------------------
        # boundedByに設定する値取得
        # --------------------------------------------------------------------------
        # lower,upper格納用
        result_list = []

        for i in range(len(xml_string_list)):

            # 文字列をlxmlで読み込み
            root = etree.fromstring(
                xml_string_list[i].encode(CITYGML_ENCODING))

            # namspace取得
            NAMESPACE_DICT = root.nsmap

            # root直下のboundedBy取得
            boundedby = root.find(BOUNDED_BY_TAG_NAME, NAMESPACE_DICT)

            # boundedBy内のenvelope
            envelope = boundedby.find(ENVELOPE_TAG_NAME, NAMESPACE_DICT)

            for temp in envelope:
                result_list.append(temp.text.split(' '))

        # 全boundedByの結果を集約
        result_array = np.array(result_list, dtype=np.float64)

        # lower corner→all min
        min_geometry_list = map(str, list(np.min(result_array, axis=0)))

        # upper corner→all max
        max_geometry_list = map(str, list(np.max(result_array, axis=0)))

        # boundedByに設定する文字列
        lower_text = " ".join(min_geometry_list)
        upper_text = " ".join(max_geometry_list)

        return lower_text, \
            upper_text

    def update_boundedby_text(self,
                              xml_string_list,
                              lower_text,
                              upper_text):
        xml_value_list = []
        # boundedByに値を設定
        for i in range(len(xml_string_list)):

            # 文字列をlxmlで読み込み
            root = etree.fromstring(
                xml_string_list[i].encode(CITYGML_ENCODING))

            # namspace取得
            NAMESPACE_DICT = root.nsmap

            # root直下のboundedBy取得
            boundedby = root.find(BOUNDED_BY_TAG_NAME, NAMESPACE_DICT)

            # boundedBy内のenvelope
            envelope = boundedby.find(ENVELOPE_TAG_NAME, NAMESPACE_DICT)

            # lowerに設定
            lower = envelope.find(LOWER_TAG_NAME, NAMESPACE_DICT)
            lower.text = lower_text

            # upper
            upper = envelope.find(UPPER_TAG_NAME, NAMESPACE_DICT)
            upper.text = upper_text

            et = etree.ElementTree(root)

            # 改行文字をwindows用に変換
            et = etree.tostring(et,
                                pretty_print=True,
                                encoding=CITYGML_ENCODING,
                                doctype=CITYGML_HEADER).decode(CITYGML_ENCODING)

            # boundedBy更新結果を格納
            xml_value_list.append(et.replace("\\n", "\\r\\n"))
        return xml_value_list

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # Value列からxmlの文字列を取得しリストに格納する
            # --------------------------------------------------------------------------
            xml_string_list\
                = WM.calc_func_time(self.logger)(self.get_xml_string_list)(target_value_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # boundedByに設定する値取得
            # --------------------------------------------------------------------------
            lower_text, \
                upper_text\
                = WM.calc_func_time(self.logger)(self.get_boundedby_text)(xml_string_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # boundedBy更新
            # --------------------------------------------------------------------------
            xml_value_list\
                = WM.calc_func_time(self.logger)(self.update_boundedby_text)(xml_string_list,
                                                                             lower_text,
                                                                             upper_text)
            # --------------------------------------------------------------------------

            result_value_list = []
            result_type_list = []
            result_dwh_list = []

            for i in range(len(target_type_list)):

                result_dwh_list, \
                    result_type_list, \
                    result_value_list\
                    = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(result_dwh_list,
                                                                                            result_type_list,
                                                                                            result_value_list,
                                                                                            target_dwh_list[i],
                                                                                            target_type_list[i],
                                                                                            xml_value_list[i])

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_value\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(result_dwh_list,
                                                                                 result_type_list,
                                                                                 result_value_list)
            # --------------------------------------------------------------------------

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_value)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
