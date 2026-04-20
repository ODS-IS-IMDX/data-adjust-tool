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

import os
import json

class GetJsonWorkContentLogic:
    """
    引数のjson_filenameを取得し、そのファイル内容より、WorkContentを抜き出して属性設定にあうもののみに
    抜き出してattributeのwork_contentを書き換える、NiFi Python Processor。
    """

    def __init__(self):
        """現状は特に状態を持たない初期化のみを行うコンストラクタ。"""
        pass

    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        NiFi attributeのwork_contentを書き換える

        :param byte_data: FlowFile本文(JSON)
        :param attribute: FlowFile属性(dict)
        :param properties: プロセッサのプロパティ(dict)
        :return: (出力JSON bytes, 更新属性 dict)
        """

        OBJECT_NO_FIELD = "object_no"
        EXT_GEOJSON = ".geojson"  
        
        try:
            # === プロパティ取得 ===
            json_filename = properties.get("JSON_FILENAME", "")
            work_type = properties.get("WORK_TYPE", "")
            missing_json_policy = properties.get("MISSING_JSON_POLICY", "")
            input_field_names = properties.get("INPUT_FIELD_NAMES", "")

            # 設定するwork_content
            set_work_content = ""
            
            # geojsonファイルパスも作成
            geojson_filename = os.path.splitext(json_filename)[0] + EXT_GEOJSON
            # jsonファイルが無ければ、geojsonファイルを代入
            target_filename = json_filename if os.path.exists(json_filename) else geojson_filename
            
            # ファイルの存在チェック
            if not os.path.isfile(target_filename):  
                    # ファイルが存在しない場合にエラーとする場合
                    if missing_json_policy == 'error':
                        raise FileNotFoundError(f"ファイルが存在しません: {target_filename}")
                    
            else:
                # JSONファイルを開ける
                with open(target_filename, "r", encoding="utf-8") as file:
                    json_load = json.load(file)

                # JSONよりfeaturesのみを抜き出す
                features = json_load.get("features", [])

                #属性設定値リストを生成する
                field_list = input_field_names.split(":")
                field_list.insert(0, OBJECT_NO_FIELD)

                # featuresのpropertiesから属性設定リストにある項目を追加または設定する
                grownup = 1
                for feature in features:
                    # propertiesがない場合は作成する
                    if not isinstance(feature.get("properties"), dict):
                        feature["properties"] = {}
                    props = feature["properties"]

                    # geometryの型がwork_typeと一致しているかチェックする
                    geom = feature.get("geometry")
                    if not geom:
                        if work_type != "4":  # TEXT
                            raise Exception(f"geometryが存在しません。")

                    if work_type == "4":    # TEXT
                        if geom is not None:
                            raise Exception(f"geometryがNULLではありません。")
                    else:
                        if geom is None:
                            raise Exception(f"geometryがNULLです。")

                        geom_type = geom.get("type")
                        if work_type == "1":    # POINT
                            if geom_type != "Point":
                                raise Exception(f"geometryがPointではありません。")
                        elif work_type == "2":  # LINE
                            if geom_type != "LineString":
                                raise Exception(f"geometryがLineStringではありません。")
                        elif work_type == "3":  # POLYGON
                            if geom_type != "Polygon":
                                raise Exception(f"geometryがPolygonではありません。")
                        elif work_type == "5":  # RECTANGLE
                            if geom_type != "Polygon":
                                raise Exception(f"geometryがPolygonではありません。")

                    # field_list にない properties を削除
                    for key in list(props.keys()):
                        if key not in field_list:
                            del props[key]

                    # フィールドリストの属性を設定する
                    for field in field_list:
                        if field not in props:
                            if field == OBJECT_NO_FIELD:
                                props[field] = grownup
                                grownup += 1
                            else:
                                props[field] = ""

                # 新しい JSON を生成
                new_json = {
                    "type": "FeatureCollection",
                    "features": features
                }

                set_work_content = json.dumps(new_json, ensure_ascii=False, separators=(",", ":"))

            attribute.update({
                "work_content": set_work_content
            })

            return byte_data, attribute
        except Exception as e:
            raise Exception(f"[GetJsonWorkContentLogic_Exception]: {str(e)}")
