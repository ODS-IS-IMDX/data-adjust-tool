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

# Python標準ライブラリ
import base64
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")
translate = getattr(import_module("shapely.affinity"), "translate")
scale = getattr(import_module("shapely.affinity"), "scale")

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class AlignMapGridLogic:

    def __init__(self):
        pass

    def convert_df_to_gdf(self, dataframe, all_params):
        """
        DataFrameをGeoDataFrameに変換する。

        :param dataframe: 入力データを含むDataFrame
        :type dataframe: pandas.DataFrame
        :param all_params: 出力するFieldSetFileのDwh名などを含むプロパティ情報の辞書
        :type all_params: dict
        
        :return: meshのGeoDataFrame、geometryのGeoDataFrame、DwhとTypeの関連情報
        :rtype: tuple(geopandas.GeoDataFrame, geopandas.GeoDataFrame, dict{str, str})

        :raises ValueError: 入力データが空である、または必須Dwhが存在しない場合に例外をスローする。
        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            if dataframe.empty:
                raise ValueError("Input data is empty.")
            
            required_dwhs = [f'{all_params["Data Name"]}/mesh', f'{all_params["Data Name"]}/geometry']
            for dwh in required_dwhs:
                if not (dataframe["Dwh"] == dwh).any():
                    raise ValueError(f'{dwh} does not exist.')
    
            # mesh, geometry, その他属性データを抽出
            mesh_df = dataframe[dataframe["Dwh"] == required_dwhs[0]]
            geometry_df = dataframe[dataframe["Dwh"] == required_dwhs[1]]
            attr_df = dataframe[(dataframe["Dwh"].str.startswith(f'{all_params["Data Name"]}/')) & 
                                (~dataframe["Dwh"].isin(required_dwhs))]
    
            src_df = pd.concat([mesh_df, geometry_df, attr_df], axis=0)
            dwh_type_info = dict(zip(src_df["Dwh"].tolist(), src_df["Type"].tolist()))
    
            def create_gdf(df, all_params):
                """
                GeoDataFrameを作成する関数。
    
                :param df: 対象のデータフレーム
                :type df: pandas.DataFrame
                :param all_params: プロパティ情報
                :type all_params: dict
    
                :return: GeoDataFrame
                :rtype: geopandas.GeoDataFrame
                """
                geometry = df["Value"].tolist()[0]
                gdf = gpd.GeoDataFrame(geometry=geometry, crs=f"epsg:{all_params['CRS']}")
                return gdf
    
            # GeoDataFrameの作成
            mesh_gdf = create_gdf(mesh_df, all_params)
            geometry_gdf = create_gdf(geometry_df, all_params)
    
            return mesh_gdf, geometry_gdf, dwh_type_info
        
        except Exception as e:
            raise Exception(f"[convert_df_to_gdf]: {str(e)}")

    def calculate_origin(self, mesh_gdf, geometry_gdf, reference_corner):
        """
        指定した位置に基づき、meshとgeometryのGeoDataFrameの外接矩形の原点座標を計算する。

        :param mesh_gdf: mesh座標のGeoDataFrame
        :type mesh_gdf: geopandas.GeoDataFrame
        :param geometry_gdf: geometry座標のGeoDataFrame
        :type geometry_gdf: geopandas.GeoDataFrame
        :param reference_corner: 原点を定義する位置 ('top-left', 'top-right', 'bottom-left', 'bottom-right')
        :type reference_corner: str

        :return: meshとgeometryの原点座標 (mesh_x, mesh_y, geometry_x, geometry_y)
        :rtype: tuple(numpy.float64, numpy.float64, numpy.float64)

        :raises ValueError: 無効なreference_cornerが指定された場合に例外をスローする。
        """
        mesh_min_x, mesh_min_y, mesh_max_x, mesh_max_y = mesh_gdf.total_bounds
        geometry_min_x, geometry_min_y, geometry_max_x, geometry_max_y = geometry_gdf.total_bounds

        origin_map = {
            "top-left": (mesh_min_x, mesh_max_y, geometry_min_x, geometry_max_y),
            "top-right": (mesh_max_x, mesh_max_y, geometry_max_x, geometry_max_y),
            "bottom-left": (mesh_min_x, mesh_min_y, geometry_min_x, geometry_min_y),
            "bottom-right": (mesh_max_x, mesh_min_y, geometry_max_x, geometry_min_y)
        }

        if reference_corner not in origin_map:
            raise ValueError("Invalid reference corner. Choose from 'top-left', 'top-right', 'bottom-left', 'bottom-right'.")
        
        return origin_map[reference_corner]

    def adjust_gdf(self, mesh_gdf, geometry_gdf, all_params):
        """
        指定された原点とスケールに基づいて、meshとgeometryのGeoDataFrameの座標を調整する。

        :param mesh_gdf: mesh座標のGeoDataFrame
        :type mesh_gdf: geopandas.GeoDataFrame
        :param geometry_gdf: geometry座標のGeoDataFrame
        :type geometry_gdf: geopandas.GeoDataFrame
        :param all_params: プロパティ情報
        :type all_params: dict

        :return: 座標調整後のmeshとgeometryのGeoDataFrame
        :rtype: tuple(geopandas.GeoDataFrame, geopandas.GeoSeries)
        
        :raises Exception: 調整中にエラーが発生した場合に例外をスローする。
        """
        try:
            mesh_origin_x, mesh_origin_y, geometry_origin_x, geometry_origin_y = self.calculate_origin(
                mesh_gdf, geometry_gdf, all_params["Reference Corner"]
            )
    
            # X軸、Y軸の方向調整
            origin_x = -all_params["Origin X"] if all_params["Source X-Axis Orientation"] == "Right to Left" else all_params["Origin X"]
            origin_y = -all_params["Origin Y"] if all_params["Source Y-Axis Orientation"] == "Top to Bottom" else all_params["Origin Y"]
    
            # オフセットとスケール適用
            geometry_gdf = geometry_gdf.apply(
                lambda row: scale(translate(row.geometry, xoff=-geometry_origin_x + mesh_origin_x + origin_x, 
                                            yoff=-geometry_origin_y + mesh_origin_y + origin_y),
                                  xfact=all_params["Scale"], yfact=all_params["Scale"], 
                                  origin=(mesh_origin_x + origin_x, mesh_origin_y + origin_y)), axis=1)
    
            return mesh_gdf, geometry_gdf

        except Exception as e:
            raise Exception(f"[adjust_gdf]: {str(e)}")

    def convert_gdf_to_field_set_file(self, mesh_gdf, geometry_gdf, all_params, dwh_type_info):
        """
        meshとgeometryのGeoDataFrameをFieldSetFile形式に変換する。

        :param mesh_gdf: mesh座標のGeoDataFrame
        :type mesh_gdf: geopandas.GeoDataFrame
        :param geometry_gdf: geometry座標のGeoDataFrame
        :type geometry_gdf: geopandas.GeoDataFrame
        :param all_params: プロパティ情報の辞書
        :type all_params: dict
        :param dwh_type_info: DwhとTypeの関連情報
        :type dwh_type_info: dict

        :return: FieldSetFile形式のデータ
        :rtype: str

        :raises Exception: FieldSetFile作成中にエラーが発生した場合に例外をスローする。
        """
        def extract_data_for_field_set_file(mesh_gdf, geometry_gdf, data_name, dwh_type_info):
            """
            FieldSetFile用データを抽出する関数。

            :param mesh_gdf: mesh座標のGeoDataFrame
            :type mesh_gdf: geopandas.GeoDataFrame
            :param geometry_gdf: geometry座標のGeoDataFrame
            :type geometry_gdf: geopandas.GeoDataFrame
            :param data_name: FieldSetFileのDwh名
            :type data_name: str
            :param dwh_type_info: DwhとTypeの関連情報
            :type dwh_type_info: dict

            :return: FieldSetFile用のType、Value、Dwhのリスト
            :rtype: tuple(list[str], list[str], list[str])

            :raises Exception: 関数内でエラーが発生した場合に例外をスローする。
            """
            try:
                # Meshデータ
                mesh_encoded = base64.b64encode(pickle.dumps(mesh_gdf.geometry.tolist())).decode("utf-8")
                mesh_suffix = f"{data_name}/mesh".split("/")[1]

                # Geometryデータ
                geom_encoded = base64.b64encode(pickle.dumps(geometry_gdf.geometry.tolist())).decode("utf-8")
                geometry_suffix = f"{data_name}/geometry".split("/")[1]

                type_list = [dwh_type_info[f"{data_name}/mesh"], dwh_type_info[f"{data_name}/geometry"]]
                value_list = [mesh_encoded, geom_encoded]
                dwh_list = [f"{data_name}/{mesh_suffix}", f"{data_name}/{geometry_suffix}"]

                return type_list, value_list, dwh_list
            except Exception as e:
                raise Exception(f"Error extracting data for FieldSetFile: {str(e)}")

        type_list, value_list, dwh_list = extract_data_for_field_set_file(mesh_gdf, geometry_gdf, all_params["Data Name"], dwh_type_info)
        return NSP.get_field_set_file(type_list, value_list, dwh_list)

    def get_params(self, properties):
        """
        プロパティの値を取得し、辞書にして返す。

        :param properties: プロパティ情報を格納した辞書
        :type properties: dict

        :return: プロパティの値を格納した辞書
        :rtype: dict

        :raises Exception: プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            all_params = {}

            # プロパティの値を取得する
            all_params["Data Name"] = str(
                properties['DATA_NAME']
            )
            all_params["Scale"] = CU.get_number_from_string(
                properties['SCALE']
            )
            # scaleが0以下の場合は失敗とする
            if all_params["Scale"] <= 0:
                raise ValueError("scale must be greater than 0")

            all_params["Origin X"] = CU.get_number_from_string(
                properties['ORIGIN_X']
            )
            all_params["Origin Y"] = CU.get_number_from_string(
                properties['ORIGIN_Y']
            )
            all_params["Source X-Axis Orientation"] = properties['SOURCE_X_AXIS_ORIENTATION']
            all_params["Source Y-Axis Orientation"] = properties['SOURCE_Y_AXIS_ORIENTATION']
            all_params["Reference Corner"] = properties['REFERENCE_CORNER']
            all_params["CRS"] = CU.get_number_from_string(properties['CRS'], num_type=int)

            return all_params

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def __call__(self, byte_data, attributes, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力となるFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attributes: FlowFileの属性情報を格納する辞書
        :type attributes: dict
        :param properties: 処理に必要なプロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            all_params = self.get_params(properties)

            # CSV形式のFieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(byte_data)

            # DataFrameをGeoDataFrameに変換
            mesh_gdf, geometry_gdf, dwh_type_info = self.convert_df_to_gdf(
                dataframe, all_params
            )

            # GeoDataFrameをオフセットとスケール適用
            mesh_gdf, geometry_gdf = self.adjust_gdf(mesh_gdf, geometry_gdf, all_params)

            # ２つのGeoDataFrameを1つのFieldSetFileに変換
            output_field_set_file = self.convert_gdf_to_field_set_file(
                mesh_gdf, geometry_gdf, all_params, dwh_type_info
            )

            return output_field_set_file, attributes

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
