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

# Python標準ライブラリ
from importlib import import_module
import base64
import os
import pickle

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")
pd = import_module("pandas")

import cad.common.cad_utils as CU


class OverlayGeoJsonOnImageLogic:

    def __init__(self):
        pass

    def extract_ndarray(self, value):
        """
        入力値から numpy.ndarray を抽出して返す関数。

        ndarray、(idx, ndarray) のタプル、またはそれらを1要素にもつリスト形式の
        入力に対応し、ndarray を返却する。

        :param value: ndarray、(idx, ndarray)、またはそれらを含むリスト
        :type value: Any

        :return: 抽出された numpy.ndarray
        :rtype: numpy.ndarray

        :raises Exception: ndarray の抽出処理中にエラーが発生した場合
        """
        try:
            if isinstance(value, np.ndarray):
                return value

            # [(idx, ndarray)]
            if isinstance(value, list) and len(value) == 1:
                value = value[0]

            # (idx, ndarray)
            if isinstance(value, tuple) and len(value) == 2:
                if isinstance(value[1], np.ndarray):
                    return value[1]

        except Exception as e:
            raise Exception(f"[ERROR extract_ndarray]: {str(e)}")

    def draw_geometry_overlay(
            self,
            img: np.ndarray,
            geometry_array: np.ndarray,
            color: tuple,
            alpha: float,
            thickness: int
    ) -> np.ndarray:
        """
        画像上にジオメトリを重ねて描画する関数。

        Polygon は内側を半透明で塗り、
        輪郭線も指定色で描画する。
        Polygon の外側の元画像は変更しない。

        :param img: 描画対象画像
        :type img: numpy.ndarray

        :param geometry_array: [id, x, y] 形式のジオメトリ配列
        :type geometry_array: numpy.ndarray

        :param color: 描画色（B, G, R）
        :type color: tuple

        :param alpha: ポリゴン塗りつぶし時のアルファ値
        :type alpha: float

        :param thickness: 輪郭線の太さ
        :type thickness: int

        :return: ジオメトリを重ね描画した画像
        :rtype: numpy.ndarray

        :raises Exception: 描画処理中にエラーが発生した場合
        """
        try:
            group_ids = np.unique(geometry_array[:, 0].astype(int))

            # 元画像は壊さず、コピーに描画する
            result_img = img.copy()

            # グレースケール画像なら、色付き塗り・色付き線を描けるように BGR 化する
            if len(result_img.shape) == 2:
                result_img = cv2.cvtColor(result_img, cv2.COLOR_GRAY2BGR)

            # Polygon ごとの塗りつぶし
            for gid in group_ids:
                mask = geometry_array[:, 0] == gid
                pts = geometry_array[mask][:, 1:3].astype(np.int32)

                if pts.shape[0] == 0:
                    continue

                # Polygon 判定
                is_polygon = (
                        pts.shape[0] >= 4 and
                        np.array_equal(pts[0], pts[-1])
                )

                # Polygon のときだけ内側を半透明で塗る
                if is_polygon:
                    # Polygon 部分だけを表すマスク画像を作る
                    polygon_mask = np.zeros(result_img.shape[:2], dtype=np.uint8)
                    cv2.fillPoly(polygon_mask, [pts], 255)

                    # 塗り用のオーバーレイ画像を作る
                    overlay = result_img.copy()
                    cv2.fillPoly(overlay, [pts], color)

                    # Polygon 内側だけを半透明合成する
                    blended = cv2.addWeighted(overlay, alpha, result_img, 1.0 - alpha, 0)

                    # マスク部分だけ result_img を blended に置き換える
                    result_img[polygon_mask == 255] = blended[polygon_mask == 255]

            # Point / Line / Polygon の輪郭線を描画
            for gid in group_ids:
                mask = geometry_array[:, 0] == gid
                pts = geometry_array[mask][:, 1:3].astype(np.int32)

                if pts.shape[0] == 0:
                    continue

                # Point
                if pts.shape[0] == 1:
                    cv2.circle(
                        result_img,
                        center=tuple(pts[0]),
                        radius=5,
                        color=color,
                        thickness=-1,
                        lineType=cv2.LINE_AA
                    )
                    continue

                # Polygon 判定
                is_polygon = (
                        pts.shape[0] >= 4 and
                        np.array_equal(pts[0], pts[-1])
                )

                # 輪郭線を描画
                cv2.polylines(
                    result_img,
                    [pts],
                    isClosed=is_polygon,
                    color=color,
                    thickness=thickness,
                    lineType=cv2.LINE_AA
                )

            return result_img

        except Exception as e:
            raise Exception(f"[ERROR draw_geometry_overlay]: {str(e)}")

    def normalize_image_by_colorspace(self, img: np.ndarray, color_space: str) -> np.ndarray:
        """
        属性の ColorSpace を見て、
        入力画像を OpenCV で扱いやすい BGR 形式へ正規化する関数。

        この関数を入れる理由は、
        draw_geometry_overlay() や cv2.imencode() は
        OpenCV前提のチャンネル順で扱ったほうが安定するためである。

        想定する ColorSpace:
        - GRAY
        - BGR
        - BGRA
        - RGB
        - RGBA

        :param img: 入力画像
        :type img: numpy.ndarray
        :param color_space: 属性から取得した ColorSpace
        :type color_space: str
        :return: OpenCV向けに正規化した画像
        :rtype: numpy.ndarray
        """
        try:
            normalized_color_space = str(color_space).strip().upper()

            # 2次元配列はグレースケール画像なので、そのまま返す
            if len(img.shape) == 2:
                return img

            # 3次元配列以外は想定外
            if len(img.shape) != 3:
                raise ValueError(f"未対応の画像shapeです: {img.shape}")

            channel_count = img.shape[2]

            # 3チャンネル画像
            if channel_count == 3:
                if normalized_color_space == "RGB":
                    return cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

                if normalized_color_space in ["BGR", ""]:
                    return img

                raise ValueError(f"未対応のColorSpaceです(3ch): {normalized_color_space}")

            # 4チャンネル画像
            if channel_count == 4:
                if normalized_color_space == "RGBA":
                    return cv2.cvtColor(img, cv2.COLOR_RGBA2BGR)

                if normalized_color_space in ["BGRA", ""]:
                    return cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)

                raise ValueError(f"未対応のColorSpaceです(4ch): {normalized_color_space}")

            raise ValueError(f"未対応のチャンネル数です: {channel_count}")

        except Exception as e:
            raise Exception(f"[ERROR normalize_image_by_colorspace]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        メインの処理の入り口部分。GeoJSON(ピクセル座標)の地物を画像に重ねる

        :param byte_data: 入力データとしてバイト型のフィールドセットファイルを受け取る
        :type byte_data: bytes
        :param attribute: 属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: エンコードされた処理結果画像含むFieldSetFile と attribute
        :rtype: tuple

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            geometry_name = properties["GEOMETRY_NAME"]
            image_name = properties["IMAGE_NAME"]
            color_space = attribute.get("ColorSpace", "BGR")
            r, g, b = map(int, properties["GEOMETRY_COLOR"].split(","))
            color = (b, g, r)
            alpha = float(properties["POLYGON_TRANSMITTANCE"])
            thickness = int(properties["THICKNESS"])
            output_dir = properties["OUTPUTDIR"]
            output_file = properties["OUTPUTFILENAME"]
            ex = properties["EXTENSION"]
            path = os.path.join(output_dir, f"{output_file}.{ex}")

            # FieldSetFileをDataFrameに変換
            df = CU.field_set_file_to_dataframe(byte_data)

            # 処理対象行抽出
            geometry_value = df.loc[df["Dwh"] == geometry_name, "Value"].iloc[0]
            image_value = df.loc[df["Dwh"] == image_name, "Value"].iloc[0]

            # ndarray 抽出
            # ndarray 抽出
            geometry_array = self.extract_ndarray(geometry_value)
            img = self.extract_ndarray(image_value)

            # ColorSpace を見て OpenCV向けの並びへ正規化
            img = self.normalize_image_by_colorspace(
                img=img,
                color_space=color_space
            )

            # 描画
            img = self.draw_geometry_overlay(
                img=img,
                geometry_array=geometry_array,
                color=color,
                alpha=alpha,
                thickness=thickness
            )

            # エンコード
            success, encoded = cv2.imencode(f".{ex}", img)
            if not success:
                raise ValueError("画像のエンコードに失敗しました")

            # 対象ディレクトリが無ければ作成
            dir_path = os.path.dirname(path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            # 画像保存
            encoded.tofile(path)

            # 出力Content候補
            img_base64 = base64.b64encode(encoded).decode("utf-8")

            # DataFrameに挿入
            df.loc[len(df)] = {
                "Dwh": "Overlay",
                "Type": "String",
                "Value": img_base64
            }

            # DataFrameからFieldSetFileを生成
            field_set_file = CU.dataframe_to_field_set_file(df)

            return field_set_file, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
