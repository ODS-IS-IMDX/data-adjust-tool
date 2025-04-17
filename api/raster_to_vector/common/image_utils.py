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
from importlib import import_module

# 外部ライブラリの動的インポート
cv2 = import_module("cv2")
np = import_module("numpy")

def calc_line_thickness(line_list, img_gray, threshold=200):
    """
    与えられた複数の線分に対して、画像上での太さ(厚み)を計測します。

    Parameters
    ----------
    line_list : list
        [(x1,y1), (x2,y2)] 形式の直線座標ペアを要素とするリスト
    img_gray : numpy.ndarray
        グレースケール画像データ（2次元配列）
    threshold : int, optional
        太さ計測時に用いる画素値閾値、デフォルトは200

    Returns
    -------
    thickness_list : list
        対応する線分ごとの太さを格納したリスト
    """

    thickness_list = []

    # debug_img = cv2.cvtColor(img_gray, cv2.COLOR_GRAY2BGR)

    for index, line in enumerate(line_list):
        ((x1, y1), (x2, y2)) = line
        thickness = measure_line_thickness_parallel_refined(
            (x1, y1, x2, y2),
            img_gray,
            threshold=threshold,
            max_offset=6)

        # display_thickness_at_center_line(x1, y1, x2, y2, 0, thickness, debug_img)
        # Image.fromarray(debug_img).show()

        thickness_list.append(thickness)

    return thickness_list


def measure_line_thickness_parallel_refined(line, img, threshold=200, max_offset=6):
    """
    与えられた線分付近で、法線方向にオフセットをずらしながら最適な中心線を特定し、
    そのライン上で太さ（画素値が一定以上暗い領域の広がり）を計測します。
    オフセット範囲を探索し、0より大きなratioが得られるラインを元に厚みを求めます。

    Parameters
    ----------
    line : tuple
        (x1, y1, x2, y2) 形式の直線座標
    img : numpy.ndarray
        グレースケール画像（2次元配列）
    threshold : int, optional
        ピクセル値判定用閾値。これより小さい(暗い)ピクセルを線とみなします。
    max_offset : int, optional
        線に垂直方向へずらす最大ピクセル数。デフォルト6。

    Returns
    -------
    thickness_count : float
        求めた線の厚さ（ratio値の合計値）。0の場合は線なし判定。
    """
    x1, y1, x2, y2 = line
    dx = x2 - x1
    dy = y2 - y1
    length = int(np.hypot(dx, dy))
    if length == 0:
        return 0

    # 正規化された方向ベクトル
    dir_vec = np.array([dx / length, dy / length])
    # 垂直方向ベクトル(正規化)
    perp_vec = np.array([-dir_vec[1], dir_vec[0]])

    # 元のライン付近の中央を求める
    cx = (x1 + x2) / 2.0
    cy = (y1 + y2) / 2.0

    def get_line_pixels(cx, cy, offset):
        # offsetピクセル垂直にずらす
        start_x = cx + offset * perp_vec[0] - (length / 2) * dir_vec[0]
        start_y = cy + offset * perp_vec[1] - (length / 2) * dir_vec[1]

        line_pixels = []
        for i in range(length):
            px = int(start_x + i * dir_vec[0])
            py = int(start_y + i * dir_vec[1])
            if 0 <= px < img.shape[1] and 0 <= py < img.shape[0]:
                line_pixels.append(img[py, px])
            else:
                # 画像外に出たらline_pixelsを空で返す
                return []
        return line_pixels

    def calc_ratio(line_pixels):
        if len(line_pixels) == 0:
            return 0.0
        count_below_threshold = sum(p <= threshold for p in line_pixels)
        ratio = count_below_threshold / len(line_pixels)
        return ratio

    # Step1: offsetを-6から6まで試して最大ratioのラインを探す
    candidate_offsets = range(-max_offset, max_offset+1)
    ratios = []
    best_ratio = -1.0
    best_offset = 0

    for off in candidate_offsets:
        line_pixels = get_line_pixels(cx, cy, off)
        ratio = calc_ratio(line_pixels)
        if ratio > best_ratio:
            best_ratio = ratio
            best_offset = off
        elif ratio == best_ratio:
            # 同率の場合、中心に近い方（絶対値が小さいoffset）を採用
            if abs(off) < abs(best_offset):
                best_offset = off

        if 0 < ratio:
            ratios.append(ratio)

    # ratiosがすべて0以下なら線なしと判定
    if len(ratios) == 0:
        return 0

    # best_offsetで得られたラインを新しい中心とする
    def is_line_at_offset(offset):
        line_pixels = get_line_pixels(cx, cy, best_offset + offset)
        ratio = calc_ratio(line_pixels)
        return ratio

    thickness_count = 0

    # 正方向(+offset)へ探索
    for off in range(0, max_offset+1):
        ratio = is_line_at_offset(off)
        if 0.1 < ratio:
            thickness_count += ratio
        else:
            break

    # 負方向(-offset)へ探索
    for off in range(-1, -(max_offset+1), -1):
        ratio = is_line_at_offset(off)
        if 0.1 < ratio:
            thickness_count += ratio
        else:
            break

    return thickness_count


def display_thickness_at_center_line(x1, y1, x2, y2, angle, thickness, output_image):
    """
    線の中心付近に太さ情報を表示し、線を緑色で描画する。

    Parameters
    ----------
    x1, y1, x2, y2 : int
        線分の始点と終点座標
    angle : float
        線の角度情報（未使用だがインターフェースとして保持）
    thickness : float
        計測された線の太さ
    output_image : numpy.ndarray
        線と太さ文字情報を書き込む対象の画像データ

    Returns
    -------
    output_image : numpy.ndarray
        線と文字が描画された画像を返します。
    """
    # 線の中央点を計算
    center_x = (x1 + x2) // 2
    center_x += 5
    center_y = (y1 + y2) // 2
    center_y -= 7

    # 線を描画（緑色）
    line_color = (0, 255, 0)  # 緑色 (BGR形式)
    line_thickness = 2  # 線の太さ
    cv2.line(output_image, (x1, y1), (x2, y2), line_color, line_thickness)

    # 太さを文字列に変換
    thickness_text = f"{thickness:.2f}"
    # thickness_text = f"{thickness:.2f}, {angle:.1f}"

    # テキストを描画（赤字）
    font = cv2.FONT_HERSHEY_SIMPLEX
    # font_scale = 0.7  # テキストのサイズ
    font_scale = 0.5  # テキストのサイズ
    color = (255, 0, 0)  # 赤色 (BGR形式)
    thickness_line = 1  # テキストの線の太さ

    # テキストを中央点に描画
    cv2.putText(output_image, thickness_text, (center_x, center_y), font, font_scale, color, thickness_line)

    print(f"太さ {thickness_text} を座標 ({center_x}, {center_y}) に表示しました。")
    return output_image

