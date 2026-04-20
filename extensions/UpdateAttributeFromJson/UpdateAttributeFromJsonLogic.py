# MIT License
#
# Copyright (c) 2026 NTT InfraNet
# Copyright (c) 2026 NTT DATA Japan Co., Ltd.
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

import json
from importlib import import_module

pd = import_module("pandas")

class UpdateAttributeFromJsonLogic:
    """
    UpdateAttributeFromJson の実処理部分。
    content の JSON をフラット化して属性に展開する。
    """

    def __call__(self, content: bytes, attributes: dict, properties: dict):
        if not content:
            raise ValueError("content が空です。")

        # -----------------------------
        # JSON 読み込み
        # -----------------------------
        try:
            json_data = json.loads(content.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"JSON の読み込みに失敗しました: {e}")

        # pandas でフラット化
        df = pd.json_normalize(json_data, sep='.')
        flat_dict = df.to_dict(orient='records')[0] if not df.empty else {}

        # -----------------------------
        # 重複キー処理
        # -----------------------------
        strategy = str(properties.get("UPDATE_MODE", "overwrite")).strip().lower()
        prefix = str(properties.get("KEY_NAME_PREFIX", "")).strip()
        new_attributes = {}

        for k, v in flat_dict.items():
            key = str(k)
            value = str(v)

            if prefix:
                key = f"{prefix}.{key}"

            if key in attributes:

                # --- 重複キーがあった場合の分岐 ---
                if strategy == "overwrite":
                    # 値を上書きする
                    attributes[key] = value

                elif strategy == "skip":
                    # 値を上書きしない(スキップする)
                    continue

                elif strategy == "addsuffix":
                    # キーに接尾語を付与(1から始まる連番。無制限。)
                    i = 1
                    new_key = f"{key}.{i}"
                    while new_key in attributes or new_key in new_attributes:
                        i += 1
                        new_key = f"{key}.{i}"
                    new_attributes[new_key] = value

                elif strategy == "failure":
                    # Failure(異常終了)にする
                    raise ValueError(f"属性キーが重複しています: {key}")

                else:
                    raise ValueError(
                        f"UpdateMode プロパティの値が無効です: {strategy}"
                    )

            else:
                new_attributes[key] = value

        # -----------------------------
        # 属性統合
        # -----------------------------
        attributes.update(new_attributes)

        attributes["mime.type"] = "application/json"

        # -----------------------------
        # ContentClear プロパティに基づく content 制御
        # -----------------------------
        clear_flag = str(properties.get("CLEAR_CONTENTS", "false")).strip().lower()
        new_content = b"" if clear_flag == "true" else content

        return new_content, attributes
