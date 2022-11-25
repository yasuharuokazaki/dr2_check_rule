import json
import re
import unicodedata
from typing import Dict, List
import pandas as pd


def load_rule(rulepath: str) -> dict:
    """性能フォローシートの抽出ルールのjsonの読み込み"""
    with open(rulepath, "r") as f:
        rule = json.load(f)
    return rule


def load_fs(fs_path: str, sheetname: str) -> pd.DataFrame:
    """性能フォローシートの読み込み

    Args:
        fs_path (str): 読み込むエクセルファイルのパス_
        sheetname (str): 対象のシート名

    Returns:
        pd.DataFrame: 性能フォローシート
    """
    df = pd.read_excel(fs_path, sheet_name=sheetname)
    return df


def extract_select(df: pd.DataFrame, candidates: Dict[str, List[int]]) -> str:
    """■の選択項目の抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        candidates (Dict[str, List[int]]): key:値, value:■がある座標のdict

    Returns:
        str: 該当する項目
    """
    values = []
    for value, coords in candidates.items():
        row, col = coords
        if df.iloc[row, col] == "■":
            values.append(value)
    return ",".join(values)


def extract_number(df: pd.DataFrame, coords: List[int]) -> str:
    """数字記載項目の抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        coords (List[int]): 記載される座標. row, columnのstart, columnのend.

    Returns:
        str: 抽出した数字
    """
    row, col_s, col_e = coords
    cells = df.iloc[row, col_s:col_e].values
    description = "".join([cell for cell in cells if type(cell) == str])
    number = re.sub(r"[^\d.]", "", description)
    return number


def extract_pattern_description(
    df: pd.DataFrame, coords: List[int], prepattern: str, postpattern: str
) -> str:
    """テンプレート項目の記載内容抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        coords (List[int]): 記載される座標. 行数, 列の左端, 列の右端.
        prepattern (str): 抽出前のパターン
        postpattern (str): 抽出直前のパターン

    Returns:
        str: 抽出した記載内容
    """
    row, col_s, col_e = coords
    cells = df.iloc[row, col_s:col_e].values
    description = "".join([cell for cell in cells if type(cell) == str])

    description = unicodedata.normalize("NFKC", description).strip()
    description = re.sub(r"[\u3000 \t]", "", description)

    if not prepattern:
        start = 0
    elif match := re.search(prepattern, description):
        start = match.span()[1]
    else:
        start = 0

    if not postpattern:
        end = len(description)
    elif match := re.search(postpattern, description[start:]):
        end = match.span()[0] + start
    else:
        end = len(description)

    description = description[start:end]
    description = re.sub(r"[\[\]]", "", description)

    return description


def extract_fs1_152(df: pd.DataFrame, fs1_151: int) -> str:
    """fs1_152の記載項目抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        fs1_151 (int): FS-1-151の抽出結果

    Returns:
        str: FS-1-152の該当内容
    """
    if fs1_151 == 0:
        description = df.iloc[30, 35]
        description = unicodedata.normalize("NFKC", description).strip()
        description = re.sub(r"[\u3000 \t]", "", description)
        end = re.search("mm/h", description).span()[0]
    elif fs1_151 == 1:
        description = df.iloc[30, 42]
        description = unicodedata.normalize("NFKC", description).strip()
        description = re.sub(r"[\u3000 \t]", "", description)
        end = re.search("mm/h", description).span()[0]
    elif fs1_151 == 2:
        description = df.iloc[30, 48]
        description = unicodedata.normalize("NFKC", description).strip()
        description = re.sub(r"[\u3000 \t]", "", description)
        end = re.search("mm/10min", description).span()[0]
    else:
        return ""

    return description[:end]


def extract_fs1(df: pd.DataFrame, fs1_rule: dict) -> Dict[str, str]:
    """性能フォローシート1のトリガーテーブル抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        fs1_rule (dict): 性能フォローシート1の抽出ルール

    Returns:
        Dict[str, str]: トリガーテーブルの辞書
    """
    trigger_table: Dict[str, str] = {}
    # 選択項目の抽出
    for trigger_index, candidates in fs1_rule["extract_select"].items():
        select = extract_select(df, candidates)
        trigger_table[trigger_index] = select
    # 数字項目の抽出
    for trigger_index, coords in fs1_rule["extract_number"].items():
        description = extract_number(df, coords)
        trigger_table[trigger_index] = description
    # 記載項目の抽出
    for trigger_index, pattern in fs1_rule["extract_pattern"].items():
        description = extract_pattern_description(df, *pattern.values())
        trigger_table[trigger_index] = description
    # fs-1-152の抽出
    trigger_table["FS-1-152"] = extract_fs1_152(df, trigger_table["FS-1-151"])
    return trigger_table


def extract_fs2(df: pd.DataFrame, fs2_rule: dict) -> Dict[str, str]:
    """性能フォローシート1のトリガーテーブル抽出

    Args:
        df (pd.DataFrame): 抽出元の性能フォローシート
        fs2_rule (dict): 性能フォローシート1の抽出ルール

    Returns:
        Dict[str, str]: トリガーテーブルの辞書
    """
    trigger_table: Dict[str, str] = {}
    # 選択項目の抽出
    for trigger_index, candidates in fs2_rule["extract_select"].items():
        select = extract_select(df, candidates)
        trigger_table[trigger_index] = select
    # 数字項目の抽出
    for trigger_index, coords in fs2_rule["extract_number"].items():
        description = extract_number(df, coords)
        trigger_table[trigger_index] = description
    # 記載項目の抽出
    for trigger_index, pattern in fs2_rule["extract_pattern"].items():
        description = extract_pattern_description(df, *pattern.values())
        trigger_table[trigger_index] = description
    return trigger_table


if __name__ == "__main__":
    import glob
    import os

    xlsxs = glob.glob(os.path.join("教師データ", "*", "*", "*性能フォローシート.xls*"))
    print(len(xlsxs))
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for xlsx_path in xlsxs:
        subject_id = xlsx_path.split("/")[-2]
        # 性能フォローシート1の読み込み
        df_fs1 = load_fs(xlsx_path, "性能ﾌｫﾛｰｼｰﾄ１")
        fs1_rule = load_rule("fs1.json")
        trigger_fs1 = extract_fs1(df_fs1, fs1_rule)

        # 性能フォローシート2の読み込み
        fs2_rule = load_rule("fs2.json")
        df_fs2 = load_fs(xlsx_path, "性能ﾌｫﾛｰｼｰﾄ２")
        trigger_fs2 = extract_fs2(df_fs2, fs2_rule)

        # 抽出結果の保存
        trigger_fs = sorted(dict(**trigger_fs1, **trigger_fs2).items())
        df = pd.DataFrame(trigger_fs)
        df.columns = ["index", "value"]
        df.to_csv(os.path.join(output_dir, f"{subject_id}.csv"), index=None)
