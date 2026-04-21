# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable=None, *args, **kwargs):
        return iterable


INPUT_FILE_NAME = "1510-过程组件分析结果-20260421.xlsx"
INPUT_FILE_GLOB = "1510-*20260421.xlsx"
INPUT_SHEET_NAME = "分析结果"
CHILD_COLUMN = "子件号"

BASE_NEW_COLUMNS = ["成套模具号", "初始模具号"]
VERSION_SUMMARY_COLUMNS = ["变更履历", "当前版本", "版本数量"]
DEFAULT_VERSION_COLUMNS = [f"{letter}版" for letter in "ABCDEF"]


def configure_console_encoding() -> None:
    """尽量保证 Windows 控制台中文提示正常显示。"""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_input_file() -> Path:
    exact_path = Path(INPUT_FILE_NAME)
    if exact_path.exists():
        return exact_path

    matches = sorted(Path(".").glob(INPUT_FILE_GLOB))
    if matches:
        return matches[0]

    raise FileNotFoundError(f"未找到输入文件: {INPUT_FILE_NAME}")


def build_output_path(input_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return input_path.with_name(f"{input_path.stem}【处理后】{timestamp}{input_path.suffix}")


def split_child_no(value: object) -> tuple[str, str, str]:
    AA = ""
    BB = ""
    CC = ""

    text = clean_text(value)
    if not text:
        return AA, BB, CC

    if "/" in text:
        AA = text.split("/", 1)[0].strip()
    else:
        AA = text

    if AA and AA[-1].isalpha():
        BB = AA[:-1]
        CC = AA[-1].upper()
    else:
        BB = AA

    return AA, BB, CC


def main() -> None:
    configure_console_encoding()

    log("开始处理分析结果模具版本列。")
    input_path = find_input_file()
    log(f"读取文件: {input_path.resolve()}")
    log(f"读取工作表: {INPUT_SHEET_NAME}")

    try:
        df1 = pd.read_excel(input_path, sheet_name=INPUT_SHEET_NAME)
    except ValueError as exc:
        raise ValueError(f"未找到工作表: {INPUT_SHEET_NAME}") from exc

    if CHILD_COLUMN not in df1.columns:
        raise KeyError(f"分析结果缺少必要列: {CHILD_COLUMN}")

    log(f"读取完成: df1 共 {len(df1)} 行，{len(df1.columns)} 列。")

    log("步骤1/5: 解析子件号，生成成套模具号、初始模具号和版本字母。")
    parsed_rows: list[tuple[str, str, str]] = []
    found_versions: set[str] = set()

    for row_idx in tqdm(df1.index, total=len(df1), desc="解析子件号", unit="行", file=sys.stdout):
        AA = ""
        BB = ""
        CC = ""
        AA, BB, CC = split_child_no(df1.at[row_idx, CHILD_COLUMN])
        parsed_rows.append((AA, BB, CC))
        if CC:
            found_versions.add(CC)

    extra_versions = sorted(found_versions - set("ABCDEF"))
    version_columns = DEFAULT_VERSION_COLUMNS + [f"{letter}版" for letter in extra_versions]
    new_columns = BASE_NEW_COLUMNS + VERSION_SUMMARY_COLUMNS + version_columns

    log("步骤2/5: 追加空白列。")
    for col in new_columns:
        df1[col] = ""
    log("已新增变更履历、当前版本、版本数量列。")

    if found_versions:
        detected = "、".join(sorted(found_versions))
        version_cols_text = "、".join(version_columns)
        log(f"检测到版本字母: {detected}")
        log(f"将写入版本列: {version_cols_text}")
    else:
        log("未检测到版本字母，仅保留默认 A版-F版 空白列。")

    log("步骤3/5: 回填成套模具号、初始模具号和版本列。")
    for row_idx, (AA, BB, CC) in tqdm(
        zip(df1.index, parsed_rows),
        total=len(parsed_rows),
        desc="回填版本列",
        unit="行",
        file=sys.stdout,
    ):
        df1.at[row_idx, "成套模具号"] = AA
        df1.at[row_idx, "初始模具号"] = BB
        if CC:
            df1.at[row_idx, f"{CC}版"] = CC

    log("步骤4/5: 按初始模具号合并变更履历并计算当前版本。")
    versions_by_initial: dict[str, set[str]] = {}
    for row_idx in tqdm(df1.index, total=len(df1), desc="建立版本分组", unit="行", file=sys.stdout):
        initial_mold_no = clean_text(df1.at[row_idx, "初始模具号"])
        if not initial_mold_no:
            continue

        versions_by_initial.setdefault(initial_mold_no, set())
        for col in version_columns:
            version = clean_text(df1.at[row_idx, col])
            if version:
                versions_by_initial[initial_mold_no].add(version)

    version_order = [col.removesuffix("版") for col in version_columns]
    version_summary_by_initial: dict[str, tuple[str, str, int]] = {}
    for initial_mold_no, versions in versions_by_initial.items():
        ordered_versions = [version for version in version_order if version in versions]
        if ordered_versions:
            history = f"【{''.join(ordered_versions)}】"
            current_version = f"-{ordered_versions[-1]}-"
            version_summary_by_initial[initial_mold_no] = (history, current_version, len(ordered_versions))

    for row_idx in tqdm(df1.index, total=len(df1), desc="回填分组版本", unit="行", file=sys.stdout):
        initial_mold_no = clean_text(df1.at[row_idx, "初始模具号"])
        summary = version_summary_by_initial.get(initial_mold_no)
        if summary:
            df1.at[row_idx, "变更履历"] = summary[0]
            df1.at[row_idx, "当前版本"] = summary[1]
            df1.at[row_idx, "版本数量"] = summary[2]

    output_path = build_output_path(input_path)
    log("步骤5/5: 写出处理后的分析结果。")
    log(f"输出文件: {output_path.resolve()}")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name=INPUT_SHEET_NAME, index=False)

    log("处理完成。")


if __name__ == "__main__":
    main()
