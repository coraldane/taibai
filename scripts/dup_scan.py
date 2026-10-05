#!/usr/bin/env python3
"""重复代码扫描与防回潮门禁。

用法:
  python scripts/dup_scan.py scan                 # 输出报告到 target/dup_report.md
  python scripts/dup_scan.py baseline             # 生成/覆盖 scripts/dup_baseline.json
  python scripts/dup_scan.py check                # 对比基线，重复/allow 预算超限则退出码 1

原理: 以"显著行"(去空白、去注释、字符串字面量归一化)的 9 行滑动窗口做指纹,
贪心向两侧扩展得到重复块。窗口太小会误报惯用法,太小阈值会漏报,
默认参数为 2026-10 代码治理时标定的诊断基线,请勿随意调整。
"""

import hashlib
import json
import os
import re
import sys
from collections import defaultdict

WIN = 9          # 滑动窗口大小(显著行)
MIN_EXT = 12     # 报告的最小扩展块长度(显著行)
TOLERANCE = 50   # check 模式允许的非测试重复行新增量(约一个块)

IS_TEST_RE = re.compile(r"(^|/)([^/]*_test\.rs|tests/.*)$")


def normalize(line):
    s = line.strip()
    if not s or s.startswith("//"):
        return None
    s = re.sub(r'"(?:\\.|[^"\\])*"', '"S"', s)
    s = re.sub(r"'(?:\\.|[^'\\])*'", "'C'", s)
    s = re.sub(r"//.*$", "", s)
    s = re.sub(r"\s+", "", s)
    return s or None


def collect_files(root):
    files = {}
    for dirpath, _, names in os.walk(root):
        for n in names:
            if not n.endswith(".rs"):
                continue
            p = os.path.join(dirpath, n)
            rel = os.path.relpath(p, root).replace("\\", "/")
            with open(p, encoding="utf-8", errors="replace") as f:
                raw = f.readlines()
            norm = [normalize(l) for l in raw]
            sig = [(i, t) for i, t in enumerate(norm) if t is not None]
            files[rel] = {"raw": raw, "sig": sig}
    return files


def find_blocks(files):
    index = defaultdict(list)
    for rel, d in files.items():
        sig = d["sig"]
        for k in range(len(sig) - WIN + 1):
            window = tuple(t for _, t in sig[k:k + WIN])
            h = hashlib.md5("\n".join(window).encode()).hexdigest()
            index[h].append((rel, k))

    pairs = set()
    for occs in index.values():
        if len(occs) < 2:
            continue
        for a in range(len(occs)):
            for b in range(a + 1, len(occs)):
                (f1, k1), (f2, k2) = occs[a], occs[b]
                if f1 == f2 and abs(k1 - k2) < WIN:
                    continue
                pairs.add((f1, k1, f2, k2))

    def sig_lines(rel):
        return [t for _, t in files[rel]["sig"]]

    def raw_pos(rel, k):
        return files[rel]["sig"][k][0] + 1

    blocks, seen = [], set()
    for f1, k1, f2, k2 in pairs:
        s1, s2 = sig_lines(f1), sig_lines(f2)
        n = 0
        while k1 + n < len(s1) and k2 + n < len(s2) and s1[k1 + n] == s2[k2 + n]:
            n += 1
        m = 0
        while k1 - m - 1 >= 0 and k2 - m - 1 >= 0 and s1[k1 - m - 1] == s2[k2 - m - 1]:
            m += 1
        length = n + m
        if length < MIN_EXT:
            continue
        start1, start2 = k1 - m, k2 - m
        key = frozenset([
            (f1, start1, start1 + length),
            (f2, start2, start2 + length),
        ])
        if key in seen:
            continue
        seen.add(key)
        blocks.append({
            "len": length,
            "test": IS_TEST_RE.search(f1) is not None or IS_TEST_RE.search(f2) is not None,
            "a": {"file": f1, "start": raw_pos(f1, start1), "end": raw_pos(f1, start1 + length - 1)},
            "b": {"file": f2, "start": raw_pos(f2, start2), "end": raw_pos(f2, start2 + length - 1)},
        })
    blocks.sort(key=lambda x: -x["len"])
    return blocks


def count_allows(root):
    """clippy allow 预算:新代码禁止再膨胀函数签名,计数只降不升。"""
    counts = {"too_many_arguments": 0, "type_complexity": 0}
    for dirpath, _, names in os.walk(root):
        for n in names:
            if not n.endswith(".rs"):
                continue
            with open(os.path.join(dirpath, n), encoding="utf-8", errors="replace") as f:
                text = f.read()
            for key in counts:
                counts[key] += len(re.findall(r"allow\(\s*clippy::" + key, text))
    return counts


def summarize(files, blocks):
    per_file = defaultdict(int)
    total = non_test = 0
    for blk in blocks:
        total += blk["len"]
        if not blk["test"]:
            non_test += blk["len"]
            per_file[blk["a"]["file"]] += blk["len"]
            per_file[blk["b"]["file"]] += blk["len"]
    return {
        "block_count": len(blocks),
        "total_dup_lines": total,
        "non_test_dup_lines": non_test,
        "top_non_test_files": dict(sorted(per_file.items(), key=lambda x: -x[1])[:20]),
        "allows": count_allows(files and os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")),
    }


def repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def write_report(root, files, blocks, summary, out_path):
    lines = [
        f"# Duplication scan (window={WIN} sig-lines, min block={MIN_EXT})",
        "",
        f"blocks={len(blocks)} total_dup_lines={summary['total_dup_lines']} "
        f"non_test_dup_lines={summary['non_test_dup_lines']}",
        "",
        "## Top blocks",
    ]
    for blk in blocks[:120]:
        lines.append(
            f"- [{blk['len']} lines] `{blk['a']['file']}:{blk['a']['start']}-{blk['a']['end']}`"
            f"  <->  `{blk['b']['file']}:{blk['b']['start']}-{blk['b']['end']}`"
        )
    lines += ["", "## Per-file duplicated lines (non-test, double-counted)"]
    for f, v in summary["top_non_test_files"].items():
        lines.append(f"- {v:6d} lines  `{f}`")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    root = repo_root()
    src = os.path.join(root, "src")
    files = collect_files(src)
    blocks = find_blocks(files)
    summary = summarize(files, blocks)
    report = os.path.join(root, "target", "dup_report.md")

    if cmd == "baseline":
        path = os.path.join(root, "scripts", "dup_baseline.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"baseline written: {path}")
        print(f"  non_test_dup_lines={summary['non_test_dup_lines']} blocks={summary['block_count']} allows={summary['allows']}")
        return 0

    if cmd == "check":
        path = os.path.join(root, "scripts", "dup_baseline.json")
        with open(path, encoding="utf-8") as f:
            baseline = json.load(f)
        failed = False
        d = summary["non_test_dup_lines"] - baseline["non_test_dup_lines"]
        print(f"non-test duplicated lines: {summary['non_test_dup_lines']} (baseline {baseline['non_test_dup_lines']}, delta {d:+d})")
        if d > TOLERANCE:
            print(f"FAIL: non-test duplication grew by {d} lines (tolerance {TOLERANCE})")
            failed = True
        for key, limit in baseline["allows"].items():
            now = summary["allows"][key]
            print(f"allow(clippy::{key}): {now} (baseline {limit})")
            if now > limit:
                print(f"FAIL: allow(clippy::{key}) count increased {limit} -> {now}")
                failed = True
        write_report(root, files, blocks, summary, report)
        if not failed:
            print("OK")
        return 1 if failed else 0

    write_report(root, files, blocks, summary, report)
    print(f"blocks={summary['block_count']} total={summary['total_dup_lines']} "
          f"non_test={summary['non_test_dup_lines']} report={report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
