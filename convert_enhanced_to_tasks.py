import argparse
import json
import re
from pathlib import Path

TEST_PATTERNS = (
    "test/", "tests/", "spec/", "__tests__", "/test_", "_test.",
    "/spec_", "_spec."
)

def is_test_file(path: str) -> bool:
    p = path.lower()
    if any(tok in p for tok in TEST_PATTERNS):
        return True
    if re.search(r"(^|/)(test_|spec_).+\.", p):
        return True
    if re.search(r"(_test|_spec)\.[a-z0-9]+$", p):
        return True
    return False

def build_file_diff(path: str, patch_body: str) -> str:
    patch_body = patch_body.rstrip("\n")
    header = [
        f"diff --git a/{path} b/{path}",
        f"--- a/{path}",
        f"+++ b/{path}",
        patch_body,
        ""
    ]
    return "\n".join(header)

def extract_pr_header(segments):
    return next((s for s in segments if s.get("segment_type") == "pr_header"), {})

def extract_repo_name(segments, pr_header):
    if pr_header.get("repo_name"):
        return pr_header["repo_name"]
    ctx = next((s for s in segments if s.get("segment_type") == "context"), {})
    return ctx.get("repo_name", "")

def extract_language(segments, pr_header):
    if pr_header.get("language"):
        return pr_header["language"]
    ctx = next((s for s in segments if s.get("segment_type") == "context"), {})
    intro = ctx.get("repo_intro", "")
    # strip common markdown markers and match more loosely
    intro_clean = re.sub(r"[`*_]", "", intro)
    m = re.search(r"language[^\\w]{0,3}[:\\-]\\s*([A-Za-z0-9_+.-]+)", intro_clean, flags=re.IGNORECASE)
    return m.group(1) if m else ""

def collect_base_commit(segments):
    commits = set()
    for seg in segments:
        if seg.get("segment_type") != "context":
            continue
        for rf in seg.get("related_files", []) or []:
            c = rf.get("commit")
            if c:
                commits.add(c)
    return commits

def convert_one(obj, skip_missing_commit=True):
    segments = obj.get("segments", [])
    pr_number = obj.get("pr_number")
    repo_id = obj.get("repo_id")

    pr_header = extract_pr_header(segments)
    repo_name = extract_repo_name(segments, pr_header)
    commits = collect_base_commit(segments)
    if not commits:
        if skip_missing_commit:
            return None
        base_commit = ""
    else:
        base_commit = next(iter(commits))

    file_diffs = []
    test_diffs = []
    for seg in segments:
        if seg.get("segment_type") != "pr_commit":
            continue
        for fobj in seg.get("files", []) or []:
            path = fobj.get("path")
            patch_body = fobj.get("patch", "")
            if not path or not patch_body:
                continue
            diff = build_file_diff(path, patch_body)
            file_diffs.append(diff)
            if is_test_file(path):
                test_diffs.append(diff)

    full_patch = "\n".join(file_diffs)
    test_patch = "\n".join(test_diffs)

    title = pr_header.get("title", "") or ""
    desc = pr_header.get("description", "") or ""
    problem_statement = f"{title}\n{desc}".strip()

    instance_id = f"{repo_name.replace('/', '__')}-{pr_number}"
    language = extract_language(segments, pr_header)

    return {
        "instance_id": instance_id,
        "repo": repo_name,
        "pull_number": pr_number,
        "base_commit": base_commit,
        "patch": full_patch,
        "test_patch": test_patch,
        "problem_statement": problem_statement,
        "version": "",  # fill later via versioning scripts
        "language": language,
        "created_at": pr_header.get("created_at", ""),
        "repo_id": repo_id,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="enhanced_data_*.jsonl")
    ap.add_argument("--output", required=True, help="converted tasks jsonl")
    ap.add_argument("--skip-missing-commit", action="store_true", help="skip PRs without related_files.commit")
    args = ap.parse_args()

    src = Path(args.input)
    out_path = Path(args.output)

    kept = 0
    skipped = 0
    with src.open(encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            record = convert_one(obj, skip_missing_commit=args.skip_missing_commit)
            if record is None:
                skipped += 1
                continue
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            kept += 1
    print(f"Done. Kept {kept}, skipped {skipped}. Output -> {out_path}")

if __name__ == "__main__":
    main()
