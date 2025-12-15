import os
import shutil
import subprocess
import re
import json
import argparse
from contextlib import contextmanager
from typing import List, Dict, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

def run_command(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {cmd}, {e}")
        raise

def is_git_repo_path(path: str) -> bool:
    """
    Check whether a path looks like a git repository.
    Supports normal clones (with .git/) and bare repos (no .git directory).
    """
    if not os.path.isdir(path):
        return False
    if os.path.isdir(os.path.join(path, ".git")):
        return True
    head = os.path.join(path, "HEAD")
    objects = os.path.join(path, "objects")
    return os.path.isfile(head) and os.path.isdir(objects)

def get_version_by_git(cloned_dir: str) -> str:
    if not os.path.isdir(cloned_dir):
        raise NotADirectoryError(f"Invalid directory: {cloned_dir}")
    with cd(cloned_dir):
        result = run_command(["git", "describe", "--tags"], capture_output=True, text=True)
        version = result.stdout.strip()
        print(f"✔️ Current version: {version}")
        match = re.search(r"(\d+\.\d+)(?:\.\d+)?", version)
        if match:
            return match.group(1)
        raise RuntimeError(f"Unrecognized version: {version}")

def get_instances(instance_path: str) -> List[Dict]:
    if instance_path.endswith((".jsonl", ".jsonl.all")):
        with open(instance_path, encoding="utf-8") as f:
            return [json.loads(line) for line in f]
    with open(instance_path, encoding="utf-8") as f:
        return json.load(f)

def prepare_repo_cache(tasks: List[Dict], cache_dir: str, local_cache_dir: Optional[str] = None, skip_missing_repo: bool = False) -> Dict[str, str]:
    os.makedirs(cache_dir, exist_ok=True)
    repo_cache = {}
    for task in tasks:
        repo = task["repo"]
        if repo in repo_cache:
            continue
        if local_cache_dir:
            owner_repo_flat = repo.replace("/", "__")
            # 支持多种本地布局，包括 repo.git 目录
            candidates = [
                os.path.join(local_cache_dir, owner_repo_flat),
                os.path.join(local_cache_dir, owner_repo_flat + ".git"),
                os.path.join(local_cache_dir, *repo.split("/")),
                os.path.join(local_cache_dir, *repo.split("/")) + ".git",
                os.path.join(local_cache_dir, repo.split("/")[-1]),
                os.path.join(local_cache_dir, repo.split("/")[-1] + ".git"),
            ]
            local_path = next((p for p in candidates if is_git_repo_path(p)), None)
            if local_path:
                repo_cache[repo] = os.path.abspath(local_path)
                print(f"Reusing cached repo: {repo}")
                continue
            if skip_missing_repo:
                print(f"Skip missing local repo: {repo}")
                continue
            print(f"Missing local repo for {repo} and cloning is disabled.")
            continue
        repo_url = f"https://github.com/{repo}.git"
        local_path = os.path.join(cache_dir, repo.replace("/", "__"))
        try:
            run_command(["git", "clone", repo_url, local_path], capture_output=True)
            repo_cache[repo] = local_path
            print(f"✅ Cached repo: {repo}")
        except Exception as e:
            print(f"❌ Failed to clone {repo}: {e}")
    return repo_cache

def process_repo_task(task: Dict, testbed: str, repo_cache: Dict[str, str]) -> Dict | None:
    instance_id = task["instance_id"]
    repo = task["repo"]
    base_commit = task["base_commit"]
    repo_dir = os.path.join(testbed, instance_id)
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir, ignore_errors=True)

    try:
        cached_repo = repo_cache.get(repo)
        if not cached_repo or not os.path.exists(cached_repo):
            raise RuntimeError(f"Missing cached repo for {repo}")
        # If cache has a working tree (.git present), copy; otherwise treat as bare and clone
        if os.path.isdir(os.path.join(cached_repo, ".git")):
            shutil.copytree(cached_repo, repo_dir, dirs_exist_ok=True)
        else:
            run_command(["git", "clone", cached_repo, repo_dir], capture_output=True)
        # mark the working dir as safe to avoid ownership warnings
        run_command(["git", "config", "--global", "--add", "safe.directory", repo_dir], capture_output=True)
        with cd(repo_dir):
            run_command(["git", "checkout", base_commit], capture_output=True)
        version = get_version_by_git(repo_dir)
        result = task.copy()
        result["version"] = version
        return result
    except Exception as e:
        print(f"❌ Failed: {instance_id} | {e}")
        return None
    finally:
        shutil.rmtree(repo_dir, ignore_errors=True)

def process_repos(tasks: List[Dict], testbed: str, repo_cache: Dict[str, str], max_workers: int = 4) -> tuple[List[Dict], List[Dict]]:
    os.makedirs(testbed, exist_ok=True)
    results, failures = [], []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_repo_task, t, testbed, repo_cache): t for t in tasks
        }
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                else:
                    failures.append(task)
            except Exception as e:
                print(f"Unexpected error in {task['instance_id']}: {e}")
                failures.append(task)
    return results, failures

def save_results(results: List[Dict], output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if output_path.endswith((".jsonl", ".jsonl.all")):
        with open(output_path, "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

def generate_output_path(instance_path: str, suffix="_versions") -> str:
    base, ext = os.path.splitext(instance_path)
    return f"{base}{suffix}{ext}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance_path", type=str, required=True, help="Path to input task file (.json or .jsonl)")
    parser.add_argument("--testbed", type=str, required=True, help="Temp working directory for cloning repos")
    parser.add_argument("--local-cache-dir", type=str, default=None, help="Path to pre-cloned repos (skip cloning when present)")
    parser.add_argument("--skip-missing-repo", action="store_true", help="When using --local-cache-dir, skip tasks whose repo is not found locally")
    parser.add_argument("--max-workers", type=int, default=10, help="Number of processes (default: 4)")
    args = parser.parse_args()

    try:
        tasks = get_instances(args.instance_path)
    except Exception as e:
        print(f"❌ Error reading instance file: {e}")
        return

    required_keys = {"repo", "base_commit", "instance_id"}
    for t in tasks:
        if not required_keys.issubset(t):
            print(f"Invalid task format: {t}")
            return

    repo_cache_dir = os.path.join(args.testbed, "_cache")
    repo_cache = prepare_repo_cache(
        tasks,
        repo_cache_dir,
        local_cache_dir=args.local_cache_dir,
        skip_missing_repo=args.skip_missing_repo,
    )

    if args.local_cache_dir and args.skip_missing_repo:
        tasks = [t for t in tasks if t["repo"] in repo_cache]

    results, failures = process_repos(tasks, args.testbed, repo_cache, args.max_workers)

    output_path = generate_output_path(args.instance_path, "_versions")
    save_results(results, output_path)
    print(f"\n✅ {len(results)} results saved to {output_path}")

    if failures:
        fail_path = generate_output_path(args.instance_path, "_failures")
        save_results(failures, fail_path)
        print(f"⚠️  {len(failures)} failures saved to {fail_path}")

    for r in results:
        print(json.dumps(r, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
