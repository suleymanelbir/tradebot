#!/usr/bin/env python3
import os
import ast
import json
import fnmatch

ROOT_DIR = "/opt/tradebot"
OUTPUT_FILE = "iskelet.txt"
GITIGNORE_FILE = os.path.join(ROOT_DIR, ".gitignore")

# Hariç tutulacak dizin ve dosyalar
EXCLUDE_DIRS = {
    "__pycache__", ".git", ".idea", ".vscode", "venv", "env", "config", "tests", "docs", "scripts", "trade_env"
}
EXCLUDE_FILES = {
    "__init__.py", "setup.py", "pyproject.toml", "requirements.txt", ".env", ".gitignore"
}
INCLUDE_DIRS = {"systemd"}  # Bu dizin özel olarak dahil edilecek

def load_gitignore(path):
    ignore_patterns = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    ignore_patterns.append(line)
    except FileNotFoundError:
        pass
    return ignore_patterns

def is_ignored(path, ignore_patterns):
    for pattern in ignore_patterns:
        if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(os.path.basename(path), pattern):
            return True
    return False

def should_include_dir(dir_name):
    return dir_name in INCLUDE_DIRS or dir_name not in EXCLUDE_DIRS

def summarize_docstring(doc):
    if not doc:
        return ""
    lines = doc.strip().splitlines()
    summary = lines[0] if lines else ""
    return summary[:100].strip()

def extract_python_structure(file_path):
    structure = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=file_path)
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef):
                doc = summarize_docstring(ast.get_docstring(node))
                structure.append(f"  - def {node.name} (line {node.lineno}): {doc}")
            elif isinstance(node, ast.ClassDef):
                structure.append(f"  - class {node.name} (line {node.lineno})")
                for sub in node.body:
                    if isinstance(sub, ast.FunctionDef):
                        doc = summarize_docstring(ast.get_docstring(sub))
                        structure.append(f"    - def {sub.name} (line {sub.lineno}): {doc}")
    except Exception as e:
        structure.append(f"  [HATA: {e}]")
    return structure

def extract_json_keys(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return [f"  - {key}" for key in data.keys()]
        else:
            return ["  [JSON formatı dict değil]"]
    except Exception as e:
        return [f"  [HATA: {e}]"]

def extract_service_summary(file_path):
    summary = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        exec_start = next((line for line in lines if line.strip().startswith("ExecStart=")), None)
        description = next((line for line in lines if line.strip().startswith("Description=")), None)
        if description:
            summary.append(f"  - {os.path.basename(file_path)}: {description.strip().split('=', 1)[-1]}")
        if exec_start:
            summary.append(f"    ExecStart → {exec_start.strip().split('=', 1)[-1]}")
    except Exception as e:
        summary.append(f"  [HATA: {e}]")
    return summary

def main():
    ignore_patterns = load_gitignore(GITIGNORE_FILE)
    file_counter = 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for root, dirs, files in os.walk(ROOT_DIR):
            dirs[:] = [d for d in dirs if should_include_dir(d)]

            for file in files:
                if file in EXCLUDE_FILES:
                    continue
                if not file.endswith((".py", ".json", ".service")):
                    continue

                rel_path = os.path.relpath(os.path.join(root, file), ROOT_DIR)
                if is_ignored(rel_path, ignore_patterns):
                    continue

                full_path = os.path.join(root, file)

                # systemd filtrelemesi
                if "systemd" in root and file.endswith(".service"):
                    if any(k in file for k in ["trade", "bot", "future", "position", "reconciler", "supervisor"]):
                        out.write(f"{file_counter}. {rel_path}\n")
                        summary = extract_service_summary(full_path)
                        out.write("  [Systemd Servis Tanımı]\n")
                        out.write("\n".join(summary) + "\n\n")
                        file_counter += 1
                    continue

                out.write(f"{file_counter}. {rel_path}\n")

                if file.endswith(".py"):
                    structure = extract_python_structure(full_path)
                    out.write("\n".join(structure) + "\n\n")

                elif file.endswith(".json"):
                    keys = extract_json_keys(full_path)
                    out.write("  [JSON Anahtarları]\n")
                    out.write("\n".join(keys) + "\n\n")

                file_counter += 1

if __name__ == "__main__":
    main()
