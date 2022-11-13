# -*- coding: utf-8 -*-

import sys
import subprocess

from .paths import dir_project_root, dir_htmlcov, bin_pytest

if sys.platform == "win32":
    open_cmd = "open"
else:
    open_cmd = "open"


def normalize_module(module: str) -> str:
    if module.endswith(".py"):
        return module[:-3]
    else:
        return module


def _run_cov_test(
    bin_pytest: str,
    script: str,
    module: str,
    root_dir: str,
    htmlcov_dir: str,
):
    """
    A simple wrapper around pytest + coverage cli command.
    :param bin_pytest: the path to pytest executable
    :param script: the path to test script
    :param module: the dot notation to the python module you want to calculate
        coverage
    :param root_dir: the dir to dump coverage results binary file
    :param htmlcov_dir: the dir to dump HTML output
    """
    module = normalize_module(module)
    args = [
        bin_pytest,
        "-s", "--tb=native",
        f"--rootdir={root_dir}",
        f"--cov={module}",
        "--cov-report", "term-missing",
        "--cov-report", f"html:{htmlcov_dir}",
        script,
    ]
    subprocess.run(args)


def run_cov_test(script: str, module: str, open_browser: bool = False):
    _run_cov_test(
        bin_pytest=f"{bin_pytest}",
        script=script,
        module=module,
        root_dir=f"{dir_project_root}",
        htmlcov_dir=f"{dir_htmlcov}",
    )
    if open_browser:
        _open_cov_report(module)


def _open_cov_report(module: str):
    module = normalize_module(module)
    parts = module.split(".")
    module_name = parts[-1]
    module_path = str(dir_project_root.joinpath(*parts)) + ".py"
    print(module_path)
    found_cov_report_html = False
    html_path = None
    for p in dir_htmlcov.glob("**/*_py.html"):
        if p.name.endswith(f"{module_name}_py.html"):
            if module_path in p.read_text():
                found_cov_report_html = True
                html_path = str(p)
                break
    if found_cov_report_html:
        subprocess.run([open_cmd, html_path])
