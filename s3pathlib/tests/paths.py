# -*- coding: utf-8 -*-

import sys
from pathlib import Path

dir_project_root = Path(__file__).absolute().parent.parent.parent

# code structure
dir_tests: Path = dir_project_root / "tests"
dir_htmlcov: Path = dir_project_root / "htmlcov"
path_cov_index_html = dir_htmlcov / "index.html"

# virtual environment
dir_bin: Path = Path(sys.executable).parent
bin_pytest: Path = dir_bin / "pytest"

# test data
dir_test_data: Path = dir_tests / "test_data"
dir_test_iter_objects_folder: Path = dir_tests / "list_objects_folder"
dir_test_upload_dir_folder: Path = dir_tests / "upload_dir_folder"
