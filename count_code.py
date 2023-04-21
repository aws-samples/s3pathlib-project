# -*- coding: utf-8 -*-

import typing as T
import json
import subprocess
from pathlib_mate import Path

dir_project_root = Path.dir_here(__file__).absolute()


def cloc(path: T.Union[Path, T.List[Path]]) -> dict:
    if isinstance(path, list):
        cloc_list_file.write_text("\n".join([str(p) for p in path]))
        args = [
            "cloc",
            f"--list-file={cloc_list_file}",
            "--json",
        ]
    else:
        args = [
            "cloc",
            f"{path}",
            "--json",
        ]
    result = subprocess.run(args, capture_output=True)
    data = json.loads(result.stdout.decode("utf-8"))
    del data["header"]
    return data


def count_code(title, path: T.Union[Path, T.List[Path]]):
    data = cloc(path)
    print(f"-------------------- {title} --------------------")
    print(json.dumps(data, indent=4))


if __name__ == "__main__":
    cloc_list_file = dir_project_root.joinpath(".cloc-list-file")

    count_code(
        "source code",
        [
            dir_project_root.joinpath("s3pathlib"),
        ],
    )
    count_code(
        "test code",
        [
            dir_project_root.joinpath("tests"),
        ],
    )
