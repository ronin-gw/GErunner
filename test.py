#!/usr/bin/env python
import os
from piperunner import GEJob, GESeriesJob, GEParallelJob


def _main():
    try:
        os.mkdir("test")
    except OSError as e:
        if e.errno != 17:
            raise e

    grep_class = GEJob(
        "/bin/grep",
        args=["'class '", "piperunner.py"],
        binary=True, cwd=True,
        stdout="./test/grep_class.txt",
        stderr="/dev/null"
    )
    cut_class = GEJob(
        "/bin/cut",
        args=["-d", "' '", "-f", '2', "./test/grep_class.txt"],
        binary=True, cwd=True,
        stdout="./test/cut_class.txt",
        stderr="/dev/null"
    )
    get_class = GESeriesJob([grep_class, cut_class])

    grep_def = GEJob(
        "/bin/grep",
        args=["'def '", "piperunner.py"],
        binary=True, cwd=True,
        stdout="./test/grep_def.txt",
        stderr="/dev/null"
    )
    cut_def = GEJob(
        "/bin/awk",
        args=["'{print $2}'", "./test/grep_def.txt"],
        binary=True, cwd=True,
        stdout="./test/cut_def.txt",
        stderr="/dev/null"
    )
    get_def = GESeriesJob([grep_def, cut_def])

    get_attrs = GEParallelJob([get_class, get_def])
    cat_attrs = GEJob(
        "/bin/cat",
        args=["./test/cut_class.txt", "./test/cut_def.txt"],
        binary=True, cwd=True,
        stdout="./test/cat_attrs.txt",
        stderr="/dev/null"
    )

    pipeline = GESeriesJob([get_attrs, cat_attrs])
    pipeline.submit()


if __name__ == "__main__":
    _main()
