#!/usr/bin/env python
import os
from piperunner import GEArrayJob, GESeriesJob


def _main():
    try:
        os.mkdir("test")
    except OSError as e:
        if e.errno != 17:
            raise e

    treats = ("piperunner.py", "arrayrunner.py", "qsubparse.py")

    grep_def = GEArrayJob("grep", args=["^\s*def", "{1}"], arg1=treats, cwd=True, binary=True,
                          stderr="/dev/null", stdout="./test/grep_arraytest$TASK_ID.txt")
    sed_bracket = GEArrayJob("sed", args=["-e", "s/(.*//g", "{1}"],
                             arg1=["./test/grep_arraytest{}.txt".format(i) for i in range(1, 4)],
                             cwd=True, binary=True, stderr="/dev/null", stdout="./test/sed_arraytest$TASK_ID.txt")
    # grep "[[:space:]]def" piperunner.py|sed -e "s/(.*//g"
    GESeriesJob((grep_def, sed_bracket), as_array=True).submit()


if __name__ == "__main__":
    _main()
