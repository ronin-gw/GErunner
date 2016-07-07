#!/usr/bin/env python
import argparse
import os
import subprocess
import shlex


def _main():
    parser = argparse.ArgumentParser(description='Array Job Runner. Try all argument combinations')
    parser.add_argument("commands", nargs=argparse.REMAINDER, help="Job command")

    for i in range(1, 100):
        meta = "-{}".format(i)
        dest = "a{}".format(i)
        helpstr = "Argument {}".format(i)
        parser.add_argument(meta, dest=dest, nargs="+", help=helpstr, default=None)

    options = parser.parse_args()
    exit_status = 0

    args = tuple(getattr(options, "a{}".format(i)) for i in range(1, 100)
                 if getattr(options, "a{}".format(i)) is not None)

    if not args:
        combinations = ()
    else:
        combinations = zip(*args)

    arg = slice_by_jobnumber(combinations)[0]

    command_template = ' '.join(options.commands[1:])
    commandline = shlex.split(command_template.format(*([None]+list(arg))))
    this_exit = subprocess.call(commandline)

    exit_status = max(exit_status, this_exit)

    # exit(100)
    exit(exit_status)


def get_job_number():
    job_number = 0
    number_of_jobs = 1

    if 'SGE_TASK_ID' in os.environ:
        array_task_id = os.environ['SGE_TASK_ID']
        array_task_last = os.environ['SGE_TASK_LAST']
        # array_task_first = os.environ['SGE_TASK_FIRST']
        try:
            job_number = int(array_task_id)-1
            number_of_jobs = int(array_task_last)
        except:
            job_number = 0
            number_of_jobs = 1

    return (job_number, number_of_jobs)


def slice_by_jobnumber(sequence):
    num, jobs = get_job_number()
    size = float(len(sequence)) / jobs
    if num != jobs-1:
        return sequence[int(size*num):int(size*(num+1))]
    return sequence[int(size*num):]


if __name__ == '__main__':
    _main()
