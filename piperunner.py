import subprocess
import os
from os import path
import re
import sys
from itertools import product
from collections import Iterable


class GEJob(object):
    default_mail = 'n'
    default_mail_address = None

    @staticmethod
    def has_path(command):
        for p in os.getenv("PATH").split(path.pathsep):
            if path.exists(path.join(p, command)):
                return True
        return False

    @staticmethod
    def iter_or_item2list(val):
        if val is None:
            return []
        elif isinstance(val, Iterable) and not isinstance(val, basestring):
            return reduce(lambda a, b: a+b, map(GEJob.iter_or_item2list, val), [])
        else:
            return [val]

    def __init__(self,
                 command,
                 args=None,
                 optionfile=None,
                 exectime=None,
                 additional_contexts=None,
                 ar_id=None,
                 account_string=None,
                 binary=None,
                 binding=None,
                 checkpoint=None,
                 ckpt=None,
                 clear=None,
                 cwd=None,
                 prefix_string=None,
                 delete_contexts=None,
                 display=None,
                 deadline=None,
                 stderr=None,
                 hard=None,
                 hold=None,
                 hold_jid=None,
                 hold_jid_ad=None,
                 stdin=None,
                 join=None,
                 job_share=None,
                 jsv=None,
                 resource=None,
                 mem=None,  # specific option
                 mail=None,
                 mail_address=None,
                 masterq=None,
                 notify=None,
                 now=None,
                 name=None,
                 stdout=None,
                 project_name=None,
                 priority=None,
                 parallel_env=None,
                 slot=None,  # specific option
                 pty=None,
                 queue=None,
                 reservation=None,
                 rerun=None,
                 set_contexts=None,
                 shell=None,
                 soft=None,
                 sync=None,
                 interpreter=None,
                 array=None,
                 max_running=None,
                 terse=None,  # will be ignored
                 username=None,
                 var=None,
                 verbose=None,
                 verify=None,
                 allval=None,
                 validation_level=None,
                 working_dir=None,
                 **kwargs):

        if not (path.isfile(command) or self.has_path(command)):
            raise IOError("Script/Binary not found: {}".format(command))

        self.command = command
        self.args = [] if args is None else args

        self.optionfile = optionfile
        self.exectime = exectime
        self.additional_contexts = additional_contexts
        self.ar_id = ar_id
        self.account_string = account_string
        self.binary = binary
        self.binding = binding
        self.checkpoint = checkpoint
        self.ckpt = ckpt
        self.clear = clear
        self.cwd = cwd
        self.prefix_string = prefix_string
        self.delete_contexts = self.iter_or_item2list(delete_contexts)
        self.display = display
        self.deadline = deadline
        self.stderr = self.iter_or_item2list(stderr)
        self.hard = hard
        self.hold = hold
        self.hold_jid = [] if hold_jid is None else self.iter_or_item2list(hold_jid)
        self.hold_jid_ad = [] if hold_jid_ad is None else self.iter_or_item2list(hold_jid_ad)
        self.stdin = self.iter_or_item2list(stdin)
        self.join = join
        self.job_share = job_share
        self.jsv = jsv

        self.resource = {} if resource is None else resource
        if mem:
            self.resource["s_vmem"] = mem
            self.resource["mem_req"] = mem

        self.mail = GEJob.default_mail if mail is None else mail
        self.mail_address = self.iter_or_item2list(GEJob.default_mail_address if mail_address is None else mail_address)
        self.masterq = self.iter_or_item2list(masterq)
        self.notify = notify
        self.now = now

        self.name = path.basename(self.command) if name is None else name
        if re.match("^\d", self.name):
            raise ValueError("Invalid job name (cannot start with a digit): {}".format(self.name))

        self.stdout = self.iter_or_item2list(stdout)
        self.project_name = project_name
        self.priority = priority

        self.parallel_env = parallel_env if parallel_env else {}
        if slot:
            self.parallel_env = dict(def_slot=[slot])

        self.pty = pty
        self.queue = self.iter_or_item2list(queue)
        self.reservation = reservation
        self.rerun = rerun
        self.set_contexts = set_contexts
        self.shell = shell
        self.soft = soft
        self.sync = sync
        self.interpreter = self.iter_or_item2list(interpreter)
        self.array = array
        self.max_running = max_running
        self.terse = True  # terse will be ignored
        self.username = self.iter_or_item2list(username)
        self.var = {} if var is None else var
        self.verbose = verbose
        self.verify = verify
        self.allval = allval
        self.validation_level = validation_level
        self.working_dir = working_dir

        self.additionals = kwargs
        self.job_id = None
        self.next_job = []

    def append_hold_jid(self, jid):
        self.hold_jid += self.iter_or_item2list(jid)

    def append_hold_jid_ad(self, jid):
        self.hold_jid_ad += self.iter_or_item2list(jid)

    def append_next_job(self, job, as_array=False):
        self.next_job.append((job, as_array))

    def _build_command(self):
        self.commandline = ["qsub"]

        simpleargs = (
            ("-@", "optionfile"),
            ("-a", "exectime"),
            ("-ar", "ar_id"),
            ("-A", "account_string"),
            ("-c", "checkpoint"),
            ("-ckpt", "ckpt"),
            ("-C", "prefix_string"),
            ("-display", "display"),
            ("-dl", "deadline"),
            ("-js", "job_share"),
            ("-jsv", "jsv"),
            ("-m", "mail"),
            ("-N", "name"),
            ("-P", "project_name"),
            ("-p", "priority"),
            ("-t", "array"),
            ("-tc", "max_running"),
            ("-w", "validation_level"),
            ("-wd", "working_dir")
        )
        for arg, name in simpleargs:
            val = getattr(self, name)
            if val is not None:
                self.commandline += [arg, val]

        ynargs = (
            ("-b", "binary"),
            ("-j", "join"),
            ("-now", "now"),
            ("-pty", "pty"),
            ("-R", "reservation"),
            ("-r", "rerun"),
            ("-shell", "shell"),
            ("-sync", "sync")
        )
        for arg, name in ynargs:
            val = getattr(self, name)
            if val is not None:
                if val is True:
                    self.commandline += [arg, "yes"]
                elif val is False:
                    self.commandline += [arg, "no"]

        flagvals = (
            ("-clear", "clear"),
            ("-cwd", "cwd"),
            ("-hard", "hard"),
            ("-h", "hold"),
            ("-notify", "notify"),
            ("-soft", "soft"),
            ("-terse", "terse"),
            ("-verify", "verify"),
            ("-V", "allval")
        )
        for arg, name in flagvals:
            val = getattr(self, name)
            if val is True:
                self.commandline.append(arg)

        commasepargs = (
            ("-dc", "delete_contexts"),
            ("-e", "stderr"),
            ("-hold_jid", "hold_jid"),
            ("-hold_jid_ad", "hold_jid_ad"),
            ("-i", "stdin"),
            ("-M", "mail_address"),
            ("-masterq", "masterq"),
            ("-o", "stdout"),
            ("-q", "queue"),
            ("-S", "interpreter"),
            ("-u", "username")
        )
        for arg, name in commasepargs:
            val = getattr(self, name)
            if val:
                self.commandline += [arg, ','.join(map(str, val))]

        kvargs = (
            ("-sc", "set_contexts"),
            ("-v", "var"),
            ("-ac", "additional_contexts"),
            ("-l", "resource")
        )
        for arg, name in kvargs:
            val = getattr(self, name)
            if val:
                self.commandline += [arg, ','.join([("{}={}".format(k, v) if v else k) for k, v in val.items()])]

        if self.binding:
            self.commandline += ["-binding"] + self.binding

        if self.parallel_env:
            k, v = self.parallel_env.items()[0]
            self.commandline += ["-pe", k, ','.join(map(str, v))]

        self.commandline.append(self.command)

        if self.args:
            self.commandline += self.args

        self.commandline = map(str, self.commandline)

    def submit(self):
        if self.job_id is not None:
            raise IOError("This job has already been submitted as {}".format(self.job_id))

        sys.stdout.write("{}\t-> ".format(self.name))

        self._build_command()

        stdout = subprocess.check_output(self.commandline)
        if '.' in stdout:
            job_id, self.array = stdout.rstrip().split('.')
        else:
            job_id, self.array = stdout.rstrip(), None
        self.job_id = int(job_id)

        sys.stdout.write("{}{} [ SUBMITTED ]".format(self.job_id, ('.' + self.array) if self.array else ''))
        holds = []
        holds += self.hold_jid if self.hold_jid else []
        holds += self.hold_jid_ad if self.hold_jid_ad else []
        print " (waiting {})".format(','.join(map(str, holds))) if holds else ''

        for next_job, as_array in self.next_job:
            if as_array:
                next_job.append_hold_jid_ad(self.job_id)
            else:
                next_job.append_hold_jid(self.job_id)
            next_job.submit()

        return self.job_id


class GEArrayJob(GEJob):
    INTERPRETER = "python"
    ARRAYRUNNER = path.join(path.dirname(path.abspath(__file__)), "arrayrunner.py")

    def __init__(self, command, make_combination=False, **kwargs):
        super(GEArrayJob, self).__init__(command, **kwargs)

        args = tuple(kwargs["arg{}".format(i)] for i in range(1, 100) if "arg{}".format(i) in kwargs)

        if make_combination:
            args = zip(*product(*args))
        else:
            args = zip(*zip(*args))

        self.cwd = True
        self.binary = True
        if not self.array:
            self.array = "1-{}".format(len(args[0]))
        self.var["PATH"] = os.environ.get("PATH", '')
        self.var["LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", '')

        arraycommand = [self.ARRAYRUNNER]
        for i, arg in enumerate(args):
            arraycommand += ["-{}".format(i+1)] + ['"{}"'.format(str(x).replace('"', '\"')) for x in arg]
        self.args = arraycommand + ["--", self.command] + ['"{}"'.format(x.replace('"', '\"')) for x in self.args]

        self.command = self.INTERPRETER


class GESeriesJob(object):
    def __init__(self, jobs, as_array=False):
        self.jobs = list(jobs)
        self.as_array = as_array

    def append_hold_jid(self, jid):
        self.jobs[0].append_hold_jid(jid)

    def append_hold_jid_ad(self, jid):
        self.jobs[0].append_hold_jid_ad(jid)

    def submit(self):
        for job, next_job in zip(self.jobs, self.jobs[1:]+[None]):
            hold_jid = job.submit()
            if next_job is not None:
                if self.as_array:
                    next_job.append_hold_jid_ad(hold_jid)
                else:
                    next_job.append_hold_jid(hold_jid)

        return hold_jid


class GEParallelJob(object):
    def __init__(self, jobs, as_array=False):
        self.jobs = jobs
        self.as_array = as_array

    def append_hold_jid(self, jid):
        for job in self.jobs:
            job.append_hold_jid(jid)

    def append_hold_jid_ad(self, jid):
        for job in self.jobs:
            job.append_hold_jid_ad(jid)

    def submit(self):
        return tuple(job.submit() for job in self.jobs)


if __name__ == "__main__":
    t = GEArrayJob("cat", args="{1}".split(), arg1=["arrayrunner.py", "piperunner.py", "test.py"])
    t.submit()
