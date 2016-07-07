# GErunner
GErunner, the Python tool for Sun/Univa GridEngine enables to describe jobs or processing pipeline and submit them from Python.

## Installation
This tool is not yet packaged. Just clone this repository into your directory.

## Usage
### Basics
`GEJob` object has almost of all attributes and functions correspond to `qsub` command.

```
>>> from GErunner import GEJob
>>> myjob = GEJob("ls", args=["-l"], cwd=True, binary=True)
>>> myjob.submit()
ls	-> 12345 [ SUBMITTED ]
12345
```

`mem` is shortcut attribute for setting memory resouce and `slot` is for def_slot.
```
>>> myjob = GEJob("hoge.sh", mem="4G", slot=8)
>>> myjob.resource
{'s_vmem': '4G', 'mem_req': '4G'}
>>> myjob.parallel_env
{'def_slot': [8]}
```

### Describe pipeline
`GESeriesJob` and `GEParallelJob` are automatically assign job id to `-hold_jid` (`-hold_jid_ad`) option.

```
>>> from GErunner import GEJob, GESeriesJob
>>> job1 = GEJob("pwd", binary=True)
>>> job2 = GEJob("ls", binary=True)
>>> pipeline = GESeriesJob([job1, job2])
>>> pipeline.submit()
pwd	-> 12345 [ SUBMITTED ]
ls	-> 12346 [ SUBMITTED ] (waiting 12345)
12346
```

They can be nested.

```
>>> from GErunner import GEJob, GESeriesJob,GEParallelJob
>>> job1_1 = GEJob("ls", binary=True)
>>> job1_2 = GEJob("ls", args=["-l"], binary=True)
>>> job2 = GEJob("pwd", binary=True)
>>> parallel = GEParallelJob([job1_1, job1_2])
>>> pipeline = GESeriesJob([parallel, job2])
>>> pipeline.submit()
ls	-> 12345 [ SUBMITTED ]
ls	-> 12346 [ SUBMITTED ]
pwd	-> 12347 [ SUBMITTED ] (waiting 12345,12346)
12347
```

### Array runner
`GEArrayJob` makes it easy to submit array job from arguments list.

```
>>> from GErunner import GEArrayJob
>>> args = ("spam", "ham", "egg")
>>> myjob = GEArrayJob("grep", args=["{1}", "huga.txt"], arg1=args, binary=True, cwd=True)
>>> myjob.submit()
grep	-> 12345.1-3:1 [ SUBMITTED ]
12345
```

### Convert `qsub` command line into `GEJob` instance
`GErunner.qsubparse` has the argument parser for `qsub` command.

```
>>> from GErunner.qsubparse import command2GEJob
>>> myjob = command2GEJob("-cwd -b y ls -l".split())
```
