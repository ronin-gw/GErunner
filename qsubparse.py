import argparse
import sys

from piperunner import GEJob


def command2GEJob(commands):
    return GEJob(**vars(parse_args(commands)))


def required_length(nmin, nmax):
    class RequiredLength(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if not nmin <= len(values) <= nmax:
                msg = 'argument "{}" requires between {} and {} arguments'.format(
                    self.dest, nmin, nmax
                )
                raise argparse.ArgumentTypeError(msg)
            setattr(namespace, self.dest, values)
    return RequiredLength


class parse_eq(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        d = {}
        for eq in values.split(','):
            splited = eq.split('=')
            if len(splited) == 1:
                d[splited[0]] = ''
            else:
                d[splited[0]] = splited[1]

        setattr(namespace, self.dest, d)


class yesno(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values.lower() in ('y', 'ye', 'yes'):
            val = True
        elif values.lower() in ('n', 'no'):
            val = False
        else:
            msg = 'argument "{}" accepts only y[es]|n[o]'.format(self.dest)
            raise argparse.ArgumentTypeError(msg)

        setattr(namespace, self.dest, val)


class split_comma(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))


class parse_pe(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, {values[0]: values[1].split(',')})


class unlist_nargs1(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values[0])


def parse_args(commands=None):
    p = argparse.ArgumentParser(add_help=False,
                                description="Argument parser for GE qsub command",
                                epilog="refer qsub man page for details")

    p.add_argument("--help", action="help")

    # qsub options
    p.add_argument("-@", nargs=1, action=unlist_nargs1, metavar="optionfile", dest="optionfile")
    p.add_argument("-a", nargs=1, action=unlist_nargs1, metavar="date_time", dest="exectime")
    p.add_argument("-ac", action=parse_eq, metavar="variable{=value}...", dest="additional_contexts")
    p.add_argument("-ar", nargs=1, action=unlist_nargs1, metavar="ar_id", dest="ar_id")
    p.add_argument("-A", nargs=1, action=unlist_nargs1, metavar="account_string", dest="account_string")
    p.add_argument("-b", action=yesno, metavar="y{es}|n{o}", dest="binary")
    p.add_argument("-binding", nargs='+', action=required_length(1, 2), dest="binding")
    p.add_argument("-c", nargs=1, action=unlist_nargs1, metavar="occasion_specifier", dest="checkpoint")
    p.add_argument("-ckpt", nargs=1, action=unlist_nargs1, metavar="ckpt_name")
    p.add_argument("-clear", action="store_true")
    p.add_argument("-cwd", action="store_true")
    p.add_argument("-C", nargs=1, action=unlist_nargs1, metavar="prefix_string", dest="prefix_string")
    p.add_argument("-dc", action=split_comma, metavar="variable,..", dest="delete_contexts")
    p.add_argument("-display", nargs=1, action=unlist_nargs1, metavar="display_specifier")
    p.add_argument("-dl", nargs=1, action=unlist_nargs1, metavar="date_time", dest="deadline")
    p.add_argument("-e", action=split_comma, metavar="{{hostname}:}path,...", dest="stderr")
    p.add_argument("-hard", action="store_true")
    p.add_argument("-h", action="store_true", dest="hold")
    p.add_argument("-hold_jid", action=split_comma, metavar="wc_job_list")
    p.add_argument("-hold_jid_ad", action=split_comma, metavar="wc_job_list")
    p.add_argument("-i", action=split_comma, metavar="{{hostname}:}file,", dest="stdin")
    p.add_argument("-j", action=yesno, metavar="y{es}|n{o}", dest="join")
    p.add_argument("-js", nargs=1, action=unlist_nargs1, metavar="job_share", dest="job_share")
    p.add_argument("-jsv", nargs=1, action=unlist_nargs1, metavar="jsv_url")
    p.add_argument("-l", action=parse_eq, metavar="resource=value,...", dest="resource")
    p.add_argument("-m", nargs=1, action=unlist_nargs1, metavar="b|e|a|s|n,...", dest="mail")
    p.add_argument("-M", action=split_comma, metavar="user{@host},...", dest="mail_address")
    p.add_argument("-masterq", action=split_comma, metavar="wc_queue_list")
    p.add_argument("-notify", action="store_true")
    p.add_argument("-now", action=yesno, metavar="y{es}|n{o}")
    p.add_argument("-N", nargs=1, action=unlist_nargs1, metavar="name", dest="name")
    p.add_argument("-o", action=split_comma, metavar="{{hostname}:}path,...", dest="stdout")
    p.add_argument("-P", nargs=1, action=unlist_nargs1, metavar="project_name", dest="project_name")
    p.add_argument("-p", nargs=1, action=unlist_nargs1, metavar="priority", dest="priority")
    p.add_argument("-pe", nargs=2, action=parse_pe, metavar="parallel_env", dest="parallel_env")
    p.add_argument("-pty", action=yesno, metavar="y{es}|n{o}")
    p.add_argument("-q", action=split_comma, metavar="wc_queue_list", dest="queue")
    p.add_argument("-R", action=yesno, metavar="y{es}|n{o}", dest="reservation")
    p.add_argument("-r", action=yesno, metavar="y{es}|n{o}", dest="rerun")
    p.add_argument("-sc", action=parse_eq, metavar="variable{=value},...", dest="set_contexts")
    p.add_argument("-shell", action=yesno, metavar="y{es}|n{o}")
    p.add_argument("-soft", action="store_true")
    p.add_argument("-sync", action=yesno, metavar="y{es}|n{o}")
    p.add_argument("-S", action=split_comma, metavar="{{hostname}:}pathname,...", dest="interpreter")
    p.add_argument("-t", nargs=1, action=unlist_nargs1, metavar="n{-m{:s}}", dest="array")
    p.add_argument("-tc", nargs=1, action=unlist_nargs1, metavar="max_running_tasks", dest="max_running")
    p.add_argument("-terse", action="store_true", help="This option will be ignored")
    p.add_argument("-u", action=split_comma, metavar="username,...", dest="username")
    p.add_argument("-v", action=parse_eq, metavar="variable{=value},...", dest="var")
    p.add_argument("-verify", action="store_true")
    p.add_argument("-V", action="store_true", dest="allval")
    p.add_argument("-w", nargs=1, action=unlist_nargs1, metavar="e|w|n|p|v", dest="validation_level")
    p.add_argument("-wd", nargs=1, action=unlist_nargs1, metavar="working_dir", dest="working_dir")

    # command and command's argments
    p.add_argument("command", nargs=1, action=unlist_nargs1)
    p.add_argument("args", nargs=argparse.REMAINDER, metavar="command_arg")

    return p.parse_args(commands)


if __name__ == "__main__":
    commands = sys.argv[1:]
    print parse_args(commands)
    j = command2GEJob(commands)
    j._build_command()
    print j.commandline
