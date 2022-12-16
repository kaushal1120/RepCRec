import re

VALID_COMMANDS = ['begin', 'beginRO', 'R',  'W',
                  'fail', 'recover',  'dump', 'end', ]


def parse(line: str):

    line = line.replace(" ", "")  # remove any white space
    res = re.split(r'[^\w]', line)  # split at special chars
    res = res[:-1]  # remvoe empty string
    if len(res) == 0:
        return None
    cur_command = res[0]
    if cur_command not in VALID_COMMANDS:
        raise ValueError(f"unknown command: {cur_command}, must be in {VALID_COMMANDS}")

    return res
