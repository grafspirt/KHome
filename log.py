#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__logfile = ''


def init(logfile: str = ''):
    global __logfile
    __logfile = logfile


def out(message, interchange: bool = False):
    # Prepare
    from time import ctime
    message = str(ctime()) + ' ' + message
    # Print
    if __logfile and not interchange:
        try:
            f = open(__logfile, 'a')
        except FileNotFoundError:
            f = open(__logfile, 'w')
        f.write(message + '\n')
        f.close()
    else:
        print(message)


def error(message):
    out('<!ERROR!> %s' % message)


def warning(message):
    out('<-WARN-> %s' % message)


def info(message):
    out('<+INFO+> %s' % message)


def debug(message):
    out('<~DEBUG~> %s' % message)


def bus_income(topic, message):
    out('>>[%s]>> %s' % (topic, message), True)


def bus_outcome(topic, message):
    out('<<[%s]<< %s' % (topic, message), True)
