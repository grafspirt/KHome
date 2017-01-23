#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Generic linux daemon base class for Python 3.x. """

import sys
import os
import time
import atexit
import signal


class Daemon(object):
    """
    A generic daemon class.
    Usage: subclass the daemon class and override the run() method.
    """

    def __init__(self, pidfile, name='Daemon'):
        self.pidfile = pidfile
        self.name = name

    def daemonize(self):
        """ Deamonize class. UNIX double fork mechanism. """

        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('fork #1 failed: %s\n' % err)
            sys.exit(1)

        # decouple from parent environment
        os.chdir('/')
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('fork #2 failed: %s\n' % err)
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(os.devnull, 'r')
        so = open(os.devnull, 'a+')
        se = open(os.devnull, 'a+')

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.del_pid)

        pid = str(os.getpid())
        with open(self.pidfile, 'w+') as f:
            f.write(pid + '\n')

    def del_pid(self):
        os.remove(self.pidfile)

    def get_pid(self):
        # Get the pid from the pidfile
        try:
            with open(self.pidfile, 'r') as pf:
                return int(pf.read().strip())
        except IOError:
            return None

    def start(self):
        """ Start the Daemon """

        # Check for a pidfile to see if the Daemon already runs
        if self.get_pid():
            sys.stderr.write("pidfile %s exists already.\n%s is already running.\n" % (self.pidfile, self.name))
            sys.exit(1)

        # Start the Daemon
        self.daemonize()
        self.run()

    def stop(self, restarted=False):
        """ Stop the Daemon """

        # Get the pid from the pidfile
        pid = self.get_pid()
        if not self.get_pid():
            if not restarted:
                sys.stderr.write("pidfile %s does not exist.\n%s is not running?\n" % (self.pidfile, self.name))
                sys.exit(1)
            else:
                return  # when restart is called while the Daemon has not been started yet

        # Trying to kill the daemon process
        try:
            while True:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            e = str(err.args)
            if e.find("No such process") > 0:
                # Normal process finishing
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err.args))
                sys.exit(1)

    def restart(self):
        """ Restart the Daemon """
        self.stop(True)
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon.
        It will be called after the process has been daemonized by start() or restart().
        """
        return False
