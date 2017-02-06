#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import datetime
import re
from threading import Timer


class ScheduleTime(object):
    """
    Replicates Actor time templates.
    String source: [[[YYYY:]MM:]DD:]hh:]mm[.ss]
    """
    def __init__(self, time_obj):
        self.is_template = False

        if isinstance(time_obj, str):
            class Parser(object):
                def __init__(self, time_str):
                    self.time_str = time_str
                    self.pos_end = len(time_str)

                def get(self, delimiter) -> int:
                    if self.pos_end >= 0:
                        pos = self.time_str.rfind(delimiter, 0, self.pos_end)
                        try:
                            result = int(self.time_str[pos + 1:self.pos_end])
                        except ValueError:
                            result = -1
                        self.pos_end = pos
                        return result
                    else:
                        return -1

            if '.' not in time_obj:
                time_obj += '.0'
            tp = Parser(time_obj)

            self.second = tp.get('.')
            self.minute = tp.get(':')
            self.hour = tp.get(':')
            self.day = tp.get(':')
            self.month = tp.get(':')
            self.year = tp.get(':')
            self.is_template = self.year == -1

        elif isinstance(time_obj, datetime.datetime):
            self.second = time_obj.second
            self.minute = time_obj.minute
            self.hour = time_obj.hour
            self.day = time_obj.day
            self.month = time_obj.month
            self.year = time_obj.year
            self.is_template = True

    @staticmethod
    def _nvl(value, null_value):
        return value if value > -1 else null_value

    def __str__(self):
        return '%s:%s:%s:%s:%s.%s' %\
               (self.year if self.year > -1 else '****',
                '%02d' % self.month if self.month > -1 else '**',
                '%02d' % self.day if self.day > -1 else '**',
                '%02d' % self.hour if self.hour > -1 else '**',
                '%02d' % self.minute if self.minute > -1 else '**',
                '%02d' % self.second)

    def _cmp(self, other) -> int:
        def _cmp(x1, x2):
            if x1 != -1 and x2 != -1:
                if x1 > x2:
                    return 1
                if x1 < x2:
                    return -1
            return 0

        res = _cmp(self.year, other.year)
        if res:
            return res

        res = _cmp(self.month, other.month)
        if res:
            return res

        res = _cmp(self.day, other.day)
        if res:
            return res

        res = _cmp(self.hour, other.hour)
        if res:
            return res

        res = _cmp(self.minute, other.minute)
        if res:
            return res

        return _cmp(self.second, other.second)

    def __lt__(self, other):
        return self._cmp(other) == -1

    def __gt__(self, other):
        return self._cmp(other) == 1

    def __eq__(self, other):
        return self._cmp(other) == 0

    def get_datetime(self, shift=0, start_date=None) -> datetime.datetime:
        if not start_date:
            start_date = datetime.datetime.now()

        if not shift:
            return datetime.datetime(
                self._nvl(self.year, start_date.year),
                self._nvl(self.month, start_date.month),
                self._nvl(self.day, start_date.day),
                self._nvl(self.hour, start_date.hour),
                self._nvl(self.minute, start_date.minute),
                self.second)
        else:
            shift_year = 0
            shift_month = 0
            shift_day = 0
            shift_hour = 0
            shift_minute = 0
            if self.year == -1:
                if self.month == -1:
                    if self.day == -1:
                        if self.hour == -1:
                            if self.minute == -1:
                                if self.second == -1:
                                    shift_minute = shift
                            else:
                                shift_hour = shift
                        else:
                            shift_day = shift
                    else:
                        shift_month = shift
                else:
                    shift_year = shift

            date_shifted_by_day = start_date + datetime.timedelta(shift_day, 0, 0, 0, shift_minute, shift_hour)
            if date_shifted_by_day.month + shift_month > 12:
                shift_year += 1
                shift_month -= 12

            return datetime.datetime(
                self._nvl(self.year, date_shifted_by_day.year + shift_year),
                self._nvl(self.month, date_shifted_by_day.month + shift_month),
                self._nvl(self.day, date_shifted_by_day.day),
                self._nvl(self.hour, date_shifted_by_day.hour),
                self._nvl(self.minute, date_shifted_by_day.minute),
                self.second)

    def get_timedelta(self) -> datetime.timedelta:
        return datetime.timedelta(
            self._nvl(self.day, 0),
            self._nvl(self.second, 0),
            0, 0,
            self._nvl(self.minute, 0),
            self._nvl(self.hour, 0))


class Job(object):
    """ Job unit which could be scheduled at a start_time to be handled by Actors tied to handler. """
    def __init__(self, handler):
        self.handler = handler  # Handler (Actor)
        self.start_time = None  # the time which the Job shall be scheduled at

    def schedule(self):
        """ Schedule this job. """
        if self.start_time:
            sch.add_job(self)

    def process(self):
        pass


# Scheduler - Manage job objects

class Scheduler(object):
    def __init__(self):
        self.timetable = {}             # scheduled jobs list. Structure: [time_minutes] -> list of time_seconds
        self.jobs_to_schedule = []      # list of jobs to be rescheduled
        self.clean_timetable = False    # necessity of timetable cleaning from obsolete jobs

    @staticmethod
    def init_timer():
        """ Init the timer which is used by Scheduler. """
        Scheduler.on_timer()

    @staticmethod
    def on_timer():
        now = time.localtime()
        Timer(60 - now.tm_sec, Scheduler.on_timer).start()
        sch.process('%d:%02d:%02d:%02d:%02d' %
                    (now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min),
                    now.tm_sec)

    def add_job(self, job: Job) -> str:
        # calc key
        time_cell = ''
        dt_object = job.start_time
        if dt_object.minute > -1:
            time_cell = '%02d' % dt_object.minute + time_cell
        if dt_object.hour > -1:
            time_cell = '%02d' % dt_object.hour + ':' + time_cell
        if dt_object.day > -1:
            time_cell = '%02d' % dt_object.day + ':' + time_cell
        if dt_object.month > -1:
            time_cell = '%02d' % dt_object.month + ':' + time_cell
        if dt_object.year > -1:
            time_cell = str(dt_object.year) + ':' + time_cell
        # register job to yhe key
        try:
            self.timetable[time_cell].append(job)
        except KeyError:
            self.timetable[time_cell] = [job]
        return time_cell

    def clean(self):
        """ Clean Timetable from all obsolete and empty time cells. """
        now = datetime.datetime.now()
        to_wipe = []
        # collect obsolete jobs to be wiped out
        for time_cell in self.timetable:
            value = ScheduleTime(time_cell)
            if (not value.is_template and not value > now) or (not self.timetable[time_cell]):
                to_wipe.append(time_cell)
        # wipe obsolete jobs found out
        for time_cell in to_wipe:
            del self.timetable[time_cell]
        # Reset
        self.clean_timetable = False

    def clear(self, handler: str = '0'):
        """ Clear Timetable from Jobs related to some Handler or all. """
        if handler:
            for time_cell in self.timetable:
                new_time_cell = [job for job in self.timetable[time_cell] if job.handler != handler]
                self.timetable[time_cell] = new_time_cell
            self.clean()    # wipe all empty time cells
        else:
            self.timetable = {}

    def process(self, time_now: str, correction_sec: int = 0):
        """
        :param time_now: time to the nearest minute
        :param correction_sec: difference in seconds if the processing is started not in :00
        :return: nothing
        """
        # Check all jobs scheduled - find jobs with time_cell related to the time_now
        for time_cell in self.timetable:
            if re.search(time_cell + '$', time_now):
                # It is time to do all jobs related to this time key
                for job in self.timetable[time_cell]:
                    if job.start_time.second:
                        # wait for item.seconds
                        Timer(job.start_time.second - correction_sec, job.process).start()
                    else:
                        # process value right now
                        job.process()
        # Clean jobs list (clean_timetable)
        if self.clean_timetable:
            self.clean()
        # Reschedule jobs (jobs_to_schedule)
        for job in self.jobs_to_schedule:
            job.schedule()
        self.jobs_to_schedule = []

# Scheduler instance
sch = Scheduler()
