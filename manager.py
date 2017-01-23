#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# TODO: if only one module could not be installed the the whole package - nack?

from pymysql import DatabaseError
import datetime
from urllib.parse import urlencode
import http.client as client
import re
import kbus
from kinventory import *


# Modules from the following list are allowed for processing
KHOME_MODULE_TYPES = {
    # Sensors
    '1': "Generic Sensor",
    '2': "IR Sensor",
    '3': "DHT Sensor",
    # Actuators
    '51': "Switch"
}

# Available pins
PINS_ESP8266 = ['0', '1', '3', '2', '4', '5', '9', '10', '12', '13', '14', '15', '16']


# Actors

class ActorWithMapping(Handler):
    """ Actor using mapping data in config. """
    class MapUnit(KDBObject):
        """ Object used as mapping record in ActorWithMapping configuration. """
        def __init__(self, cfg):
            super().__init__(cfg, cfg['in'])

    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        self.mapping = {}
        # init mapping data
        if 'map' in self.config['data']:
            for map_cfg in self.config['data']['map']:
                self.append_mapping(map_cfg)
        else:
            self.config['data']['map'] = []

    def append_mapping(self, cfg) -> MapUnit:
        """
        Create MapUnit basing on its config and add it to registry.
        :param cfg: dict - config object of this mapping unit
        :return: result object
        """
        map_unit = ActorWithMapping.MapUnit(cfg)  # create
        self.mapping[map_unit.id] = map_unit  # add to the register
        return map_unit

    def add_mapping(self, cfg) -> MapUnit:
        """
        Create/Add MapUnit to registry basing on its config and add it to config.
        :param cfg: dict - config object of this mapping unit
        :return: result object
        """
        map_unit = self.append_mapping(cfg)
        self.config['data']['map'].append(cfg)
        return map_unit

    def del_mapping(self, map_id):
        if map_id in self.mapping:
            self.config['data']['map'].remove(self.mapping[map_id].config)
            del self.mapping[map_id]

    def process_request(self, command, params) -> int:
        count = 0

        if command == 'add-mapping':
            for map_unit in params['map']:
                self.add_mapping(map_unit)
                count += 1

        elif command == 'del-mapping':
            for map_id in params['maps']:
                self.del_mapping(map_id)
                count += 1

        elif command == 'edit-actor':
            count += super().process_request(command, params)

        return count


class ActorLog(Handler):
    """
    Actor logging source data with a period defined in ticks.
    The logging action [self.log(sig)] is defined in child classes.
    """

    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        # period
        self.count = 0
        if 'period' not in self.config['data']:
            self.config['data']['period'] = 5

    def process_request(self, command, params) -> int:
        count = 0

        if command == 'edit-actor':
            if 'period' in params:
                self.config['data']['period'] = params['period']
                count += 1

            count += super().process_request(command, params)

        return count

    def process_signal(self, sig):
        self.count += 1
        if self.count >= int(self.config['data']['period']):
            self.log(sig)
            self.count = 0

    def log(self, sig):
        pass


class Resend(ActorWithMapping):
    def process_signal(self, sig):
        # direct
        try:
            send_signal_to_module(
                inv.nodes[self.config['data']['trg']].modules[self.config['data']['trg_mdl']],
                sig)
        except KeyError:
            pass
        # mapping
        for map_key in self.mapping:
            map_unit = self.mapping[map_key]
            if map_unit['in'] == sig:
                try:
                    send_signal_to_module(
                        inv.nodes[map_unit['trg']].modules[map_unit['trg_mdl']],
                        map_unit['out'])
                except KeyError:
                    pass


class LogThingSpeak(ActorWithMapping, ActorLog):
    """ Log source data in ThingSpeak.com using mapping for complex signal. """
    def __new__(cls, cfg, db_id):
        if 'key' in cfg['data']:
            return super().__new__(cls, cfg, db_id)
        else:
            log.warning(
                'Actor %s#%s could not be loaded as it does not have "key" in config.' %
                (cfg['type'].lower(), db_id))
            return None

    def log(self, sig):
        data_to_send = {'key': self.config['data']['key']}
        if isinstance(sig, dict):
            for alias in sig:
                try:
                    data_to_send[self.mapping[alias].config['out']] = sig[alias]
                except KeyError:
                    pass
        else:
            data_to_send['field1'] = sig

        # send
        connection = client.HTTPConnection("api.thingspeak.com:80")
        connection.request(
            "POST",
            "/update",
            urlencode(data_to_send),
            {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"})
        # response = connection.getresponse()  # response.status, response.reason


class LogDB(ActorLog):
    """ Log source data in DB. """

    def log(self, sig):
        if inv.storage:
            if isinstance(sig, dict):
                str_value = ""
                for key in sorted(sig):
                    str_value += ',"%s":"%s"' % (key, sig[key])
                str_value = '{' + str_value[1:] + '}'
            elif isinstance(sig, str):
                str_value = sig
            else:
                str_value = '{"unknown-value-type":}'

            cursor = inv.storage.cursor()
            cursor.execute(
                "INSERT INTO sens_data (sensor, value) VALUES (%s, %s)",
                (self.get_box_key(), str_value)
            )
            inv.storage.commit()
            cursor.close()


class Average(Handler):
    """ Averages values on some period, defined by 'depth' parameter. """
    def __new__(cls, cfg, db_id):
        # Box is mandatory for Average actors
        if 'box' in cfg['data']:
            return super().__new__(cls, cfg, db_id)
        else:
            log.warning('Actor %s#%s could not be loaded as it does not have "box" in config.' %
                        (cfg['type'], db_id))
            return None

    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        self.__data = {}    # to store history sets per entity (entity_name: entity_set)
        # default Depth if it is not set
        if 'depth' not in self.config['data']:
            self.config['data']['depth'] = 5

    def __calc(self, key: str, number: float) -> str:
        if key not in self.__data:
            self.__data[key] = []
        self.__data[key].append(number)
        # the length of averaged stack should not exceed depth value
        if len(self.__data[key]) > int(self.config['data']['depth']):
            self.__data[key].pop(0)
        # calc average values
        return '%.1f' % (sum([float(value) for value in self.__data[key]]) / len(self.__data[key]))

    def process_signal(self, sig):
        """
        Calculate average values on data series having length less or equal to self.depth
        Store result in its own Register
        Example: {"key1": "number", "key2": "number", ...}

        :param sig: dict - signal data
        :return: nothing
        """
        if self.box:
            if isinstance(sig, dict):
                averaged_sig = sig.copy()
                for key in sig:
                    averaged_sig[key] = self.__calc(key, float(sig[key]))
                self.box.value = averaged_sig
            elif isinstance(sig, (int, float, str)):
                self.box.value = self.__calc('.', sig)

    def process_request(self, command, params) -> int:
        count = 0

        if command == 'edit-actor':
            if 'depth' in params:
                self.config['data']['depth'] = params['depth']
                count += 1

            count += super().process_request(command, params)

        return count


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
        self.start_time = None  # the time which the Job shall be scheduled at
        self.handler = handler  # Handler (Actor) which is to be a source of the values generated by this Job

    def schedule(self):
        """ Schedule this job. """
        if self.start_time:
            sch.add_job(self)

    def process(self):
        pass


class EventJob(Job):
    """
    Job which is performed once (in a period) at a time defined by time template.
    Value is processed by Performer.
    E.g.:
    start_time = ****:**:**:01:00 means the Job is performed every 01:00 once a day
    start_time = ****:**:05:01:00 means the Job is performed every the 5th day of a month at 01:00
    """
    def __init__(self, handler: str, value, start_time: ScheduleTime):
        super().__init__(handler)
        self.start_time = start_time    # the time which this Job shall start at
        self.value = value              # value which is to be processed by the post-Actor

    def process(self):
        handle_value(self.handler, self.value)

    def __str__(self):
        return 'Trigger event "%s" for Schedule[%s] at %s' % (self.value, self.handler, self.start_time)


class IntervalEventJob(Job):
    """
    Maintain job configurations (EventJob factory)
    supposing same actions performed with a period within some time interval.
    'period' - period of action to be triggered within time interval
    'start' - begin of time interval
    'stop' - end of time interval
    """
    def __init__(self, handler: str, cfg: dict):
        super().__init__(handler)
        self.config = cfg

    def schedule(self):
        start_template = ScheduleTime(self.config['start'])
        stop_template = ScheduleTime(self.config['stop'])
        period_template = ScheduleTime(self.config['period'])
        period_delta = period_template.get_timedelta()
        # shift start time to the next interval if stop time has been passed already
        start_time = start_template.get_datetime(int(not stop_template > datetime.datetime.now()))
        # shift stop time to the next period if the stop time is less than start time
        stop_time = stop_template.get_datetime(int(stop_template < start_time), start_time)

        # Schedule EventJobs for the calculated period
        while stop_time >= start_time:
            EventJob(self.handler, self.config['value'], start_time).schedule()
            start_time += period_delta

        # Schedule this interval job to stop_time in order to re-schedule it again
        self.start_time = stop_time
        super().schedule()

    def process(self):
        """ Reschedule jobs from corresponding interval again. """
        # ask Scheduler to clean job list from obsolete event jobs related to this interval job (Scheduler.process)
        sch.clean_timetable = True
        # ask Scheduler to re-schedule this interval job
        sch.jobs_to_schedule.append(self)

    def __str__(self):
        return 'Reschedule interval for Schedule[%s]: %s' % (self.handler, self.config)


class Schedule(Generator):
    """ Actor managing Scheduler job and acts as a data source for Handlers. """
    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        # Instantiate jobs from cfg
        for job_cfg in self.config['data']['jobs']:
            try:
                if 'event' in job_cfg:      # job on time - event
                    EventJob(self.id, job_cfg['value'], ScheduleTime(job_cfg['event'])).schedule()
                elif 'period' in job_cfg:   # job within a period - period
                    IntervalEventJob(self.id, job_cfg).schedule()
            except KeyError:
                pass  # not valid config - do not load


# Scheduler - Manage job objects

class Scheduler(object):
    def __init__(self):
        self.timetable = {}             # scheduled jobs list. Structure: [time_minutes] -> list of time_seconds
        self.jobs_to_schedule = []      # list of jobs to be rescheduled
        self.clean_timetable = False    # flag defining the necessity of timetable cleaning from obsolete jobs

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
        time_key = ''
        dt_object = job.start_time
        if dt_object.minute > -1:
            time_key = '%02d' % dt_object.minute + time_key
        if dt_object.hour > -1:
            time_key = '%02d' % dt_object.hour + ':' + time_key
        if dt_object.day > -1:
            time_key = '%02d' % dt_object.day + ':' + time_key
        if dt_object.month > -1:
            time_key = '%02d' % dt_object.month + ':' + time_key
        if dt_object.year > -1:
            time_key = str(dt_object.year) + ':' + time_key
        # register job to yhe key
        try:
            self.timetable[time_key].append(job)
        except KeyError:
            self.timetable[time_key] = [job]
        return time_key

    def clean(self):
        now = datetime.datetime.now()
        to_delete = []
        # collect obsolete jobs to be deleted
        for time_key in self.timetable:
            value = ScheduleTime(time_key)
            if not value.is_template and not value > now:
                to_delete.append(time_key)
        # delete obsolete jobs found
        for time_key in to_delete:
            del self.timetable[time_key]

    def process(self, time_now: str, correction_sec: int = 0):
        """
        :param time_now: time to the nearest minute
        :param correction_sec: difference in seconds if the processing is started not in :00
        :return: nothing
        """
        # Check all jobs scheduled - find jobs with time_key related to the time_now
        for time_key in self.timetable:
            if re.search(time_key + '$', time_now):
                # It is time to do all jobs related to this time key
                for job in self.timetable[time_key]:
                    if job.start_time.second:
                        # wait for item.seconds
                        Timer(job.start_time.second - correction_sec, job.process).start()
                    else:
                        # process value right now
                        job.process()
        # Clean jobs list (clean_timetable)
        if self.clean_timetable:
            self.clean()
            self.clean_timetable = False
        # Reschedule jobs (jobs_to_schedule)
        for job in self.jobs_to_schedule:
            job.schedule()
        self.jobs_to_schedule = []

sch = Scheduler()


# Bus ISR ---

def on_connect_to_bus():
    # Ask all Agents for configs
    kbus.send(
        "/config/" + ALL_MODULES,
        "i!",
        True)


def on_message_from_bus(topic, message):
    coordinates = topic.split('/')
    try:
        # Message -> Object
        message_object = json.loads(message)
        # South - data from an Agent
        if coordinates[1] in ('nodes', 'data'):
            # Result
            if handle_response_from_node(coordinates, message_object):
                pass
            # Node data
            elif coordinates[1] == 'nodes':     # /nodes/<nid>
                handle_data_from_node(
                    coordinates[2],
                    message_object)
            # Module data
            elif coordinates[1] == 'data':      # /data/<nid>/<mal>
                handle_data_from_module(
                    coordinates[2],
                    coordinates[3],
                    message_object)
        # North - request for the Manager
        elif coordinates[1] == 'manager':                           # /manager
            # Process request for the Manager
            process_request(message)
    except KeyError as error_object:
        kbus.send(
            "/error",
            "Key %s is absent in the request: %s" % (str(error_object), message))
    except IndexError:
        kbus.send(
            "/error",
            "Wrong request format in topic [%s]: %s" % (topic, message))
    except ValueError:
        kbus.send(
            "/error",
            "Request is not a JSON in topic [%s]: %s" % (topic, message))


# Interaction ---

def send_signal_to_module(module: Module, data, north_request: dict = None):
    return inv.nodes[module.nid].session.start(
        kbus.send(
            '/signal/%s/%s' % (module.nid, module.id),
            data,
            True),
        north_request)


def send_config_to_node(node: Node, config, north_request: dict = None):
    return node.session.start(
        kbus.send(
            '/config/%s' % node.id,
            config,
            True),
        north_request)


def answer_to_www(sid: str, message):
    if sid:
        kbus.send(
            '/manager/%s' % sid,
            message if message else '{"unknown":}')


# Handling South ---

def handle_data_from_node(nid: str, mes: dict):
    """
    Handle data from Node.
    :param nid: id of a Node sent data
    :param mes: data in a message object
    :return: nothing
    """
    # Node said hello
    if 'id' in mes:
        # Add/update Node
        node = inv.register_node(mes)
        # Ask the Node for Module cfg
        if node:
            mes = send_config_to_node(node, {"get": "gpio"})
            if mes:
                try:
                    node = inv.nodes[nid]
                    for module_cfg in mes['gpio']:
                        inv.register_module(node, module_cfg)
                    log.info('Modules of Node [%s] have been uploaded to Inventory: %s' %
                             (nid, str([node.modules[m].get_cfg()['name'] for m in node.modules])))
                except KeyError:
                    pass


def handle_data_from_module(nid: str, mal: str, mes: dict):
    """
    Handle data from Module.
    :param nid: id of a Node hosting Module sent data
    :param mal: alias of Module sent data
    :param mes: data in a message object
    :return: nothing
    """
    try:
        module = inv.nodes[nid].modules[mal]
        # if it is not NACK
        if 'nack' not in mes:
            # store signal in the Box bound to Module
            module.box.value = mes if 'ack' not in mes else mes['ack']  # TODO: remove when Agent will be updated
            # find/trigger right handler
            handle_value(
                form_key(nid, mal),
                module.box.value)
    except KeyError:
        pass


def handle_response_from_node(coordinates: list, response: dict) -> bool:
    try:
        node = inv.nodes[coordinates[2]]    # type: Node
        node.alive()
        if node.session.active:
            node.session.stop(response)
            if coordinates[1] != 'data':    # data from Module should be processed by handle_data_from_module
                return True                 # further processing is not necessary
    except (AttributeError, KeyError):
        pass
    return False


def handle_value(key, value):
    if key in inv.handlers:
        for actor in inv.handlers[key]:
            # Actor is triggered if it is active
            if actor.active:
                actor.process_signal(value)
            # try to process Actor Box by handlers(actors) referring to this Actor
            if actor.box:
                handle_value(actor.id, actor.box.value)


def is_south_response_success(response: dict) -> bool:
    return response and 'nack' not in response


# Handling North ---

def process_request(message):
    """
    Process command from data bus and send result back
    :param message: kind of {"session":<session-id>,"request":<command>,"params":{<params-set>}}
    """
    answer = ""
    answer_template = '{"ack": "%d"}'
    request = json.loads(message)
    # Mandatory params
    sid = request['session']
    # Process request
    try:
        request_type = request['request']

        if request_type == 'get-structure':
            answer = request_manage_structure(request)

        elif request_type == 'get-data':
            answer = request_manage_data(request)

        elif request_type == 'get-timetable':
            answer = request_manage_timetable()

        elif request_type == 'ping':
            answer = request_manage_ping(request)                       # south

        elif request_type == 'signal':
            answer = request_manage_signal(request)                     # south

        # Update Module configuration
        elif request_type in ['add-module', 'del-module', 'edit-module']:
            answer = answer_template % request_manage_modules(request)  # south

            # elif request_type in ['add-actor', 'edit-actor', 'del-actor']:
            #     answer = answer_template % request_manage_actors(request_type, params)
            #
            # elif request_type in ['add-mapping', 'del-mapping']:
            #     try:
            #         answer = '{"ack": "%d"}' % inv.actors[params['actor']].process_request(request_type, params)
            #     except KeyError:
            #         pass
    except (TypeError, ModuleError, NodeError) as err_obj:
        answer = '{"nack": "%s"}' % err_obj
    except KeyError as err_obj:
        answer = '{"nack": "Key %s is absent in the request"}' % err_obj
    except pymysql.err.OperationalError:
        answer = '{"nack": "There are problems in DB"}'

    # Answer with the same session id
    answer_to_www(sid, answer if answer else {"nack": "timeout"})


def request_manage_structure(request: dict) -> dict:
    if 'params' in request and 'revision' in request['params'] and inv.revision == int(request['params']['revision']):
        # Export revision number only as nothing has been changed
        return {'revision': inv.revision}
    else:
        # Export the whole structure
        return {
            'revision': inv.revision,
            'module-types': KHOME_MODULE_TYPES,
            'nodes': [inv.nodes[node_id].get_cfg() for node_id in inv.nodes],
            'actors': [inv.actors[act_id].get_cfg() for act_id in inv.actors]}


def request_manage_timetable() -> dict:
    timetable = {}
    for time_key in sch.timetable:
        for job in sch.timetable[time_key]:
            if isinstance(job, EventJob):
                timetable[str(job.start_time)] = job.value
    return {'timetable': timetable}


def request_manage_data(request: dict) -> dict:
    def get_box(__key):
        out = {
            'key': __key,
            'boxes': {box.name: box.value for box in inv.boxes[__key]}}
        return out
    # Gather boxes related to the particular key or all boxes otherwise
    result = [get_box(key) for key in request['params']] if 'params' in request else [get_box(key) for key in inv.boxes]
    return {'modules-data': result}


def request_manage_ping(request: dict) -> dict:
    # Mandatory fields
    nid = request['params']['node']
    # Send signal to Agent
    try:
        return send_config_to_node(inv.nodes[nid], {"ping": ""}, request)
    except KeyError:
        raise NodeError(nid)


def request_manage_signal(request: dict) -> dict:
    """
    :param request: {"request": <req>, "params": {"node": <nid>, "module": <mal>, "value": <val>}}
    :return:
    """
    # Mandatory fields
    params_in = request['params']
    nid = params_in['node']
    mal = params_in['module']
    val = params_in['value']
    # Send signal to Agent
    try:
        return send_signal_to_module(inv.nodes[nid].modules[mal], val, request)
    except KeyError:
        raise ModuleError(nid, mal)


def request_manage_modules(request: dict) -> int:
    # Mandatory params
    params_in = request['params']
    nid = params_in['node']
    # Get corresponding node
    try:
        node = inv.nodes[nid]   # type: Node
    except KeyError:
        raise NodeError(nid)

    request_type = request['request']
    count = 0

    if request_type == 'add-module':
        # prepare
        gpio_to_add = []
        for module_cfg in params_in['gpio']:
            if module_cfg['p'] not in PINS_ESP8266:         # pin is in hardware scope
                continue
            if module_cfg['t'] not in KHOME_MODULE_TYPES:   # type is in inventory scope
                continue
            if module_cfg['p'] in node.get_pins_used():     # pin is in use
                continue
            if module_cfg['a'] in node.modules:             # alias is not unique
                continue
            gpio_to_add.append(module_cfg)
        # process
        if gpio_to_add:
            # upload
            result_gpio = Node.get_gpio(node.get_cfg_modules() + gpio_to_add)
            response = send_config_to_node(node, result_gpio, request)
            # sync
            if is_south_response_success(response):
                for module_cfg in gpio_to_add:
                    if inv.register_module(node, module_cfg):
                        count += 1
    elif request_type == 'del-module':
        # prepare
        gpio_current = node.get_cfg_modules()
        gpio_to_be = [cfg for cfg in gpio_current if cfg['a'] not in params_in['modules']]
        gpio_to_del = [cfg for cfg in gpio_current if cfg['a'] in params_in['modules']]
        # process
        if gpio_to_del:
            # upload
            result_gpio = Node.get_gpio(gpio_to_be)
            response = send_config_to_node(node, result_gpio, request)
            # sync
            if is_south_response_success(response):
                for module_cfg in gpio_to_del:
                    if inv.wipe_module(node, module_cfg['a']):
                        count += 1
    elif request_type == 'edit-module':     # Module name could be updated only
        # Check mandatory params
        try:
            mal = params_in['module']
            new_config = params_in['gpio']
            new_name = new_config['name']
        except KeyError:
            raise
        # Find Module
        try:
            module = node.modules[mal]
            # Update Module name
            if module.config['name'] != new_name:
                module.config['name'] = new_name
                inv.changed()
                count = int(inv.store_module(module))
        except KeyError:
            raise ModuleError

    return count


def request_manage_actors(command, params_in: dict):
    count = 0

    # if command == 'add-actor':
    #     actor = Actor.create(params_in)
    #     actor.store_db()
    #     inv.append_actor(actor)    # create + add to reg
    #     count += 1
    # elif command == 'del-actor':
    #     for oid in params_in['actors']:
    #         if oid in inv.actors:
    #             actor = inv.actors[oid]
    #             # wipe DB
    #             actor.delete_db()
    #             # wipe boxes
    #             inv.remove_actor(actor)
    #             count += 1
    # elif command == 'edit-actor':
    #     actor = inv.actors[params_in['actor']]
    #     count = actor.process_request(command, params_in)
    #     if count:
    #         actor.store_db()

    if count:
        inv.changed()

    return count


# Initiation ---

def create_actor(cfg, aid=''):
    """
    Actors instantiation function.
    :type cfg: Actor config str or dict.
    :type aid: (optional) Actor id
    :rtype: Actor
    """
    # init config
    cfg_obj = Actor.get_cfg_dict(cfg)
    # prepare globals
    globals_lower = {k.lower(): d for k, d in globals().items()}
    # instantiate object
    try:
        return globals_lower[cfg_obj['type'].lower()](
            cfg_obj,
            str(aid))
    except KeyError:
        log.warning('Actor class "%s" could not be found for: %s' % (cfg_obj['type'], json.dumps(cfg_obj)))
    return None


def start(server_address='localhost'):
    log.info('Starting with a Server on %s host.' % server_address)
    # Configuration
    try:
        # Storage
        storage_init(server_address)
        # Load configuration - Actors to Inventory
        actor_configs = inv.load_actors()
        for aid in actor_configs:
            inv.register_actor(create_actor(actor_configs[aid], aid))
        inv.correct_box_key()
        log.info('Configuration has been loaded from Storage.')
    except DatabaseError as err:
        log.error('Cannot init Storage %s.' % err)
    # Bus and Scheduler
    try:
        # Bus
        kbus.init(server_address, on_connect_to_bus, on_message_from_bus)
        log.info('Connected to Bus.')
        # Scheduler
        sch.init_timer()
        log.info('Scheduler has been started.')
        # Start
        kbus.listen()
    except ConnectionRefusedError as err:
        log.error('Cannot connected to Bus (%s). KHome server is stopped.' % err)
        log.info('KHome server has not been started.')
