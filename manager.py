#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pymysql import DatabaseError
from urllib.parse import urlencode
import http.client as http_client
import bus
from kinventory import *
from scheduler import *


# Actors and Jobs

class ActorWithMapping(Handler):
    """ Actor using mapping data in config. """
    class MapUnit(DBObject):
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
    """ Resending source data to another Module. """
    def process_signal(self, sig):
        # as-is from 1 to 1
        try:
            send_signal_to_module(
                inv.nodes[self.config['data']['trg']].modules[self.config['data']['trg_mdl']],
                sig)
        except KeyError:
            pass
        # mapping from 1 to N
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
    """ Log source data to ThingSpeak.com using mapping for complex signal. """
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
            data_to_send['field1'] = sig    # default field

        # send
        connection = http_client.HTTPConnection("api.thingspeak.com:80")
        connection.request(
            "POST",
            "/update",
            urlencode(data_to_send),
            {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"})
        # response = connection.getresponse()  # response.status, response.reason


class LogDB(ActorLog):
    """ Log source data to DB. """
    def log(self, sig):
        if storage_client:
            if isinstance(sig, dict):
                str_value = ""
                for key in sorted(sig):
                    str_value += ',"%s":"%s"' % (key, sig[key])
                str_value = '{' + str_value[1:] + '}'
            elif isinstance(sig, str):
                str_value = sig
            else:
                str_value = '{"unknown-value-type":}'

            cursor = storage_client.cursor()
            cursor.execute(
                "INSERT INTO sens_data (sensor, value) VALUES (%s, %s)",
                (self.get_box_key(), str_value)
            )
            storage_client.commit()
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


# Bus ISR ---

def on_connect_to_bus():
    # Ask all Agents for configs
    bus.send(
        "/config/" + ALL_MODULES,
        "i!",
        True)


def on_message_from_bus(topic, message):
    coordinates = topic.split('/')
    try:
        # Message -> Object or Str
        message_object = json.loads(message)    # json-structured data
        if not isinstance(message_object, dict):
            message_object = message            # plain data
        # South - data from an Agent
        if coordinates[1] in ('nodes', 'data'):
            # Request response
            if handle_agent_response(coordinates, message_object):
                pass
            # Node data
            elif coordinates[1] == 'nodes':     # /nodes/<nid>
                handle_node_data(
                    coordinates[2],
                    message_object)
            # Module data
            elif coordinates[1] == 'data':      # /data/<nid>/<mal>
                handle_module_data(
                    coordinates[2],
                    coordinates[3],
                    message_object)
        # North - request for the Manager
        elif coordinates[1] == 'manager':                           # /manager
            # Process request for the Manager
            process_request(message)
    except KeyError as error_object:
        bus.send(
            "/error",
            "Key %s is absent in the request: %s" % (str(error_object), message))
    except IndexError:
        bus.send(
            "/error",
            "Wrong request format in topic [%s]: %s" % (topic, message))
    except ValueError:
        bus.send(
            "/error",
            "Request is not a JSON in topic [%s]: %s" % (topic, message))


# Interaction ---

def send_signal_to_module(module: Module, data, north_request: dict = None):
    return inv.nodes[module.nid].session.start(
        bus.send(
            '/signal/%s/%s' % (module.nid, module.id),
            data,
            True),
        north_request)


def send_config_to_node(node: Node, config, north_request: dict = None):
    return node.session.start(
        bus.send(
            '/config/%s' % node.id,
            config,
            True),
        north_request)


def answer_north(sid: str, message):
    if sid:
        bus.send(
            '/manager/%s' % sid,
            message if message else '{"unknown":}')


# Handling South ---

def handle_node_data(nid: str, data):
    """
    Handle data from Node.
    :param nid: id of a Node sent data
    :param data: data in a message object
    :return: nothing
    """
    # Node said hello
    if isinstance(data, dict) and 'id' in data:
        # Add/update Node
        node = inv.register_node(data)
        # Ask the Node for Module cfg
        if node:
            gpio_data = send_config_to_node(node, {"get": "gpio"})
            if gpio_data:
                try:
                    node = inv.nodes[nid]
                    for module_cfg in gpio_data['gpio']:
                        inv.register_module(node, module_cfg)
                    log.info('Modules of Node [%s] have been uploaded to Inventory: %s' %
                             (nid, str(["%s (%s)" % (node.modules[m].get_cfg()['a'], node.modules[m].get_cfg()['name']) for m in node.modules])))
                except KeyError:
                    pass
        # Ask all modules data
        send_config_to_node(inv.nodes[nid], {"get": "data"})


def handle_module_data(nid: str, mal: str, data):
    """
    Handle data from Module.
    :param nid: id of a Node hosting Module sent data
    :param mal: alias of Module sent data
    :param data: data in a message object
    :return: nothing
    """
    try:
        module = inv.nodes[nid].modules[mal]
        # if it is not NACK
        if not (isinstance(data, dict) and 'nack' in data):
            # store signal in the Box bound to Module
            module.box.value = data
            # find/trigger right handler
            handle_value(
                module.get_box_key(),
                module.box.value)
    except KeyError:
        pass


def handle_agent_response(coordinates: list, response) -> bool:
    try:
        node = inv.nodes[coordinates[2]]    # type: Node
        node.alive()
        if node.session.active:
            node.session.stop(response)
            if coordinates[1] != 'data':    # data from Module should be processed by handle_module_data
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
        # Report - Agents structure
        if request_type == 'get-structure':
            answer = request_manage_structure(request)
        # Report - Data
        elif request_type == 'get-data':
            answer = request_manage_data(request)
        # Report - Timetable
        elif request_type == 'get-timetable':
            answer = request_manage_timetable()
        # South - Agent ping
        elif request_type == 'ping':
            answer = request_manage_ping(request)
        # South - Signal sending
        elif request_type == 'signal':
            answer = request_manage_signal(request)
        # South - Module configuration
        elif request_type in ['add-module', 'del-module', 'edit-module']:
            answer = answer_template % request_manage_modules(request)

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
    finally:
        # Answer with the same session id
        answer_north(sid, answer if answer else {"nack": "timeout"})


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
        response = send_signal_to_module(inv.nodes[nid].modules[mal], val, request)
        return {"ack": response} if response else ''
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
            response = send_config_to_node(
                node,
                Node.get_gpio(node.get_cfg_modules() + gpio_to_add),
                request)
            # sync
            if is_south_response_success(response):
                for module_cfg in gpio_to_add:
                    if inv.register_module(node, module_cfg, True):
                        count += 1
            else:
                pass    # TODO: handle error which could arise during add/edit
    elif request_type == 'del-module':
        # prepare
        gpio_current = node.get_cfg_modules()
        gpio_left = [cfg for cfg in gpio_current if cfg['a'] not in params_in['modules']]
        gpio_deleted = [cfg for cfg in gpio_current if cfg['a'] in params_in['modules']]
        # process
        if gpio_deleted:
            # upload
            response = send_config_to_node(
                node,
                Node.get_gpio(gpio_left),
                request)
            # sync up
            if is_south_response_success(response):
                for module_cfg in gpio_deleted:
                    if inv.wipe_module(node, module_cfg['a']):
                        count += 1
    elif request_type == 'edit-module':
        # Check mandatory params
        try:
            mal = params_in['module']
            cfg_to_update = params_in['gpio']
        except KeyError:
            raise
        # Find Module
        try:
            module = node.modules[mal]  # type: Module
            # Update Module name
            if 'name' in cfg_to_update:
                if module.config['name'] != cfg_to_update['name']:
                    module.config['name'] = cfg_to_update['name']
                    store_module(module)
            # Update Module cfg
            module_cfg = module.get_cfg()
            gpio_updated = False
            for entity in KHOME_AGENT_INTERFACE['gpio']:
                if entity in cfg_to_update and entity != 'a':
                    module_cfg[entity] = cfg_to_update[entity]
                    gpio_updated = True
            if gpio_updated:
                # Make new Node GPIO
                new_gpio = []
                for alias in node.modules:
                    if alias == mal:
                        new_gpio.append(module_cfg)
                    else:
                        new_gpio.append(node.modules[alias].get_cfg())
                # upload
                response = send_config_to_node(
                    node,
                    Node.get_gpio(new_gpio),
                    request)
                # sync up
                if is_south_response_success(response):
                    module.config = module_cfg
                    count = 1
                    inv.changed()
                else:
                    pass    # handle error which could arise during add/edit
        except KeyError:
            raise ModuleError(nid, mal)

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
        actor_configs = load_actors()
        for aid in actor_configs:
            inv.register_actor(create_actor(actor_configs[aid], aid))
        inv.correct_box_key()
        log.info('Configuration has been loaded from Storage.')
    except DatabaseError as err:
        log.error('Cannot init Storage %s.' % err)
    # Bus and Scheduler
    try:
        # Bus
        bus.init(server_address, on_connect_to_bus, on_message_from_bus)
        log.info('Connected to Bus.')
        # Scheduler
        sch.init_timer()
        log.info('Scheduler has been started.')
        # Start
        bus.listen()
    except ConnectionRefusedError as err:
        log.error('Cannot connected to Bus (%s). KHome server is stopped.' % err)
        log.info('KHome server has not been started.')
