#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from time import time
from threading import Lock
from threading import Timer
import log
import pymysql
from pymysql import DatabaseError
import bus

# Interface description
KHOME_AGENT_INTERFACE = {
    'ver': "1",
    'positive': "ack",
    'negative': "nack",
    'commands': ["get", "ping", "clean", "gpio", "brdg"],
    'get': ["gpio", "brdg", "data"],
    'gpio': ["p", "t", "a", "prd"],
    'brdg': ["ond", "ols", "map"],
    'map': ["in", "out"],
    # Modules from the following list are allowed for processing
    'module_types': {
        # Sensors
        '1': "Generic Sensor Timer",
        '2': "Generic Trigger Sensor",
        '3': "IR Sensor",
        '4': "DHT Sensor",
        '5': "Obstacle Sensor",
        '6': "PIR Sensor",
        # Actuators
        '51': "Switch"
    },
    # Pins available for setup
    'pins_available': {
        'esp8266': ['0', '2', '4', '5', '9', '10', '12', '13', '14', '15', '16']
    },
    # Error codes Module could send back
    'error_codes': {
        '1': "Agent does not have such Module",
        '2': "Target Module is not Actuator",
        '3': "Unknown signal",
        '10': "Wrong message format - not JSON",
        '11': "Wrong GPIO configuration",
        '12': "Modules maximum is reached",
        '100': "GPIO storage failure",
        '101': "Bridge storage failure"
    }
}

# Other
SRCKEY_NOSRC = '~'
SRCKEY_SYSTEM = '$'
BOXNAME_MODULE = '@'
MODULES_ALL = '~'
TIMEOUT_RESPONSE = {KHOME_AGENT_INTERFACE['negative']: "timeout"}


# Base classes

class BaseObject(object):
    """
    Prototype of all objects in the structure.
    It has Id and Configuration.
    """
    def __init__(self, cfg):
        super().__init__()
        self.config = self.get_cfg_dict(cfg)
        self.id = ''

    def set_id(self, oid: str):
        self.id = oid
        return self.id

    def get_cfg(self) -> dict:
        """
        Get internal configuration object.
        :return: dict - object internal configuration object
        """
        return self.config.copy()

    @staticmethod
    def get_cfg_dict(cfg) -> dict:
        """
        Make config data as an object for internal storing.
        :param cfg: String/dict describing the Object
        :return: dict - object for internal storing
        """
        if isinstance(cfg, str):
            return json.loads(cfg)
        else:
            return cfg


class AgentObject(BaseObject):
    """ Prototype of an Agent in the structure. Id is stored in Configuration. """
    def __init__(self, cfg):
        super().__init__(cfg)
        # config of these object types contains ID, it should be taken from there
        self.set_id(self.extract_id(self.config))

    @staticmethod
    def extract_id(cfg: dict) -> str:
        """
        Extract ID from object internal configuration (KObject.get_cfg_dict).
        :param cfg: dict - object internal configuration
        :return: string - object ID (oid)
        """
        return cfg['id']


class DBObject(BaseObject):
    """ Prototype of an object stored in DB. """
    def __init__(self, cfg, oid: str):
        super().__init__(cfg)
        self.set_id(oid)  # ID is stored in DB entity, it should be replicated to config

    # def get_cfg(self, for_db=False) -> dict:
    #     cfg = super().get_cfg()
    #     if for_db:
    #         del cfg['id']
    #         return json.dumps(cfg)
    #     else:
    #         return cfg
    #
    # def set_id(self, oid: str):
    #     """
    #     Store an object ID in DB in a corresponding attribute and replicate it to the config.
    #     :param oid: Object ID
    #     :return: nothing
    #     """
    #     self.config['id'] = super().set_id(oid)

    def store_db(self):
        pass

    def delete_db(self):
        pass


# Agents - Nodes, Modules and attendant entities

class Module(AgentObject):
    def __init__(self, cfg, nid):
        """
        :param cfg: Config string/object describing the Module
        :param nid: Node ID hosting the Module
        :return: nothing
        """
        super().__init__(cfg)
        self.nid = nid
        self.src_key = Module.form_src_key(self.nid, self.id)
        self.box = Box(self, BOXNAME_MODULE)
        # Load/init module name
        if 'name' not in self.config:
            self.config['name'] = self.id
            cursor = storage_open()
            if cursor:
                try:
                    if cursor.execute("SELECT name FROM modules WHERE nid=%s AND mal=%s", (self.nid, self.id)):
                        self.config['name'] = cursor.fetchall()[0][0]
                except DatabaseError as err:
                    log.warning("Cannot load Module name for %s %s." % (str(self), str(err)))
                finally:
                    storage_close(cursor)

    def __str__(self):
        return "[%s]%s" % (self.nid, self.id)

    def get_cfg(self):
        cfg = super().get_cfg()
        cfg['src_key'] = self.src_key
        return cfg

    @staticmethod
    def extract_id(cfg) -> str:
        return cfg['a']

    @staticmethod
    def form_src_key(nid: str, mal: str = '') -> str:
        if mal:
            return '%s/%s' % (nid, mal)
        else:
            return nid

    def is_actuator(self):
        return int(self.config['t']) > 50

    def send_signal(self, signal, context_request: dict = None):
        return nodes[self.nid].session.start(
            bus.send(
                '/signal/%s/%s' % (self.nid, self.id),
                signal,
                True),
            context_request)

    def handle_data(self, data):
        self.periodical_alive_check()
        # Data processing
        self.box.value = data
        handle_value(self.src_key, data)

    def periodical_alive_check(self):
        try:
            period = float(self.config['prd'])
            node = nodes[self.nid]
            if time() - node.last_time_alive > period:
                node.alive(False)
            else:
                Timer(period, self.periodical_alive_check).start()
        except (KeyError, ValueError):
            # there is no period value or it is incorrect
            pass


class ModuleError(Exception):
    def __init__(self, nid, mal):
        self.nid = nid
        self.mal = mal

    def __str__(self):
        return "There is no %s module in inventory" % str(self)


class Bridge(AgentObject):
    pass


class Box(object):
    """
    An object storing value [value] of Modules and Actors [hoster].
    It is registered in Manager Box register [add_box()] with a key = nid/mal.
    All Actors processing values from some Module (having nid/mal) have the save key [get_source()].
    """
    def __init__(self, owner, name):
        self.owner = owner
        self.name = name
        self.value = ''


class Node(AgentObject):
    """ Hardware unit managing Modules. """
    def __init__(self, cfg):
        super().__init__(cfg)
        self.type = 'esp8266'                       # Node hardware type
        self.modules = {}                           # Modules installed on the Node
        self.session = NodeSession(self)            # session of interconnection with the Node
        self.is_alive = False                       # Node alive flag
        self.last_time_alive = time()               # LTA - Last Time Alive

    def __str__(self):
        return "[%s]" % self.id

    def alive(self, is_alive: bool=True):
        """ Note the latest time when Agent was alive. """
        self.is_alive = is_alive
        if is_alive:
            self.last_time_alive = time()  # LTA - Last Time Alive

    def add_module(self, module_cfg: dict):
        new_module = Module(module_cfg, self.id)
        if new_module.id not in self.modules:
            # Module does not exist in internal Inventory so add it and return result object
            self.modules[new_module.id] = new_module
            return new_module
        else:
            # Module exists in internal Inventory so return nothing
            return None

    def del_module(self, mal: str) -> bool:
        if mal in self.modules:
            # remove from Node inventory
            del self.modules[mal]
            return True
        else:
            return False

    def get_cfg_modules(self) -> list:
        return [self.modules[mal].get_cfg() for mal in self.modules]

    def get_cfg(self) -> dict:
        cfg = super().get_cfg()
        cfg['gpio'] = self.get_cfg_modules()     # inject gpio config
        return cfg

    def get_pins_used(self) -> list:
        """ Get the list of all pins used by modules installed. """
        return [self.modules[mal].config['p'] for mal in self.modules]

    @staticmethod
    def get_gpio(cfg_entity_or_list) -> dict:
        alias = 'gpio'
        tags_allowed = KHOME_AGENT_INTERFACE[alias]
        if isinstance(cfg_entity_or_list, list):
            alias_result = []
            for src_cfg_unit in cfg_entity_or_list:
                trg_cfg_unit = {}
                for tag in list(tags_allowed):
                    try:
                        trg_cfg_unit[tag] = src_cfg_unit[tag]
                    except KeyError:
                        pass
                alias_result.append(trg_cfg_unit)
        else:
            alias_result = '???'    # TODO: finish this idea
        return {alias: alias_result}

    def send_config(self, config, context_request: dict=None):
        return self.session.start(
            bus.send(
                '/config/%s' % self.id,
                config,
                True),
            context_request)


class NodeError(Exception):
    def __init__(self, nid):
        self.nid = nid

    def __str__(self):
        return "There is no %s node in inventory" % str(self)


class NodeSession(object):
    def __init__(self, node: Node):
        self.node = node            # parent
        self.active = False
        self.request = None
        self.response = None
        # north
        self.request_north = None
        self.id = ''                # Session ID (SID) if there is request from the north
        # timer and lock
        self.timeout_timer = None
        self.lock = Lock()

    def start(self, message, request_north: dict):
        """
        Start connection session with the Agent
        :param message: Request (str/dict) sent to the Agent
        :param request_north: Request from North initially sent to the Manager
        """
        self.active = True
        self.request = message
        self.response = None
        # north
        self.request_north = request_north
        self.id = self.request_north['session'] if request_north else ''
        # timer for timeout
        self.timeout_timer = Timer(3, self.timeout)
        self.timeout_timer.start()
        # lock the process till some result
        self.lock.acquire()
        self.lock.acquire()
        self.lock.release()
        # result
        return self.response

    def stop(self, result):
        """
        Stop connection session after Agent answer.
        :return: flag that north session is open.
        """
        self.active = False
        self.response = result
        # north
        self.request_north = None
        self.id = ''
        # timer
        if self.timeout_timer:
            self.timeout_timer.cancel()
            self.timeout_timer = None
        # unfreeze waiting process (if there is frozen one)
        try:
            self.lock.release()
        except RuntimeError:
            pass

    def timeout(self):
        """ Stop connection session by timeout. """
        if self.active:
            log.warning('Timeout for the message: %s' % self.request)
            self.node.alive(False)
            self.stop(TIMEOUT_RESPONSE)


# Actors

class Actor(DBObject):
    """ Units processing data came from Agents. """
    def __init__(self, cfg, db_id: str):
        super().__init__(cfg, db_id)
        self.active = bool(self.config['active']) if 'active' in self.config else True
        self.box = Box(self, self.config['data']['box']) if 'box' in self.config['data'] else None
        self.src_key = ''

    def __str__(self):
        return "%s#%s" % (self.config['type'].capitalize(), self.id)

    def set_src_key(self) -> str:
        return self.src_key

    def get_cfg(self) -> dict:
        cfg = super().get_cfg()
        cfg['src_key'] = self.src_key
        cfg['id'] = self.id
        return cfg

    def store_db(self):
        """ Store config in DB. """
        cursor = storage_open()
        if cursor:
            try:
                if self.id and int(self.id) > 0:    # -id is a temp Actors have not been saved in Storage (was down)
                    cursor.execute("UPDATE actors SET config=%s WHERE id=%s", (json.dumps(self.get_cfg()), self.id))
                else:
                    cursor.execute("INSERT INTO actors (config) VALUES (%s)", json.dumps(self.get_cfg()))
                    self.set_id(str(cursor.lastrowid))
                    storage_save()
            except DatabaseError as err:
                log.warning("Cannot store %s in Storage %s." % (str(self), str(err)))
            finally:
                storage_close(cursor)
        else:
            if not self.id:
                self.set_id(str(-id(self)))     # init Actor with a temporary id

    def delete_db(self):
        """ Delete config from DB. """
        if self.id:
            cursor = storage_open()
            if cursor:
                try:
                    cursor.execute("DELETE FROM actors WHERE id=%s", self.id)
                    storage_save()
                except DatabaseError as err:
                    log.warning("Cannot delete %s from Storage %s." % (str(self), str(err)))
                finally:
                    storage_close(cursor)

    def set_active(self, status: bool):
        self.active = status
        self.config['active'] = status

    def process_signal(self, sig):
        """
        Process action related to actor and signal.
        :param sig: Signal - a message got from the [Node]Module
        """
        pass

    def apply_changes(self):
        """ Method which is to be triggered after the Actor has been updated. """
        pass


class Handler(Actor):
    """
    Actor data source is a Module or another Actor.
    """
    def __new__(cls, cfg, db_id):
        # Source (src) is mandatory for all actors except Schedule
        if 'src' in cfg['data']:
            return super().__new__(cls)
        else:
            log.warning(
                'Actor %s#%s could not be loaded: no "src" in config.' %
                (cfg['type'].lower(), db_id))
            return None

    def set_src_key(self) -> str:
        """ Get Key of data source (Module) starting a chain of Actors. """
        data_src = self.config['data']['src']
        if 'src_mdl' in self.config['data']:
            # source - Module
            self.src_key = Module.form_src_key(data_src, self.config['data']['src_mdl'])
        elif data_src in actors:
            # source - another Actor
            self.src_key = actors[data_src].set_src_key()
        else:
            # source - unknown (could not been loaded yet - see load_actors_stop())
            self.src_key = SRCKEY_NOSRC
        return self.src_key

    def get_handler_key(self):
        return Module.form_src_key(
            self.config['data']['src'],
            self.config['data']['src_mdl'] if 'src_mdl' in self.config['data'] else '')


class Generator(Actor):
    """
    Actor data source is a system.
    """
    def set_src_key(self) -> str:
        self.src_key = SRCKEY_SYSTEM
        return self.src_key


# Storage

__storage_client = None
__storage_lock = Lock()


def storage_init(server_address: str):
    global __storage_client
    if __storage_client:
        __storage_client.close()
    __storage_client = pymysql.connect(host=server_address, user='khome', passwd='khome', db='khome')


def storage_open() -> pymysql.cursors.Cursor:
    if __storage_client:
        cursor = __storage_client.cursor()
        if cursor:
            __storage_lock.acquire()
            return cursor
    return None


def storage_close(cursor: pymysql.cursors.Cursor):
    if cursor:
        cursor.close()
        __storage_lock.release()


def storage_save():
    if __storage_client:
        __storage_client.commit()


def load_actors_start() -> dict:
    result = {}
    cursor = storage_open()
    if cursor:
        if cursor.execute("SELECT id, config FROM actors ORDER BY id"):
            result = {row[0]: row[1] for row in cursor}
        storage_close(cursor)
    return result


def load_actors_stop():
    """
    If the Box is hosted under Actor which source is another Actor which has not been loaded yet
    then this Box would be tied to SRCKEY_NOSRC.
    After all Actors are loaded the system tries to re-assign all such Boxed to correct keys.
    """
    try:
        aids_to_remove = []
        # Re-set Source Key for all Actors
        for aid in actors:
            actor = actors[aid]     # type: Actor
            if actor.src_key == SRCKEY_NOSRC:
                if actor.set_src_key() == SRCKEY_NOSRC:
                    log.warning('Actor %s is to be deleted as no source was found for it.' % actor)
                    aids_to_remove.append(aid)
                elif actor.box:
                    # Re-register Actor Box using new source key
                    __register_box(actor.box)
        # Wipe source-less Actors and Boxes
        for aid in aids_to_remove:
            wipe_actor(actors[aid])
        del boxes[SRCKEY_NOSRC]
    except KeyError:
        pass    # there are no postponed Boxes


def store_module(module: Module) -> bool:
    cursor = storage_open()
    if cursor:
        try:
            cursor.execute("INSERT INTO modules (nid, mal, name) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=%s",
                           (module.nid, module.id, module.config['name'], module.config['name']))
            storage_save()
            return True
        except DatabaseError as err:
            log.warning("Cannot store Module in Storage %s." % str(err))
        finally:
            storage_close(cursor)
    return False


def forget_module(module: Module):
    cursor = storage_open()
    if cursor:
        try:
            cursor.execute("DELETE FROM modules WHERE nid=%s AND mal=%s", (module.nid, module.id))
            storage_save()
        except DatabaseError as err:
            log.warning("Cannot remove Module from Storage %s." % str(err))
        finally:
            storage_close(cursor)


# Inventory

revision = 0    # version of KHome inventory
nodes = {}      # Nodes registered in KHome
actors = {}     # Actors processing data from Modules
handlers = {}   # Actors processing data from a related Module/Actor
boxes = {}      # Objects storing data of Modules/Actors


def changed():
    """ Mark that some changes in inventory have been made. """
    global revision
    revision += 1


def register_node(node_cfg):
    """
    Create and append Node to Manager node list.
    Node object is created/updated every time Hello message is received from an Agent.
    :return: Added Node or None if it exists
    :rtype: Node
    """
    # Parse config as a temp Node
    new_node = Node(node_cfg)
    # Store the new one
    if new_node.id not in nodes:
        nodes[new_node.id] = new_node
        changed()
        return new_node
    else:
        return None


def register_module(node: Node, module_cfg, added: bool=False) -> Module:
    new_module = node.add_module(module_cfg)
    if new_module:
        changed()
        # add Module Box to Manager Box list
        __register_box(new_module.box)
        # Store Module data in Storage
        if added:
            store_module(new_module)
    return new_module


def register_actor(actor: Actor) -> Actor:
    """
    Append the Actor to the Manager registry.
    :rtype: Actor
    """
    if actor:
        # add Actor to Actors list
        actors[actor.id] = actor
        actor.set_src_key()
        # add Actor as a handler to Handlers list
        __register_handler(actor)
        # add Actor Box to Boxes list
        if actor.box:
            __register_box(actor.box)
        # note that the structure was updated
        changed()
    return actor


def __register_box(box: Box):
    """
    Add Box object to the Manager Box list using the key based on nid/mal got from box owner.
    :param box: Box to be registered
    """
    key = box.owner.src_key
    try:
        boxes[key][box.name] = box
    except KeyError:
        boxes[key] = {}
        boxes[key][box.name] = box


def __register_handler(handler):
    """
    Register the handler (Actor) which is to process signals from [Node]Module or other Actor in chain.
    :param handler: object of handler (Actor) to be registered
    :return: nothing
    """
    if issubclass(handler.__class__, Handler):
        handler_key = handler.get_handler_key()
        try:
            handlers[handler_key].append(handler)
        except KeyError:
            handlers[handler_key] = [handler]


def wipe_module(node: Node, mal: str) -> bool:
    try:
        module = node.modules[mal]
        forget_module(module)
        if node.del_module(mal):
            __wipe_boxes_by_key(module.src_key)
            changed()
            return True
    except KeyError:
        pass
    return False


def wipe_actor(actor: Actor):
    # del Actor Box from Boxes list
    if actor.box:
        __wipe_box(actor.box)
    # del Actor-handler from Handlers list
    __wipe_handler(actor)
    # del Actor from Actors list
    del actors[actor.id]
    # del Actor from Storage
    actor.delete_db()
    # note that the structure was updated
    changed()


def __wipe_handler(handler):
    if issubclass(handler.__class__, Handler):
        handler_key = handler.get_handler_key()
        handlers[handler_key].remove(handler)


def __wipe_box(box: Box):
    """
    Wipe Box from Manager Box list.
    :param box: Box to be wiped.
    """
    del boxes[box.owner.src_key][box.name]


def __wipe_boxes_by_key(box_key: str):
    """
    Wipe set of Boxes tied to one box key.
    :param box_key: Box key to be wiped with all boxes tied to.
    """
    del boxes[box_key]


def handle_value(key, value):
    if key in handlers:
        for actor in handlers[key]:
            # Actor is triggered if it is active
            if actor.active:
                actor.process_signal(value)
            # try to process Actor Box by handlers(actors) referring to this Actor
            if actor.box:
                handle_value(actor.id, actor.box.value)
