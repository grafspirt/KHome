#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from time import time
from threading import Lock
from threading import Timer
import log
import pymysql

# Interface signature
KHOME_AGENT_INTERFACE = {
    'ver': '1',
    'commands': ['get', 'ping', 'clean', 'gpio', 'brdg'],
    'get': ['gpio'],
    'gpio': ['p', 't', 'a'],
    'brdg': ['ond', 'ols', 'map'],
    'map': ['in', 'out']
}

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

# Other
BOXKEY_NOSRC = '~'
BOXKEY_SYSTEM = '#'
ALL_MODULES = '~'


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

    def get_cfg(self, for_db=False) -> dict:
        cfg = super().get_cfg()
        if for_db:
            del cfg['id']
        return cfg

    def set_id(self, oid: str):
        """
        Store an object ID in DB in a corresponding attribute and replicate it to the config.
        :param oid: Object ID
        :return: nothing
        """
        self.config['id'] = super().set_id(oid)

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
        # Default Box for the Module
        self.box = Box(self, self.config['a'])
        # Load/init module name
        if 'name' not in self.config:
            name = self.id
            if storage_client:
                cursor = storage_client.cursor()
                if cursor.execute("SELECT name FROM modules WHERE nid=%s AND mal=%s", (self.nid, self.id)):
                    name = cursor.fetchall()[0][0]
                cursor.close()
            self.config['name'] = name

    @staticmethod
    def extract_id(cfg) -> str:
        return cfg['a']

    def get_box_key(self) -> str:
        """ Get Key of data source - this Module itself. """
        return Box.box_key(self.nid, self.id)


class ModuleError(Exception):
    def __init__(self, nid, mal):
        self.nid = nid
        self.mal = mal

    def __str__(self):
        return "There is no [%s]%s module in inventory" % (self.nid, self.mal)


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

    @staticmethod
    def box_key(nid: str, mal: str = '') -> str:
        return nid + ('/' + mal if mal else '')


class Node(AgentObject):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.type = 'esp8266'                       # Node hardware type
        self.modules = {}                           # Modules installed on the Node
        self.session = NodeSession(self)            # session of interconnection with the Node
        self.is_alive = False                       # Node is in the net

    def alive(self, is_alive: bool=True):
        """ Note the latest time when Agent was alive. """
        self.is_alive = is_alive
        self.config['alive'] = is_alive
        if is_alive:
            self.config['LTA'] = time()  # LTA - Last Time Alive

    def add_module(self, module_cfg: dict):
        # Check Module unique in Node
        new_module = Module(module_cfg, self.id)
        if new_module.id not in self.modules:
            # add to to internal inventory
            self.modules[new_module.id] = new_module
            return new_module
        else:
            return None

    def del_module(self, mal: str) -> bool:
        if mal in self.modules:
            # remove from Node inventory
            del self.modules[mal]
            return True
        else:
            return False

    def get_cfg_modules(self) -> list:
        result = []
        for mal in self.modules:
            cfg = self.modules[mal].get_cfg()
            result.append(cfg)
        return result

    def get_cfg(self):
        result_cfg = super().get_cfg()
        result_cfg['gpio'] = self.get_cfg_modules()     # inject gpio config
        return result_cfg

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
                    trg_cfg_unit[tag] = src_cfg_unit[tag]
                alias_result.append(trg_cfg_unit)
        else:
            alias_result = '???'    # TODO: finish this idea
        return {alias: alias_result}


class NodeError(Exception):
    def __init__(self, nid):
        self.nid = nid

    def __str__(self):
        return "There is no [%s] node in inventory" % self.nid


class NodeSession(object):
    def __init__(self, node: Node):
        self.node = node            # parent
        self.active = False
        self.request = None
        self.response = {}
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
        self.response = {}
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

    def stop(self, result: dict):
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
        # unfreeze waiting process
        self.lock.release()

    def timeout(self):
        """ Stop connection session by timeout. """
        if self.active:
            log.warning('Timeout for the message: %s' % self.request)
            self.node.alive(False)
            self.stop({})


# Actors - Units processing data came from Agents

class Actor(DBObject):
    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        self.active = bool(self.config['active']) if 'active' in self.config else True
        self.box = Box(self, self.config['data']['box']) if 'box' in self.config['data'] else None

    def store_db(self):
        """ Store config in DB. """
        if storage_client:
            cursor = storage_client.cursor()
            if int(self.id) > 0:
                cursor.execute("UPDATE actors SET config=%s WHERE id=%s", (self.get_cfg(True), self.id))
            else:
                cursor.execute("INSERT INTO actors (config) VALUES (%s)", (self.get_cfg(True)))
                self.set_id(cursor.lastrowid)
            storage_client.commit()
            cursor.close()
        else:
            if not self.id:
                self.set_id(str(-id(self)))     # init Actor with a temporary id

    def delete_db(self):
        """ Delete config from DB. """
        if storage_client and self.id:
            cursor = storage_client.cursor()
            cursor.execute("DELETE FROM actors WHERE id=%s", self.id)
            storage_client.commit()
            cursor.close()

    def set_active(self, status: bool):
        self.active = status
        self.config['active'] = status

    def get_box_key(self) -> str:
        """ Key of Actor data source. """
        pass

    def process_signal(self, sig):
        """
        Process action related to actor and signal.
        :param sig: Signal - a message got from the [Node]Module
        :return: nothing
        """
        pass

    def process_request(self, command, params):
        """
        Process request got and redirected by the manager.
        DNF! Call Manager().inventory_changed() in case of inventory changing
        :param command: Request alias
        :param params: Parameters transferred along with the request
        :return: A number of entities changed
        """
        count = 0
        if command == 'edit-actor':
            if 'status' in params:
                self.set_active(bool(params['status']))
                count += 1
        return count


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
                'Actor %s#%s could not be loaded as it does not have "src" in config.' %
                (cfg['type'].lower(), db_id))
            return None

    def get_box_key(self) -> str:
        """ Get Key of data source (Module) started actor chain. """
        data = self.config['data']
        data_src = data['src']
        if 'src_mdl' in data:
            # source - Module
            return Box.box_key(data_src, data['src_mdl'])
        elif data_src in inv.actors:
            # source - another Actor
            return inv.actors[data_src].get_box_key()
        return BOXKEY_NOSRC     # maybe this source has not been loaded yet (see correct_box_key())

    def get_handler_key(self):
        return Box.box_key(
            self.config['data']['src'],
            self.config['data']['src_mdl'] if 'src_mdl' in self.config['data'] else '')


class Generator(Actor):
    """
    Actor data source is a system.
    """
    def get_box_key(self):
        return BOXKEY_SYSTEM


# Storage

def storage_init(server_address):
    global storage_client
    if storage_client:
        storage_client.close()
    storage_client = pymysql.connect(host=server_address, user='khome', passwd='khome', db='khome')


def store_module(module: Module) -> bool:
    if storage_client:
        cursor = storage_client.cursor()
        cursor.execute("INSERT INTO modules (nid, mal, name) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=%s",
                       (module.nid, module.id, module.config['name'], module.config['name']))
        storage_client.commit()
        cursor.close()
        return True
    return False


def forget_module(module: Module):
    if storage_client:
        cursor = storage_client.cursor()
        cursor.execute("DELETE FROM modules WHERE nid=%s AND mal=%s", (module.nid, module.id))
        storage_client.commit()
        cursor.close()


def load_actors() -> dict:
    if storage_client:
        cursor = storage_client.cursor()
        cursor.execute("SELECT id, config FROM actors ORDER BY id")
        result = {row[0]: row[1] for row in cursor}
        cursor.close()
        return result
    else:
        return {}

storage_client = None


# Inventory - Carry data objects

class Inventory(object):
    def __init__(self):
        # version of KHome inventory
        self.revision = 0
        # Nodes registered in KHome
        self.nodes = {}     # type: {Node}
        # Actors processing data from Modules
        self.actors = {}    # type: {Actor}
        # Actors processing data from a related Module/Actor
        self.handlers = {}  # type: {[Module,Actor]}
        # Objects storing data of Modules/Actors
        self.boxes = {}     # type: {[Box]}

    def changed(self):
        """ Mark that some changes in inventory have been made. """
        self.revision += 1

    def register_node(self, node_cfg):
        """
        Create and append Node to Manager node list.
        Node object is created/updated every time Hello message is received from an Agent.
        :return: Added Node or None if it exists
        :rtype: Node
        """
        # Parse config as a temp Node
        new_node = Node(node_cfg)
        # Store the new one
        if new_node.id not in self.nodes:
            self.nodes[new_node.id] = new_node
            self.changed()
            return new_node
        else:
            return None

    def register_actor(self, actor) -> Actor:
        """
        Append the Actor to the Manager registry.
        :rtype: Actor
        """
        if actor:
            # add Actor to Actors list
            self.actors[actor.id] = actor
            # add Actor as a handler to Handlers list
            self.register_handler(actor)
            # add Actor Box to Boxes list
            if actor.box:
                self.register_box(actor.box)
            # note that the structure was updated
            self.changed()
        return actor

    def wipe_actor(self, actor):
        # del Actor Box from Boxes list
        if actor.box:
            self.wipe_box(actor.box)
        # del Actor-handler from Handlers list
        self.wipe_handler(actor)
        # del Actor from Actors list
        del self.actors[actor.id]
        # note that the structure was updated
        self.changed()

    def register_module(self, node: Node, module_cfg: dict) -> Module:
        new_module = node.add_module(module_cfg)
        if new_module:
            self.changed()
            # add Module Box to Manager Box list
            self.register_box(new_module.box)
        return new_module

    def wipe_module(self, node: Node, mal: str) -> bool:
        try:
            module = node.modules[mal]
            forget_module(module)
            if node.del_module(mal):
                self.changed()
                # remove Module Box from Manager Box list
                self.wipe_boxes_by_key(Box.box_key(node.id, mal))
                return True
        except KeyError:
            pass
        return False

    def register_handler(self, handler):
        """
        Register the handler (Actor) which is to process signals from [Node]Module or other Actor in chain.
        :param handler: object of handler (Actor) to be registered
        :return: nothing
        """
        if issubclass(handler.__class__, Handler):
            handler_key = handler.get_handler_key()
            try:
                self.handlers[handler_key].append(handler)
            except KeyError:
                self.handlers[handler_key] = [handler]

    def wipe_handler(self, handler):
        if issubclass(handler.__class__, Handler):
            handler_key = handler.get_handler_key()
            self.handlers[handler_key].remove(handler)

    def register_box(self, box: Box):
        """
        Add Box object to the Manager Box list using the key based on nid/mal got from an Actor.
        :param box: object of Register to be added
        :return: nothing
        """
        key = box.owner.get_box_key()
        try:
            self.boxes[key].append(box)
        except KeyError:
            self.boxes[key] = [box]

    def wipe_box(self, box: Box):
        key = box.owner.get_box_key()
        self.boxes[key].remove(box)

    def wipe_boxes_by_key(self, key: str):
        del self.boxes[key]

    def correct_box_key(self):
        """
        If the Box is hosted under Actor which source is another Actor which has not been loaded yet
        then this Box would be tied to BOXKEY_NOSRC.
        After all Actors are loaded the system tries to re-assign all such Boxed to correct keys.
        """
        try:
            # Try to find sources to the "pending" Handlers
            boxes_wo_src = self.boxes[BOXKEY_NOSRC].copy()
            self.boxes[BOXKEY_NOSRC] = []
            for box in boxes_wo_src:
                self.register_box(box)
            # Wipe Handlers without a source from Inventory
            for actor in [box.owner for box in self.boxes[BOXKEY_NOSRC]]:
                log.warning(
                    'Actor %s#%s is to be deleted as no source was found for it.' %
                    (actor.config['type'].lower(), actor.id))
                self.wipe_actor(actor)
        except KeyError:
            pass    # there are no postponed Boxes

# Inventory instance
inv = Inventory()
