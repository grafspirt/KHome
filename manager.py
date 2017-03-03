#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import log
import bus
import inventory as inv
from inventory import DatabaseError as StorageError
import scheduler as sch
from actors import create_actor
import json


# Initiation ---

def start(server_address: str='localhost'):
    # Init log
    log.init('/var/log/khome.log' if server_address == 'localhost' else '')
    log.info('Starting with a Server on %s.' % server_address)
    # Configuration
    try:
        # Storage
        inv.storage_init(server_address)
        # Load - Actors to Inventory
        actor_configs = inv.load_actors_start()
        for aid in actor_configs:
            inv.register_actor(create_actor(actor_configs[aid], aid))
        inv.load_actors_stop()
        log.info('Configuration has been loaded from Storage.')
    except StorageError as err:
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
    except (ConnectionRefusedError, TimeoutError) as err:
        log.error('Cannot connect to Bus (%s).' % err)
        log.info('KHome manager stops with failure.')


# Bus ISR ---

def on_connect_to_bus():
    # Ask all Agents for configs
    bus.send(
        '/config/%s' % inv.MODULES_ALL,
        "i!",
        True)


def on_message_from_bus(topic, message):
    coordinates = topic.split('/')
    try:
        # Message -> Object or Str
        try:
            message_object = json.loads(message)    # json-structured data
        except (TypeError, ValueError):
            message_object = message                # plain data
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
            handle_north(message)
    except KeyError as error_object:
        bus.send(
            "/error",
            "Key %s is absent in the request: %s" % (str(error_object), message))
    except IndexError:
        bus.send(
            "/error",
            "Wrong request format in topic [%s]: %s" % (topic, message))


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
            gpio_data = node.send_config({"get": "gpio"})
            if is_agent_response_success(gpio_data):
                try:
                    node = inv.nodes[nid]
                    for module_cfg in gpio_data['gpio']:
                        inv.register_module(node, module_cfg)
                    log.info('Node %s has been initiated with Modules: %s' %
                             (str(node), str(["%s (%s)" % (
                                 node.modules[m].config['a'],
                                 node.modules[m].config['name']) for m in node.modules])))
                except KeyError:
                    pass
        # Ask all modules data
        inv.nodes[nid].send_config({"get": "data"})


def handle_module_data(nid: str, mal: str, data):
    """
    Handle data from Module.
    :param nid: id of a Node hosting Module sent data
    :param mal: alias of Module sent data
    :param data: data in a message object
    :return: nothing
    """
    try:
        # if it is not NACK
        if is_agent_response_success(data):
            inv.nodes[nid].modules[mal].handle_data(data)
    except KeyError:
        pass


def handle_agent_response(coordinates: list, response) -> bool:
    try:
        node = inv.nodes[coordinates[2]]    # type: inv.Node
        node.alive()
        if node.session.active:
            node.session.stop(response)
            if coordinates[1] != 'data':    # data from Module should be processed by handle_module_data
                return True                 # further processing is not necessary
    except (AttributeError, KeyError):
        pass
    return False


def is_agent_response_success(response) -> bool:
    """
    Possible responses: string, dict: ack, nack, data.
    :param response: response come from Agent
    :return: True if response is not NACK
    """
    return not(isinstance(response, dict) and inv.KHOME_AGENT_INTERFACE['negative'] in response)


# Handling North ---

def handle_north(message):
    """
    Process command from data bus and send result back
    :param message: kind of {"session":<session-id>,"request":<command>,"params":{<params-set>}}
    """
    answer = ""
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
            answer = request_manage_modules(request)
        elif request_type in ['add-actor', 'del-actor', 'edit-actor']:
            answer = request_manage_actors(request)
        # elif request_type in ['add-mapping', 'del-mapping']:
        #     try:
        #         answer = '{"ack": "%d"}' % inv.actors[params['actor']].handle_north(request_type, params)
        #     except KeyError:
        #         pass
    except (TypeError, inv.ModuleError, inv.NodeError) as err:
        answer = {inv.KHOME_AGENT_INTERFACE['negative']: str(err)}
    except KeyError as err:
        answer = {inv.KHOME_AGENT_INTERFACE['negative']: "Key %s is absent in the request" % err}
    except StorageError:
        answer = {inv.KHOME_AGENT_INTERFACE['negative']: "There are problems in DB"}
    finally:
        # Answer with the same session id
        answer_north(sid, answer)


def answer_north(sid: str, message):
    if sid:
        bus.send(
            '/manager/%s' % sid,
            message if message else '{"unknown":}')


def request_manage_structure(request: dict) -> dict:
    try:
        if str(inv.revision) == request['params']['revision']:
            # Nothing has been changed - export revision number only
            return {'revision': str(inv.revision)}
    except KeyError:
        pass
    # Export the whole structure otherwise
    return {
        'revision': inv.revision,
        'module-types': inv.KHOME_AGENT_INTERFACE['module_types'],
        'nodes': [inv.nodes[nid].get_export() for nid in inv.nodes],
        'actors': [inv.actors[aid].get_export() for aid in inv.actors]}


def request_manage_timetable() -> dict:
    """ Get all EventJobs registered in the Scheduler timetable. """
    timetable = []
    for time_key in sorted(sch.timetable):
        for job in sch.timetable[time_key]:
            if isinstance(job, sch.EventJob):
                timetable.append({"time": str(job.start_time), "signal": job.value, "handler": job.handler})
    return {"timetable": timetable}


def request_manage_data(request: dict) -> dict:
    # Gather Boxes data
    def get_boxes_by_key(__key):
        return {box_name: inv.boxes[__key][box_name].value for box_name in inv.boxes[__key]}

    # Gather Nodes alive data
    def get_alive_by_nid(__nid):
        node = inv.nodes[__nid]     # type: inv.Node
        return {"alive": node.is_alive, "LTA": node.last_time_alive}

    # Result
    if 'params' in request:
        # Gather boxes with the box keys from the request
        boxes = {key: get_boxes_by_key(key) for key in request['params']}
        return {"boxes": boxes}
    else:
        # Gather all registered boxes + Nodes alive data
        boxes = {key: get_boxes_by_key(key) for key in inv.boxes}
        return {"boxes": boxes, "nodes-alive": {nid: get_alive_by_nid(nid) for nid in inv.nodes}}


def request_manage_ping(request: dict) -> dict:
    # Mandatory fields
    nid = request['params']['node']
    # Send signal to Agent
    try:
        return inv.nodes[nid].send_config({"ping": ""}, request)
    except KeyError:
        raise inv.NodeError(nid)


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
        response = inv.nodes[nid].modules[mal].send_signal(val, request)
        return {"ack": response} if is_agent_response_success(response) else response
    except KeyError:
        raise inv.ModuleError(nid, mal)


def request_manage_modules(request: dict) -> dict:
    # Mandatory params
    params_in = request['params']
    # Get target node
    try:
        node = inv.nodes[params_in['node']]   # type: inv.Node
    except KeyError:
        raise inv.NodeError(params_in['node'])
    # Initiate
    response = {inv.KHOME_AGENT_INTERFACE['negative']: "Nothing to update"}
    gpio_result = []
    # Do the job
    if request['request'] == 'add-module':
        # Check mandatory params
        try:
            gpio_from_request = params_in['gpio']
        except KeyError:
            raise
        # prepare
        gpio_to_add = []
        for updated_cfg in gpio_from_request:
            if updated_cfg['p'] not in inv.KHOME_AGENT_INTERFACE['pins_available']['esp8266']:  # pin is in hardware
                continue
            if updated_cfg['t'] not in inv.KHOME_AGENT_INTERFACE['module_types']:   # type is in inventory scope
                continue
            if updated_cfg['p'] in node.get_pins_used():     # pin is in use
                continue
            if updated_cfg['a'] in node.modules:             # alias is not unique
                continue
            gpio_to_add.append(updated_cfg)
        # process
        if gpio_to_add:
            gpio_result = node.get_cfg_modules() + gpio_to_add
            # upload
            response = node.send_config(inv.Node.get_gpio(gpio_result), request)
            # sync up
            if is_agent_response_success(response):
                for module_cfg in gpio_to_add:
                    inv.register_module(node, module_cfg, True)
    elif request['request'] == 'del-module':
        # prepare
        gpio_current = node.get_cfg_modules()
        gpio_result = [cfg for cfg in gpio_current if cfg['a'] not in params_in['modules']]
        gpio_to_delete = [cfg for cfg in gpio_current if cfg['a'] in params_in['modules']]
        # process
        if gpio_to_delete:
            # upload
            response = node.send_config(inv.Node.get_gpio(gpio_result), request)
            # sync up
            if is_agent_response_success(response):
                for module_cfg in gpio_to_delete:
                    inv.wipe_module(node, module_cfg['a'])
    elif request['request'] == 'edit-module':
        # Check mandatory params
        try:
            gpio_from_request = params_in['gpio']
        except KeyError:
            raise
        # Get GPIOs being updated
        gpio_to_update = {}
        for updated_cfg in gpio_from_request:
            try:
                mal = updated_cfg['a']
                module = node.modules[mal]  # type: inv.Module
                # Update Module name
                if 'name' in updated_cfg:
                    if module.config['name'] != updated_cfg['name']:
                        module.config['name'] = updated_cfg['name']
                        inv.store_module(module)
                # Update Module cfg
                is_gpio_updated = False
                existing_cfg = module.get_cfg()
                for entity in inv.KHOME_AGENT_INTERFACE['gpio']:
                    if entity in updated_cfg and entity != 'a':
                        existing_cfg[entity] = updated_cfg[entity]
                        is_gpio_updated = True
                # Process
                if is_gpio_updated:
                    gpio_to_update[mal] = existing_cfg
            except KeyError:
                pass
        # Merge with the rest GPIOs
        for mal in node.modules:
            if mal in gpio_to_update:
                gpio_result.append(gpio_to_update[mal])
            else:
                gpio_result.append(node.modules[mal].get_cfg())
        # upload
        response = node.send_config(inv.Node.get_gpio(gpio_result), request)
        # sync up
        if is_agent_response_success(response):
            for mal in gpio_to_update:
                module = node.modules[mal]
                module.config = gpio_to_update[mal]
                module.apply_changes()
    # Initiate Actuators with the actual values (from boxes) after Modules updating
    if is_agent_response_success(response):
        for mal in node.modules:
            module = node.modules[mal]
            if module.is_actuator() and module.box.value:
                module.send_signal(module.box.value)

    return response


def request_manage_actors(request: dict) -> dict:
    # Mandatory params
    params_in = request['params']
    # Initiate
    response = {inv.KHOME_AGENT_INTERFACE['negative']: "Nothing to update"}
    count = 0
    # Do the job
    if request['request'] == 'add-actor':
        actor = create_actor(params_in)
        if actor:
            actor.store_db()
            inv.register_actor(actor)
            count += 1
    elif request['request'] == 'del-actor':
        # Wipe from Inventory
        for aid in params_in['actors']:
            if aid in inv.actors:
                # Del from Storage
                inv.actors[aid].delete_db()
                # Del from Inventory
                inv.wipe_actor(inv.actors[aid])
                count += 1
    elif request['request'] == 'edit-actor':
        # Mandatory params
        data_from_request = params_in['data']
        aid = data_from_request['id']
        # Update
        if aid in inv.actors:
            actor = inv.actors[aid]     # type: inv.Actor
            for item in data_from_request:
                if item != 'id':
                    actor.config['data'][item] = data_from_request[item]    # TODO: there shall be merge, not set
                    count += 1
            # Store sync
            if count:
                actor.store_db()
                actor.apply_changes()

    # Answer
    if count:
        response = {"ack": "1"}
    return response
