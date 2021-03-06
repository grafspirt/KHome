#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import log
import inventory as inv
from inventory import DatabaseError as StorageError
import scheduler as sch
from urllib.parse import urlencode
import http.client as http_client


# Factory

def create_actor(cfg, aid=''):
    """
    Actors instantiation function.
    :type cfg: Actor config str or dict.
    :type aid: (optional) Actor id
    :rtype: Actor
    """
    try:
        # init config
        cfg_obj = inv.Actor.get_cfg_dict(cfg)
        actor_type = cfg_obj['type'].lower()
        try:
            # prepare globals
            globals_lower = {k.lower(): d for k, d in globals().items()}
            # instantiate object
            return globals_lower[actor_type](cfg_obj, str(aid))
        except KeyError:
            log.warning('Actor %s#%s could not be loaded - there is no appropriate class.' % (actor_type, aid))
    except ValueError:
        log.warning('Actor #%s could not be loaded - invalid configuration: %s.' % (aid, cfg))
    except KeyError:
        log.warning('Actor #%s could not be loaded - \'type\' is absent in: %s.' % (aid, cfg))
    return None


# Classes

class ActorWithMapping(inv.Handler):
    """ Actor using mapping data in config. """
    class MapUnit(inv.ConfigObject):
        """ Object used as a mapping record in ActorWithMapping configuration. """
        def __init__(self, cfg):
            super().__init__(cfg)

        @classmethod
        def extract_id(cls, cfg: dict) -> str:
            return cfg['in']

        @classmethod
        def check_cfg(cls, cfg):
            super(ActorWithMapping.MapUnit, cls).check_cfg(cfg)
            return cfg['out']

    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        # mapping data
        self.mapping = {}
        self.init_mapping()

    def append_map_unit(self, map_cfg):
        """
        Create MapUnit basing on its config and add it to registry.
        :param map_cfg: dict - config object of this mapping unit
        :return: result object
        """
        try:
            map_unit = ActorWithMapping.MapUnit(map_cfg)  # create
            self.mapping[map_unit.id] = map_unit  # add to the register
            return map_unit
        except KeyError as err:
            log.warning('MapUnit could not be loaded - %s is absent in: %s.' % (err, map_cfg))
            return None

    def init_mapping(self):
        """ Init dict with mapping units from the config. """
        if 'map' in self.config['data']:
            for map_cfg in self.config['data']['map']:
                self.append_map_unit(map_cfg)

    def apply_changes(self):
        """ Re-init mapping structure when changes done. """
        self.mapping = {}
        self.init_mapping()


class ActorLog(inv.Handler):
    """
    Actor logging source data with a period defined in ticks.
    The logging action [self.log(sig)] is defined in child classes.
    """
    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        # period
        self.count = 0
        if 'period' not in self.config['data']:
            self.config['data']['period'] = 1   # default value - every time to log

    def process_signal(self, sig):
        self.count += 1
        if self.count >= int(self.config['data']['period']):
            self.log(sig)
            self.count = 0

    def log(self, signal):
        pass

    @staticmethod
    def to_string(signal) -> str:
        # Prepare value
        if isinstance(signal, dict):
            # Sort sub-values by key
            str_value = ""
            for key in sorted(signal):
                str_value += ',"%s":"%s"' % (key, signal[key])
            return '{' + str_value[1:] + '}'
        elif isinstance(signal, str):
            return signal
        else:
            return '{"unknown-value-type":}'


class Resend(ActorWithMapping):
    """ Resending source data to another Agent. """
    def process_signal(self, signal):
        # Root values
        out = self.config['data']['out'] if 'out' in self.config['data'] else signal
        try:
            target = {"nid": self.config['data']['trg'], "mal": self.config['data']['trg_mdl']}
        except KeyError:
            target = None
        # Mapping values
        for map_id in self.mapping:
            if str(map_id) == str(signal):
                map_unit_cfg = self.mapping[map_id].config
                if 'out' in map_unit_cfg:
                    out = map_unit_cfg['out']
                try:
                    target = {"nid": map_unit_cfg['trg'], "mal": map_unit_cfg['trg_mdl']}
                except KeyError:
                    pass
        # Sending
        if target:
            try:
                inv.nodes[target['nid']].modules[target['mal']].send_signal(out)
            except KeyError:
                pass    # there is no such Agent


class LogThingSpeak(ActorWithMapping, ActorLog):
    """ Log source data to ThingSpeak.com using mapping for complex signal. """
    @classmethod
    def check_cfg(cls, cfg):
        super(LogThingSpeak, cls).check_cfg(cfg)
        return cfg['data']['key']

    def log(self, signal):
        # Prepare
        data_to_send = {'key': self.config['data']['key']}
        if isinstance(signal, dict):
            for alias in signal:
                try:
                    data_to_send[self.mapping[alias].config['out']] = signal[alias]
                except KeyError:
                    pass
        else:
            try:
                single_field = self.config['data']['out']
            except KeyError:
                single_field = 'field1'     # default field
            data_to_send[single_field] = signal
        # Send
        connection = http_client.HTTPConnection("api.thingspeak.com:80")
        connection.request(
            "POST",
            "/update",
            urlencode(data_to_send),
            {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"})
        # Response
        response = connection.getresponse()  # response.status, response.reason
        if response.status != 200:
            log.warning('%s cannot upload data, response: %d "%s"' % (self, response.status, response.reason))


class LogDB(ActorLog):
    """ Log source data to DB. """
    def log(self, signal):
        # Store value
        cursor = inv.storage_open()
        if cursor:
            try:
                cursor.execute(
                    "INSERT INTO sens_data (sensor, value) VALUES (%s, %s)",
                    (self.src_key, ActorLog.to_string(signal)))
                inv.storage_save()
            except StorageError:
                pass
            finally:
                inv.storage_close(cursor)


class LogBus(ActorWithMapping, ActorLog):
    """ Log source data to the bus (with mapping). """
    def log(self, signal):
        # Root values
        out = self.config['data']['out'] if 'out' in self.config['data'] else signal
        try:
            target = self.config['data']['trg']
        except KeyError:
            target = None
        # Mapping values
        for map_id in self.mapping:   # type: ActorWithMapping.MapUnit
            if str(map_id) == str(signal):
                map_unit_cfg = self.mapping[map_id].config
                if 'out' in map_unit_cfg:
                    out = map_unit_cfg['out']
                try:
                    target = map_unit_cfg['trg']
                except KeyError:
                    pass
        # Logging
        if target:
            inv.bus.send(target, out)


class Average(inv.Handler):
    """ Averages values on some period, defined by 'depth' parameter. """
    @classmethod
    def check_cfg(cls, cfg):
        super(Average, cls).check_cfg(cfg)
        return cfg['data']['box']

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
        Store result in its own Box
        Example: {"key1": "number", "key2": "number", ...}
        :param sig: signal data
        :return: nothing
        """
        if isinstance(sig, dict):
            averaged_sig = sig.copy()
            for key in sig:
                averaged_sig[key] = self.__calc(key, float(sig[key]))
            self.box.value = averaged_sig
        elif isinstance(sig, (int, float, str)):
            self.box.value = self.__calc('.', sig)


class Schedule(inv.Generator):
    """ Actor managing Scheduler job and acts as a data source for Handlers. """
    def __init__(self, cfg, db_id):
        super().__init__(cfg, db_id)
        # Instantiate jobs from cfg
        self.schedule()

    def schedule(self):
        for job_cfg in self.config['data']['jobs']:
            try:
                if 'event' in job_cfg:  # job on time - event
                    sch.EventJob(
                        self.id,
                        job_cfg['value'],
                        sch.JobTime(job_cfg['event'])
                    ).schedule()
                elif 'period' in job_cfg:  # job within a period - period
                    sch.IntervalEventJob(
                        self.id,
                        job_cfg
                    ).schedule()
            except KeyError:
                pass  # not valid config - do not load = do nothing

    def apply_changes(self):
        super().apply_changes()
        sch.clear(self.id)
        self.schedule()
