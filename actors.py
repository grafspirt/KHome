#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import log
import inventory as inv
from inventory import DatabaseError as StorageError
import scheduler as sch
from urllib.parse import urlencode
import http.client as http_client


class ActorWithMapping(inv.Handler):
    """ Actor using mapping data in config. """
    class MapUnit(inv.DBObject):
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
            self.config['data']['period'] = 5

    def process_signal(self, sig):
        self.count += 1
        if self.count >= int(self.config['data']['period']):
            self.log(sig)
            self.count = 0

    def log(self, sig):
        pass

    @staticmethod
    def to_string(sig) -> str:
        # Prepare value
        if isinstance(sig, dict):
            # Sort sub-values by key
            str_value = ""
            for key in sorted(sig):
                str_value += ',"%s":"%s"' % (key, sig[key])
            return '{' + str_value[1:] + '}'
        elif isinstance(sig, str):
            return sig
        else:
            return '{"unknown-value-type":}'


class Resend(ActorWithMapping):
    """ Resending source data to another Module. """
    def process_signal(self, signal):
        # as-is from 1 to 1
        try:
            inv.nodes[self.config['data']['trg']].modules[self.config['data']['trg_mdl']].send_signal(signal)
        except KeyError:
            pass
        # mapping from 1 to N
        for map_key in self.mapping:
            cfg = self.mapping[map_key].config
            try:
                if cfg['in'] == signal:
                    inv.nodes[cfg['trg']].modules[cfg['trg_mdl']].send_signal(cfg['out'])
            except KeyError:
                pass


class LogThingSpeak(ActorWithMapping, ActorLog):
    """ Log source data to ThingSpeak.com using mapping for complex signal. """
    def __new__(cls, cfg, db_id):
        if 'key' in cfg['data']:
            return super().__new__(cls, cfg, db_id)
        else:
            log.warning(
                'Actor %s#%s could not be loaded: no "key" in config.' %
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
        # Store value
        cursor = inv.storage_open()
        if cursor:
            try:
                cursor.execute(
                    "INSERT INTO sens_data (sensor, value) VALUES (%s, %s)",
                    (self.get_box_key(), ActorLog.to_string(sig)))
                inv.storage_save()
            except StorageError:
                pass
            finally:
                inv.storage_close(cursor)


class Average(inv.Handler):
    """ Averages values on some period, defined by 'depth' parameter. """
    def __new__(cls, cfg, db_id):
        # Box is mandatory for Average actors
        if 'box' in cfg['data']:
            return super().__new__(cls, cfg, db_id)
        else:
            log.warning('Actor %s#%s could not be loaded: no "box" in config.' %
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
        sch.clear(self.id)
        self.schedule()
