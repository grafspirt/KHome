import unittest
import inventory as inv
from actors import create_actor


class InventoryTestCases(unittest.TestCase):
    def test01_addNode(self):
        prev_rev = inv.revision
        node = inv.register_node({"id": "I23456", "ver": "1", "inf": {"ip": "192.168.0.201", "rssi": "-77"}})
        self.assertGreater(inv.revision, prev_rev)
        self.assertIsNotNone(node)
        self.assertEquals(node.id, node.config['id'])
        self.assertEquals(node.id, "I23456")
        print('Node: %s' % node)
        self.assertEquals(inv.nodes['I23456'], node)
        self.assertEquals(node.config['inf']['ip'], "192.168.0.201")

    def test02_addModuleIR(self):
        node = inv.nodes['I23456']
        self.assertNotIn('IR', node.modules)
        prev_rev = inv.revision
        module = inv.register_module(node, {"t": "3", "a": "IR", "p": "2"})
        self.assertGreater(inv.revision, prev_rev)
        self.assertIsNotNone(module)
        self.assertEquals(module.id, module.config['a'])
        self.assertEquals(module.id, 'IR')
        print('Module: %s' % module)
        self.assertEquals(module.src_key, inv.Module.form_src_key(node.id, module.id))
        self.assertEquals(node.modules['IR'], module)
        self.assertEquals(inv.boxes[inv.Module.form_src_key(node.id, module.id)][module.box.name], module.box)  # Box

    def test03_addModuleTemp(self):
        prev_rev = inv.revision
        node = inv.nodes['I23456']
        self.assertNotIn('TEMP', node.modules)
        module = inv.register_module(node, {"t": "1", "a": "TEMP", "p": "5"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['TEMP'], module)
        self.assertIn(module.box.name, inv.boxes['I23456/TEMP'])  # Boxes
        self.assertGreater(inv.revision, prev_rev)

    def test04_addSecondNodeModule(self):
        prev_rev = inv.revision
        # Another Node
        node = inv.register_node({"id": "J23456", "ver": "1", "inf": {"ip": "192.168.0.202", "rssi": "-75"}})
        self.assertIsNotNone(node)
        self.assertEquals(inv.nodes['J23456'], node)
        # Another Module
        module = inv.register_module(node, {"t": "51", "a": "SW", "p": "2"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['SW'], module)
        self.assertIn(module.box.name, inv.boxes['J23456/SW'])
        self.assertGreater(inv.revision, prev_rev)

    def test05_tryNodeWOId(self):
        node = inv.register_node({"idddd": "I23456", "ver": "1", "inf": {"ip": "192.168.0.201", "rssi": "-77"}})
        self.assertIsNone(node)

    def test06_tryModuleWOAlias(self):
        node = inv.nodes['I23456']
        module = inv.register_module(node, {"t": "3", "aaa": "IR", "p": "2"})
        self.assertIsNone(module)

    def test07_tryActorWOType(self):
        actor = create_actor(
            {"typeeeeeee": "logthingspeak", "data": {"src":"I23456", "src_mdl": "TEMP"}},
            '51')
        self.assertIsNone(actor)

    def test08_tryActorWOData(self):
        actor = create_actor(
            {"type": "logthingspeak", "dataaaaa": {"src_mdl": "TEMP"}},
            '52')
        self.assertIsNone(actor)

    def test09_tryActorHandlerWOSrc(self):
        actor = create_actor(
            {"type": "logthingspeak", "data": {"src_mdl": "TEMP"}},
            '53')
        self.assertIsNone(actor)

    def test10_tryActorNone(self):
        actor = create_actor(
            {"type": "nosuchactor", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '15')
        self.assertIsNone(actor)

    def test11_addActorResend(self):
        actor = create_actor(
            {"type": "resend", "data": {"src": "I23456", "src_mdl": "IR", "map": [
                {"trg": "J23456", "trg_mdl": "SW", "in": "20df8976", "out": "3"},
                {"trg": "J23456", "trg_mdl": "SW", "in": "XXX", "outttt": "3"}
            ]}},
            '1')
        self.assertIsNotNone(actor)
        self.assertIn('20df8976', actor.mapping)
        self.assertNotIn('XXX', actor.mapping)
        print('Actor: %s' % actor)
        prev_rev = inv.revision
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(actor.src_key, 'I23456/IR')
        self.assertEquals(inv.actors['1'], actor)
        self.assertGreater(inv.revision, prev_rev)

    def test12_tryActorAverageWOBox(self):
        actor = create_actor(
            {"type": "average", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '2')
        self.assertIsNone(actor)

    def test13_addAverageBeforeSrc(self):
        actor = create_actor(
            {"type": "average", "data": {"src": "3", "box": "Avenger"}},
            '33')
        self.assertIsNotNone(actor)
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(actor.src_key, inv.SRCKEY_NOSRC)

    def test14_addActorLogdb(self):
        actor = create_actor(
            {"type": "logdb", "data": {"src": "3", "period": "10"}},
            '4')
        self.assertIsNotNone(actor)
        prev_rev = inv.revision
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(inv.actors['4'], actor)
        self.assertEquals(actor.config['data']['period'], '10')     # shall have 'period' param
        self.assertGreater(inv.revision, prev_rev)

    def test15_tryActorLogthingspeakWOKey(self):
        actor = create_actor(
            {"type": "logthingspeak", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '5')
        self.assertIsNone(actor)

    def test16_addActorLogthingspeak(self):
        actor = create_actor(
            {"type": "logthingspeak", "data": {"src": "I23456", "src_mdl": "TEMP", "key": "qwertyuiop", "map": [
                {"in": "temp", "out": "field1"},
                {"in": "humid", "out": "field2"}
            ]}},
            '6')
        self.assertIsNotNone(actor)
        prev_rev = inv.revision
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertGreater(inv.revision, prev_rev)

    def test17_addActorAverage(self):
        actor = create_actor(
            {"type": "average", "data": {"src": "I23456", "src_mdl": "TEMP", "box": "Average"}},
            '3')
        self.assertIsNotNone(actor)
        prev_rev = inv.revision
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(inv.actors['3'], actor)
        self.assert_('depth' in actor.config['data'])           # shall have 'depth' param
        self.assertIn(actor.box.name, inv.boxes['I23456/TEMP'])     # shall have box
        self.assertGreater(inv.revision, prev_rev)

    def test18_addActorIntervaljob(self):
        actor = create_actor(
            {"type": "schedule", "data": {"jobs": [
                {"event": "01:05", "value": "1"},
                {"event": "01:05.30", "value": "0"},
                {"period": "0.30", "start": "0", "stop": "5", "value": "3"},
                {"period": "0.30", "start": "10", "stop": "15", "value": "5"},
                {"period": "0.30", "start": "20", "stop": "25", "value": "7"},
                {"period": "0.30", "start": "30", "stop": "35", "value": "9"},
                {"period": "0.30", "start": "40", "stop": "45", "value": "11"},
                {"period": "0.30", "start": "50", "stop": "55", "value": "13"}]}},
            '17')
        self.assertIsNotNone(actor)

    def test20_finalizeActorLoad(self):
        actor_to_wipe = create_actor(
            {"type": "average", "data": {"src": "000", "box": "DeadAvenger"}},
            '333')
        self.assertIsNotNone(actor_to_wipe)
        self.assertEquals(inv.register_actor(actor_to_wipe), actor_to_wipe)
        self.assertIn('DeadAvenger', inv.boxes[inv.SRCKEY_NOSRC])
        self.assertIn('333', inv.actors)
        self.assertIn('Avenger', inv.boxes[inv.SRCKEY_NOSRC])
        actor_to_resrc = inv.actors['33']   # type: inv.Actor
        inv.load_actors_stop()
        self.assertEquals(actor_to_resrc.src_key, 'I23456/TEMP')
        self.assertIn('Avenger', inv.boxes[inv.actors['3'].src_key])
        self.assertNotIn('333', inv.actors)

    def test21_processActorResend(self):
        inv.handle_value('I23456/IR', "20df8976")
        self.assertEquals(inv.nodes['J23456'].session.request, '3')

    def test30_getManagerStructure(self):
        from manager import request_manage_structure
        data = request_manage_structure({"session": "123", "request": "get-structure"})
        # Overall
        self.assertIn('nodes', data)
        self.assertIn('actors', data)
        self.assertIn('module-types', data)
        self.assertIn('revision', data)
        # Nodes ---
        node_cfg = data['nodes'][0]
        nid = node_cfg['id']
        self.assertIn('inf', node_cfg)
        self.assertIn('gpio', node_cfg)
        # Nodes - gpio
        gpio_cfg = node_cfg['gpio'][0]
        self.assertIn('p', gpio_cfg)
        self.assertIn('t', gpio_cfg)
        self.assertIn('a', gpio_cfg)
        mal = gpio_cfg['a']
        self.assertIn('name', gpio_cfg)
        self.assertIn('src_key', gpio_cfg)
        self.assertEquals(gpio_cfg['src_key'], inv.Module.form_src_key(nid, mal))
        # Nodes - inf
        inf_cfg = node_cfg['inf']
        self.assertIn('ip', inf_cfg)
        self.assertIn('rssi', inf_cfg)
        # Actors ---
        actor_cfg = data['actors'][0]
        aid = actor_cfg['id']
        self.assertIn('type', actor_cfg)
        self.assertIn('data', actor_cfg)
        self.assertIn('src_key', actor_cfg)

    def test31_getManagerData(self):
        from manager import request_manage_data
        data = request_manage_data({"session": "456", "request": "get-data"})
        self.assertIn('boxes', data)
        self.assertIn('nodes-alive', data)
        boxes_data = data['boxes']
        alive_data = data['nodes-alive']
        for nid in inv.nodes:
            node = inv.nodes[nid]
            self.assertIn(nid, alive_data)
            self.assertIn('LTA', alive_data[nid])
            self.assertIn('alive', alive_data[nid])
            for mal in node.modules:
                self.assertIn(inv.Module.form_src_key(nid, mal), boxes_data)
                self.assertIn(inv.BOXNAME_MODULE, boxes_data[inv.Module.form_src_key(nid, mal)])

    def test98_wipeActor(self):
        actor = inv.actors['3']
        self.assertIn(actor.box.name, inv.boxes[actor.src_key])
        prev_rev = inv.revision
        inv.wipe_actor(actor)
        self.assertFalse('3' in inv.actors)
        self.assertGreater(inv.revision, prev_rev)
        self.assertNotIn(actor.box.name, inv.boxes[actor.src_key])

    def test99_wipeModule(self):
        # Module
        node = inv.nodes['I23456']
        self.assertTrue('IR' in node.modules)
        prev_rev = inv.revision
        inv.wipe_module(node, 'IR')
        self.assertFalse('IR' in node.modules)      # modules
        self.assertFalse('I23456/IR' in inv.boxes)  # boxes
        self.assertGreater(inv.revision, prev_rev)


if __name__ == '__main__':
    unittest.main()
