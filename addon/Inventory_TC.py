import unittest
import inventory as inv
from actors import create_actor


class InventoryTestCases(unittest.TestCase):
    def test01_addNode(self):
        prev_rev = inv.revision
        node = inv.register_node({"id": "I23456", "ver": "1", "inf": {"ip": "192.168.0.201", "rssi": "-77"}})
        self.assertIsNotNone(node)
        self.assertEquals(node.id, "I23456")
        self.assertEquals(inv.nodes['I23456'], node)
        self.assertEquals(node.config['inf']['ip'], "192.168.0.201")
        self.assertGreater(inv.revision, prev_rev)

    def test02_addModule(self):
        prev_rev = inv.revision
        node = inv.nodes['I23456']
        self.assert_('IR' not in node.modules)
        module = inv.register_module(node, {"t": "3", "a": "IR", "p": "2"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['IR'], module)
        self.assertEquals(module.config['p'], "2")
        self.assertIn(module.box.name, inv.boxes['I23456/IR'])  # Boxes
        self.assertGreater(inv.revision, prev_rev)

    def test03_addModuleTemp(self):
        prev_rev = inv.revision
        node = inv.nodes['I23456']
        self.assert_('TEMP' not in node.modules)
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

    def test09_tryActorHandlerWOSrc(self):
        actor = create_actor(
            {"type": "logdb", "data": {"src_mdl": "TEMP"}},
            '5555')
        self.assertIsNone(actor)

    def test10_tryActorNone(self):
        actor = create_actor(
            {"type": "nosuchactor", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '15')
        self.assertIsNone(actor)

    def test11_addActorResend(self):
        actor = create_actor(
            {"type": "resend", "data": {"src": "I23456", "src_mdl": "IR", "map": [
                {"trg": "J23456", "trg_mdl": "SW", "in": "20df8976", "out": "3"}
            ]}},
            '1')
        self.assertIsNotNone(actor)
        prev_rev = inv.revision
        self.assertEquals(inv.register_actor(actor), actor)
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
        actor = create_actor(
            {"type": "average", "data": {"src": "000", "box": "DeadAvenger"}},
            '333')
        self.assertIsNotNone(actor)
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertIn('DeadAvenger', inv.boxes[inv.BOXKEY_NOSRC])
        self.assertIn('333', inv.actors)
        self.assertIn('Avenger', inv.boxes[inv.BOXKEY_NOSRC])
        inv.load_actors_finalize()
        self.assertIn('Avenger', inv.boxes[inv.actors['3'].get_box_key()])
        self.assertNotIn('333', inv.actors)

    def test21_processActorResend(self):
        inv.handle_value('I23456/IR', "20df8976")
        self.assertEquals(inv.nodes['J23456'].session.request, '3')

    def test98_wipeActor(self):
        actor = inv.actors['3']
        self.assertIn(actor.box.name, inv.boxes[actor.get_box_key()])
        prev_rev = inv.revision
        inv.wipe_actor(actor)
        self.assertFalse('3' in inv.actors)
        self.assertGreater(inv.revision, prev_rev)
        self.assertNotIn(actor.box.name, inv.boxes[actor.get_box_key()])

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
