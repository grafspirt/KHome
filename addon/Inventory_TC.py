import unittest
from inventory import *
from manager import create_actor


class MyTestCase(unittest.TestCase):
    def test01_addNode(self):
        node = inv.register_node({"id": "I23456", "ver": "1", "inf": {"ip":"192.168.0.201","rssi":"-77"}})
        self.assertIsNotNone(node)
        self.assertEquals(node.id, "I23456")
        self.assertEquals(inv.nodes['I23456'], node)
        self.assertEquals(node.config['inf']['ip'], "192.168.0.201")

    def test02_addModule(self):
        node = inv.nodes['I23456']
        self.assert_('IR' not in node.modules)
        module = inv.register_module(node, {"t": "3", "a": "IR", "p": "2"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['IR'], module)
        self.assertEquals(module.config['p'], "2")
        self.assert_(module.box in inv.boxes['I23456/IR'])  # Boxes

    def test03_addModuleTemp(self):
        node = inv.nodes['I23456']
        self.assert_('TEMP' not in node.modules)
        module = inv.register_module(node, {"t": "1", "a": "TEMP", "p": "5"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['TEMP'], module)
        self.assert_(module.box in inv.boxes['I23456/TEMP'])  # Boxes

    def test04_addSecondNodeModule(self):
        # Another Node
        node = inv.register_node({"id": "J23456", "ver": "1", "inf": {"ip": "192.168.0.202", "rssi": "-75"}})
        self.assertIsNotNone(node)
        self.assertEquals(inv.nodes['J23456'], node)
        # Another Module
        module = inv.register_module(node, {"t": "51", "a": "SW", "p": "2"})
        self.assertIsNotNone(module)
        self.assertEquals(node.modules['SW'], module)
        self.assert_(module.box in inv.boxes['J23456/SW'])

    def test09_addActorHandlerWOSrc(self):
        actor = create_actor({"type": "logdb", "data": {"src_mdl": "TEMP"}})
        self.assertIsNone(actor)

    def test10_addActorNone(self):
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
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(inv.actors['1'], actor)

    def test12_addActorAverageWOBox(self):
        actor = create_actor(
            {"type": "average", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '3')
        self.assertIsNone(actor)

    def test13_addActorAverage(self):
        actor = create_actor(
            {"type": "average", "data": {"src": "I23456", "src_mdl": "TEMP", "box": "Average"}},
            '3')
        self.assertIsNotNone(actor)
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(inv.actors['3'], actor)
        self.assert_('depth' in actor.config['data'])           # shall have 'depth' param
        self.assert_(actor.box in inv.boxes['I23456/TEMP'])     # shall have box

    def test14_addActorLogdb(self):
        actor = create_actor(
            {"type": "logdb", "data": {"src": "3", "period": "10"}},
            '4')
        self.assertIsNotNone(actor)
        self.assertEquals(inv.register_actor(actor), actor)
        self.assertEquals(inv.actors['4'], actor)
        self.assertEquals(actor.config['data']['period'], '10')     # shall have 'period' param

    def test15_addActorLogthingspeakWOKey(self):
        actor = create_actor(
            {"type": "logthingspeak", "data": {"src": "I23456", "src_mdl": "TEMP"}},
            '11')
        self.assertIsNone(actor)

    def test16_addActorLogthingspeak(self):
        actor = create_actor(
            {"type": "logthingspeak", "data": {"src": "I23456", "src_mdl": "TEMP", "key": "qwertyuiop", "map": [
                {"in": "temp", "out": "field1"},
                {"in": "humid", "out": "field2"}
            ]}},
            '11')
        self.assertIsNotNone(actor)

    def test21_processActorResend(self):
        handle_value('I23456/IR', "20df8976")
        self.assertEquals(inv.nodes['J23456'].session.request, '3')

    def test98_wipeBox(self):
        module = inv.nodes['J23456'].modules['SW']
        self.assert_(module.box in inv.boxes['J23456/SW'])
        inv.wipe_box(module.box)
        self.assert_(module.box not in inv.boxes['J23456/SW'])

    def test99_wipeModule(self):
        # Module
        node = inv.nodes['I23456']
        self.assert_('IR' in node.modules)
        inv.wipe_module(node, 'IR')
        self.assert_('IR' not in node.modules)      # modules
        self.assert_('I23456/IR' not in inv.boxes)  # boxes


if __name__ == '__main__':
    unittest.main()
