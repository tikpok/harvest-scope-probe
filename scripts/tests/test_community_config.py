import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('community_config', pathlib.Path(__file__).parents[1] / 'community-config.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class ConfigTests(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(module.render({}), 'COMMUNITY_CPUS=0.100000\nCOMMUNITY_MEMORY=268435456\n')

    def test_raised_allocation(self):
        self.assertEqual(module.render({'cpuCores': 0.5, 'maxMemoryBytes': 536870912}),
                         'COMMUNITY_CPUS=0.500000\nCOMMUNITY_MEMORY=536870912\n')

    def test_invalid_or_injected_values(self):
        for value in [True, 0, -1, float('nan'), '0.1\nOTHER=value']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                module.render({'cpuCores': value})
        with self.assertRaises(ValueError):
            module.render({'limits': {'maxConcurrency': 0}})
        with self.assertRaises(ValueError):
            module.render({'maxMemoryBytes': 1})

if __name__ == '__main__':
    unittest.main()
