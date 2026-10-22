import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('community_config', pathlib.Path(__file__).parents[1] / 'community-config.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class ConfigTests(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(module.render({}), 'COMMUNITY_CPUS=0.200000\nCOMMUNITY_MEMORY=469762048\n')

    def test_example_matches_defaults_and_legacy_capacity(self):
        import json
        example = json.loads((pathlib.Path(__file__).parents[2] / 'community-limits.example.json').read_text(encoding='utf-8'))
        self.assertEqual(module.render(example), module.render({}))
        self.assertEqual(example['limits'], {'maxConcurrency': 4, 'requestsPerSecond': 3.0,
                         'burst': 4, 'dailyTrafficBytes': 5368709120, 'spoolBytes': 104857600})

    def test_participant_can_lower_every_limit(self):
        self.assertEqual(module.render({'cpuCores': 0.1, 'maxMemoryBytes': 268435456,
            'limits': {'maxConcurrency': 1, 'requestsPerSecond': 0.2, 'burst': 1,
                       'dailyTrafficBytes': 104857600, 'spoolBytes': 1048576}}),
                         'COMMUNITY_CPUS=0.100000\nCOMMUNITY_MEMORY=268435456\n')

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
