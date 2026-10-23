"""Build contract tests without Cargo, native binaries or cloud calls."""
import os
import unittest
from unittest.mock import patch

from remote_stages import environment, validate_artifact


class BuildProfileTests(unittest.TestCase):
    def test_inherited_debug_is_explicitly_overridden(self):
        with patch.dict(os.environ, {'CARGO_PROFILE_RELEASE_DEBUG': '2'}, clear=True):
            selected = environment()
        self.assertEqual(selected['CARGO_PROFILE_RELEASE_DEBUG'], '0')

    def test_other_inherited_profile_override_rejects(self):
        for key in ['CARGO_PROFILE_RELEASE_OPT_LEVEL', 'CARGO_PROFILE_RELEASE_LTO', 'CARGO_PROFILE_DEV_DEBUG']:
            with self.subTest(key=key), patch.dict(os.environ, {key: '0'}, clear=True):
                with self.assertRaisesRegex(RuntimeError, 'Unreviewed inherited'):
                    environment()

    def test_exact_artifact_profile(self):
        artifact = {'profile': {'opt_level': '3', 'debuginfo': 0}}
        self.assertIs(validate_artifact([artifact]), artifact)
        for profile in [{'opt_level': '3', 'debuginfo': 2}, {'opt_level': '0', 'debuginfo': 0}, {'opt_level': '3'}]:
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                validate_artifact([{'profile': profile}])
        for artifacts in [[], [artifact, artifact]]:
            with self.assertRaises(ValueError):
                validate_artifact(artifacts)


if __name__ == '__main__':
    unittest.main()
