"""参加者向け配布情報の署名・期限・固定イメージの検証。"""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone

SCRIPT = Path(__file__).resolve().parents[1] / "community-release.py"


class ReleaseVerificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("community_release", SCRIPT)
        cls.release = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.release)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.private = self.root / "private.pem"
        self.public = self.root / "public.pem"
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(self.private)],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["openssl", "pkey", "-in", str(self.private), "-pubout", "-out", str(self.public)],
            check=True, capture_output=True,
        )
        self.manifest = self.root / "release.json"
        self.signature = self.root / "release.sig"
        self.data = {
            "schemaVersion": 1,
            "version": "0.2.0",
            "protocolVersion": 2,
            "image": "registry.example/probe@sha256:" + "a" * 64,
            "platforms": ["linux/amd64", "linux/arm64"],
            "releasedAt": "2026-10-20T00:00:00Z",
            "expiresAt": "2027-01-20T00:00:00Z",
            "changes": ["任意参加の試験配布"],
        }
        self.now = datetime(2026, 10, 21, tzinfo=timezone.utc)

    def sign(self):
        self.manifest.write_text(json.dumps(self.data), encoding="utf-8")
        subprocess.run(
            ["openssl", "pkeyutl", "-sign", "-inkey", str(self.private),
             "-rawin", "-in", str(self.manifest), "-out", str(self.signature)],
            check=True, capture_output=True,
        )

    def verify(self):
        return self.release.verify(
            self.manifest, self.signature, self.public, "linux/amd64", self.now
        )

    def test_signed_pinned_release_is_accepted(self):
        self.sign()
        self.assertEqual(self.verify()["image"], self.data["image"])

    def test_modified_manifest_is_rejected(self):
        self.sign()
        self.manifest.write_text(
            self.manifest.read_text(encoding="utf-8").replace("0.2.0", "0.3.0"),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "署名"):
            self.verify()

    def test_tag_instead_of_digest_is_rejected_even_when_signed(self):
        self.data["image"] = "registry.example/probe:latest"
        self.sign()
        with self.assertRaisesRegex(ValueError, "digest"):
            self.verify()

    def test_expired_release_is_rejected(self):
        self.data["expiresAt"] = "2026-10-20T01:00:00Z"
        self.sign()
        with self.assertRaisesRegex(ValueError, "期限"):
            self.verify()

    def test_wrong_platform_or_protocol_is_rejected(self):
        self.data["platforms"] = ["linux/arm64"]
        self.sign()
        with self.assertRaisesRegex(ValueError, "platform"):
            self.verify()
        self.data["platforms"] = ["linux/amd64"]
        self.data["protocolVersion"] = 3
        self.sign()
        with self.assertRaisesRegex(ValueError, "protocol"):
            self.verify()

    def test_signed_shell_metacharacters_are_not_an_image(self):
        self.data["image"] = "registry.example/$(id)@sha256:" + "a" * 64
        self.sign()
        with self.assertRaisesRegex(ValueError, "digest"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
