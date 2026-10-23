"""SSH uses only exact keys admitted independently of its own connection."""
import json
from pathlib import Path
import tempfile
import unittest
from .dynamic_ssh import write_known_hosts,ssh_argv
from .tree_manifest import file_hash


class SSHAdmission(unittest.TestCase):
    def test_exact_account_driver_worker_and_authenticated_key_are_required(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);receipt=root/'receipt.json';target=root/'known_hosts';key=root/'key';key.write_text('private fixture key');key.chmod(0o600)
            rows=[dict(host='172.31.1.'+str(i),instance_id='worker'+str(i),actual_ed25519_public_key='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOrJdIyc5BHY2EELokwax3IQI8vbUaQ7d8qVVrTp0el0 fixture') for i in (1,2)]
            receipt.write_text(json.dumps(dict(account='079819354890',driver_instance_id='driver',rows=rows)))
            admitted={row['instance_id']:dict(account='079819354890',private_ip=row['host'],key_type='ssh-ed25519',key_base64='AAAAC3NzaC1lZDI1NTE5AAAAIOrJdIyc5BHY2EELokwax3IQI8vbUaQ7d8qVVrTp0el0') for row in rows}
            with self.assertRaisesRegex(ValueError,'control-plane'):write_known_hosts(receipt,file_hash(receipt),{},target,'driver')
            self.assertFalse(target.exists())
            with self.assertRaisesRegex(ValueError,'driver'):write_known_hosts(receipt,file_hash(receipt),admitted,target,'other')
            write_known_hosts(receipt,file_hash(receipt),admitted,target,'driver')
            argv=ssh_argv(rows[0]['host'],key,target)
            for option in ('StrictHostKeyChecking=yes','HostKeyAlgorithms=ssh-ed25519','ControlMaster=no','ForwardAgent=no','GlobalKnownHostsFile=/dev/null'):self.assertIn(option,argv)
            self.assertEqual(argv[-1],'ubuntu@172.31.1.1')
            key.chmod(0o644)
            with self.assertRaisesRegex(ValueError,'permissions'):ssh_argv(rows[0]['host'],key,target)


if __name__=='__main__':unittest.main()
