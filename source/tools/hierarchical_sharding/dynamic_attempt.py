"""Retain a durable private-operation record before any remote build request."""
from pathlib import Path
from .tree_manifest import canonical_bytes, _atomic_write
from .dynamic_runtime import durable_namespace


class PreparationAttempt:
    def __init__(self,path,parent_sha256,operation,private_output,assignments):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        durable_namespace(self.path.parent)
        with self.path.open('xb') as stream:
            stream.write(b'{}\n');stream.flush();__import__('os').fsync(stream.fileno())
        self.state=dict(status='preparing',parent_sha256=parent_sha256,operation=operation,
                        private_output=str(private_output),assignments=dict(assignments),remote_reservations=[])
        self.save()

    def save(self):
        _atomic_write(self.path,canonical_bytes(self.state))

    def reserve(self,worker,replica,sid,generation,directory):
        self.state['remote_reservations'].append(dict(worker_id=worker,replica_id=replica,
            subtree_id=sid,generation=generation,local_mirror=str(directory)))
        self.save()

    def prepared(self,candidate):
        self.state.update(status='prepared',candidate=candidate)
        self.save()
