"""Measure save-only framed application bytes from retained phase snapshots."""
import json
import sys
from pathlib import Path


def summarize(receipts_path):
    receipts = json.loads(Path(receipts_path).read_text())
    logs = [value for name, value in receipts.items() if name.endswith('/events.jsonl')]
    assert len(logs) == 1, 'Expected one worker event log.'
    events = [json.loads(line) for line in logs[0].splitlines() if line.strip()]
    saves = [event for event in events if event.get('event') == 'generation_saved']
    assert len(saves) == 1, 'Expected one completed save.'
    event = saves[0]
    before, after = event['rpc_before_save'], event['rpc_after_save']
    keys = ['application_framed_bytes_received', 'application_framed_bytes_sent', 'application_framed_bytes_total']
    delta = {key: after[key] - before[key] for key in keys}
    assert all(value >= 0 for value in delta.values())
    raw = event['save_profile']['remote_copy']['bytes']
    return dict(receipts=str(receipts_path), save_seconds=event['save_seconds'], remote_raw_object_bytes=raw,
                framed_application_bytes=delta, received_over_raw=delta[keys[0]]/raw if raw else None,
                interpretation='Save-only endpoint frame deltas include metadata and control messages. These are application bytes, not physical network or device bytes. Raw object counts depend on the independently constructed tree.')


if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), indent=2))
