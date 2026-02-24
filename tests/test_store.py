from pathlib import Path

from agentbus.events import make_event
from agentbus.models import Actor
from agentbus.store import EventCursor, JsonlEventStore


def test_read_from_cursor(tmp_path: Path) -> None:
    log_file = tmp_path / "events.jsonl"
    store = JsonlEventStore(str(log_file))
    actor = Actor(type="user", id="u1")

    store.append(make_event(kind="one", actor=actor, data={"x": 1}))
    store.append(make_event(kind="two", actor=actor, data={"x": 2}))

    events, cursor = store.read_from(EventCursor(line_no=0))
    assert len(events) == 2

    events2, cursor2 = store.read_from(cursor)
    assert len(events2) == 0
    assert cursor2.line_no >= cursor.line_no

    store.append(make_event(kind="three", actor=actor, data={"x": 3}))
    events3, _ = store.read_from(cursor2)
    assert len(events3) == 1
    assert events3[0]["kind"] == "three"
