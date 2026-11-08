"""Q2: with ``delegation.wake_unviewed_completion`` on, an unviewed *process completion* wakes its parent.

A ``completion`` event exists only because the agent asked for it (``notify_on_complete=True``; a
message_agent delivery process always does), so the request itself is the authorization. Watch
matches, cron results and every other producer stay notes. Independently of the switch, a note that
cannot be written because ANOTHER process holds the session's turn lease must not be dropped
(2026-11-05 21:16: Gateway failed the write, the reply never reached the transcript): a turn is run,
which waits for the lease.
"""
import pytest

from hermes_state_errors import SessionTurnLeaseLostError
from tests.gateway.test_session_end_delegations import _PROCESS, _admissions, _deliver, _local, _notes
from tests.gateway.test_unviewed_completion_notes import _target, _turns
from tests.gateway.test_wake_unviewed_delegation import _enable


@pytest.fixture(autouse=True)
def _fresh_wake_state():
    from gateway import session_completion_notes as notes
    notes._wake_times.clear()
    notes._wake_decided.clear()
    yield
    notes._wake_times.clear()
    notes._wake_decided.clear()


@pytest.mark.asyncio
async def test_enabled_unviewed_process_completion_starts_a_turn(tmp_path, monkeypatch, caplog):
    import logging
    caplog.set_level(logging.INFO, logger='gateway.session_completion_notes')
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target = _target(runner, authority, ref)
        seen = _turns(runner, authority, target, 'proc_lc7')
        await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(seen) == 1 and not _notes(authority, target)
        assert [r['outcome'] for r in _admissions(authority, ref)] == ['completed']
        assert 'completion wakes parent without a viewer' in caplog.text
    finally:
        authority.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('config', [None, False], ids=['unset', 'false'])
async def test_off_process_completion_stays_a_note(tmp_path, monkeypatch, config):
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    if config is not None:
        _enable(tmp_path, config)
    try:
        async def no_turn(event):
            raise AssertionError('switch off: stays a note')
        runner._handle_message = no_turn
        target = await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(_notes(authority, target)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('producer', ['watch_match', 'watch_disabled'])
async def test_enabled_watch_notices_stay_notes(tmp_path, monkeypatch, producer):
    from tests.gateway import test_session_end_delegations as base
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    base._CARDS.setdefault(producer, 'process_complete')
    try:
        async def no_turn(event):
            raise AssertionError('only a completion the agent asked for may wake')
        runner._handle_message = no_turn
        target = await _deliver(runner, authority, ref, (producer, 'proc_w', 1700000001.0))
        await authority._drain(ref)
        assert len(_notes(authority, target)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_enabled_ended_session_is_not_revived_by_a_process(tmp_path, monkeypatch):
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target = _target(runner, authority, ref)
        authority.db.end_session(target, 'user_exit')

        async def no_turn(event):
            raise AssertionError('ended')
        runner._handle_message = no_turn
        await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(_notes(authority, target)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_repeat_process_offer_never_wakes_twice(tmp_path, monkeypatch):
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target = _target(runner, authority, ref)
        seen = _turns(runner, authority, target, 'proc_lc7')
        await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(seen) == 1 and len(_admissions(authority, ref)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('enabled', [False, True], ids=['switch_off', 'switch_on'])
async def test_note_write_refused_by_another_process_turn_runs_instead_of_failing(tmp_path, monkeypatch, enabled):
    """Another process (the Desktop backend) holds the turn lease: the Gateway's note write is
    refused. Upstream fails the admission and the result vanishes; the result must reach the model."""
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    if enabled:
        _enable(tmp_path)
    try:
        def refuse(*args, **kwargs):
            raise SessionTurnLeaseLostError('Session has an active turn lease; refusing transcript mutation')
        monkeypatch.setattr(authority.db, 'append_completion_note', refuse)
        target = _target(runner, authority, ref)
        seen = _turns(runner, authority, target, 'proc_lc7')
        await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(seen) == 1
        assert [r['outcome'] for r in _admissions(authority, ref)] == ['completed']
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_wake_chain_is_capped_then_falls_back_to_a_note(tmp_path, monkeypatch):
    """Ping-pong guard: at most WAKE_CAP unviewed wakes per session inside WAKE_WINDOW_S; the next one
    is stored as a note (visible, no model turn), and the window then reopens."""
    from gateway import session_completion_notes as notes
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    clock = [1000.0]
    monkeypatch.setattr(notes, '_now', lambda: clock[0])
    try:
        target = _target(runner, authority, ref)
        seen = []

        async def handle(event):
            seen.append(event.text)
            return ''
        runner._handle_message = handle
        for n in range(notes.WAKE_CAP + 1):
            await _deliver(runner, authority, ref, ('completion', f'proc_hop{n}', 1700000100.0 + n))
            await authority._drain(ref)
            clock[0] += 1
        assert len(seen) == notes.WAKE_CAP
        assert len(_notes(authority, target)) == 1 and 'proc_hop%d' % notes.WAKE_CAP in str(_notes(authority, target)[0]['content'])
        clock[0] += notes.WAKE_WINDOW_S
        await _deliver(runner, authority, ref, ('completion', 'proc_after', 1700000200.0))
        await authority._drain(ref)
        assert len(seen) == notes.WAKE_CAP + 1
    finally:
        authority.db.close()
