"""Opt-in: an unviewed local async-delegation completion wakes its parent instead of becoming a note.

Local patch "Q" (upstream #143649 asks for the same opt-in; default stays upstream's LC-7 behaviour).
Setting: ``delegation.wake_unviewed_completion`` in the OWNING profile's config.yaml (default false).

Rita's real shape: producer=async_delegation, an ``async_delegations`` ledger row exists, the local
route has no subscribers (no viewer attached to this authority).
"""
import json

import pytest

from tests.gateway.test_completion_admission import pending
from tests.gateway.test_session_end_delegations import _DELEGATION, _PROCESS, _admissions, _deliver, _local, _notes
from tests.gateway.test_unviewed_completion_notes import _target, _turns
from tools import async_delegation as delegation


@pytest.fixture(autouse=True)
def _no_delegations():
    delegation._reset_for_tests()
    yield
    delegation._reset_for_tests()


def _enable(tmp_path, value=True):
    (tmp_path / 'config.yaml').write_text(f'delegation:\n  wake_unviewed_completion: {str(value).lower()}\n')


def _notes_of(authority, target, name):
    return [m for m in authority.db.get_messages(target) if name in str(m.get('content'))]


async def _offer(runner, authority, ref, name):
    """Rita's path: the ledger row exists (dispatch + completion persisted), the group is delivered."""
    route = authority.sessions[ref.session_id].route
    target = _target(runner, authority, ref)
    evt = dict(pending(route, name), parent_session_id=target)
    assert await runner._deliver_async_delegation_group([dict(evt)]) is True
    return target, evt


@pytest.mark.asyncio
async def test_enabled_unviewed_delegation_completion_starts_a_turn(tmp_path, monkeypatch, caplog):
    import logging
    caplog.set_level(logging.INFO, logger='gateway.session_completion_notes')
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target, _ = await _offer(runner, authority, ref, 'deleg_wake')
        seen = _turns(runner, authority, target, 'deleg_wake')
        await authority._drain(ref)
        assert len(seen) == 1 and 'deleg_wake' in seen[0][0], 'the parent must be woken with the result'
        assert not [m for m in authority.db.get_messages(target) if (m.get('display_metadata') or {}).get('completion_note')]
        assert [r['outcome'] for r in _admissions(authority, ref)] == ['completed']
        assert 'completion wakes parent without a viewer' in caplog.text
        assert 'stored as note' not in caplog.text
    finally:
        authority.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('config', [None, False], ids=['unset', 'false'])
async def test_default_and_off_behave_exactly_like_upstream(tmp_path, monkeypatch, config, caplog):
    import logging
    caplog.set_level(logging.INFO, logger='gateway.session_completion_notes')
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    if config is not None:
        _enable(tmp_path, config)
    try:
        target, _ = await _offer(runner, authority, ref, 'deleg_off')

        async def no_turn(event):
            raise AssertionError('switch off: an unviewed completion must stay a note')
        runner._handle_message = no_turn
        await authority._drain(ref)
        notes = _notes_of(authority, target, 'deleg_off')
        assert len(notes) == 1 and notes[0]['display_metadata'].get('completion_note')
        assert 'completion stored as note, no model turn' in caplog.text
        assert 'wakes parent' not in caplog.text
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_enabled_without_a_ledger_row_stays_a_note(tmp_path, monkeypatch):
    """The wake is authorised by the dispatch row; an async_delegation identity with no row is not."""
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        async def no_turn(event):
            raise AssertionError('no dispatch row: must stay a note')
        runner._handle_message = no_turn
        target = await _deliver(runner, authority, ref, _DELEGATION)  # admits the identity, writes no ledger row
        await authority._drain(ref)
        assert len(_notes(authority, target)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_enabled_process_completion_is_still_a_note(tmp_path, monkeypatch):
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        async def no_turn(event):
            raise AssertionError('only async delegations may wake; a process exit stays a note')
        runner._handle_message = no_turn
        target = await _deliver(runner, authority, ref, _PROCESS)
        await authority._drain(ref)
        assert len(_notes(authority, target)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_enabled_cron_result_is_still_a_note(tmp_path, monkeypatch):
    from gateway.platforms.event import MessageEvent
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        async def no_turn(event):
            raise AssertionError('a cron result is always a note')
        runner._handle_message = no_turn
        live = authority.sessions[ref.session_id]
        entry = runner.session_store.lookup_by_session_key(live.route)
        event = MessageEvent(text='[cron] RESULT_cron_job', source=live.source, internal=True, metadata={
            'gateway_session_key': live.route, 'gateway_session_id': entry.session_id,
            'display_kind': 'internal_notification', 'display_text': 'cron'})
        await authority.admit_automation(runner._adapter_for_source(live.source), event,
                                         json.dumps(['cron', 'job1', ''], separators=(',', ':')))
        await authority._drain(ref)
        assert len(_notes(authority, entry.session_id)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_viewer_attached_is_unchanged_with_the_switch_on(tmp_path, monkeypatch):
    runner, authority, ref, actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        authority.sessions[ref.session_id].subscribers['watching'] = actor
        target, _ = await _offer(runner, authority, ref, 'deleg_viewed')
        seen = _turns(runner, authority, target, 'deleg_viewed')
        await authority._drain(ref)
        assert len(seen) == 1 and not [m for m in authority.db.get_messages(target)
                                       if (m.get('display_metadata') or {}).get('completion_note')]
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_repeat_offer_never_wakes_twice(tmp_path, monkeypatch):
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target, evt = await _offer(runner, authority, ref, 'deleg_once')
        seen = _turns(runner, authority, target, 'deleg_once')
        await authority._drain(ref)
        await runner._deliver_async_delegation_group([dict(evt)])  # already delivered: nothing new is admitted
        await authority._drain(ref)
        assert len(seen) == 1 and len(_admissions(authority, ref)) == 1
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_a_session_the_user_ended_is_not_revived(tmp_path, monkeypatch):
    """#143649: opt-in must not revive an explicitly ended conversation."""
    runner, authority, ref, _actor = await _local(tmp_path, monkeypatch)
    _enable(tmp_path)
    try:
        target, _ = await _offer(runner, authority, ref, 'deleg_ended')
        authority.db.end_session(target, 'user_exit')

        async def no_turn(event):
            raise AssertionError('an ended session must not be woken')
        runner._handle_message = no_turn
        await authority._drain(ref)
        assert len(_notes_of(authority, target, 'deleg_ended')) == 1
    finally:
        authority.db.close()
