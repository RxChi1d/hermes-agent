"""An admission_conflict must leave one INFO line that names where it was raised.

Without it the gateway log shows only ``Watch notification not admitted ... admission_conflict`` (or the
sender's ``ambiguous``), and the several raise sites cannot be told apart. No message content is logged.
"""
import logging
from types import SimpleNamespace

import pytest
import pytest_asyncio

from gateway.session_contract import SessionActor
from hermes_state_runtime import RuntimeStoreError

DIAG = 'gateway.admission_conflict'  # must start with a gateway.log component prefix


def _records(caplog):
    return [r for r in caplog.records if 'admission_conflict at' in r.getMessage()]


def test_admission_conflict_logs_its_raise_site(caplog):
    with caplog.at_level(logging.INFO, logger=DIAG):
        err = RuntimeStoreError('admission_conflict')
    recs = _records(caplog)
    assert len(recs) == 1 and recs[0].levelno == logging.INFO
    assert 'test_admission_conflict_diagnostics.py' in recs[0].getMessage()
    assert err.reason == 'admission_conflict'  # behaviour unchanged


def test_other_reasons_and_request_replays_stay_silent(caplog):
    from hermes_state_runtime import ADMITTED_REQUEST_CONFLICT
    with caplog.at_level(logging.INFO, logger=DIAG):
        RuntimeStoreError('not_found')
        RuntimeStoreError('admission_conflict', **ADMITTED_REQUEST_CONFLICT)  # a client's own retry
    assert _records(caplog) == []


@pytest_asyncio.fixture
async def bot(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(run, '_load_gateway_config',
                        lambda *_p: {'platform_toolsets': {'cli': []}, 'model': {'default': 'fixture'}})
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    store._db = SessionDB(db_path=tmp_path / 'state.db')
    runner = SimpleNamespace(session_store=store, _session_db=store._db, adapters={}, _draining=False,
                             _evict_cached_agent=lambda r: None)
    runner._adapter_for_source = lambda s: runner.adapters.get(s.platform)
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='fixture')
    owner = SessionActor('uid:1000', 'default', frozenset({'session:create', 'session:read', 'session:submit'}), 'native')
    try:
        yield SimpleNamespace(authority=authority, owner=owner, store=store)
    finally:
        store._db.close()


@pytest.mark.asyncio
async def test_target_without_a_gateway_entry_logs_route_and_tips(bot, caplog):
    from gateway.session_bot import _target
    ref, live, _entry = await _target(bot.authority, bot.owner)
    bot.store.lookup_by_session_key = lambda key: None  # the gateway store has no entry for the route
    with caplog.at_level(logging.INFO, logger='gateway.session_bot'):
        with pytest.raises(RuntimeStoreError):
            await _target(bot.authority, bot.owner)
    msgs = [r.getMessage() for r in caplog.records if 'admission_conflict' in r.getMessage()
            and 'gateway entry' in r.getMessage()]
    assert len(msgs) == 1
    assert live.route in msgs[0] and ref.session_id in msgs[0] and 'authority tip' in msgs[0]


def test_diagnostic_logger_is_routed_to_gateway_log():
    from hermes_logging import COMPONENT_PREFIXES
    assert DIAG.startswith(tuple(COMPONENT_PREFIXES['gateway']))
