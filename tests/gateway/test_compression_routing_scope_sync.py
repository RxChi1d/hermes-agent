"""A compression run by another process must advance every ``gateway_routing`` scope for that route.

``gateway_routing`` holds the same route under two namespaces: ``scope=''`` (the state layer's copy,
read and written by ``advance_local_target`` / ``_commit_local_target``) and ``scope=<sessions dir>``
(the copy a long-lived gateway store loads and saves). Compression used to advance only ``scope=''``,
so after the desktop backend compressed a chat the gateway still pointed at the ended parent and the
next completion was refused with ``admission_conflict`` (upstream issue #150056).

The other process is modelled by a second SessionDB on the same file: it runs the real compression
path, so whatever the state layer writes is what this test judges.
"""
from types import SimpleNamespace

import pytest
import pytest_asyncio

from gateway.session_contract import SessionActor


@pytest_asyncio.fixture
async def bot(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from gateway.session_bot import _target
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
    actor = SessionActor('uid:1000', 'default', frozenset({'session:create', 'session:read', 'session:submit'}), 'native')
    ref, live, _entry = await _target(authority, actor)
    with store._lock:
        store._save()  # the gateway store's own routing row exists, as in a long-running gateway
    store_scope = store._routing_scope()
    serve = SessionDB(db_path=tmp_path / 'state.db')
    row = serve.get_session(ref.session_id)
    serve.publish_compression_child(
        parent_session_id=ref.session_id, child_session_id='tip_after_compression', source=row['source'],
        messages=[{'role': 'user', 'content': 'summary'}], require_compression_lease=False)
    try:
        yield SimpleNamespace(authority=authority, runner=runner, store=store, ref=ref, live=live,
                              old=ref.session_id, tip='tip_after_compression', scope=store_scope,
                              db_path=tmp_path / 'state.db')
    finally:
        serve.close()
        store._db.close()


def _rows(bot):
    """Every ``gateway_routing`` row for the route, keyed by scope."""
    import json
    import sqlite3
    conn = sqlite3.connect(bot.db_path)
    try:
        rows = conn.execute("SELECT scope, entry_json FROM gateway_routing WHERE session_key=?",
                            (bot.live.route,)).fetchall()
    finally:
        conn.close()
    return {scope: json.loads(entry)['session_id'] for scope, entry in rows}


def test_the_route_exists_in_both_scopes_before_the_compression(bot):
    """Precondition: two rows, so a scope-aware write is the only thing being tested."""
    rows = _rows(bot)
    assert '' in rows and bot.scope in rows


def test_a_compression_advances_the_gateway_scope_too(bot):
    """The state layer must not leave the copy a gateway loads pointing at an ended parent."""
    rows = _rows(bot)
    assert rows[''] == bot.tip, 'state layer did not advance its own row'
    assert rows[bot.scope] == bot.tip, (
        'the gateway scope still points at the ended parent: %r' % rows[bot.scope])

