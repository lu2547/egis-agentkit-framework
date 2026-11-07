"""A reply owns only files produced while its native tools execute."""
import asyncio

import pytest
from test_domain import expert, identities

from egis_agentkit.service.agent.definition import AgentService
from test_workspace_service import make_workspace


@pytest.mark.asyncio
async def test_reply_artifacts_survive_reload_without_claiming_old_project_files(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    first = await svc.tasks.create(alice, definition['id'], 'private')
    second = await svc.tasks.create(alice, definition['id'], 'private')
    pid, sid = first['project_id'], first['session_id']
    svc.files.write('alice', pid, 'old.pptx', b'old', area='artifacts')
    svc.files.write('alice', pid, 'previous.md', b'previous work')
    async with svc.changes.track_deliveries('alice', pid, sid, 'reply-one'):
        svc.files.write('alice', pid, 'new.pptx', b'new')
    async with svc.changes.track_deliveries('alice', pid, sid, 'reply-two'):
        pass
    async with svc.changes.track_deliveries('alice', pid, second['session_id'], 'reply-other'):
        svc.files.write('alice', pid, 'other.md', b'other', area='artifacts')
    reloaded = make_workspace(storage, tmp_path)
    from egis_agentkit.errors import Forbidden
    with pytest.raises(Forbidden):
        await reloaded.workspace.session_artifacts(bob, sid)
    items = (await reloaded.workspace.session_artifacts(alice, sid))['items']
    assert [(row['path'], row['reply_id']) for row in items] == [('new.pptx', 'reply-one')]
    assert {row['path'] for row in (await reloaded.workspace.list_project_files(alice, pid, 'artifacts'))['items']} >= {'old.pptx', 'new.pptx', 'other.md'}
    assert [(r['path'], r['reply_id']) for r in (await reloaded.workspace.session_artifacts(alice, second['session_id']))['items']] == [('other.md', 'reply-other')]


@pytest.mark.asyncio
async def test_same_project_tools_serialize_attribution(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition['id'], 'private')
    pid, sid = task['project_id'], task['session_id']
    started = asyncio.Event()
    async def second():
        started.set()
        async with svc.changes.track_deliveries('alice', pid, sid, 'two'):
            svc.files.write('alice', pid, 'two.pptx', b'two', area='artifacts')
    async with svc.changes.track_deliveries('alice', pid, sid, 'one'):
        pending = asyncio.create_task(second())
        await started.wait()
        svc.files.write('alice', pid, 'one.pptx', b'one', area='artifacts')
    await pending
    rows = (await svc.workspace.session_artifacts(alice, sid))['items']
    assert {(r['path'], r['reply_id']) for r in rows} == {('one.pptx', 'one'), ('two.pptx', 'two')}


@pytest.mark.asyncio
async def test_reconcile_only_verified_project_session_artifacts(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition['id'], 'private')
    pid, sid = task['project_id'], task['session_id']
    legacy = svc.files.user_root('alice') / 'artifacts' / sid
    legacy.mkdir(parents=True)
    (legacy / 'late.pptx').write_bytes(b'pptx')
    unknown = legacy.parent / 'unknown-session'
    unknown.mkdir()
    (unknown / 'unowned.pptx').write_bytes(b'unknown')
    items = (await svc.workspace.list_project_files(alice, pid, 'artifacts'))['items']
    assert [r['path'] for r in items] == ['late.pptx']
    assert (unknown / 'unowned.pptx').read_bytes() == b'unknown'
    assert not (legacy / 'late.pptx').exists()
    assert (await svc.workspace.session_artifacts(alice, sid))['items'] == []


@pytest.mark.asyncio
async def test_failed_tool_still_records_only_its_new_output(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition['id'], 'private')
    pid, sid = task['project_id'], task['session_id']
    with pytest.raises(RuntimeError, match='native failure'):
        async with svc.changes.track_deliveries('alice', pid, sid, 'reply-one'):
            legacy = svc.files.user_root('alice') / 'artifacts' / sid
            legacy.mkdir(parents=True)
            (legacy / 'recovered.pptx').write_bytes(b'pptx')
            raise RuntimeError('native failure')
    assert [(r['path'], r['reply_id']) for r in (await svc.workspace.session_artifacts(alice, sid))['items']] == [('recovered.pptx', 'reply-one')]


@pytest.mark.asyncio
async def test_scan_failure_does_not_prevent_native_tool_execution(storage, tmp_path, monkeypatch):
    from egis_agentkit.errors import Conflict
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition['id'], 'private')
    def failed(*args):
        raise Conflict('scan limit')
    monkeypatch.setattr(svc.changes, 'scan', failed)
    executed = False
    async with svc.changes.track_deliveries('alice', task['project_id'], task['session_id'], 'reply-one'):
        executed = True
    assert executed
    assert (await svc.workspace.session_artifacts(alice, task['session_id']))['items'] == []
