"""Scoped authorization reads; callers own the operation lifetime and identity."""

from collections import defaultdict

from sqlalchemy import or_, select, tuple_

from egis_agentkit.errors import NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._definition import (
    _build_definition_query,
    _build_definition_record,
    _get_definition_table,
    _get_version_table,
    _validate_version_number,
)
from egis_agentkit.storage.skill._definition import _asset_record, _definition_query


async def resource_rows(engine, references):
    """Read only requested identities, their pointers and explicit fixed versions."""
    grouped = defaultdict(list)
    for kind, identifier, version in references:
        grouped[kind].append((identifier, version))
    result = []
    async with engine.connect() as conn:
        for kind, refs in grouped.items():
            identifiers = {identifier for identifier, _ in refs}
            if kind in ("agent", "team"):
                summary = _get_definition_table(kind)
                detail = _get_version_table(summary)
                fixed = set()
                for identifier, version in refs:
                    if version is not None:
                        try:
                            fixed.add((identifier, _validate_version_number(version)))
                        except NotFound:
                            pass
                query = _build_definition_query(summary, detail.c.version).where(
                    summary.c.id.in_(identifiers),
                    or_(
                        detail.c.version == summary.c.owner_version,
                        detail.c.version == summary.c.shared_version,
                        tuple_(summary.c.id, detail.c.version).in_(fixed),
                    ),
                )
                result.extend(
                    _build_definition_record(summary, row)
                    for row in (await conn.execute(query)).mappings()
                )
            elif kind == "skill":
                summary, detail = t.skill_summary, t.skill_detail
                fixed = {
                    (identifier, str(version))
                    for identifier, version in refs
                    if version is not None
                }
                query = _definition_query().where(
                    summary.c.skill_id.in_(identifiers),
                    or_(
                        detail.c.version == summary.c.owner_version,
                        detail.c.version == summary.c.shared_version,
                        tuple_(detail.c.skill_id, detail.c.resource_version).in_(fixed),
                    ),
                )
                result.extend(
                    _asset_record(row) for row in (await conn.execute(query)).mappings()
                )
            else:
                table = t.assets
                # Compare version keys with the same Python ordering used by the
                # single-resource reader. Only scoped version metadata is read;
                # payloads are fetched for the selected rows below.
                from egis_agentkit.storage.model._resource import asset_version_order

                metadata = (
                    await conn.execute(
                        select(
                            table.c.resource_type,
                            table.c.resource_id,
                            table.c.resource_version,
                            table.c.publication_revision,
                            table.c.is_shared_current,
                        ).where(
                            table.c.resource_type == kind,
                            table.c.resource_id.in_(identifiers),
                        )
                    )
                ).mappings()
                current = {}
                fixed = {
                    (identifier, str(version))
                    for identifier, version in refs
                    if version is not None
                }
                for row in metadata:
                    identifier = row["resource_id"]
                    if row["is_shared_current"]:
                        fixed.add((identifier, row["resource_version"]))
                    previous = current.get(identifier)
                    if previous is None or asset_version_order(
                        row
                    ) > asset_version_order(previous):
                        current[identifier] = row
                fixed.update(
                    (identifier, row["resource_version"])
                    for identifier, row in current.items()
                )
                query = select(table).where(
                    table.c.resource_type == kind,
                    tuple_(table.c.resource_id, table.c.resource_version).in_(fixed),
                )
                result.extend(
                    dict(row) for row in (await conn.execute(query)).mappings()
                )
    return result
