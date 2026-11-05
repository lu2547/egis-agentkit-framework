# Session file lifecycle

Every task file operation authorizes the user against the native session first.
Files are relative to one of these roots:

- `workspace/user/<uid>/uploads/<sid>`: input files, immediately available to the session. Model tools may read these inputs; writes belong in work/artifacts.
- `workspace/user/<uid>/work/<sid>`: working files and intermediate results.
- `workspace/user/<uid>/artifacts/<sid>`: deliveries exposed to the user.

## HTTP contract

`GET /manager/tasks/<sid>/files?area=uploads|work|artifacts&path=<relative-dir>` returns `{items, area, path}`. Every item includes `name`, `path`, `area`, `is_directory`, `size`, and `modified_at`. Paths are relative to the chosen session/area. `recursive=true` returns a bounded flat list of files, including nested paths, for message delivery cards. The default remains a directory listing.

`POST /manager/tasks/<sid>/files` uploads multipart `file` into session uploads. Optional `path` chooses a relative subdirectory. `area` defaults to `uploads`; legacy `area=work` requests are accepted with the same uploads semantics. The response always says `area: uploads`. No copy is placed in work or artifacts. Duplicate names receive a unique suffix.

`GET /manager/tasks/<sid>/file` and `/preview` accept the same `area` and relative `path`. Absolute paths, parent traversal, symlinks, and nonregular files are rejected. Global `/workspace/*` endpoints remain legacy user archive access and must not be used to render a session's files.

## Delivery discovery

The framework reconciles work outputs after a reply and when the artifacts listing is requested. Common document, media, archive, and plain-text formats are copied to artifacts; hidden files and dependency/cache directories are skipped. Nested outputs retain their relative paths. Process scripts (`.py`, `.sh`, `.js`, etc.) and code are **not automatically promoted**. They remain in work. When source code itself is the requested deliverable, write it explicitly into artifacts or use `/deliver`.

Unchanged session upload copies and unchanged legacy inputs archived directly in the user's uploads root are excluded by content hash. Other sessions' uploads are not inspected. Existing artifacts are preserved: identical content is idempotent, changed work content receives a deterministic content suffix, and work older than an explicit artifact cannot supersede it. Files written directly into artifacts remain available regardless of automatic discovery format support. Explicit `/deliver` also supports other work file types.

Automatic discovery uses the existing download size limit (20 MiB by default), and recursive enumeration is bounded to 10,000 entries, 200 MiB and the existing path/depth limits. Scan failures retain the prior file-change baseline and surface through `file-changes.scan_error`. No historical inputs are bulk migrated or deleted.

## Deletion

A running leader or team member causes a 409 with a stop-before-delete message. The task and files are retained. An idle deletion holds every affected native session lock and the file-observation PG advisory lock, deletes the native session/team cascade, purges the affected public bus replay/inbox/registry namespaces, removes only those sessions' uploads/work/artifacts trees without following symlinks, and removes their file-change, fingerprint, baseline and scan-error rows. Upload/deliver routes reauthorize under the same advisory lock. Late scans and reads cannot recreate deleted session directories or observations. Other sessions and legacy user uploads remain intact. File cleanup errors are propagated.
