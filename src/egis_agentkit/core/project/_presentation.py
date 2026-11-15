"""管理 PPTX 页面结构，保留未修改部件的原始内容，不执行文件或数据库 IO。

核心能力
- list_slides 按演示顺序返回稳定页面 ID、标题。
- update_slides 新增、复制、删除、移动页面，并同步关系、备注和分节。
- 对压缩包、XML 和页面数量执行统一预算校验。
"""

import io
import posixpath
import re
from copy import deepcopy
from typing import Literal
from uuid import uuid4
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from lxml import etree as ET

from egis_agentkit.client.nacos._settings import ProjectPresentationConfig
from egis_agentkit.errors import Conflict, InvalidRequest

P = "http://schemas.openxmlformats.org/presentationml/2006/main"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
CT = "http://schemas.openxmlformats.org/package/2006/content-types"
P14 = "http://schemas.microsoft.com/office/powerpoint/2010/main"
PRESENTATION = "ppt/presentation.xml"
PRESENTATION_RELS = "ppt/_rels/presentation.xml.rels"
CONTENT_TYPES = "[Content_Types].xml"
MIN_SLIDE_ID = 256  # PresentationML 规范中页面标识的最小值。
SlideOperation = Literal["add", "duplicate", "remove", "move"]


class Presentation:
    """一个有界 PPTX 包的页面能力；每次编辑独立创建实例。"""

    def __init__(self, content: bytes, limits: ProjectPresentationConfig):
        self.limits = limits
        self.parts: dict[str, bytes] = {}
        self.infos = []
        # 1. 校验压缩包预算和部件身份，禁止含糊的重复文件名。
        if len(content) > limits.max_bytes:
            raise InvalidRequest("演示文稿超过文件大小限制")
        try:
            with ZipFile(io.BytesIO(content)) as archive:
                self.infos = archive.infolist()
                if len(self.infos) > limits.max_entries:
                    raise InvalidRequest("演示文稿部件数量超过限制")
                if (
                    sum(info.file_size for info in self.infos)
                    > limits.max_expanded_bytes
                ):
                    raise InvalidRequest("演示文稿展开大小超过限制")
                for info in self.infos:
                    if (
                        info.filename in self.parts
                        or info.flag_bits & 1
                        or info.file_size > limits.max_entry_bytes
                    ):
                        raise InvalidRequest("演示文稿压缩包不符合编辑限制")
                    self.parts[info.filename] = archive.read(info)
            # 2. 读取页面列表和关系；不重建原有幻灯片、主题、图表或媒体。
            self.presentation = self._xml(PRESENTATION)
            self.relationships = self._xml(PRESENTATION_RELS)
            self.types = self._xml(CONTENT_TYPES)
            self.slide_ids = self.presentation.find(f"{{{P}}}sldIdLst")
            if self.slide_ids is None or not len(self.slide_ids):
                raise InvalidRequest("演示文稿没有可管理的页面")
            if len(self.slide_ids) > limits.max_slides:
                raise InvalidRequest("演示文稿页数超过配置限制")
            self._slide_paths()
        except (
            BadZipFile,
            KeyError,
            ET.XMLSyntaxError,
            ValueError,
            RuntimeError,
        ) as exc:
            if isinstance(exc, InvalidRequest):
                raise
            raise InvalidRequest(
                "无法解析 PPTX 页面结构，文件可能已损坏或加密"
            ) from None

    def _xml(self, path: str):
        """保留命名空间及扩展声明，禁止 XML 外部实体和网络解析。"""
        parser = ET.XMLParser(resolve_entities=False, no_network=True)
        result = ET.fromstring(self.parts[path], parser)
        if result.getroottree().docinfo.doctype:
            raise InvalidRequest("演示文稿 XML 不允许包含 DTD")
        return result

    @staticmethod
    def _bytes(element) -> bytes:
        return ET.tostring(
            element, encoding="UTF-8", xml_declaration=True, standalone=True
        )

    @staticmethod
    def _target(source: str, target: str) -> str:
        result = (
            posixpath.normpath(posixpath.join(posixpath.dirname(source), target))
            if not target.startswith("/")
            else target.lstrip("/")
        )
        if result.startswith("../") or ":" in result:
            raise InvalidRequest("PPTX 页面关系不合法")
        return result

    @staticmethod
    def _rels_path(path: str) -> str:
        directory, name = posixpath.split(path)
        return f"{directory}/_rels/{name}.rels"

    def _slide_paths(self) -> dict[str, str]:
        relationships = {r.get("Id"): r for r in self.relationships}
        result = {}
        for slide in self.slide_ids:
            relation = relationships.get(slide.get(f"{{{R}}}id"))
            if (
                relation is None
                or relation.get("Type") != R + "/slide"
                or relation.get("TargetMode") == "External"
            ):
                raise InvalidRequest("页面关系缺失或无效")
            identifier = slide.get("id")
            if not identifier or identifier in result or not identifier.isdecimal():
                raise InvalidRequest("页面标识缺失或重复")
            path = self._target(PRESENTATION, relation.get("Target", ""))
            if path not in self.parts:
                raise InvalidRequest("页面部件不存在")
            result[identifier] = path
        return result

    def list_slides(self) -> list[dict[str, str]]:
        """按演示顺序取标题；没有标题时使用首个非空文本，不把页码当稳定身份。"""
        result = []
        for identifier, path in self._slide_paths().items():
            slide = self._xml(path)
            title = ""
            for shape in slide.findall(f".//{{{P}}}sp"):
                placeholder = shape.find(f".//{{{P}}}ph")
                if placeholder is not None and placeholder.get("type") in (
                    "title",
                    "ctrTitle",
                ):
                    title = " ".join(shape.itertext()).strip()
                    break
            if not title:
                title = " ".join(
                    t.text or "" for t in slide.findall(f".//{{{A}}}t")
                ).strip()
            result.append(
                {"id": identifier, "title": title[: self.limits.max_title_chars]}
            )
        return result

    def _new_part(self, directory: str, prefix: str) -> str:
        numbers = [
            int(match.group(1))
            for name in self.parts
            if (
                match := re.fullmatch(
                    re.escape(directory + "/" + prefix) + r"(\d+)\.xml", name
                )
            )
        ]
        return f"{directory}/{prefix}{max(numbers, default=0) + 1}.xml"

    def _override(self, path: str, content_type: str):
        ET.SubElement(
            self.types,
            f"{{{CT}}}Override",
            PartName="/" + path,
            ContentType=content_type,
        )

    def _new_slide(self, current_path: str) -> tuple[str, str]:
        """新增真实空白幻灯片，沿用当前主题的空白版式或原版式。"""
        path = self._new_part("ppt/slides", "slide")
        slide = ET.Element(f"{{{P}}}sld", nsmap={"a": A, "r": R, "p": P})
        tree = ET.SubElement(ET.SubElement(slide, f"{{{P}}}cSld"), f"{{{P}}}spTree")
        nv = ET.SubElement(tree, f"{{{P}}}nvGrpSpPr")
        ET.SubElement(nv, f"{{{P}}}cNvPr", id="1", name="")
        ET.SubElement(nv, f"{{{P}}}cNvGrpSpPr")
        ET.SubElement(nv, f"{{{P}}}nvPr")
        ET.SubElement(tree, f"{{{P}}}grpSpPr")
        ET.SubElement(
            ET.SubElement(slide, f"{{{P}}}clrMapOvr"), f"{{{A}}}masterClrMapping"
        )
        relations = self._xml(self._rels_path(current_path))
        layout = next(
            (
                r
                for r in relations
                if r.get("Type") == R + "/slideLayout"
                and r.get("TargetMode") != "External"
            ),
            None,
        )
        if layout is None:
            raise InvalidRequest("当前页面缺少版式关系")
        new_rels = ET.Element(f"{{{PKG}}}Relationships", nsmap={None: PKG})
        new_layout = deepcopy(layout)
        target = self._target(current_path, layout.get("Target"))
        # 优先选择同一母版的空白版式，防止新页出现原版式固定文字。
        master_rels_path = self._rels_path(target)
        if master_rels_path in self.parts:
            master_relation = next(
                (
                    r
                    for r in self._xml(master_rels_path)
                    if r.get("Type") == R + "/slideMaster"
                ),
                None,
            )
            if master_relation is not None:
                master = self._target(target, master_relation.get("Target"))
                for candidate in self._xml(self._rels_path(master)):
                    if candidate.get("Type") == R + "/slideLayout":
                        candidate_path = self._target(master, candidate.get("Target"))
                        if self._xml(candidate_path).get("type") == "blank":
                            target = candidate_path
                            break
        new_layout.set("Target", posixpath.relpath(target, posixpath.dirname(path)))
        new_rels.append(new_layout)
        self.parts[path] = self._bytes(slide)
        self.parts[self._rels_path(path)] = self._bytes(new_rels)
        self._override(
            path,
            "application/vnd.openxmlformats-officedocument.presentationml.slide+xml",
        )
        return path, str(
            max(MIN_SLIDE_ID - 1, *(int(row.get("id")) for row in self.slide_ids)) + 1
        )

    def _duplicate_slide(self, current_path: str) -> tuple[str, str]:
        """复制页面，复用共享媒体/版式，独立复制带反向页面关系的备注。"""
        path = self._new_part("ppt/slides", "slide")
        self.parts[path] = self.parts[current_path]
        rels = self._xml(self._rels_path(current_path))
        for relation in rels:
            if relation.get("TargetMode") == "External":
                continue
            target = self._target(current_path, relation.get("Target"))
            if relation.get("Type") == R + "/notesSlide":
                note = self._new_part("ppt/notesSlides", "notesSlide")
                self.parts[note] = self.parts[target]
                note_rels_path = self._rels_path(target)
                if note_rels_path in self.parts:
                    note_rels = self._xml(note_rels_path)
                    for nr in note_rels:
                        if nr.get("Type") == R + "/slide":
                            nr.set(
                                "Target",
                                posixpath.relpath(path, posixpath.dirname(note)),
                            )
                    self.parts[self._rels_path(note)] = self._bytes(note_rels)
                self._override(
                    note,
                    "application/vnd.openxmlformats-officedocument.presentationml.notesSlide+xml",
                )
                target = note
            relation.set("Target", posixpath.relpath(target, posixpath.dirname(path)))
        self.parts[self._rels_path(path)] = self._bytes(rels)
        self._override(
            path,
            "application/vnd.openxmlformats-officedocument.presentationml.slide+xml",
        )
        return path, str(max(int(row.get("id")) for row in self.slide_ids) + 1)

    def _remove_slide(self, element, path: str):
        rid = element.get(f"{{{R}}}id")
        removed = {path, self._rels_path(path)}
        for relation in self._xml(self._rels_path(path)):
            if relation.get("Type") == R + "/notesSlide":
                note = self._target(path, relation.get("Target"))
                removed.update((note, self._rels_path(note)))
        for name in removed:
            self.parts.pop(name, None)
        for override in list(self.types):
            if override.get("PartName", "").lstrip("/") in removed:
                self.types.remove(override)
        for relation in list(self.relationships):
            if relation.get("Id") == rid:
                self.relationships.remove(relation)
        for show in self.presentation.findall(f".//{{{P}}}custShow"):
            for item in show.findall(f".//{{{P}}}sld"):
                if item.get(f"{{{R}}}id") == rid:
                    item.getparent().remove(item)
        self.slide_ids.remove(element)

    def _sync_sections(
        self, original_id: str, active_id: str, operation: SlideOperation
    ):
        """页面分节跟随顺序及插入位置，避免保留已删除页面的引用。"""
        sections = self.presentation.findall(f".//{{{P14}}}section")
        if not sections:
            return
        owner = {
            item.get("id"): section
            for section in sections
            for item in section.findall(f".//{{{P14}}}sldId")
        }
        ids = [row.get("id") for row in self.slide_ids]
        if operation in ("add", "duplicate"):
            index = ids.index(active_id)
            neighbour = ids[index - 1] if index else ids[1]
            owner[active_id] = owner.get(neighbour, sections[0])
        elif operation == "move":
            index = ids.index(active_id)
            neighbour = (
                ids[index - 1] if index else (ids[1] if len(ids) > 1 else active_id)
            )
            owner[active_id] = owner.get(neighbour, sections[0])
        for section in sections:
            group = section.find(f"{{{P14}}}sldIdLst")
            if group is None:
                continue
            for item in list(group):
                group.remove(item)
            for identifier in ids:
                if owner.get(identifier, sections[0]) is section:
                    ET.SubElement(group, f"{{{P14}}}sldId", id=identifier)

    def update_slides(
        self, operation: SlideOperation, slide_id: str, target_index: int | None = None
    ) -> tuple[bytes, str]:
        """以稳定页面 ID 执行一次结构变更，返回新 PPTX 及应激活的页面 ID。"""
        paths = self._slide_paths()
        if slide_id not in paths:
            raise InvalidRequest("操作的幻灯片不存在，请重新打开文件")
        element = next(row for row in self.slide_ids if row.get("id") == slide_id)
        index = list(self.slide_ids).index(element)
        active = slide_id
        # 1. 校验操作边界，再变更页面及直接依赖。
        if operation in ("add", "duplicate"):
            if len(self.slide_ids) >= self.limits.max_slides:
                raise Conflict(f"演示文稿最多允许 {self.limits.max_slides} 页")
            if target_index is not None and (
                type(target_index) is not int
                or not 1 <= target_index <= len(self.slide_ids) + 1
            ):
                raise InvalidRequest("插入目标页码超出范围")
            path, active = (
                self._new_slide(paths[slide_id])
                if operation == "add"
                else self._duplicate_slide(paths[slide_id])
            )
            rid = "rId" + uuid4().hex
            ET.SubElement(
                self.relationships,
                f"{{{PKG}}}Relationship",
                Id=rid,
                Type=R + "/slide",
                Target=posixpath.relpath(path, "ppt"),
            )
            new_element = ET.Element(f"{{{P}}}sldId", id=active)
            new_element.set(f"{{{R}}}id", rid)
            self.slide_ids.insert(
                target_index - 1 if target_index is not None else index + 1, new_element
            )
        elif operation == "remove":
            if len(self.slide_ids) <= 1:
                raise Conflict("演示文稿至少需要保留一页")
            self._remove_slide(element, paths[slide_id])
            active = self.slide_ids[min(index, len(self.slide_ids) - 1)].get("id")
        elif operation == "move":
            if type(target_index) is not int or not 1 <= target_index <= len(
                self.slide_ids
            ):
                raise InvalidRequest("移动目标页码超出范围")
            self.slide_ids.remove(element)
            self.slide_ids.insert(target_index - 1, element)
        else:
            raise InvalidRequest("不支持的幻灯片操作")
        # 2. 同步分节、包关系与页数；原始页面等未修改部件保持字节不变。
        self._sync_sections(slide_id, active, operation)
        self.parts[PRESENTATION] = self._bytes(self.presentation)
        self.parts[PRESENTATION_RELS] = self._bytes(self.relationships)
        self.parts[CONTENT_TYPES] = self._bytes(self.types)
        if "docProps/app.xml" in self.parts:
            properties = self._xml("docProps/app.xml")
            count = properties.find("{*}Slides")
            if count is not None:
                count.text = str(len(self.slide_ids))
                self.parts["docProps/app.xml"] = self._bytes(properties)
        if (
            len(self.parts) > self.limits.max_entries
            or any(len(value) > self.limits.max_entry_bytes for value in self.parts.values())
            or sum(map(len, self.parts.values())) > self.limits.max_expanded_bytes
        ):
            raise Conflict("变更后的演示文稿部件数量或展开大小超过限制，未保存")
        output = io.BytesIO()
        with ZipFile(output, "w", ZIP_DEFLATED) as archive:
            for info in self.infos:
                if info.filename in self.parts:
                    archive.writestr(info, self.parts[info.filename])
            previous = {info.filename for info in self.infos}
            for name in self.parts.keys() - previous:
                archive.writestr(name, self.parts[name])
        content = output.getvalue()
        if len(content) > self.limits.max_bytes:
            raise Conflict("变更后的演示文稿超过大小限制，未保存")
        return content, active
