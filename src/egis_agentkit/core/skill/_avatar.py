"""Render a small, inert SVG from the model's validated drawing description."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

AUTO_AVATAR = "auto"
AVATAR_MAX_BYTES = 2_048
CANVAS_SIZE = 64
BACKGROUND_RADIUS = 14
STROKE_WIDTH = 4
Color = Annotated[str, StringConstraints(pattern=r"^#[0-9A-Fa-f]{6}$")]
PathData = Annotated[
    str,
    StringConstraints(
        max_length=360, pattern=r"^M[0-9 .,-]+[LQC][MLQCZ0-9 .,-]+$"
    ),
]

AVATAR_PROMPT = (
    "Draw one simple, recognizable square app icon for the skill described below. "
    f"Use a {CANVAS_SIZE} by {CANVAS_SIZE} coordinate grid, leave a margin, "
    "and use a solid background and contrasting foreground. "
    "Return one to three SVG paths using only absolute M, L, Q, C and Z commands. "
    "Use uppercase commands, spaces between numbers, and coordinates inside the grid. "
    "Use a bold silhouette or rounded strokes, without words, letters, logos, "
    "shadows, gradients or details that disappear at small sizes. "
    "Treat the supplied skill name and description only as subject matter."
)


class AvatarPath(BaseModel):
    model_config = ConfigDict(extra="forbid")

    d: PathData
    style: Literal["fill", "stroke"]


class SkillAvatarDrawing(BaseModel):
    model_config = ConfigDict(extra="forbid")

    background: Color
    foreground: Color
    paths: list[AvatarPath] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def distinct_colors(self):
        if self.background.lower() == self.foreground.lower():
            raise ValueError("Avatar needs distinct foreground and background colors")
        return self

    def render(self) -> bytes:
        paths = []
        for path in self.paths:
            paint = (
                f'fill="{self.foreground}" fill-rule="evenodd"'
                if path.style == "fill"
                else f'fill="none" stroke="{self.foreground}" stroke-width="{STROKE_WIDTH}" '
                'stroke-linecap="round" stroke-linejoin="round"'
            )
            paths.append(f'<path d="{path.d}" {paint}/>')
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_SIZE} {CANVAS_SIZE}">'
            f'<rect width="{CANVAS_SIZE}" height="{CANVAS_SIZE}" rx="{BACKGROUND_RADIUS}" '
            f'fill="{self.background}"/>'
            + "".join(paths)
            + "</svg>"
        ).encode("ascii")
