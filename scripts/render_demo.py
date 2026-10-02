"""Render an actual asciinema event recording to a terminal GIF."""

import argparse
import json
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input", type=Path, nargs="?", default=Path("docs/demo/failure-recovery.cast")
    )
    parser.add_argument("--output", type=Path, default=Path("docs/demo/failure-recovery.gif"))
    args = parser.parse_args()
    records = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines()]
    events = [row for row in records[1:] if row[1] == "o"]
    if not events:
        raise ValueError("recording has no output events")
    fonts = [
        Path("C:/Windows/Fonts/consola.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
    ]
    font = next(
        (ImageFont.truetype(str(path), 16) for path in fonts if path.exists()),
        ImageFont.load_default(),
    )
    frames, durations, lines = [], [], []
    for index, (timestamp, _, content) in enumerate(events):
        lines.extend(
            part
            for line in content.replace("\r", "").splitlines()
            for part in textwrap.wrap(line, 96) or [""]
        )
        frame = Image.new("RGB", (980, 570), "#0d1422")
        draw = ImageDraw.Draw(frame)
        draw.rounded_rectangle(
            (12, 12, 968, 558), radius=16, fill="#111d30", outline="#31425e", width=2
        )
        for x, color in [(35, "#ff5c72"), (57, "#f7c25e"), (79, "#58d5a2")]:
            draw.ellipse((x, 33, x + 11, 44), fill=color)
        draw.text((110, 28), "STRATA / actual Docker failure recovery", font=font, fill="#9bb2d1")
        draw.line((24, 61, 956, 61), fill="#31425e")
        for row, line in enumerate(lines[-21:]):
            color = (
                "#70ddb1"
                if "SUCCEEDED" in line or "Recovered" in line
                else "#eabf72"
                if "LOST" in line or "Fault" in line
                else "#d9e4f2"
            )
            draw.text((32, 78 + row * 21), line, font=font, fill=color)
        draw.text(
            (32, 531),
            f"Recorded elapsed: {timestamp:.1f}s | source: live API event history",
            font=font,
            fill="#849aba",
        )
        frames.append(frame.quantize(colors=32))
        next_time = events[index + 1][0] if index + 1 < len(events) else timestamp + 2
        durations.append(max(30, round((next_time - timestamp) * 1000)))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        args.output,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"Rendered {len(events)} recorded events to {args.output}")


if __name__ == "__main__":
    main()
