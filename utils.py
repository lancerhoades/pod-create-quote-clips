import os, re, json, subprocess
from typing import List, Dict, Any, Optional
import requests

TIME_RE = re.compile(r"^(?:(\d+):)?([0-5]?\d):([0-5]?\d(?:\.\d+)?)$")  # [hh:]mm:ss[.ms]

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def download_to(path: str, url: str, chunk=1024*1024):
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        ensure_dir(os.path.dirname(path))
        with open(path, "wb") as f:
            for c in r.iter_content(chunk_size=chunk):
                if c:
                    f.write(c)
    return path

def parse_timecode(t: Any) -> float:
    """Accepts seconds (int/float/str) or 'hh:mm:ss.ms' string."""
    if isinstance(t, (int, float)):
        return float(t)
    if isinstance(t, str):
        s = t.strip()
        if s.replace('.', '', 1).isdigit():
            return float(s)
        m = TIME_RE.match(s)
        if m:
            h = int(m.group(1)) if m.group(1) else 0
            mnt = int(m.group(2))
            sec = float(m.group(3))
            return h*3600 + mnt*60 + sec
    raise ValueError(f"Unrecognized time format: {t}")

def load_clips_from_json(json_obj: Any) -> List[Dict[str, Any]]:
    """
    Accepts:
      - {"clips":[{"id","start","end","quote"?}, ...]}
      - [{"id","start","end"}, ...]
    start/end may be seconds or timecode strings.
    """
    if isinstance(json_obj, dict) and "clips" in json_obj:
        clips = json_obj["clips"]
    elif isinstance(json_obj, list):
        clips = json_obj
    else:
        raise ValueError("clips JSON must be a list or have a 'clips' key.")

    out = []
    for idx, c in enumerate(clips, start=1):
        start = parse_timecode(c.get("start"))
        end = parse_timecode(c.get("end")) if c.get("end") is not None else None
        dur = parse_timecode(c.get("duration")) if c.get("duration") is not None else None
        if end is None and dur is None:
            raise ValueError("Each clip needs either 'end' or 'duration'.")
        if end is None:
            end = start + dur
        clip_id = c.get("id") or f"clip{idx:03d}"
        out.append({
            "id": clip_id,
            "start": max(0.0, float(start)),
            "end": max(0.0, float(end)),
            "quote": c.get("quote", "")
        })
    return out

def run_ffmpeg_subclip(src: str, dst: str, start_s: float, end_s: float) -> None:
    """Re-encode for robust clipping independent of keyframes."""
    ensure_dir(os.path.dirname(dst))
    duration = max(0.01, end_s - start_s)
    cmd = [
        "ffmpeg", "-hide_banner", "-y",
        "-ss", f"{start_s:.3f}",
        "-i", src,
        "-t", f"{duration:.3f}",
        "-vf", "scale=iw:ih:flags=bicubic",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        dst
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed for {dst}\n{proc.stderr[-8000:]}")
