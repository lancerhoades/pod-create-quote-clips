import os, json, traceback
from typing import Dict, Any, Optional
import runpod
from utils import ensure_dir, download_to, load_clips_from_json, run_ffmpeg_subclip

STORAGE_ROOT = "/storage"  # RunPod network storage mount

def default_paths(job_id: str):
    base = os.path.join(STORAGE_ROOT, job_id)
    return {
        "video_local": os.path.join(base, "splits", "sermon.mp4"),
        "clips_json_local": os.path.join(base, "clips", "clips.json"),
        "clips_out_dir": os.path.join(base, "clips")
    }

def extract_clips(
    *,
    job_id: str,
    input_mode: str = "storage",
    input_video_url: Optional[str] = None,
    input_video_local: Optional[str] = None,
    clips_json_url: Optional[str] = None,
    clips_json_local: Optional[str] = None,
    reextract: bool = False
) -> Dict[str, Any]:
    """
    Produces subclips under /storage/{job_id}/clips/*.mp4
    """
    if not job_id:
        raise RuntimeError("job_id is required.")
    paths = default_paths(job_id)

    # Resolve video path
    video_path = None
    if input_video_local and os.path.isabs(input_video_local):
        video_path = input_video_local
    elif input_video_url:
        video_path = paths["video_local"]
        download_to(video_path, input_video_url)
    else:
        if input_mode == "url":
            raise RuntimeError("input_mode=url but input_video_url was not provided.")
        video_path = paths["video_local"]

    if not os.path.exists(video_path):
        raise RuntimeError(f"Input video not found at {video_path}")

    # Resolve clips JSON
    clips_meta = None
    if clips_json_local and os.path.exists(clips_json_local):
        with open(clips_json_local, "r", encoding="utf-8") as f:
            clips_meta = json.load(f)
    elif clips_json_url:
        import requests
        r = requests.get(clips_json_url, timeout=30)
        r.raise_for_status()
        clips_meta = r.json()
    else:
        cj = paths["clips_json_local"]
        if not os.path.exists(cj):
            raise RuntimeError(f"No clips JSON found at {cj}; provide clips_json_url or clips_json_local.")
        with open(cj, "r", encoding="utf-8") as f:
            clips_meta = json.load(f)

    clips = load_clips_from_json(clips_meta)

    # Output directory
    out_dir = paths["clips_out_dir"]
    ensure_dir(out_dir)

    results = []
    for c in clips:
        name = f"{c['id']}.mp4"
        out_path = os.path.join(out_dir, name)

        if os.path.exists(out_path) and not reextract:
            results.append({"name": name, "path": out_path, "skipped": True})
            continue

        run_ffmpeg_subclip(video_path, out_path, c["start"], c["end"])
        results.append({"name": name, "path": out_path})

    return {
        "files": results,
        "count": len(results),
        "stdout": f"Processed {len(results)} clip(s) for job {job_id}."
    }

def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        data = event.get("input", {}) if isinstance(event, dict) else {}
        return extract_clips(
            job_id=(data.get("job_id") or "").strip(),
            input_mode=(data.get("input_mode") or "storage").lower(),
            input_video_url=data.get("input_video_url"),
            input_video_local=data.get("input_video_local"),
            clips_json_url=data.get("clips_json_url"),
            clips_json_local=data.get("clips_json_local"),
            reextract=bool(data.get("reextract", False)),
        )
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()[-4000:]}

runpod.serverless.start({"handler": handler})
