import os, json, subprocess, tempfile, uuid, logging, re, asyncio, aiohttp
from typing import Dict, Any, Optional, List
import boto3
from botocore.client import Config
import runpod

# ----------------------- ENV / Logging -----------------------
AWS_REGION     = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET  = os.getenv("AWS_S3_BUCKET")
S3_PREFIX_BASE = os.getenv("S3_PREFIX_BASE", "jobs")
LOG_LEVEL      = os.getenv("LOG_LEVEL","INFO").upper()
TRIM_SILENCE   = os.getenv("TRIM_SILENCE", "false").lower() in ("1","true","yes","on")
SILENCE_DB     = float(os.getenv("SILENCE_DB", "-35"))
MIN_SILENCE_SEC = float(os.getenv("MIN_SILENCE_SEC", "0.35"))
KEEP_PAD_SEC   = float(os.getenv("KEEP_PAD_SEC", "0.08"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("pod-create-quote-clips")

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET must be set for S3-only operation.")

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(s3={"addressing_style":"virtual"}))

# ----------------------- Helpers -----------------------
def s3_key(job_id: str, *parts: str) -> str:
    safe = [p.strip("/").replace("\\","/") for p in parts if p]
    return "/".join([S3_PREFIX_BASE.strip("/"), job_id] + safe)

def presign(bucket: str, key: str, expires: int = 7*24*3600) -> str:
    return s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires)

async def http_download(url: str, dst: str):
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=None)) as r:
            if r.status != 200:
                raise RuntimeError(f"GET {r.status}: {await r.text()}")
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, "wb") as f:
                async for chunk in r.content.iter_chunked(1<<20):
                    f.write(chunk)

def slugify(text: str, maxlen: int = 40) -> str:
    text = re.sub(r"[^a-zA-Z0-9\\-_.]+", "-", (text or "").strip())
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return (text[:maxlen] or "clip").lower()

def ffmpeg_subclip_url_aware(src_url_or_path: str, dst: str, start_s: float, end_s: float):
    """Use -ss before -i so ffmpeg can input-seek even over HTTP (range)."""
    duration = max(0.01, float(end_s) - float(start_s))
    cmd = [
        "ffmpeg","-hide_banner","-y",
        "-ss", f"{start_s:.3f}",
        "-i", src_url_or_path,
        "-t", f"{duration:.3f}",
        "-c:v","libx264","-preset","veryfast","-crf","23",
        "-c:a","aac","-b:a","160k",
        dst
    ]
    log.info("FFmpeg: %s", " ".join(cmd))
    subprocess.check_call(cmd)

def materialize_source_if_needed(video_url: Optional[str], video_path: Optional[str]) -> str:
    """
    Return a source string usable by ffmpeg:
      - http(s) URL: returned as-is
      - s3://bucket/key: converted to presigned URL
      - local path: returned as-is
    """
    if video_url:
        if video_url.startswith("s3://"):
            _, _, rest = video_url.partition("s3://")
            bkt, _, key = rest.partition("/")
            return presign(bkt or AWS_S3_BUCKET, key, 3600)
        return video_url
    if video_path:
        if video_path.startswith("s3://"):
            _, _, rest = video_path.partition("s3://")
            bkt, _, key = rest.partition("/")
            return presign(bkt or AWS_S3_BUCKET, key, 3600)
        return video_path
    raise ValueError("Provide video_url (preferred) or video_path")

async def load_clips_config(job_id: str, clips_json_url: Optional[str]) -> List[Dict[str, Any]]:
    """Load and normalize the clip windows from clips.json (http or s3 default)."""
    if clips_json_url and clips_json_url.startswith("http"):
        tmp = os.path.join(tempfile.gettempdir(), f"clips-{uuid.uuid4().hex}.json")
        await http_download(clips_json_url, tmp)
        obj = json.loads(open(tmp, "r", encoding="utf-8").read())
    else:
        key = s3_key(job_id, "clips", "clips.json")
        tmp = os.path.join(tempfile.gettempdir(), f"clips-{uuid.uuid4().hex}.json")
        url = presign(AWS_S3_BUCKET, key, 3600)
        await http_download(url, tmp)
        obj = json.loads(open(tmp, "r", encoding="utf-8").read())

    # normalize to list
    if isinstance(obj, dict) and "clips" in obj:
        clips = obj["clips"]
    elif isinstance(obj, list):
        clips = obj
    else:
        raise ValueError("clips.json must be a list or an object with a 'clips' key")

    log.info(f"Loaded {len(clips)} clip windows from clips.json")
    norm: List[Dict[str, Any]] = []
    for idx, c in enumerate(clips, start=1):
        start_s = c.get("start") or c.get("start_s") or c.get("from")
        end_s   = c.get("end")   or c.get("end_s")   or c.get("to")
        if start_s is None or end_s is None:
            dur = c.get("duration")
            if start_s is not None and dur is not None:
                end_s = float(start_s) + float(dur)
        if start_s is None or end_s is None:
            continue
        title = c.get("title") or c.get("label") or (c.get("text") or f"clip{idx:03d}")
        norm.append({"idx": idx, "title": title, "start": float(start_s), "end": float(end_s)})
    if not norm:
        raise ValueError("No valid clips found in clips.json")
    return norm

def pick_single_window(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    If caller intends single-clip mode, return a single normalized window:
      - prefer 'restrict_to_window': {start,end,title?}
      - else start/end (or start+duration)
      - optional 'title'
    """
    if "restrict_to_window" in data and isinstance(data["restrict_to_window"], dict):
        rw = data["restrict_to_window"]
        st = rw.get("start"); en = rw.get("end")
        if st is not None and en is not None:
            return {"idx": 1, "title": rw.get("title") or data.get("title") or "clip",
                    "start": float(st), "end": float(en)}

    st = data.get("start"); en = data.get("end"); dur = data.get("duration")
    if st is not None and (en is not None or dur is not None):
        if en is None:
            en = float(st) + float(dur)
        return {"idx": 1, "title": data.get("title") or "clip",
                "start": float(st), "end": float(en)}
    return None

def make_compat_payload(url: str, key: str, s3_uri: str, clip_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Provide multiple shapes so older callers can find the URL:
      - top-level 'url'
      - 'output': {'url': ...}
      - 'urls': {'url': ..., 'mp4_url': ..., 'clips': [{'url': ...}]}
      - 'clip': single item
      - 'clips': [item]
    """
    item = {
        "index": clip_item.get("index", 1),
        "title": clip_item.get("title"),
        "start": clip_item.get("start"),
        "end": clip_item.get("end"),
        "key": key,
        "url": url,
        "s3_uri": s3_uri
    }
    return {
        "ok": True,
        "url": url,
        "output": {"url": url, "key": key, "s3_uri": s3_uri},
        "urls": {"url": url, "mp4_url": url, "clips": [ {"url": url} ]},
        "clip": item,
        "clips": [item]
    }

# ---- NEW: probe duration & tiny-file helpers ----
def probe_duration(src: str) -> Optional[float]:
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-show_entries","format=duration","-of","default=noprint_wrappers=1:nokey=1", src],
            stderr=subprocess.STDOUT, text=True
        ).strip()
        return float(out)
    except Exception as e:
        log.warning("probe_duration failed for %s: %s", src, e)
        return None

def _tiny_file(path: str, min_bytes: int = 100*1024) -> bool:
    try:
        return os.path.getsize(path) < min_bytes
    except Exception:
        return True

def _detect_silence_bounds(path: str):
    dur = probe_duration(path)
    if dur is None:
        return None
    cmd = [
        "ffmpeg","-hide_banner","-i", path,
        "-af", f"silencedetect=n={SILENCE_DB}dB:d={MIN_SILENCE_SEC}",
        "-f","null","-"
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = proc.stderr or ""
    start_re = re.compile(r"silence_start:\s*([0-9.]+)")
    end_re = re.compile(r"silence_end:\s*([0-9.]+)")

    events = []
    cur_start = None
    for line in out.splitlines():
        m = start_re.search(line)
        if m:
            cur_start = float(m.group(1))
            continue
        m = end_re.search(line)
        if m:
            end = float(m.group(1))
            if cur_start is None:
                cur_start = max(0.0, end - MIN_SILENCE_SEC)
            events.append((cur_start, end))
            cur_start = None
    if cur_start is not None:
        events.append((cur_start, None))

    lead = 0.0
    trail = 0.0
    if events and events[0][0] <= 0.01 and events[0][1] is not None:
        lead = events[0][1]
    if events and events[-1][1] is None:
        trail = max(0.0, dur - events[-1][0])

    return lead, trail, dur

def _trim_dead_air(path: str) -> bool:
    info = _detect_silence_bounds(path)
    if not info:
        return False
    lead, trail, dur = info
    if lead <= 0 and trail <= 0:
        return False
    lead_trim = max(0.0, lead - KEEP_PAD_SEC)
    trail_trim = max(0.0, trail - KEEP_PAD_SEC)
    new_start = lead_trim
    new_end = dur - trail_trim
    if new_end - new_start < 0.2:
        return False

    tmp = path + ".trim.mp4"
    cmd = [
        "ffmpeg","-hide_banner","-y",
        "-ss", f"{new_start:.3f}",
        "-to", f"{new_end:.3f}",
        "-i", path,
        "-c:v","libx264","-preset","veryfast","-crf","23",
        "-c:a","aac","-b:a","160k",
        tmp
    ]
    log.info("FFmpeg trim silence: %s", " ".join(cmd))
    subprocess.check_call(cmd)
    os.replace(tmp, path)
    return True

# ----------------------- Handler -----------------------
async def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    INPUT (single-clip mode):
      job_id (str)            REQUIRED
      video_url (str)         PREFERRED (http or s3://)
      video_path (str)        optional (local or s3://)
      start (float)           REQUIRED in single-clip OR restrict_to_window.start
      end (float)             REQUIRED in single-clip OR use duration
      duration (float)        optional (if end omitted)
      title (str)             optional
      restrict_to_window (dict{start,end,title?}) optional
      single_clip/no_batch    optional flags (truthy -> single-clip)
      trim_silence (bool)     optional override for TRIM_SILENCE env
      output_basename (str)   optional, filename without path (e.g., 'clip_01.mp4')
      fallback_full_url (str) optional HTTP URL to full recording for auto-retry

    INPUT (batch / backwards-compatible):
      job_id (str)            REQUIRED
      video_url/video_path    see above
      clips_json_url (str)    optional; defaults to s3://$BUCKET/jobs/{job_id}/clips/clips.json

    OUTPUT as before.
    """
    try:
        data = event.get("input", {}) if isinstance(event, dict) else {}
        job_id = (data.get("job_id") or "").strip()
        if not job_id:
            return {"error": "job_id is required"}

        # Source selection & optional fallback
        video_url = data.get("video_url") or data.get("input_video_url")
        video_path = data.get("video_path") or data.get("input_video_local")
        fallback_full_url = data.get("fallback_full_url") or data.get("full_url")

        def choose_src_for_window(start_s: float, end_s: float) -> str:
            """Prefer section src; if end exceeds section duration, use fallback full URL."""
            section_src = materialize_source_if_needed(video_url, video_path)
            dur = probe_duration(section_src)
            if dur is not None and end_s > (dur - 0.25) and isinstance(fallback_full_url, str) and fallback_full_url.startswith("http"):
                log.info("Window %.2f–%.2f exceeds section duration %.2f; switching to fallback_full_url", start_s, end_s, dur)
                return fallback_full_url
            return section_src

        def cut_with_auto_retry(src: str, dst_local: str, start_s: float, end_s: float, trim_enabled: bool) -> None:
            """Cut; if result looks tiny and we have a fallback, retry once with fallback_full_url."""
            ffmpeg_subclip_url_aware(src, dst_local, start_s, end_s)
            if _tiny_file(dst_local) and isinstance(fallback_full_url, str) and fallback_full_url.startswith("http") and src != fallback_full_url:
                log.warning("Produced tiny file (%s). Retrying against fallback_full_url", dst_local)
                ffmpeg_subclip_url_aware(fallback_full_url, dst_local, start_s, end_s)
            if trim_enabled:
                try:
                    if _trim_dead_air(dst_local):
                        log.info("Trimmed silence for %s", dst_local)
                except Exception as e:
                    log.warning("Trim silence failed for %s: %s", dst_local, e)

        # Decide single-clip vs batch
        single_window = pick_single_window(data)
        single_forced = bool(data.get("single_clip") or data.get("no_batch"))
        single_mode = bool(single_window or single_forced)

        trim_enabled = TRIM_SILENCE if data.get("trim_silence") is None else bool(data.get("trim_silence"))

        # ---------------- Single-clip Mode ----------------
        if single_mode:
            if not single_window:
                return {"error": "single-clip mode requested but no start/end (or duration) provided."}

            output_basename = (data.get("output_basename") or "").strip()
            title = single_window.get("title") or data.get("title") or "clip"
            base_noext = output_basename.rsplit(".", 1)[0] if output_basename else slugify(title)
            dst_local = os.path.join(tempfile.gettempdir(), f"{base_noext}.mp4")

            start_s = float(single_window["start"])
            end_s   = float(single_window["end"])
            log.info(f"[single] {job_id} {start_s:.3f}-{end_s:.3f} → {base_noext}.mp4")

            src = choose_src_for_window(start_s, end_s)
            cut_with_auto_retry(src, dst_local, start_s, end_s, trim_enabled)

            key = s3_key(job_id, "clips", f"{base_noext}.mp4")
            s3.upload_file(dst_local, AWS_S3_BUCKET, key)
            url = presign(AWS_S3_BUCKET, key)
            s3_uri = f"s3://{AWS_S3_BUCKET}/{key}"

            payload = make_compat_payload(url, key, s3_uri, {
                "index": 1, "title": title, "start": start_s, "end": end_s
            })
            payload["job_id"] = job_id
            return payload

        # ---------------- Batch Mode ----------------
        clips_json_url = data.get("clips_json_url")
        windows = await load_clips_config(job_id, clips_json_url)

        out_items: List[Dict[str, Any]] = []
        url_list: List[str] = []
        for w in windows:
            idx = w["idx"]
            title = w["title"]
            start_s = w["start"]; end_s = w["end"]
            slug = slugify(title) if title else f"clip-{idx:03d}"
            dst_local = os.path.join(tempfile.gettempdir(), f"{slug}-{idx:03d}.mp4")

            log.info(f"[batch] {job_id} [{idx}] {start_s:.3f}-{end_s:.3f} → {slug}-{idx:03d}.mp4")

            src = choose_src_for_window(start_s, end_s)
            cut_with_auto_retry(src, dst_local, start_s, end_s, trim_enabled)

            key = s3_key(job_id, "clips", f"{slug}-{idx:03d}.mp4")
            s3.upload_file(dst_local, AWS_S3_BUCKET, key)
            url = presign(AWS_S3_BUCKET, key)
            url_list.append(url)

            out_items.append({
                "index": idx, "title": title, "start": start_s, "end": end_s,
                "key": key, "url": url, "s3_uri": f"s3://{AWS_S3_BUCKET}/{key}"
            })

        return {"ok": True, "job_id": job_id, "clips": out_items, "urls": {"clips": url_list}}

    except subprocess.CalledProcessError as e:
        return {"error": f"ffmpeg failed: {e}"}
    except Exception as e:
        log.exception("handler failed")
        return {"error": str(e)}

# Runpod entrypoint
runpod.serverless.start({"handler": handler})
