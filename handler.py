import os, json, subprocess, tempfile, uuid, logging, re, asyncio, aiohttp
from typing import Dict, Any, Optional, List
import boto3
from botocore.client import Config
import runpod

AWS_REGION     = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET  = os.getenv("AWS_S3_BUCKET")
S3_PREFIX_BASE = os.getenv("S3_PREFIX_BASE", "jobs")
LOG_LEVEL      = os.getenv("LOG_LEVEL","INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("pod-create-quote-clips")

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET must be set for S3-only operation.")

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(s3={"addressing_style":"virtual"}))

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
    text = re.sub(r"[^a-zA-Z0-9\-_.]+", "-", (text or "").strip())
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return (text[:maxlen] or "clip").lower()

def ffmpeg_subclip(src: str, dst: str, start_s: float, end_s: float):
    duration = max(0.01, float(end_s) - float(start_s))
    cmd = [
        "ffmpeg","-hide_banner","-y",
        "-ss", f"{start_s:.3f}",
        "-i", src,
        "-t", f"{duration:.3f}",
        "-c:v","libx264","-preset","veryfast","-crf","23",
        "-c:a","aac","-b:a","160k",
        dst
    ]
    subprocess.check_call(cmd)

async def ensure_local_video(url_or_path: str) -> str:
    if url_or_path.startswith("http"):
        tmp = os.path.join(tempfile.gettempdir(), f"input-{uuid.uuid4().hex}.mp4")
        await http_download(url_or_path, tmp)
        return tmp
    if url_or_path.startswith("s3://"):
        # s3://bucket/key → presign then download
        _, _, rest = url_or_path.partition("s3://")
        bucket, _, key = rest.partition("/")
        tmp = os.path.join(tempfile.gettempdir(), f"input-{uuid.uuid4().hex}.mp4")
        url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600)
        await http_download(url, tmp)
        return tmp
    return url_or_path  # assume local path

async def load_clips_config(job_id: str, clips_json_url: Optional[str]) -> List[Dict[str, Any]]:
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

    norm = []
    for idx, c in enumerate(clips, start=1):
        start_s = c.get("start") or c.get("start_s") or c.get("from")
        end_s   = c.get("end")   or c.get("end_s")   or c.get("to")
        if start_s is None or end_s is None:
            # allow duration instead of end
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

def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    input:
      job_id (str) REQUIRED
      video_url (str) PREFERRED  (or input_video_url)
      video_path (str) optional  (legacy)
      clips_json_url (str) optional; default: s3://$BUCKET/jobs/{job_id}/clips/clips.json

    output:
      { ok, job_id, clips: [ {index,title,start,end,key,url,s3_uri}, ... ] }
    """
    try:
        data = event.get("input", {}) if isinstance(event, dict) else {}
        job_id = (data.get("job_id") or "").strip()
        if not job_id:
            return {"error": "job_id is required"}

        # source video
        video_url = data.get("video_url") or data.get("input_video_url")
        video_path = data.get("video_path") or data.get("input_video_local")

        # clips metadata
        clips_json_url = data.get("clips_json_url")

        # load clips windows
        windows = asyncio.run(load_clips_config(job_id, clips_json_url))

        # get source locally
        if video_url:
            src_local = asyncio.run(ensure_local_video(video_url))
        elif video_path:
            src_local = video_path
        else:
            return {"error": "Provide video_url (preferred) or video_path."}

        out_items = []
        for w in windows:
            idx = w["idx"]
            title = w["title"]
            start_s = w["start"]; end_s = w["end"]
            slug = slugify(title) if title else f"clip-{idx:03d}"
            dst_local = os.path.join(tempfile.gettempdir(), f"{slug}-{idx:03d}.mp4")
            ffmpeg_subclip(src_local, dst_local, start_s, end_s)

            key = s3_key(job_id, "clips", f"{slug}-{idx:03d}.mp4")
            s3.upload_file(dst_local, AWS_S3_BUCKET, key)
            url = presign(AWS_S3_BUCKET, key)

            out_items.append({
                "index": idx, "title": title, "start": start_s, "end": end_s,
                "key": key, "url": url, "s3_uri": f"s3://{AWS_S3_BUCKET}/{key}"
            })

        return {"ok": True, "job_id": job_id, "clips": out_items}

    except subprocess.CalledProcessError as e:
        return {"error": f"ffmpeg failed: {e}"}
    except Exception as e:
        log.exception("handler failed")
        return {"error": str(e)}

runpod.serverless.start({"handler": handler})
