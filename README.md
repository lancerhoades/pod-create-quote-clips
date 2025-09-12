# pod-create-quote-clips

RunPod Serverless worker that cuts quote clips from a split sermon video.

## Inputs (event.input)
- job_id (string, required): job identifier; files live under /storage/{job_id}
- input_mode (string, optional): "storage" (default) or "url"
- input_video_local (string, optional): absolute path, e.g. /storage/{job_id}/splits/sermon.mp4
- input_video_url (string, optional): presigned URL for the input video
- clips_json_local (string, optional): defaults to /storage/{job_id}/clips/clips.json
- clips_json_url (string, optional): URL to clip metadata JSON
- reextract (bool, optional): overwrite existing outputs (default false)

## Outputs
Returns JSON with:
- files: list of generated clip files (name, path)
- count: number of items
- stdout: summary message

## Storage Layout Used
/storage/
  └── {job_id}/
       ├── splits/
       │    └── sermon.mp4
       ├── clips/
       │    ├── clip001.mp4
       │    └── clips.json
       └── ...

## Build (optional local)
docker build -t pod-create-quote-clips:latest .
