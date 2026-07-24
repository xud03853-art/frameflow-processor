# FrameFlow Processor

Video analysis worker for FrameFlow. It accepts a TikTok or Instagram URL,
downloads the media, detects scene cuts with FFmpeg, extracts representative
frames, and returns structured JSON.

## API

- `GET /health`
- `POST /analyze` with `{ "url": "https://..." }`
