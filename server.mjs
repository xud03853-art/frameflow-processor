import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const exec = promisify(execFile);
const port = Number(process.env.PORT || process.env.FRAMEFLOW_PROCESSOR_PORT || 8788);
const ytDlp = process.env.YT_DLP_PATH || "yt-dlp";
const ffmpeg = process.env.FFMPEG_PATH || "ffmpeg";
const ffprobe = process.env.FFPROBE_PATH || "ffprobe";

function json(res, status, payload) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET, POST, OPTIONS",
  });
  res.end(JSON.stringify(payload));
}

function allowedUrl(value) {
  try {
    const url = new URL(value);
    return ["www.tiktok.com", "tiktok.com", "www.instagram.com", "instagram.com"].includes(url.hostname);
  } catch {
    return false;
  }
}

async function run(file, args) {
  return exec(file, args, { maxBuffer: 16 * 1024 * 1024 });
}

async function analyze(sourceUrl) {
  const workDir = await mkdtemp(join(tmpdir(), "frameflow-"));
  const videoPath = join(workDir, "source.mp4");
  const scenePath = join(workDir, "scenes.txt");

  try {
    await run(ytDlp, [
      "--no-playlist",
      "--merge-output-format", "mp4", "--max-filesize", "120M",
      "-o", videoPath, sourceUrl,
    ]);
    const probe = await run(ffprobe, [
      "-v", "error", "-show_entries",
      "format=duration:stream=codec_type,width,height,r_frame_rate",
      "-of", "json", videoPath,
    ]);
    const media = JSON.parse(probe.stdout);
    const video = media.streams.find((stream) => stream.codec_type === "video");
    const duration = Number(media.format.duration);
    if (!Number.isFinite(duration) || duration <= 0 || duration > 180) {
      throw new Error("目前仅支持 3 分钟以内的视频");
    }

    await run(ffmpeg, [
      "-hide_banner", "-loglevel", "error", "-i", videoPath,
      "-vf", `scdet=threshold=8,metadata=print:file=${scenePath}`,
      "-an", "-f", "null", "-",
    ]);
    const sceneText = await readFile(scenePath, "utf8");
    const cuts = [...sceneText.matchAll(/lavfi\.scd\.time=([\d.]+)/g)]
      .map((match) => Number(match[1]))
      .filter((time) => time > 0.35 && time < duration - 0.35);
    const boundaries = [0, ...cuts, duration];
    const shots = [];

    for (let index = 0; index < boundaries.length - 1; index += 1) {
      const start = boundaries[index];
      const end = boundaries[index + 1];
      const framePath = join(workDir, `frame-${index + 1}.jpg`);
      await run(ffmpeg, [
        "-hide_banner", "-loglevel", "error", "-ss", (start + (end - start) / 2).toFixed(3),
        "-i", videoPath, "-frames:v", "1", "-vf", "scale=540:-2", "-q:v", "4", framePath,
      ]);
      const image = await readFile(framePath);
      shots.push({
        id: index + 1,
        title: `镜头 ${String(index + 1).padStart(2, "0")}`,
        start, end, duration: end - start,
        description: "自动检测到的独立镜头，可补充画面描述与生成提示词。",
        image: `data:image/jpeg;base64,${image.toString("base64")}`,
      });
    }
    return {
      sourceUrl, duration, width: video?.width || 0, height: video?.height || 0,
      hasAudio: media.streams.some((stream) => stream.codec_type === "audio"), shots,
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

createServer(async (req, res) => {
  if (req.method === "OPTIONS") return json(res, 204, {});
  if (req.method === "GET" && req.url === "/health") return json(res, 200, { status: "ok" });
  if (req.method !== "POST" || req.url !== "/analyze") return json(res, 404, { error: "Not found" });
  let body = "";
  req.on("data", (chunk) => { body += chunk; });
  req.on("end", async () => {
    try {
      const { url } = JSON.parse(body || "{}");
      if (!allowedUrl(url)) return json(res, 400, { error: "请输入有效的 TikTok 或 Instagram 视频链接" });
      return json(res, 200, await analyze(url));
    } catch (error) {
      return json(res, 500, { error: error instanceof Error ? error.message : "视频处理失败" });
    }
  });
}).listen(port, "0.0.0.0", () => {
  console.log(`FrameFlow processor ready on port ${port}`);
});
