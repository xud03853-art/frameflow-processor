import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
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
    "access-control-allow-headers": "content-type, x-file-name",
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

async function run(file, args, timeout = 120_000) {
  return exec(file, args, {
    maxBuffer: 16 * 1024 * 1024,
    timeout,
    killSignal: "SIGKILL",
  });
}

async function analyze(sourceUrl) {
  const requestId = crypto.randomUUID().slice(0, 8);
  const workDir = await mkdtemp(join(tmpdir(), "frameflow-"));
  const videoPath = join(workDir, "source.mp4");

  try {
    console.log(`[${requestId}] download started`);
    await run(ytDlp, [
      "--no-playlist",
      "--socket-timeout", "30",
      "--retries", "2",
      "--merge-output-format", "mp4", "--max-filesize", "120M",
      "-o", videoPath, sourceUrl,
    ]);
    console.log(`[${requestId}] download finished`);
    return await analyzeFile(videoPath, workDir, sourceUrl, requestId);
  } catch (error) {
    console.error(`[${requestId}] analysis failed`, error);
    throw error;
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

async function analyzeFile(videoPath, workDir, sourceUrl, requestId) {
  const scenePath = join(workDir, "scenes.txt");
  try {
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
      .filter((time, index, values) =>
        time > 0.35
        && time < duration - 0.35
        && (index === 0 || time - values[index - 1] >= 0.35)
      )
      .slice(0, 23);
    const boundaries = [0, ...cuts, duration];
    const shots = [];

    for (let index = 0; index < boundaries.length - 1; index += 1) {
      const start = boundaries[index];
      const end = boundaries[index + 1];
      const shotDuration = end - start;
      const frameCount = shotDuration >= 4 ? 3 : shotDuration >= 1.5 ? 2 : 1;
      const strategy = frameCount === 1 ? "single_frame" : "multi_frame";
      const positions = frameCount === 1
        ? [start + shotDuration / 2]
        : frameCount === 2
          ? [start + shotDuration * 0.2, start + shotDuration * 0.8]
          : [start + shotDuration * 0.12, start + shotDuration * 0.5, start + shotDuration * 0.88];
      const keyframes = [];
      for (let frameIndex = 0; frameIndex < positions.length; frameIndex += 1) {
        const framePath = join(workDir, `frame-${index + 1}-${frameIndex + 1}.jpg`);
        await run(ffmpeg, [
          "-hide_banner", "-loglevel", "error", "-ss", positions[frameIndex].toFixed(3),
          "-i", videoPath, "-frames:v", "1", "-vf", "scale=420:-2", "-q:v", "5", framePath,
        ]);
        const frame = await readFile(framePath);
        keyframes.push(`data:image/jpeg;base64,${frame.toString("base64")}`);
      }
      const image = keyframes[Math.floor(keyframes.length / 2)];
      shots.push({
        id: index + 1,
        title: `镜头 ${String(index + 1).padStart(2, "0")}`,
        start, end, duration: shotDuration,
        description: "自动检测到的独立镜头，可补充画面描述与生成提示词。",
        image,
        keyframes,
        decision: {
          strategy,
          frameCount,
          canConnect: shotDuration >= 1.5,
          characterConsistency: "需在生成阶段锁定人物参考图与服装特征",
          reason: frameCount === 1
            ? "镜头较短，单张关键帧即可保持构图并生成动态。"
            : `镜头持续 ${shotDuration.toFixed(1)} 秒，建议使用 ${frameCount} 张关键帧衔接动作与构图。`,
          prompt: `Vertical ${video?.height >= video?.width ? "9:16" : "16:9"} cinematic shot, preserve subject identity, clothing, scene layout and camera angle, coherent motion between ${frameCount} keyframe${frameCount > 1 ? "s" : ""}, natural lighting, high detail.`,
        },
      });
    }
    console.log(`[${requestId}] analysis finished (${shots.length} shots)`);
    return {
      sourceUrl, duration, width: video?.width || 0, height: video?.height || 0,
      hasAudio: media.streams.some((stream) => stream.codec_type === "audio"), shots,
    };
  } catch (error) {
    console.error(`[${requestId}] analysis failed`, error);
    throw error;
  }
}

async function analyzeUpload(req) {
  const requestId = crypto.randomUUID().slice(0, 8);
  const workDir = await mkdtemp(join(tmpdir(), "frameflow-upload-"));
  const videoPath = join(workDir, "upload.mp4");
  const declaredSize = Number(req.headers["content-length"] || 0);
  if (declaredSize > 120 * 1024 * 1024) throw new Error("视频不能超过 120MB");

  let received = 0;
  req.on("data", (chunk) => {
    received += chunk.length;
    if (received > 120 * 1024 * 1024) req.destroy(new Error("视频不能超过 120MB"));
  });
  try {
    console.log(`[${requestId}] upload started`);
    await pipeline(req, createWriteStream(videoPath));
    if (!received) throw new Error("请选择要上传的视频");
    console.log(`[${requestId}] upload finished (${received} bytes)`);
    return await analyzeFile(videoPath, workDir, "uploaded-video", requestId);
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

createServer(async (req, res) => {
  if (req.method === "OPTIONS") return json(res, 204, {});
  if (req.method === "GET" && req.url === "/health") return json(res, 200, { status: "ok" });
  if (req.method === "POST" && req.url === "/analyze-upload") {
    try {
      return json(res, 200, await analyzeUpload(req));
    } catch (error) {
      return json(res, 500, { error: error instanceof Error ? error.message : "视频处理失败" });
    }
  }
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
