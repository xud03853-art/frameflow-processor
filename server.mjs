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
const aiApiKey = process.env.AI_API_KEY || "";
const aiBaseUrl = (process.env.AI_BASE_URL || "https://api.jumengai.net/v1").replace(/\/$/, "");
const aiModel = process.env.AI_MODEL || "gpt-5.6-luna";
const imageApiKey = process.env.IMAGE_API_KEY || "";
const imageBaseUrl = (process.env.IMAGE_BASE_URL || aiBaseUrl).replace(/\/$/, "");
const imageModel = process.env.IMAGE_MODEL || "gpt-image-2";

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

function parseModelJson(content) {
  const cleaned = String(content || "").replace(/^```json\s*/i, "").replace(/```$/i, "").trim();
  const start = cleaned.indexOf("{");
  const end = cleaned.lastIndexOf("}");
  if (start < 0 || end < start) throw new Error("视觉模型没有返回有效的分析结果");
  return JSON.parse(cleaned.slice(start, end + 1));
}

async function analyzeShotWithAI(keyframes, shot) {
  if (!aiApiKey) throw new Error("视觉模型尚未配置");
  const images = keyframes.slice(0, 3).map((url) => ({
    type: "image_url",
    image_url: { url },
  }));
  const prompt = `你是短视频分镜导演。分析这些按时间顺序排列的关键帧。
镜头时长：${Number(shot?.duration || 0).toFixed(2)} 秒。
只返回一个 JSON 对象，不要 Markdown。字段必须包含：
description（中文，一句话准确描述人物、场景、动作）；
characters（中文字符串，人物数量、外貌、服装；无人则写“无人物”）；
scene（中文字符串，环境、物品、光线）；
action（中文字符串，动作及首尾变化）；
camera（中文字符串，景别、机位、构图、镜头运动）；
continuity（中文字符串，与前后帧的可衔接性）；
strategy（只能是 keep、single_frame、multi_frame、ai_remake 之一）；
frameCount（1 到 3 的整数）；
reason（中文字符串，推荐该处理方式的理由）；
prompt（英文，可直接用于生图或生视频，强调人物、服装、场景和构图一致性）。`;
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const response = await fetch(`${aiBaseUrl}/chat/completions`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${aiApiKey}`,
        },
        body: JSON.stringify({
          model: aiModel,
          messages: [
            { role: "system", content: "你只输出符合要求的 JSON，不添加解释。" },
            { role: "user", content: [{ type: "text", text: prompt }, ...images] },
          ],
        }),
        signal: AbortSignal.timeout(45_000),
      });
      const payload = await response.json();
      if (!response.ok || payload.error) {
        throw new Error(payload.error?.message || `视觉模型请求失败 (${response.status})`);
      }
      const result = parseModelJson(payload.choices?.[0]?.message?.content);
      return { ...result, model: payload.model || aiModel, usage: payload.usage || null };
    } catch (error) {
      lastError = error;
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, attempt * 2_000));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("视觉模型暂时不可用");
}

function dataUrlToBlob(dataUrl) {
  const match = String(dataUrl || "").match(/^data:(image\/[\w.+-]+);base64,(.+)$/s);
  if (!match) throw new Error("参考图格式无效");
  const bytes = Buffer.from(match[2], "base64");
  if (bytes.length > 8 * 1024 * 1024) throw new Error("参考图不能超过 8MB");
  return { blob: new Blob([bytes], { type: match[1] }), type: match[1] };
}

async function generateImage(prompt, referenceImage) {
  if (!imageApiKey) throw new Error("生图模型尚未配置");
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      let endpoint = `${imageBaseUrl}/images/generations`;
      let body;
      let headers;
      if (referenceImage) {
        const { blob, type } = dataUrlToBlob(referenceImage);
        const form = new FormData();
        form.append("model", imageModel);
        form.append("prompt", prompt);
        form.append("image", blob, type === "image/png" ? "reference.png" : "reference.jpg");
        endpoint = `${imageBaseUrl}/images/edits`;
        body = form;
        headers = { authorization: `Bearer ${imageApiKey}` };
      } else {
        body = JSON.stringify({ model: imageModel, prompt, n: 1 });
        headers = {
          "content-type": "application/json",
          authorization: `Bearer ${imageApiKey}`,
        };
      }
      const response = await fetch(endpoint, {
        method: "POST",
        headers,
        body,
        signal: AbortSignal.timeout(180_000),
      });
      const payload = await response.json();
      if (!response.ok || payload.error) {
        throw new Error(payload.error?.message || `图片生成失败 (${response.status})`);
      }
      const item = payload.data?.[0];
      const image = item?.b64_json
        ? `data:image/png;base64,${item.b64_json}`
        : item?.url;
      if (!image) throw new Error("生图模型没有返回图片");
      return {
        image,
        revisedPrompt: item.revised_prompt || prompt,
        model: imageModel,
        usedReference: Boolean(referenceImage),
      };
    } catch (error) {
      lastError = error;
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, attempt * 3_000));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("生图服务暂时不可用");
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
  if (req.method === "POST" && req.url === "/analyze-shot") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 8 * 1024 * 1024) req.destroy(new Error("关键帧数据过大"));
    });
    req.on("end", async () => {
      try {
        const { keyframes, shot } = JSON.parse(body || "{}");
        if (!Array.isArray(keyframes) || !keyframes.length || keyframes.some((item) => !String(item).startsWith("data:image/"))) {
          return json(res, 400, { error: "缺少有效的镜头关键帧" });
        }
        return json(res, 200, { analysis: await analyzeShotWithAI(keyframes, shot) });
      } catch (error) {
        return json(res, 500, { error: error instanceof Error ? error.message : "视觉识别失败" });
      }
    });
    return;
  }
  if (req.method === "POST" && req.url === "/generate-image") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 12 * 1024 * 1024) req.destroy(new Error("生图数据过大"));
    });
    req.on("end", async () => {
      try {
        const { prompt, referenceImage } = JSON.parse(body || "{}");
        if (!String(prompt || "").trim()) return json(res, 400, { error: "缺少生图提示词" });
        return json(res, 200, await generateImage(String(prompt).slice(0, 6000), referenceImage));
      } catch (error) {
        return json(res, 500, { error: error instanceof Error ? error.message : "图片生成失败" });
      }
    });
    return;
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
