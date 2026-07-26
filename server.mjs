import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
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
const videoApiKey = process.env.VIDEO_API_KEY || "";
const videoBaseUrl = (process.env.VIDEO_BASE_URL || "https://ark.cn-beijing.volces.com/api/v3").replace(/\/$/, "");
const videoModel = process.env.VIDEO_MODEL || "doubao-seedance-2-0-fast-260128";
const previewFiles = new Map();
const videoTasks = new Map();

function registerPreview(workDir, videoPath) {
  if (previewFiles.size >= 6) {
    const [oldestId, oldest] = previewFiles.entries().next().value;
    previewFiles.delete(oldestId);
    void rm(oldest.workDir, { recursive: true, force: true });
  }
  const id = crypto.randomUUID();
  previewFiles.set(id, { path: videoPath, workDir, expiresAt: Date.now() + 60 * 60 * 1000 });
  setTimeout(async () => {
    previewFiles.delete(id);
    await rm(workDir, { recursive: true, force: true });
  }, 60 * 60 * 1000).unref();
  return `/preview/${id}`;
}

async function servePreview(req, res, id) {
  const preview = previewFiles.get(id);
  if (!preview || preview.expiresAt < Date.now()) {
    previewFiles.delete(id);
    return json(res, 404, { error: "视频预览已过期，请重新分析" });
  }
  const info = await stat(preview.path);
  const range = String(req.headers.range || "");
  const commonHeaders = {
    "content-type": "video/mp4",
    "accept-ranges": "bytes",
    "access-control-allow-origin": "*",
    "cache-control": "private, no-store",
  };
  if (range.startsWith("bytes=")) {
    const [rawStart, rawEnd] = range.slice(6).split("-");
    const start = Math.max(0, Number(rawStart) || 0);
    const end = Math.min(info.size - 1, Number(rawEnd) || info.size - 1);
    if (start > end) {
      res.writeHead(416, { ...commonHeaders, "content-range": `bytes */${info.size}` });
      return res.end();
    }
    res.writeHead(206, {
      ...commonHeaders,
      "content-length": end - start + 1,
      "content-range": `bytes ${start}-${end}/${info.size}`,
    });
    return createReadStream(preview.path, { start, end }).pipe(res);
  }
  res.writeHead(200, { ...commonHeaders, "content-length": info.size });
  return createReadStream(preview.path).pipe(res);
}

async function readJsonBody(req, limit = 24 * 1024 * 1024) {
  let body = "";
  for await (const chunk of req) {
    body += chunk;
    if (body.length > limit) throw new Error("请求内容过大");
  }
  return JSON.parse(body || "{}");
}

async function createVideoTask(req) {
  if (!videoApiKey) throw new Error("Seedance 尚未配置");
  const { image, prompt, duration } = await readJsonBody(req);
  if (!String(image || "").startsWith("http") && !String(image || "").startsWith("data:image/")) {
    throw new Error("请先生成并确认镜头图片");
  }
  const targetDuration = Math.max(0.5, Math.min(15, Number(duration) || 5));
  const generationDuration = Math.max(5, Math.ceil(targetDuration));
  const response = await fetch(`${videoBaseUrl}/contents/generations/tasks`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${videoApiKey}`,
    },
    body: JSON.stringify({
      model: videoModel,
      content: [
        {
          type: "text",
          text: `${String(prompt || "Subtle natural subject motion and a gentle cinematic camera move.").trim()} Keep the confirmed first frame's product, pattern, colors, room layout and subject identity unchanged. No scene cut, no new objects. --ratio adaptive --dur ${generationDuration} --resolution 720p`,
        },
        {
          type: "image_url",
          image_url: { url: image },
          role: "first_frame",
        },
      ],
      return_last_frame: false,
    }),
    signal: AbortSignal.timeout(30_000),
  });
  const payload = await response.json();
  if (!response.ok || payload.error) {
    throw new Error(payload.error?.message || `Seedance 任务提交失败 (${response.status})`);
  }
  videoTasks.set(payload.id, { targetDuration, previewUrl: null, processing: null });
  return {
    taskId: payload.id,
    status: "queued",
    model: videoModel,
    targetDuration,
    generationDuration,
  };
}

async function trimGeneratedVideo(taskId, sourceUrl, targetDuration) {
  const task = videoTasks.get(taskId);
  if (task?.previewUrl) return task.previewUrl;
  if (task?.processing) return task.processing;
  const processing = (async () => {
    const workDir = await mkdtemp(join(tmpdir(), "frameflow-generated-"));
    const sourcePath = join(workDir, "seedance.mp4");
    const outputPath = join(workDir, "shot.mp4");
    try {
      const response = await fetch(sourceUrl, { signal: AbortSignal.timeout(120_000) });
      if (!response.ok || !response.body) throw new Error("Seedance 视频下载失败");
      await pipeline(response.body, createWriteStream(sourcePath));
      await run(ffmpeg, [
        "-hide_banner", "-loglevel", "error", "-i", sourcePath,
        "-t", targetDuration.toFixed(3),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-movflags", "+faststart", "-y", outputPath,
      ], 180_000);
      const previewUrl = registerPreview(workDir, outputPath);
      videoTasks.set(taskId, { ...videoTasks.get(taskId), previewUrl, processing: null });
      return previewUrl;
    } catch (error) {
      await rm(workDir, { recursive: true, force: true });
      throw error;
    }
  })();
  videoTasks.set(taskId, { ...task, processing });
  return processing;
}

async function getVideoTask(taskId) {
  if (!videoApiKey) throw new Error("Seedance 尚未配置");
  const response = await fetch(`${videoBaseUrl}/contents/generations/tasks/${encodeURIComponent(taskId)}`, {
    headers: { authorization: `Bearer ${videoApiKey}` },
    signal: AbortSignal.timeout(30_000),
  });
  const payload = await response.json();
  if (!response.ok || payload.error) {
    throw new Error(payload.error?.message || `Seedance 状态查询失败 (${response.status})`);
  }
  if (payload.status !== "succeeded") {
    return { taskId, status: payload.status, error: payload.error?.message || null };
  }
  const sourceUrl = payload.content?.video_url
    || payload.content?.video?.url
    || payload.content?.[0]?.video_url
    || payload.output?.video_url;
  if (!sourceUrl) throw new Error("Seedance 已完成，但没有返回视频地址");
  const targetDuration = videoTasks.get(taskId)?.targetDuration || Number(payload.duration) || 5;
  const previewUrl = await trimGeneratedVideo(taskId, sourceUrl, targetDuration);
  return {
    taskId,
    status: "succeeded",
    previewUrl,
    duration: targetDuration,
    model: payload.model || videoModel,
  };
}

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
  let retained = false;

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
    const result = await analyzeFile(videoPath, workDir, sourceUrl, requestId);
    const previewUrl = registerPreview(workDir, videoPath);
    retained = true;
    return { ...result, previewUrl };
  } catch (error) {
    console.error(`[${requestId}] analysis failed`, error);
    throw error;
  } finally {
    if (!retained) await rm(workDir, { recursive: true, force: true });
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
  let retained = false;
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
    const result = await analyzeFile(videoPath, workDir, "uploaded-video", requestId);
    const previewUrl = registerPreview(workDir, videoPath);
    retained = true;
    return { ...result, previewUrl };
  } finally {
    if (!retained) await rm(workDir, { recursive: true, force: true });
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

async function planCarpetWithAI(shots, products, strategy) {
  if (!aiApiKey) throw new Error("视觉模型尚未配置");
  const strategyGuide = {
    natural: "自然植入：只在非常合理的镜头加入地毯，广告感弱，通常约 25%–35% 的镜头。",
    balanced: "平衡展示：兼顾叙事与产品曝光，通常约 40%–60% 的镜头，避免连续重复展示。",
    strong: "强产品展示：增加完整外观和纹理特写，但仍保留必要的过渡镜头，通常约 65%–80% 的镜头。",
  }[strategy] || "平衡展示";
  const productList = products.slice(0, 6);
  const content = [
    {
      type: "text",
      text: `你是地毯短视频广告导演。以下图片按镜头编号排列，参考视频中可能完全没有地毯。
你的任务不是给所有镜头加地毯，而是判断哪些镜头适合自然植入、重点展示或不植入。
整体策略：${strategyGuide}
必须保留原视频的镜头数量、顺序、节奏和每镜头时长，只规划画面内容。
项目中有 ${productList.length} 款不同地毯。它们是独立产品，不能融合图案或混合颜色。相邻同场景镜头尽量使用同一款，不同款尽量获得合理曝光。
只返回 JSON：{"plans":[...]}。plans 必须覆盖全部镜头，每项字段：
id（镜头编号）；
useCarpet（布尔值）；
type（只能是 none、natural、hero）；
productId（植入时必须填写下方产品编号；不植入时为 null）；
placement（中文，说明地毯放在哪里或“不出现地毯”）；
reason（中文，结合地面可见度、景别、动作和叙事节奏说明原因）；
imagePrompt（英文，供生成后续图生视频关键帧使用；none 时明确 no carpet）。`,
    },
  ];
  productList.forEach((product) => {
    content.push({ type: "text", text: `地毯产品 ${product.id}，名称：${product.name}` });
    content.push({ type: "image_url", image_url: { url: product.image } });
  });
  shots.slice(0, 24).forEach((shot) => {
    content.push({ type: "text", text: `镜头 ${shot.id}，时长 ${Number(shot.duration || 0).toFixed(2)} 秒` });
    content.push({ type: "image_url", image_url: { url: shot.image } });
  });
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
            { role: "system", content: "你是专业广告导演，只输出符合要求的 JSON。" },
            { role: "user", content },
          ],
        }),
        signal: AbortSignal.timeout(90_000),
      });
      const payload = await response.json();
      if (!response.ok || payload.error) throw new Error(payload.error?.message || `植入规划失败 (${response.status})`);
      const result = parseModelJson(payload.choices?.[0]?.message?.content);
      if (!Array.isArray(result.plans)) throw new Error("视觉模型没有返回镜头植入方案");
      let productIndex = 0;
      return result.plans.map((plan) => {
        const useCarpet = Boolean(plan.useCarpet) && plan.type !== "none" && productList.length > 0;
        const requestedProduct = productList.find((product) => product.id === String(plan.productId || ""));
        const fallbackProduct = useCarpet ? productList[productIndex++ % productList.length] : null;
        return {
          id: Number(plan.id),
          useCarpet,
          type: useCarpet && ["natural", "hero"].includes(plan.type) ? plan.type : "none",
          productId: useCarpet ? (requestedProduct || fallbackProduct)?.id : undefined,
          placement: String(plan.placement || "未说明"),
          reason: String(plan.reason || "未说明"),
          imagePrompt: String(plan.imagePrompt || ""),
        };
      });
    } catch (error) {
      lastError = error;
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, attempt * 2_000));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("地毯植入规划暂时不可用");
}

function dataUrlToBlob(dataUrl) {
  const match = String(dataUrl || "").match(/^data:(image\/[\w.+-]+);base64,(.+)$/s);
  if (!match) throw new Error("参考图格式无效");
  const bytes = Buffer.from(match[2], "base64");
  if (bytes.length > 8 * 1024 * 1024) throw new Error("参考图不能超过 8MB");
  return { blob: new Blob([bytes], { type: match[1] }), type: match[1] };
}

async function generateImage(prompt, referenceImages) {
  if (!imageApiKey) throw new Error("生图模型尚未配置");
  const references = (Array.isArray(referenceImages) ? referenceImages : [referenceImages])
    .filter(Boolean)
    .slice(0, 2);
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      let endpoint = `${imageBaseUrl}/images/generations`;
      let body;
      let headers;
      if (references.length) {
        const form = new FormData();
        form.append("model", imageModel);
        form.append("prompt", prompt);
        references.forEach((reference, index) => {
          const { blob, type } = dataUrlToBlob(reference);
          const field = references.length > 1 ? "image[]" : "image";
          const extension = type === "image/png" ? "png" : type === "image/webp" ? "webp" : "jpg";
          form.append(field, blob, `reference-${index + 1}.${extension}`);
        });
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
        usedReference: references.length > 0,
        referenceCount: references.length,
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
  if (req.method === "GET" && req.url?.startsWith("/preview/")) {
    try {
      return await servePreview(req, res, req.url.slice("/preview/".length).split("?")[0]);
    } catch (error) {
      return json(res, 500, { error: error instanceof Error ? error.message : "视频预览失败" });
    }
  }
  if (req.method === "GET" && req.url === "/health") return json(res, 200, { status: "ok" });
  if (req.method === "POST" && req.url === "/generate-video") {
    try {
      return json(res, 200, await createVideoTask(req));
    } catch (error) {
      return json(res, 500, { error: error instanceof Error ? error.message : "视频生成失败" });
    }
  }
  if (req.method === "GET" && req.url?.startsWith("/video-task/")) {
    try {
      return json(res, 200, await getVideoTask(req.url.slice("/video-task/".length).split("?")[0]));
    } catch (error) {
      return json(res, 500, { error: error instanceof Error ? error.message : "视频状态查询失败" });
    }
  }
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
  if (req.method === "POST" && req.url === "/plan-carpet") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 40 * 1024 * 1024) req.destroy(new Error("镜头规划数据过大"));
    });
    req.on("end", async () => {
      try {
        const { shots, products = [], strategy } = JSON.parse(body || "{}");
        if (!Array.isArray(shots) || !shots.length || shots.some((shot) => !String(shot.image || "").startsWith("data:image/") && !String(shot.image || "").startsWith("https://"))) {
          return json(res, 400, { error: "缺少有效的镜头画面" });
        }
        if (!Array.isArray(products) || !products.length || products.length > 6 || products.some((product) => !product.id || !String(product.image || "").startsWith("data:image/"))) {
          return json(res, 400, { error: "请上传 1–6 款有效的地毯产品" });
        }
        return json(res, 200, { plans: await planCarpetWithAI(shots, products, strategy) });
      } catch (error) {
        return json(res, 500, { error: error instanceof Error ? error.message : "地毯植入规划失败" });
      }
    });
    return;
  }
  if (req.method === "POST" && req.url === "/generate-image") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 24 * 1024 * 1024) req.destroy(new Error("生图数据过大"));
    });
    req.on("end", async () => {
      try {
        const { prompt, referenceImage, referenceImages } = JSON.parse(body || "{}");
        if (!String(prompt || "").trim()) return json(res, 400, { error: "缺少生图提示词" });
        return json(res, 200, await generateImage(String(prompt).slice(0, 6000), referenceImages || referenceImage));
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
