// Private song-lyrics bot as a Cloudflare Worker (JavaScript, no server).
// Port of the Python bot v1.0.2, same behaviour, adapted to Workers:
//
//   * Webhook, not polling: Telegram POSTs updates to /webhook. The
//     handler answers 200 fast and heavy work runs via ctx.waitUntil,
//     so Telegram never retries an update and duplicates a job.
//   * State lives in one KV document per user (key "u:<userId>"):
//     jobs, queue, per-user state and the processing flag. One song
//     costs about 4 KV writes, so the free KV quota (1,000 writes/day)
//     covers roughly 200 songs/day for a personal bot. Reads are
//     100,000/day on the free plan. See README.md for the limits.
//   * Per-user FIFO queue: a user's songs run one at a time; different
//     users run in parallel (each in their own invocation). There is
//     no cross-isolate global semaphore on Workers, and for a personal
//     bot the per-user queue is the protection that matters.
//   * If a processing loop dies (e.g. a free-plan CPU/time limit),
//     the processing flag carries a timestamp: the next update from
//     that user, or the optional cron trigger, requeues a job whose
//     flag is older than 5 minutes instead of leaving it stuck.
//
// Token safety: the Telegram token is NEVER in this file. It is a
// Worker secret (wrangler secret put TELEGRAM_BOT_TOKEN, or the
// dashboard Secrets tab). The Telegraph token is stored only in KV
// (key "meta:telegraph_token"). Never commit either to git or chat.
//
// Deploy (dashboard, no computer needed): paste this file into a new
// Worker, bind a KV namespace as LYRICS_KV, add the secrets, deploy,
// then set the Telegram webhook to https://<worker>/webhook (README).
//
// v1.0.3-worker changes (translation reliability on shared datacenter
// IPs): Google and MyMemory translation calls are retried once with a
// backoff after a failure, so a transient 429/5xx no longer kills a
// whole song; a failed translation now reports WHICH service failed,
// with which HTTP status, and on which lyric line, both in the stored
// job error and in the user's Telegram message — so the next failure
// is self-diagnosing and no Cloudflare log hunt is needed; lines that
// contain no letters or digits (e.g. "...", "—") are kept as-is
// instead of being sent to a translator that may reject them; and an
// optional MYMEMORY_EMAIL secret (your own email address, not a token)
// raises the MyMemory quota above the shared anonymous IP quota.

export const VERSION = "1.0.3-worker";
export const MAX_QUEUE_PER_USER = 10;
export const TEXT_CHUNK_LIMIT = 4000;
const AUDIO_EXTS = [".mp3", ".m4a", ".flac", ".wav", ".ogg", ".opus", ".aac"];
const STALE_PROCESSING_MS = 5 * 60 * 1000;

export const LANG_NAMES = {
  en: "انگلیسی", ar: "عربی", tr: "ترکی استانبولی",
  fa: "فارسی", es: "اسپانیایی", fr: "فرانسوی",
  de: "آلمانی", it: "ایتالیایی", pt: "پرتغالی",
  ru: "روسی", ku: "کردی", hy: "ارمنی",
};

export class LyricsNotFound extends Error {}
export class TranslationFailed extends Error {}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

// ------------------------------------------------------- pure helpers
export function parseSongFromFilename(filename) {
  if (!filename) return [null, null];
  let name = filename.split(/[\\/]/).pop();
  for (const ext of AUDIO_EXTS) {
    if (name.toLowerCase().endsWith(ext)) { name = name.slice(0, -ext.length); break; }
  }
  if (!AUDIO_EXTS.some((e) => filename.toLowerCase().endsWith(e)) && name.includes(".")) {
    name = name.slice(0, name.lastIndexOf("."));
  }
  name = name.trim();
  if (!name) return [null, null];
  const parts = name.split(/\s+[-–—]\s+/).map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) return [parts[0], parts.slice(1).join(" - ")];
  return [null, parts[0]];
}

export function parseManualInput(text) {
  if (!text) return [null, null];
  let singer = null, title = null;
  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;
    let m = line.match(/^(خواننده|singer|artist)\s*[:：]\s*(.+)$/i);
    if (m) { singer = m[2].trim(); continue; }
    m = line.match(/^(آهنگ|song|title|نام آهنگ)\s*[:：]\s*(.+)$/i);
    if (m) { title = m[2].trim(); continue; }
    if (singer === null && title === null && / - | – | — /.test(line)) {
      const [a, t] = parseSongFromFilename(line);
      if (t) { singer = a; title = t; continue; }
    }
    if (singer === null) singer = line;
    else if (title === null) title = line;
  }
  if (title === null && singer !== null) { title = singer; singer = null; }
  return [singer, title];
}

export function parseSongFromText(text) {
  if (!text) return [null, null];
  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.includes("telegra.ph")) continue;
    const [artist, title] = parseSongFromFilename(line);
    if (title) return [artist, title];
  }
  return [null, null];
}

export function extractSong(message) {
  const audio = message.audio;
  if (audio) {
    const performer = (audio.performer || "").trim() || null;
    const title = (audio.title || "").trim() || null;
    if (title) return [performer, title, "audio-metadata"];
    if (audio.file_name) {
      const [artist, song] = parseSongFromFilename(audio.file_name);
      if (song) return [artist, song, "file-name"];
    }
  }
  const doc = message.document;
  if (doc) {
    const mime = doc.mime_type || "";
    const fileName = doc.file_name || "";
    if (mime.startsWith("audio") || AUDIO_EXTS.some((e) => fileName.toLowerCase().endsWith(e))) {
      const [artist, song] = parseSongFromFilename(fileName);
      if (song) return [artist, song, "file-name"];
    }
  }
  if (message.caption) {
    const [artist, song] = parseSongFromText(message.caption);
    if (song) return [artist, song, "caption"];
  }
  return [null, null, "unknown"];
}

export function isAudioMessage(message) {
  if (message.audio) return true;
  const doc = message.document;
  if (!doc) return false;
  const mime = doc.mime_type || "";
  const fileName = doc.file_name || "";
  return mime.startsWith("audio") || AUDIO_EXTS.some((e) => fileName.toLowerCase().endsWith(e));
}

export function cleanLyricLines(plainLyrics) {
  const lines = [];
  for (const raw of (plainLyrics || "").split("\n")) {
    let line = raw.trim();
    if (!line) continue;
    if (/^\[.*\]$/.test(line)) continue;
    line = line.replace(/^(?:\[\d{1,2}:\d{2}(?:\.\d+)?\]\s*)+/, "").trim();
    if (line) lines.push(line);
  }
  return lines;
}

export function validatePastedLyrics(text) {
  const lines = cleanLyricLines(text);
  if (lines.length < 3) return null;
  if (lines.reduce((s, l) => s + l.length, 0) < 40) return null;
  return lines;
}

export function pickBestLyrics(results, artist, title) {
  if (!results) return null;
  const artistL = (artist || "").toLowerCase();
  const titleL = (title || "").toLowerCase();
  let best = null, bestScore = -1;
  for (const item of results) {
    const plain = item.plainLyrics || "";
    if (!plain.trim()) continue;
    let score = 0;
    const itemTitle = (item.trackName || "").toLowerCase();
    const itemArtist = (item.artistName || "").toLowerCase();
    if (titleL && titleL === itemTitle) score += 10;
    else if (titleL && itemTitle.includes(titleL)) score += 5;
    if (artistL && artistL === itemArtist) score += 10;
    else if (artistL && itemArtist.includes(artistL)) score += 5;
    if (/\[\d{1,2}:\d{2}/.test(plain)) score -= 5;
    if (score > bestScore) { best = item; bestScore = score; }
  }
  return best ? best.plainLyrics : null;
}

export function buildTelegraphContent(pairs) {
  const content = [];
  for (const [original, persian] of pairs) {
    content.push({ tag: "p", children: [original] });
    content.push({ tag: "p", children: [persian] });
  }
  return content;
}

export function songHeader(artist, title) {
  return artist ? `🎵 ${title}\n🎤 ${artist}` : `🎵 ${title}`;
}

export function buildTextOutput(artist, title, pairs) {
  const parts = [songHeader(artist, title), "", "📜 متن و ترجمه:"];
  for (const [original, persian] of pairs) {
    parts.push("", original, persian);
  }
  return parts.join("\n").trim();
}

export function splitTextMessages(fullText, limit = TEXT_CHUNK_LIMIT) {
  if (fullText.length <= limit) return [fullText];
  const blocks = fullText.split("\n\n");
  const chunks = [];
  let current = "";
  for (const block of blocks) {
    const piece = current ? current + "\n\n" + block : block;
    if (piece.length <= limit) { current = piece; continue; }
    if (current) chunks.push(current);
    if (block.length <= limit) current = block;
    else {
      for (let i = 0; i < block.length; i += limit) chunks.push(block.slice(i, i + limit));
      current = "";
    }
  }
  if (current) chunks.push(current);
  return chunks;
}

export function normalizeTitle(title) {
  if (!title) return title;
  let out = title.replace(/\s*[\(\[][^\)\]]*[\)\]]\s*/g, " ");
  out = out.replace(/\s+[-–—]\s*(remix|remaster|live|acoustic|version).*/i, "");
  out = out.replace(/\s+(feat\.?|ft\.?|featuring)\s+.*/i, "");
  return out.trim() || title;
}

// ------------------------------------------- lyrics from a user URL
const BOILERPLATE_MARKERS = [
  "advertisement", "cookie", "subscribe", "newsletter", "share this",
  "lyrics provided by", "all rights reserved", "copyright",
  "read more", "you may also like", "follow us",
];

export function decodeHtmlEntities(text) {
  return text
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCodePoint(parseInt(n, 16)));
}

export function htmlFragmentToLines(fragment) {
  let text = fragment.replace(/<(br|\/p|\/div|\/li|\/h\d)\b[^>]*>/gi, "\n");
  text = text.replace(/<[^>]+>/g, "");
  text = decodeHtmlEntities(text);
  const lines = [];
  for (const raw of text.split("\n")) {
    const line = raw.replace(/\s+/g, " ").trim();
    if (!line) continue;
    if (BOILERPLATE_MARKERS.some((m) => line.toLowerCase().includes(m))) continue;
    if (line.length > 300) continue;
    lines.push(line);
  }
  return lines;
}

const VOID_TAGS = new Set(["br", "hr", "img", "input", "link", "meta", "wbr"]);

// Stack-based lyric-div parser: a regex like <div>(.*?)</div> stops at
// the first inner </div> and truncates the lyrics. This one only closes
// a container when its own opening tag closes, however deep the nesting.
export class LyricsDivParser {
  constructor() { this.containers = []; this.buf = null; this.stack = []; }
  isLyricsTag(tag, attrStr) {
    if (tag === "div" && /\bdata-lyrics-container\b/i.test(attrStr)) return true;
    const m = attrStr.match(/class\s*=\s*["']([^"']*)["']/i);
    return !!m && /lyrics/i.test(m[1]);
  }
  handleStart(tag, attrStr) {
    if (this.buf !== null) {
      if (!VOID_TAGS.has(tag)) this.stack.push(tag);
      return;
    }
    if ((tag === "div" || tag === "section") && this.isLyricsTag(tag, attrStr)) {
      this.buf = []; this.stack = [tag];
    }
  }
  handleEnd(tag) {
    if (this.buf === null) return;
    if (tag === "p" || tag === "li" || tag === "div") this.buf.push("\n");
    const idx = this.stack.lastIndexOf(tag);
    if (idx >= 0) {
      this.stack.splice(idx);
      if (this.stack.length === 0) { this.containers.push(this.buf.join("")); this.buf = null; }
    }
  }
  feed(html) {
    const re = /<[^>]*>|[^<]+/g;
    let m;
    while ((m = re.exec(html)) !== null) {
      const tok = m[0];
      if (tok.startsWith("<")) {
        const tagMatch = tok.match(/^<\/?\s*([a-zA-Z0-9]+)/);
        if (!tagMatch) continue;
        const tag = tagMatch[1].toLowerCase();
        if (/^<\/\s*/.test(tok)) this.handleEnd(tag);
        else if (tag === "br" && this.buf !== null) this.buf.push("\n");
        else if (!/\/\s*>$/.test(tok)) this.handleStart(tag, tok);
      } else if (this.buf !== null) {
        this.buf.push(decodeHtmlEntities(tok));
      }
    }
  }
}

export function extractLyricsFromHtml(pageHtml) {
  if (!pageHtml) return null;
  let cleaned = pageHtml.replace(/<!--[\s\S]*?-->/g, "");
  cleaned = cleaned.replace(/<(script|style|head|nav|footer)[^>]*>[\s\S]*?<\/\1>/gi, "");
  const parser = new LyricsDivParser();
  try { parser.feed(cleaned); } catch { parser.containers = []; }
  for (const container of parser.containers) {
    const lines = cleanLyricLines(container);
    if (lines.length >= 4 && lines.reduce((s, l) => s + l.length, 0) >= 80) return lines;
  }
  const lines = htmlFragmentToLines(cleaned);
  if (lines.length >= 6 && lines.reduce((s, l) => s + l.length, 0) >= 150) {
    return cleanLyricLines(lines.join("\n"));
  }
  return null;
}

export function normalizeLyricsUrl(url) {
  if (!url) return url;
  return url.replace(/(musixmatch\.com\/lyrics\/[^/]+\/[^/]+)\/translation\b.*/i, "$1");
}

// ------------------------------------------------------------- HTTP
export const BROWSER_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9,fa;q=0.8",
};

async function httpGetJson(url) {
  const res = await fetch(url, { headers: { "User-Agent": "LyricsPrivateBot/1.0" } });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function httpGetText(url, maxBytes = 2_000_000) {
  const headers = { ...BROWSER_HEADERS };
  try {
    const parsed = new URL(url);
    headers.Referer = `${parsed.protocol}//${parsed.host}/`;
  } catch { /* keep headers as-is for a malformed URL; fetch will fail below */ }
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const text = await res.text();
  if (text.length > maxBytes) throw new Error("Page too large");
  return text;
}

async function httpPostForm(url, payload) {
  const body = new URLSearchParams(payload);
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  return res.json();
}

async function searchLyricsLrclib(artist, title) {
  const titles = [];
  for (const cand of [title, normalizeTitle(title)]) {
    if (cand && !titles.includes(cand)) titles.push(cand);
  }
  const queries = [];
  for (const cand of titles) {
    if (artist) {
      queries.push(`https://lrclib.net/api/search?artist_name=${encodeURIComponent(artist)}&track_name=${encodeURIComponent(cand)}`);
    }
    queries.push(`https://lrclib.net/api/search?q=${encodeURIComponent(`${artist || ""} ${cand}`.trim())}`);
  }
  for (const url of queries) {
    let results;
    try { results = await httpGetJson(url); } catch { continue; }
    const plain = pickBestLyrics(results, artist, title);
    if (plain) {
      const lines = cleanLyricLines(plain);
      if (lines.length) return lines;
    }
  }
  return null;
}

async function searchLyricsOvh(artist, title) {
  if (!artist || !title) return null;
  for (const cand of [title, normalizeTitle(title)]) {
    if (!cand) continue;
    try {
      const data = await httpGetJson(`https://api.lyrics.ovh/v1/${encodeURIComponent(artist)}/${encodeURIComponent(cand)}`);
      const lines = cleanLyricLines(data.lyrics || "");
      if (lines.length) return lines;
    } catch { continue; }
  }
  return null;
}

export async function searchLyrics(artist, title) {
  const fromLrclib = await searchLyricsLrclib(artist, title);
  if (fromLrclib) return fromLrclib;
  const fromOvh = await searchLyricsOvh(artist, title);
  if (fromOvh) return fromOvh;
  throw new LyricsNotFound(`No lyrics found for ${artist} - ${title}`);
}

export async function extractLyricsFromUrl(url) {
  const fetchUrl = normalizeLyricsUrl(url);
  let page;
  try {
    page = await httpGetText(fetchUrl);
  } catch (err) {
    // A fetch failure is a LyricsNotFound, so the caller re-offers the
    // paste / new-link buttons instead of ending the job with a
    // dead-end "an error occurred" message (the Musixmatch 403 bug).
    throw new LyricsNotFound(`Lyrics page could not be fetched: ${err.message}`);
  }
  const lines = extractLyricsFromHtml(page);
  if (!lines) throw new LyricsNotFound(`No lyrics extracted from ${fetchUrl}`);
  return lines;
}

async function translateOneGoogle(text) {
  const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=fa&dt=t&q=${encodeURIComponent(text)}`;
  const data = await httpGetJson(url);
  const parts = [];
  for (const segment of data[0] || []) {
    if (segment && segment[0]) parts.push(segment[0]);
  }
  const out = parts.join("").trim();
  if (!out) throw new TranslationFailed("Empty Google translation");
  return out;
}

export function detectSourceLang(lines) {
  const text = typeof lines === "string" ? lines : (lines || []).join(" ");
  if (!text.trim()) return "en";
  if (/[پچگژ]/.test(text)) return "fa";
  const letters = [...text].filter((ch) => /\p{L}/u.test(ch));
  const countRange = (lo, hi) => letters.filter((ch) => ch >= lo && ch <= hi).length;
  if (letters.length && countRange("\u0600", "\u06ff") / letters.length > 0.3) return "ar";
  if (letters.length && countRange("\u0400", "\u04ff") / letters.length > 0.3) return "ru";
  if (/[ğĞşŞİı]/.test(text)) return "tr";
  const words = new Set((text.toLowerCase().match(/[a-zçğıöşü]+/g) || []));
  const turkishWords = new Set(["aşk", "sen", "ben", "değil", "ve", "bir", "çok", "güzel", "yaparım", "bilirsin", "sev", "olurum", "bulurum", "yakarım"]);
  if ([...words].filter((w) => turkishWords.has(w)).length >= 2) return "tr";
  const vocab = {
    es: ["el", "la", "que", "los", "una", "amor", "corazón"],
    fr: ["les", "des", "une", "avec", "amour", "coeur"],
    de: ["und", "die", "der", "ich", "liebe", "herz"],
    it: ["che", "di", "una", "con", "amore", "cuore"],
    pt: ["que", "uma", "com", "amor", "coração"],
  };
  for (const [code, list] of Object.entries(vocab)) {
    if (list.filter((w) => words.has(w)).length >= 2) return code;
  }
  return "en";
}

async function translateOneMyMemory(text, sourceLang = "en", mymemoryEmail = null) {
  const src = (sourceLang || "en").trim().toLowerCase();
  const safeSrc = src === "auto" || !src ? "en" : src;
  const emailParam = mymemoryEmail ? `&de=${encodeURIComponent(mymemoryEmail)}` : "";
  const data = await httpGetJson(
    `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text)}&langpair=${encodeURIComponent(`${safeSrc}|fa`)}${emailParam}`);
  if (String(data.responseStatus) !== "200") {
    throw new TranslationFailed(`MyMemory failed: status ${data.responseStatus}`);
  }
  const out = ((data.responseData || {}).translatedText || "").trim();
  const badMarkers = ["MYMEMORY WARNING", "QUERY LENGTH", "INVALID SOURCE", "INVALID TARGET", "IS AN INVALID"];
  if (!out || badMarkers.some((m) => out.toUpperCase().includes(m))) {
    throw new TranslationFailed("MyMemory failed");
  }
  return out;
}

// Lines with no letter or digit (punctuation, dashes, "...") are not
// translated at all: translators may return empty text for them, which
// previously turned a whole song into a TranslationFailed.
export function isNonTranslatableLine(line) {
  return !/[\p{L}\p{N}]/u.test(line || "");
}

function truncateForError(text, max = 80) {
  const s = String(text ?? "");
  return s.length > max ? s.slice(0, max) + "…" : s;
}

// Try fn, wait `delayMs`, try once more. Returns the result, or throws
// the LAST error so the caller reports the most recent failure.
async function withOneRetry(fn, delayMs = 1000) {
  try {
    return await fn();
  } catch (firstErr) {
    await sleep(delayMs);
    return await fn();
  }
}

export async function translateLines(lines, opts = {}) {
  if (!lines || !lines.length) return [];
  const sourceLang = detectSourceLang(lines);
  if (sourceLang === "fa") return [...lines];
  const out = [];
  for (let start = 0; start < lines.length; start += 20) {
    const chunk = lines.slice(start, start + 20);
    let batched = false;
    try {
      const batch = await withOneRetry(() => translateOneGoogle(chunk.join("\n")), 1000);
      const batchLines = batch.split("\n").map((l) => l.trim()).filter(Boolean);
      if (batchLines.length === chunk.length) { out.push(...batchLines); batched = true; }
    } catch { /* fall through to line-by-line */ }
    if (batched) continue;
    for (const line of chunk) {
      if (isNonTranslatableLine(line)) { out.push(line); continue; }
      let fa = null;
      let googleErr = null;
      try { fa = await withOneRetry(() => translateOneGoogle(line), 800); }
      catch (err) { googleErr = err; }
      if (fa === null) {
        let mymemoryErr = null;
        try { fa = await withOneRetry(() => translateOneMyMemory(line, sourceLang, opts.mymemoryEmail), 800); }
        catch (err) { mymemoryErr = err; }
        if (fa === null) {
          throw new TranslationFailed(
            `Translation failed for line: ${truncateForError(line)} ` +
            `(Google: ${truncateForError(googleErr && googleErr.message)}; ` +
            `MyMemory: ${truncateForError(mymemoryErr && mymemoryErr.message)})`);
        }
      }
      out.push(fa);
      await sleep(150);
    }
  }
  return out;
}

export async function makePairsFromLines(lines, opts = {}) {
  const lang = detectSourceLang(lines);
  const persian = await translateLines(lines, opts);
  return { pairs: lines.map((line, i) => [line, persian[i]]), lang };
}

async function getTelegraphToken(kv) {
  const existing = await kv.get("meta:telegraph_token");
  if (existing) return existing;
  const result = await httpPostForm("https://api.telegra.ph/createAccount", {
    short_name: "LyricsPrivate", author_name: "Lyrics Bot",
  });
  if (!result.ok) throw new Error(`Telegraph createAccount failed: ${JSON.stringify(result)}`);
  const token = result.result.access_token;
  await kv.put("meta:telegraph_token", token);
  return token;
}

async function telegraphCreatePage(token, title, pairs) {
  const result = await httpPostForm("https://api.telegra.ph/createPage", {
    access_token: token,
    title,
    author_name: "Lyrics Bot",
    content: JSON.stringify(buildTelegraphContent(pairs)),
    return_content: "false",
  });
  if (!result.ok) throw new Error(`Telegraph createPage failed: ${JSON.stringify(result)}`);
  return result.result.url;
}

// ----------------------------------------------------------------- KV store
// One document per user: { jobs, queue, state, processingJobId, processingAt }
export function emptyUserDoc() {
  return { jobs: [], queue: [], state: null, processingJobId: null, processingAt: null };
}

export async function loadUserDoc(kv, userId) {
  const raw = await kv.get(`u:${userId}`);
  if (!raw) return emptyUserDoc();
  try {
    const doc = JSON.parse(raw);
    return { ...emptyUserDoc(), ...doc, jobs: doc.jobs || [], queue: doc.queue || [] };
  } catch {
    return emptyUserDoc();
  }
}

export async function saveUserDoc(kv, userId, doc) {
  await kv.put(`u:${userId}`, JSON.stringify(doc));
}

export function newJobId() {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => (b % 36).toString(36)).join("");
}

export function createJob(doc, userId, chatId, artist, title) {
  const job = {
    id: newJobId(), userId, chatId, artist, title,
    outputMode: null, status: "awaiting_output",
    telegraphUrl: null, lyricsUrl: null, providedLyrics: null,
    resultPairs: null, error: null,
    createdAt: nowIso(), updatedAt: nowIso(),
  };
  doc.jobs.push(job);
  if (doc.jobs.length > 20) doc.jobs = doc.jobs.slice(-20);
  return job;
}

export function findJob(doc, jobId) {
  return doc.jobs.find((j) => j.id === jobId) || null;
}

export function countActiveJobs(doc) {
  const active = new Set(["awaiting_output", "queued", "processing", "need_lyrics", "waiting_paste", "waiting_url"]);
  return doc.jobs.filter((j) => active.has(j.status)).length;
}

export function displayPosition(doc, jobId) {
  const idx = doc.queue.indexOf(jobId);
  const pos = idx >= 0 ? idx + 1 : null;
  const isBusy = !!doc.processingJobId;
  if (isBusy || (pos && pos > 1)) {
    return { waiting: true, wait: (pos || 1) + (isBusy ? 1 : 0) };
  }
  return { waiting: false, wait: 1 };
}

export function allowedUserIds(env) {
  const raw = (env.ALLOWED_USER_IDS || "").trim();
  if (!raw) return null;
  const ids = new Set();
  for (const part of raw.split(",")) {
    const n = Number(part.trim());
    if (Number.isFinite(n) && part.trim() !== "") ids.add(n);
  }
  return ids.size ? ids : null;
}

export function isAllowed(env, user) {
  const allowed = allowedUserIds(env);
  if (!allowed) return true;
  return !!user && allowed.has(user.id);
}

// ---------------------------------------------------------- Telegram API
export function makeTg(token) {
  const call = async (method, payload) => {
    const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!data.ok) throw new Error(`Telegram ${method} failed: ${JSON.stringify(data)}`);
    return data.result;
  };
  return {
    sendMessage: (chatId, text, replyMarkup) =>
      call("sendMessage", { chat_id: chatId, text, ...(replyMarkup ? { reply_markup: replyMarkup } : {}) }),
    editMessageText: (chatId, messageId, text, replyMarkup) =>
      call("editMessageText", { chat_id: chatId, message_id: messageId, text, ...(replyMarkup ? { reply_markup: replyMarkup } : {}) }),
    answerCallbackQuery: (callbackQueryId) =>
      call("answerCallbackQuery", { callback_query_id: callbackQueryId }),
  };
}

function mainMenu() {
  return {
    inline_keyboard: [
      [{ text: "✍️ تایپ اسم آهنگ و خواننده", callback_data: "manual_start" }],
      [
        { text: "📋 صف من", callback_data: "show_queue" },
        { text: "❓ راهنما", callback_data: "show_help" },
      ],
    ],
  };
}

export const HELP_TEXT = (
  "🎵 راهنمای سریع\n\n"
  + "۱) فایل آهنگ (mp3 و…) را همین‌جا بفرست — حتی اگر از جای دیگری "
  + "فوروارد شده باشد، فرقی نمی‌کند.\n"
  + "۲) یا دکمه‌ی «✍️ تایپ اسم آهنگ» را بزن و اول نام خواننده، بعد نام "
  + "آهنگ را بنویس.\n"
  + "۳) قبل از هر کاری، با دکمه انتخاب کن خروجی «تلگراف» باشد یا "
  + "«پیام متنی» در همین چت.\n"
  + "۴) اگر چند آهنگ بفرستی، پشت سر هم در صف می‌مانند و یکی‌یکی "
  + "پردازش می‌شوند تا بات هنگ نکند. با /queue صف خودت را ببین.\n"
  + "۵) اگر متن آهنگ پیدا نشد، بات از تو می‌خواهد متن را خودت بفرستی "
  + "یا لینک صفحه‌ی متن را بدهی.\n\n"
  + "دستورها:\n"
  + "/new — شروع آهنگ جدید (تایپ دستی)\n"
  + "/queue — صف و وضعیت آهنگ‌های تو\n"
  + "/history — آهنگ‌های تمام‌شده‌ی تو\n"
  + "/cancel — لغو همه‌ی آهنگ‌های در صف تو\n"
  + "/help — همین راهنما"
);

const STATUS_FA = {
  awaiting_output: "⏸ در انتظار انتخاب خروجی",
  queued: "⏳ در صف",
  processing: "▶️ در حال پردازش",
  need_lyrics: "⚠️ در انتظار متن از تو",
  waiting_paste: "✍️ در انتظار متن از تو",
  waiting_url: "🔗 در انتظار لینک از تو",
  done: "✅ تمام شده",
  failed: "❌ ناموفق",
  cancelled: "🚫 لغو شده",
};

// ---------------------------------------------------------- processing
async function deliverResult(kv, tg, doc, job, pairs, lang) {
  const langName = LANG_NAMES[lang] || "زبان اصلی";
  const header = songHeader(job.artist, job.title);
  if (job.outputMode === "text") {
    const full = buildTextOutput(job.artist, job.title, pairs);
    const chunks = splitTextMessages(full);
    for (let i = 0; i < chunks.length; i++) {
      await tg.sendMessage(job.chatId, chunks[i]);
      if (i < chunks.length - 1) await sleep(400);
    }
    await tg.sendMessage(
      job.chatId,
      `تمام شد ✅\n${header}\nزبان آهنگ: ${langName} — ترجمه به فارسی\nمتن در ${chunks.length} پیام بالا فرستاده شد.`,
      {
        inline_keyboard: [
          [{ text: "📜 همین را به صورت تلگراف هم بساز", callback_data: `also_telegraph:${job.id}` }],
          [{ text: "🎵 آهنگ جدید", callback_data: "new_song" }],
        ],
      });
  } else {
    const token = await getTelegraphToken(kv);
    const pageTitle = job.artist ? `${job.title} - ${job.artist}` : job.title;
    const url = await telegraphCreatePage(token, pageTitle, pairs);
    job.telegraphUrl = url;
    await tg.sendMessage(
      job.chatId,
      `تمام شد ✅\n${header}\nزبان آهنگ: ${langName} — ترجمه به فارسی\n📜 متن و ترجمه:\n${url}`,
      {
        inline_keyboard: [
          [{ text: "💬 متن کامل را همین‌جا بفرست", callback_data: `also_text:${job.id}` }],
          [{ text: "🎵 آهنگ جدید", callback_data: "new_song" }],
        ],
      });
  }
}

async function askLyricsFallback(tg, job, reason = null) {
  const head = reason
    ? `⚠️ این لینک نشد.\n${songHeader(job.artist, job.title)}\n\nدلیل: ${reason}\n\n`
    : `⚠️ متن آهنگ پیدا نشد.\n${songHeader(job.artist, job.title)}\n\nدر چند سایت رایگان گشتم و نبود. `;
  await tg.sendMessage(
    job.chatId,
    head
    + "نگران نباش، دو راه داری:\n"
    + "• دکمه‌ی اول: متن آهنگ را خودت اینجا کپی و پیست کن تا ترجمه‌اش کنم.\n"
    + "• دکمه‌ی دوم: لینک دیگری از صفحه‌ای که متن آهنگ در آن است را "
    + "بفرست تا متن را از همان صفحه بردارم. (اگر سایتی به بات اجازه "
    + "نداد، لینک سایت دیگری یا متن دستی جواب می‌دهد.)\n\n"
    + "خروجی نهایی همان چیزی می‌شود که اول انتخاب کردی.",
    {
      inline_keyboard: [
        [{ text: "✍️ متن را خودم می‌فرستم", callback_data: `paste:${job.id}` }],
        [{ text: "🔗 لینک صفحه‌ی متن را می‌فرستم", callback_data: `link:${job.id}` }],
        [{ text: "❌ لغو این آهنگ", callback_data: `cancel:${job.id}` }],
      ],
    });
}

export async function processJob(kv, tg, doc, job, userId, env = {}) {
  job.status = "processing";
  job.updatedAt = nowIso();
  await saveUserDoc(kv, userId, doc);
  try {
    let lines;
    if (job.providedLyrics) lines = job.providedLyrics;
    else if (job.lyricsUrl) lines = await extractLyricsFromUrl(job.lyricsUrl);
    else lines = await searchLyrics(job.artist, job.title);
    const { pairs, lang } = await makePairsFromLines(lines, { mymemoryEmail: env.MYMEMORY_EMAIL || null });
    job.status = "done";
    job.resultPairs = pairs;
    job.updatedAt = nowIso();
    await saveUserDoc(kv, userId, doc);
    await deliverResult(kv, tg, doc, job, pairs, lang);
    await saveUserDoc(kv, userId, doc);
  } catch (err) {
    if (err instanceof LyricsNotFound) {
      const failedUrl = job.lyricsUrl;
      job.lyricsUrl = null;
      job.status = "need_lyrics";
      job.updatedAt = nowIso();
      await saveUserDoc(kv, userId, doc);
      let reason = null;
      if (failedUrl) {
        const m = /HTTP (\d+)/.exec(err.message);
        reason = m
          ? `سایت خطای ${m[1]} داد؛ یعنی به درخواست بات اجازه نداد.`
          : "صفحه‌ی لینک باز نشد یا متنی در آن پیدا نشد.";
      }
      await askLyricsFallback(tg, job, reason);
      return;
    }
    job.status = "failed";
    job.error = err.message || String(err);
    job.updatedAt = nowIso();
    await saveUserDoc(kv, userId, doc);
    if (err instanceof TranslationFailed) {
      const detail = truncateForError(job.error, 300);
      await tg.sendMessage(job.chatId, `⚠️ ترجمه برای ${songHeader(job.artist, job.title)} انجام نشد.\nعلت: ${detail}\nدوباره تلاش کن یا /queue را ببین.`);
    } else {
      await tg.sendMessage(job.chatId, `⚠️ خطایی برای ${songHeader(job.artist, job.title)} پیش آمد: ${job.error}`);
    }
  }
}

export async function processQueue(kv, tg, userId, env = {}) {
  let doc = await loadUserDoc(kv, userId);
  if (doc.processingJobId) {
    const age = doc.processingAt ? Date.now() - doc.processingAt : Infinity;
    if (age < STALE_PROCESSING_MS) return; // another loop is running
    // Stale loop (killed invocation): requeue its job at the front.
    const stale = findJob(doc, doc.processingJobId);
    if (stale && stale.status === "processing") {
      stale.status = "queued";
      doc.queue.unshift(stale.id);
    }
    doc.processingJobId = null;
    doc.processingAt = null;
    await saveUserDoc(kv, userId, doc);
  }
  while (doc.queue.length) {
    const jobId = doc.queue.shift();
    const job = findJob(doc, jobId);
    if (!job || job.status === "cancelled" || job.status === "done") {
      await saveUserDoc(kv, userId, doc);
      continue;
    }
    doc.processingJobId = jobId;
    doc.processingAt = Date.now();
    await saveUserDoc(kv, userId, doc);
    try {
      await processJob(kv, tg, doc, job, userId, env);
    } catch (err) {
      // processJob handles its own errors; this is only a last resort
      // so one bad job never blocks the rest of the queue.
      job.status = "failed";
      job.error = err.message || String(err);
    }
    doc.processingJobId = null;
    doc.processingAt = null;
    await saveUserDoc(kv, userId, doc);
    // Reload: a webhook that arrived during processing may have
    // appended jobs to the stored document.
    doc = await loadUserDoc(kv, userId);
  }
}

// ---------------------------------------------------------- update handling
async function askOutputChoice(tg, chatId, job) {
  await tg.sendMessage(
    chatId,
    `آهنگ ثبت شد:\n${songHeader(job.artist, job.title)}\n\n`
    + "خروجی را چطور می‌خواهی؟\n"
    + "• تلگراف: یک لینک تمیز می‌سازی که می‌توانی برای دیگران هم بفرستی.\n"
    + "• پیام متنی: متن و ترجمه همین‌جا، در چند پیام، فرستاده می‌شود.\n"
    + "بعد از انتخاب تو، پردازش شروع می‌شود.",
    {
      inline_keyboard: [
        [{ text: "📜 تلگراف (لینک قابل اشتراک)", callback_data: `out_telegraph:${job.id}` }],
        [{ text: "💬 پیام متنی در همین چت", callback_data: `out_text:${job.id}` }],
        [{ text: "❌ لغو", callback_data: `cancel:${job.id}` }],
      ],
    });
}

async function enqueueOrTell(kv, tg, doc, job, userId, front = false) {
  if (countActiveJobs(doc) > MAX_QUEUE_PER_USER && !front) {
    job.status = "cancelled";
    await saveUserDoc(kv, userId, doc);
    await tg.sendMessage(job.chatId, "صف تو پر است (حداکثر ۱۰ آهنگ در انتظار). بعد از تمام شدن قبلی‌ها دوباره بفرست. /queue را ببین.");
    return false;
  }
  job.status = "queued";
  if (front) doc.queue.unshift(job.id);
  else doc.queue.push(job.id);
  await saveUserDoc(kv, userId, doc);
  const { waiting, wait } = displayPosition(doc, job.id);
  if (waiting) {
    await tg.sendMessage(job.chatId, `⏳ در صف قرار گرفت (نفر ${wait} در صف تو).\n${songHeader(job.artist, job.title)}\nبه‌محض تمام شدن آهنگ قبلی، همین آهنگ شروع می‌شود.`);
  } else {
    await tg.sendMessage(job.chatId, `▶️ پردازش شروع شد:\n${songHeader(job.artist, job.title)}`);
  }
  return true;
}

function queueText(doc, userId) {
  const jobs = [...doc.jobs].reverse().slice(0, 10);
  if (!jobs.length) return null;
  const lines = ["📋 وضعیت آهنگ‌های تو:\n"];
  const buttons = [];
  for (const job of jobs) {
    lines.push(`• ${songHeader(job.artist, job.title)} — ${STATUS_FA[job.status] || job.status}`);
    if (job.telegraphUrl) lines.push(`  ${job.telegraphUrl}`);
    if (["queued", "awaiting_output", "need_lyrics"].includes(job.status)) {
      buttons.push([{ text: `❌ لغو: ${job.title}`, callback_data: `cancel:${job.id}` }]);
    }
  }
  return { text: lines.join("\n"), buttons: buttons.length ? { inline_keyboard: buttons } : null };
}

export async function handleUpdate(env, update, tg, kv, schedule) {
  const runInBackground = (promise) => {
    if (schedule) schedule(promise.catch(() => {}));
    else promise.catch(() => {});
  };

  if (update.callback_query) {
    const query = update.callback_query;
    const user = query.from;
    const chatId = query.message ? query.message.chat.id : user.id;
    const messageId = query.message ? query.message.message_id : null;
    if (!isAllowed(env, user)) {
      await tg.answerCallbackQuery(query.id);
      return;
    }
    await tg.answerCallbackQuery(query.id);
    const data = query.data || "";
    const edit = (text, markup) => messageId
      ? tg.editMessageText(chatId, messageId, text, markup)
      : tg.sendMessage(chatId, text, markup);

    if (data === "manual_start" || data === "new_song") {
      const doc = await loadUserDoc(kv, user.id);
      doc.state = { state: "awaiting_singer", jobId: null, draftArtist: null, draftText: null };
      await saveUserDoc(kv, user.id, doc);
      await edit("✍️ نام خواننده را بنویس.\nاگر خواننده را نمی‌دانی، بنویس «نمی‌دانم» یا «-».");
      return;
    }
    if (data === "show_help") { await edit(HELP_TEXT, mainMenu()); return; }
    if (data === "show_queue") { await edit("برای دیدن صف، /queue را بزن."); return; }

    const jobId = data.includes(":") ? data.split(":")[1] : null;
    if (!jobId) return;
    const doc = await loadUserDoc(kv, user.id);
    const job = findJob(doc, jobId);
    if (!job || job.userId !== user.id) { await edit("این آهنگ پیدا نشد."); return; }

    if (data.startsWith("out_")) {
      if (job.status !== "awaiting_output") { await edit("این انتخاب دیگر معتبر نیست."); return; }
      job.outputMode = data.startsWith("out_telegraph") ? "telegraph" : "text";
      const modeFa = job.outputMode === "telegraph" ? "تلگراف 📜" : "پیام متنی 💬";
      await edit(`انتخاب شد: ${modeFa}\n${songHeader(job.artist, job.title)}`);
      if (await enqueueOrTell(kv, tg, doc, job, user.id)) {
        runInBackground(processQueue(kv, tg, user.id, env));
      }
      return;
    }
    if (data.startsWith("cancel:")) {
      doc.queue = doc.queue.filter((id) => id !== jobId);
      job.status = "cancelled";
      if (doc.state && doc.state.jobId === jobId) doc.state = null;
      await saveUserDoc(kv, user.id, doc);
      await edit(`🚫 لغو شد:\n${songHeader(job.artist, job.title)}`);
      return;
    }
    if (data.startsWith("paste:")) {
      job.status = "waiting_paste";
      doc.state = { state: "waiting_paste", jobId, draftArtist: null, draftText: null };
      await saveUserDoc(kv, user.id, doc);
      await edit(`✍️ متن آهنگ را اینجا کپی و پیست کن:\n${songHeader(job.artist, job.title)}\n\nمتن کامل را بفرست؛ اگر در چند پیام آمد، اشکالی ندارد — آخرش دکمه‌ی «متن کامل شد» را بزن.`);
      return;
    }
    if (data.startsWith("link:")) {
      job.status = "waiting_url";
      doc.state = { state: "waiting_url", jobId, draftArtist: null, draftText: null };
      await saveUserDoc(kv, user.id, doc);
      await edit(`🔗 لینک صفحه‌ای که متن این آهنگ در آن است را بفرست:\n${songHeader(job.artist, job.title)}\n\nمثلاً لینک صفحه‌ی آهنگ در سایتی که متن‌ها را منتشر می‌کند. بات متن را از همان صفحه برمی‌دارد؛ اگر صفحه قابل خواندن نباشد، دوباره از تو می‌خواهم متن را دستی بفرستی.`);
      return;
    }
    if (data.startsWith("finish_paste:")) {
      const lines = validatePastedLyrics(doc.state ? doc.state.draftText || "" : "");
      if (!lines) {
        await edit("متن دریافتی هنوز خیلی کوتاه است. ✍️\nلطفاً متن کامل آهنگ را بفرست، بعد دوباره دکمه را بزن.");
        return;
      }
      doc.state = null;
      job.providedLyrics = lines;
      await saveUserDoc(kv, user.id, doc);
      await edit(`متن دریافت شد (${lines.length} خط). ✅\n${songHeader(job.artist, job.title)}\nدر حال ترجمه…`);
      if (await enqueueOrTell(kv, tg, doc, job, user.id, true)) {
        runInBackground(processQueue(kv, tg, user.id, env));
      }
      return;
    }
    if (data.startsWith("also_text:") || data.startsWith("also_telegraph:")) {
      if (!job.resultPairs) { await edit("نتیجه‌ی این آهنگ دیگر در دسترس نیست."); return; }
      if (data.startsWith("also_text:")) {
        const full = buildTextOutput(job.artist, job.title, job.resultPairs);
        const chunks = splitTextMessages(full);
        for (let i = 0; i < chunks.length; i++) {
          await tg.sendMessage(chatId, chunks[i]);
          if (i < chunks.length - 1) await sleep(400);
        }
        await edit("💬 متن کامل بالا فرستاده شد.");
      } else {
        const token = await getTelegraphToken(kv);
        const pageTitle = job.artist ? `${job.title} - ${job.artist}` : job.title;
        const url = await telegraphCreatePage(token, pageTitle, job.resultPairs);
        job.telegraphUrl = url;
        await saveUserDoc(kv, user.id, doc);
        await edit(`📜 نسخه‌ی تلگراف:\n${url}`);
      }
      return;
    }
    return;
  }

  const msg = update.message;
  if (!msg) return;
  if (msg.chat.type !== "private") return;
  const user = msg.from;
  const chatId = msg.chat.id;
  if (!isAllowed(env, user)) {
    await tg.sendMessage(chatId, "این بات برای تو فعال نیست.");
    return;
  }
  const doc = await loadUserDoc(kv, user.id);
  const text = (msg.text || "").trim();

  // Commands
  if (text.startsWith("/")) {
    const cmd = text.split(/\s+/)[0].split("@")[0];
    if (cmd === "/start") {
      await tg.sendMessage(chatId, `سلام! 👋 بات متن و ترجمه‌ی آهنگ، نسخه ${VERSION}\n\nفقط همین‌جا، در چت خصوصی بات، کار می‌کند — هیچ کانالی در کار نیست.\nیک فایل آهنگ بفرست، یا با دکمه‌ی زیر اسمش را تایپ کن.\n\n${HELP_TEXT}`, mainMenu());
      return;
    }
    if (cmd === "/help") { await tg.sendMessage(chatId, HELP_TEXT, mainMenu()); return; }
    if (cmd === "/new") {
      doc.state = { state: "awaiting_singer", jobId: null, draftArtist: null, draftText: null };
      await saveUserDoc(kv, user.id, doc);
      await tg.sendMessage(chatId, "✍️ نام خواننده را بنویس.\nاگر خواننده را نمی‌دانی، بنویس «نمی‌دانم» یا «-».");
      return;
    }
    if (cmd === "/queue") {
      const q = queueText(doc, user.id);
      if (!q) await tg.sendMessage(chatId, "هنوز آهنگی نفرستاده‌ای. 🎵");
      else await tg.sendMessage(chatId, q.text, q.buttons);
      return;
    }
    if (cmd === "/history") {
      const done = [...doc.jobs].reverse().filter((j) => j.status === "done").slice(0, 8);
      if (!done.length) { await tg.sendMessage(chatId, "هنوز آهنگ تمام‌شده‌ای نداری."); return; }
      const lines = ["🕘 آهنگ‌های تمام‌شده‌ی تو:\n"];
      for (const job of done) {
        lines.push(`• ${songHeader(job.artist, job.title)}`);
        if (job.telegraphUrl) lines.push(`  ${job.telegraphUrl}`);
        else if (job.outputMode === "text") lines.push("  (به صورت پیام متنی فرستاده شد)");
      }
      await tg.sendMessage(chatId, lines.join("\n"));
      return;
    }
    if (cmd === "/cancel") {
      let cancelled = 0;
      for (const job of doc.jobs) {
        if (["queued", "awaiting_output", "need_lyrics", "waiting_paste", "waiting_url"].includes(job.status)) {
          job.status = "cancelled";
          cancelled++;
        }
      }
      doc.queue = [];
      doc.state = null;
      doc.processingJobId = null;
      doc.processingAt = null;
      await saveUserDoc(kv, user.id, doc);
      await tg.sendMessage(chatId, cancelled ? `🚫 ${cancelled} آهنگ در صف لغو شد.` : "چیزی در صف نبود که لغو شود.");
      return;
    }
    await tg.sendMessage(chatId, HELP_TEXT, mainMenu());
    return;
  }

  // Audio (direct or forwarded from anywhere): same handling.
  if (isAudioMessage(msg)) {
    const [artist, title] = extractSong(msg);
    if (!title) {
      doc.state = { state: "awaiting_singer", jobId: null, draftArtist: null, draftText: null };
      await saveUserDoc(kv, user.id, doc);
      await tg.sendMessage(chatId, "اسم این آهنگ از فایل پیدا نشد. 🧐\nاول نام خواننده را بنویس (یا «نمی‌دانم»)، بعد نام آهنگ را.");
      return;
    }
    if (countActiveJobs(doc) >= MAX_QUEUE_PER_USER) {
      await tg.sendMessage(chatId, "صف تو پر است (حداکثر ۱۰ آهنگ در انتظار). بعد از تمام شدن قبلی‌ها دوباره بفرست. /queue را ببین.");
      return;
    }
    const job = createJob(doc, user.id, chatId, artist, title);
    await saveUserDoc(kv, user.id, doc);
    await askOutputChoice(tg, chatId, job);
    return;
  }

  if (!text) {
    await tg.sendMessage(chatId, "یک فایل آهنگ بفرست، یا /new را بزن و اسم آهنگ را تایپ کن.", mainMenu());
    return;
  }

  const state = doc.state ? doc.state.state : "idle";
  const stateJobId = doc.state ? doc.state.jobId : null;

  if (state === "awaiting_singer") {
    const singer = ["-", "نمی‌دانم", "نمیدانم", "نمی دانم"].includes(text) ? null : text;
    doc.state = { state: "awaiting_title", jobId: null, draftArtist: singer, draftText: null };
    await saveUserDoc(kv, user.id, doc);
    await tg.sendMessage(chatId, `حالا نام آهنگ را بنویس.\nخواننده: ${singer || "—"}`);
    return;
  }
  if (state === "awaiting_title") {
    const job = createJob(doc, user.id, chatId, doc.state.draftArtist, text);
    doc.state = null;
    await saveUserDoc(kv, user.id, doc);
    await askOutputChoice(tg, chatId, job);
    return;
  }
  if (state === "waiting_paste" && stateJobId) {
    const job = findJob(doc, stateJobId);
    if (!job || job.status === "cancelled") {
      doc.state = null;
      await saveUserDoc(kv, user.id, doc);
      await tg.sendMessage(chatId, "این آهنگ لغو شده است.");
      return;
    }
    const combined = ((doc.state.draftText || "") + "\n" + text).trim();
    const lines = validatePastedLyrics(combined);
    doc.state.draftText = combined;
    await saveUserDoc(kv, user.id, doc);
    await tg.sendMessage(
      chatId,
      `${lines ? "دریافت شد. ✅\n" : "دریافت شد، ولی هنوز برای یک متن آهنگ کوتاه است — ادامه‌ی متن را هم بفرست.\n"}`
      + "اگر متن تمام شده، دکمه‌ی زیر را بزن؛ اگر ادامه دارد، پیام بعدی را بفرست.",
      {
        inline_keyboard: [
          [{ text: "✅ متن کامل شد، ترجمه کن", callback_data: `finish_paste:${stateJobId}` }],
          [{ text: "❌ لغو", callback_data: `cancel:${stateJobId}` }],
        ],
      });
    return;
  }
  if (state === "waiting_url" && stateJobId) {
    const match = text.match(/https?:\/\/\S+/);
    if (!match) {
      await tg.sendMessage(chatId, "این یک لینک به نظر نمی‌رسد. 🔗\nلینک کامل صفحه‌ای که متن آهنگ در آن است را بفرست، مثلاً لینک صفحه‌ی آهنگ در یک سایت متن آهنگ.");
      return;
    }
    const url = match[0].replace(/[).,]+$/, "");
    const job = findJob(doc, stateJobId);
    if (!job || job.status === "cancelled") {
      doc.state = null;
      await saveUserDoc(kv, user.id, doc);
      await tg.sendMessage(chatId, "این آهنگ لغو شده است.");
      return;
    }
    doc.state = null;
    job.lyricsUrl = url;
    await saveUserDoc(kv, user.id, doc);
    await tg.sendMessage(chatId, "لینک دریافت شد. 🔗\nمتن را از همان صفحه برمی‌دارم و بعد ترجمه می‌کنم.");
    if (await enqueueOrTell(kv, tg, doc, job, user.id, true)) {
      runInBackground(processQueue(kv, tg, user.id, env));
    }
    return;
  }

  // Stale-processing recovery on any new message from this user.
  if (doc.processingJobId && doc.processingAt && Date.now() - doc.processingAt >= STALE_PROCESSING_MS) {
    runInBackground(processQueue(kv, tg, user.id, env));
  }

  // Plain text that looks like a song name: treat as manual entry.
  const [artist, title] = parseManualInput(text);
  if (title) {
    if (countActiveJobs(doc) >= MAX_QUEUE_PER_USER) {
      await tg.sendMessage(chatId, "صف تو پر است (حداکثر ۱۰ آهنگ در انتظار). /queue را ببین.");
      return;
    }
    const job = createJob(doc, user.id, chatId, artist, title);
    await saveUserDoc(kv, user.id, doc);
    await askOutputChoice(tg, chatId, job);
    return;
  }

  await tg.sendMessage(
    chatId,
    "متوجه نشدم. 🤔\nیک فایل آهنگ بفرست، یا اسم آهنگ را این‌طور بنویس:\n«خواننده - نام آهنگ»\nیا /new را بزن تا قدم‌به‌قدم از تو بپرسم.",
    mainMenu());
}

// ---------------------------------------------------------- Worker entry
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "GET" && (url.pathname === "/" || url.pathname === "")) {
      return new Response(`Lyrics private bot worker ${VERSION} OK`, { status: 200 });
    }
    if (request.method !== "POST" || url.pathname !== "/webhook") {
      return new Response("Not found", { status: 404 });
    }
    if (env.WEBHOOK_SECRET) {
      const secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token");
      if (secret !== env.WEBHOOK_SECRET) return new Response("Forbidden", { status: 403 });
    }
    if (!env.TELEGRAM_BOT_TOKEN) return new Response("Bot token not configured", { status: 500 });
    if (!env.LYRICS_KV) return new Response("KV binding LYRICS_KV not configured", { status: 500 });
    let update;
    try {
      update = await request.json();
    } catch {
      return new Response("Bad request", { status: 400 });
    }
    const tg = makeTg(env.TELEGRAM_BOT_TOKEN);
    try {
      await handleUpdate(env, update, tg, env.LYRICS_KV, (p) => ctx.waitUntil(p));
    } catch (err) {
      // Never let one bad update return an error to Telegram: Telegram
      // would retry it and could duplicate work. Log and answer 200.
      console.error("handleUpdate failed:", err);
    }
    return new Response("OK", { status: 200 });
  },

  // Optional cron trigger (e.g. every 10 minutes): restart at most one
  // stale user queue per run, so a killed processing loop does not
  // leave a song stuck forever. Keep it to one user per run to stay
  // well inside the free subrequest limit (50 per invocation).
  async scheduled(event, env, ctx) {
    if (!env.TELEGRAM_BOT_TOKEN || !env.LYRICS_KV) return;
    const tg = makeTg(env.TELEGRAM_BOT_TOKEN);
    try {
      const listed = await env.LYRICS_KV.list({ prefix: "u:" });
      for (const key of listed.keys || []) {
        const userId = Number(key.name.slice(2));
        if (!Number.isFinite(userId)) continue;
        const doc = await loadUserDoc(env.LYRICS_KV, userId);
        const stale = doc.processingJobId && doc.processingAt && Date.now() - doc.processingAt >= STALE_PROCESSING_MS;
        const queuedIdle = doc.queue.length > 0 && !doc.processingJobId;
        if (stale || queuedIdle) {
          await processQueue(env.LYRICS_KV, tg, userId, env);
          break;
        }
      }
    } catch (err) {
      console.error("scheduled recovery failed:", err);
    }
  },
};
