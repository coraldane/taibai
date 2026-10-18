#!/usr/bin/env node
// taibai 发版一条龙：npm run release
//
//   cargo build --release（当前平台）→ 打服务端包 → 传阿里云 OSS
//   → 调 motuo-cloud admin 接口登记 → 官网/控制台即刻可见
//
// 与 motuo/baton 的发版脚本同源同链路（app=taibai），差别只在产物形态：
// 服务端包没有自动更新器，也就没有 minisign 签名与「更新清单推送」——
// latest.json / versions.json 只供官网下载面与控制台取直链。
// 打包形态沿用 scripts/build.sh 的既有口径：linux/darwin 出 tar.gz
// （二进制 + conf/taibai.yaml + install/uninstall.sh + README），windows 出
// zip（taibai.exe + conf + install/uninstall.ps1）。同一版本可在不同系统上
// 重复跑：产物按平台合并，重复跑只覆盖自己带来的平台。
//
// 版本号不在这里 bump——服务端发版走「版本号随功能提交进 Cargo.toml」的
// 惯例，脚本只打包发布 Cargo.toml 里那个版本。
//
// 用法：
//   npm run release                            # 打包+上传+上架当前平台
//   npm run release -- --notes "v0.2.7 发布说明"
//   npm run release -- --notes-file RELEASE-NOTES.md
//   npm run release -- --draft                 # 只登记不上架，控制台点「上架」才生效
//   npm run release -- --dry-run               # 只打包：不传、不登记
//   npm run release -- --skip-build            # 复用现成构建（重登记/补平台用）
//   npm run release -- --skip-upload           # 只登记（OSS 已传过 / 人工传的）
//
// 凭证 conf/release.local.json（不入库，没有它只能 --dry-run），与 motuo/baton
// 同一份格式：
// {
//   "oss": { "region": "oss-cn-hangzhou", "bucket": "pub-oss-file",
//            "accessKeyId": "...", "accessKeySecret": "..." },
//   "api": { "base": "https://api.tarzip.com", "email": "...", "password": "..." }
// }

import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { execSync, spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const log = (...a) => console.log('[release]', ...a)
const die = (msg) => {
  console.error('[release] ✗', msg)
  process.exit(1)
}

// ---- 参数 ----
const argv = process.argv.slice(2)
const argOf = (name) => {
  const i = argv.indexOf(name)
  return i >= 0 ? argv[i + 1] : undefined
}
const hasFlag = (name) => argv.includes(name)

const APP = 'taibai'
const DRY = hasFlag('--dry-run')
const SKIP_BUILD = hasFlag('--skip-build')
const SKIP_UPLOAD = hasFlag('--skip-upload')
const DRAFT = hasFlag('--draft')
let notes = argOf('--notes') || ''
const notesFile = argOf('--notes-file')
if (notesFile) notes = fs.readFileSync(path.resolve(ROOT, notesFile), 'utf8').trim()

// ---- 版本号：只读 Cargo.toml [package] version，不 bump ----
const cargoText = fs.readFileSync(path.join(ROOT, 'Cargo.toml'), 'utf8')
const m = cargoText.match(/^\[package\][\s\S]*?^version\s*=\s*"([^"]+)"/m)
if (!m) die('Cargo.toml [package] 里没翻到 version')
const version = m[1]
if (!/^\d+\.\d+\.\d+$/.test(version)) die(`版本号得是 x.y.z，收到「${version}」`)
log(`应用 ${APP}，打包版本 v${version}${DRY ? '（dry-run：不落盘）' : ''}`)

// ---- 凭证（dry-run 没有它也照跑）----
const credPath = path.join(ROOT, 'conf/release.local.json')
let cred = {}
if (fs.existsSync(credPath)) {
  cred = JSON.parse(fs.readFileSync(credPath, 'utf8'))
} else if (!DRY) {
  log(`提示：没有 ${path.relative(ROOT, credPath)}，上传/登记会失败。模板：`)
  console.log(
    JSON.stringify(
      {
        oss: { region: 'oss-cn-hangzhou', bucket: 'pub-oss-file', accessKeyId: '...', accessKeySecret: '...' },
        api: { base: 'https://api.tarzip.com', email: '...', password: '...' }
      },
      null,
      2
    )
  )
}

const run = (cmd, extraEnv = {}) => {
  log('$', cmd)
  const r = spawnSync(cmd, { shell: true, stdio: 'inherit', cwd: ROOT, env: { ...process.env, ...extraEnv } })
  if (r.status !== 0) throw new Error(`命令失败（exit ${r.status}）: ${cmd}`)
}
const md5Of = (p) => crypto.createHash('md5').update(fs.readFileSync(p)).digest('hex')

// ---- 1. 构建并打包当前平台 ----
if (!SKIP_BUILD) {
  // build.rs 吃 CARGO_PKG_VERSION + git commit + 构建时间，环境变量只是与
  // scripts/build.sh 保持同一出口
  run('cargo build --release', {
    TAIBAI_VERSION: version,
    TAIBAI_BUILD_TIME: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
    TAIBAI_GIT_COMMIT: execSync('git rev-parse --short HEAD', { cwd: ROOT, encoding: 'utf8' }).trim()
  })
}

const DIST = path.join(ROOT, 'dist')
fs.mkdirSync(DIST, { recursive: true })
let arts
if (process.platform === 'win32') {
  // windows 包：taibai.exe + conf + PowerShell 装卸脚本，zip 形态
  const exe = path.join(ROOT, 'target/release/taibai.exe')
  if (!fs.existsSync(exe)) die(`没翻到 ${exe}，先 cargo build --release 或去掉 --skip-build`)
  const pkgName = `taibai-${version}-windows-x86_64`
  const stage = path.join(ROOT, 'build', pkgName)
  fs.rmSync(stage, { recursive: true, force: true })
  fs.mkdirSync(path.join(stage, 'conf'), { recursive: true })
  fs.copyFileSync(exe, path.join(stage, 'taibai.exe'))
  fs.copyFileSync(path.join(ROOT, 'conf/example.yaml'), path.join(stage, 'conf/taibai.yaml'))
  for (const f of ['install.ps1', 'uninstall.ps1']) {
    fs.copyFileSync(path.join(ROOT, 'scripts/windows', f), path.join(stage, f))
  }
  if (fs.existsSync(path.join(ROOT, 'README.md'))) {
    fs.copyFileSync(path.join(ROOT, 'README.md'), path.join(stage, 'README.md'))
  }
  const zip = path.join(DIST, `${pkgName}.zip`)
  fs.rmSync(zip, { force: true })
  run(`powershell -NoProfile -Command "Compress-Archive -Path '${stage.replace(/'/g, "''")}' -DestinationPath '${zip.replace(/'/g, "''")}' -Force"`)
  arts = [{ path: zip, fileName: `${pkgName}.zip` }]
} else {
  // linux/darwin 包：复用 scripts/build.sh 的既有打包形态（tar.gz）
  run('bash scripts/build.sh package')
  arts = fs
    .readdirSync(DIST)
    .filter((f) => f.startsWith(`taibai-${version}-`) && f.endsWith('.tar.gz'))
    .map((f) => ({ path: path.join(DIST, f), fileName: f }))
}
if (!arts.length) die(`dist/ 里没翻到 v${version} 的包`)

// 包名段 → Tauri 平台键口径（与 motuo-cloud appReleaseApps / 控制台一致）
const PLATFORM_BY_PKG = [
  ['linux-amd64', 'linux-x86_64'],
  ['linux-arm64', 'linux-aarch64'],
  ['darwin-amd64', 'darwin-x86_64'],
  ['darwin-arm64', 'darwin-aarch64'],
  ['windows-x86_64', 'windows-x86_64']
]
const artifacts = arts.map((a) => {
  const hit = PLATFORM_BY_PKG.find(([seg]) => a.fileName.includes(seg))
  if (!hit) die(`包名 ${a.fileName} 认不出平台段（认 ${PLATFORM_BY_PKG.map(([s]) => s).join(' ')}）`)
  return {
    platform: hit[1],
    file_name: a.fileName,
    url: '', // 上传后回填
    size: fs.statSync(a.path).size,
    md5: md5Of(a.path),
    signature: '', // 服务端无更新器，不验签
    _path: a.path
  }
})
log('产物：')
for (const a of artifacts) log(`  [${a.platform}] ${a.file_name}  ${(a.size / 1024 / 1024).toFixed(1)} MB  md5:${a.md5.slice(0, 12)}…`)

// ---- 2. 传 OSS ----
const bucket = cred.oss?.bucket || 'pub-oss-file'
const region = cred.oss?.region || 'oss-cn-hangzhou'
const ossUrl = (fileName) => `https://${bucket}.${region}.aliyuncs.com/${APP}/${version}/${fileName}`
for (const a of artifacts) a.url = ossUrl(a.file_name)

if (!DRY && !SKIP_UPLOAD) {
  if (!cred.oss?.accessKeyId) die('conf/release.local.json 里缺 oss 段')
  const OSS = (await import('ali-oss')).default
  const client = new OSS({
    region,
    bucket,
    accessKeyId: cred.oss.accessKeyId,
    accessKeySecret: cred.oss.accessKeySecret
  })
  for (const a of artifacts) {
    const key = `${APP}/${version}/${a.file_name}`
    await client.put(key, a._path)
    log(`已传 OSS: ${key}`)
  }
}

// ---- 3. 登记版本（官网下载面与控制台即刻生效）----
const base = (cred.api?.base || 'https://api.tarzip.com').replace(/\/$/, '')
const apiCall = async (method, apiPath, body, token) => {
  const res = await fetch(`${base}${apiPath}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: body ? JSON.stringify(body) : undefined
  })
  const data = await res.json().catch(() => ({}))
  return { status: res.status, data }
}

if (!DRY) {
  if (!cred.api?.email) die('conf/release.local.json 里缺 api 段')
  const login = await apiCall('POST', '/api/v1/auth/login', {
    email: cred.api.email,
    password: cred.api.password
  })
  if (login.data.code !== 0) die(`API 登录失败: ${login.data.message || login.status}`)
  const token = login.data.data.access_token

  const reg = await apiCall(
    'POST',
    '/api/v1/admin/releases',
    {
      app: APP,
      version,
      notes,
      status: DRAFT ? 'draft' : 'published',
      artifacts: artifacts.map(({ platform, file_name, url, size, md5, signature }) => ({
        platform,
        file_name,
        url,
        size,
        md5,
        signature
      }))
    },
    token
  )
  if (reg.data.code !== 0) die(`登记失败: ${reg.data.message || reg.status}`)
  log(`已登记：${APP} v${version}（${DRAFT ? '草稿，控制台点「上架」生效' : '已上架生效'}）`)

  // 回读公开面核账：latest 版本对、versions 清单里有这版
  const mf = await apiCall('GET', `/api/v1/updates/${APP}/latest.json`)
  if (mf.status !== 200 || mf.data.version !== version) {
    die(`清单核账不对：期望 ${version}，拿到 ${mf.status} ${JSON.stringify(mf.data).slice(0, 200)}`)
  }
  const vs = await apiCall('GET', `/api/v1/updates/${APP}/versions.json`)
  const hit = (vs.data.releases || []).some((r) => r.version === version)
  if (vs.status !== 200 || !hit) die(`历史清单里没有 v${version}`)
  log(`核账 OK：latest.json → v${mf.data.version}，versions.json 收录 ${vs.data.releases.length} 版`)
}

log('— 完 —')
console.log(`
下载直链（OSS）:
${artifacts.map((a) => `  [${a.platform}] ${a.url}`).join('\n')}
最新版清单: ${base}/api/v1/updates/${APP}/latest.json
历史版本:   ${base}/api/v1/updates/${APP}/versions.json
管理台:     https://tarzip.com/console/releases`)
