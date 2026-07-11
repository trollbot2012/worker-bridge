// Worker Bridge VS Code
//
// On activation this starts a loopback-only HTTP server on 127.0.0.1:9394 and
// writes a fresh random bearer token to ~/.worker-bridge-vscode-token. worker-bridge's worker
// bridge reads that token, probes GET /health to detect the window, and POSTs
// coding tasks to /task. A task runs the `claude` CLI in --print/JSON mode; the
// run is surfaced in an integrated terminal (created per request and used as the
// cancel handle) while stdout is captured from the child process so the endpoint
// can return a strict JSON result. Capturing from the terminal directly is not
// portable (PowerShell has no stdin redirection and shell-integration output is
// ANSI-contaminated), so the child process is the authoritative capture.

import * as vscode from 'vscode';
import * as http from 'http';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { spawn, execFile, ChildProcess } from 'child_process';

const HOST = '127.0.0.1';
const PORT = 9394;
const TOKEN_FILE = path.join(os.homedir(), '.worker-bridge-vscode-token');
const MAX_BODY = 8 * 1024 * 1024;
const CLAUDE_BIN = process.env.WORKER_BRIDGE_CLAUDE_BIN || 'claude';

// Substrings that mark an installed AI coding extension for /health.agents.
const AGENT_TOKENS = [
  'copilot', 'claude', 'cline', 'continue', 'aider',
  'opencode', 'gemini', 'cody', 'tabnine', 'codeium', 'supermaven', 'codex',
];

interface Session {
  child?: ChildProcess;
  terminal: vscode.Terminal;
}

const sessions = new Map<string, Session>();
let output: vscode.OutputChannel;

function log(message: string): void {
  output?.appendLine(`[${PORT}] ${message}`);
}

export function activate(context: vscode.ExtensionContext): void {
  output = vscode.window.createOutputChannel('Worker Bridge');
  const token = crypto.randomBytes(32).toString('hex');
  const version = String(context.extension.packageJSON?.version ?? '0.0.0');

  const server = http.createServer((req, res) => {
    handle(req, res, token, version).catch((err) => {
      log(`unhandled request error: ${err?.message ?? err}`);
      sendJson(res, 500, { error: 'internal error' });
    });
  });

  server.on('listening', () => {
    try {
      // Write the token only once we actually own the port, so a second window
      // that loses the bind race never overwrites the live window's token.
      fs.writeFileSync(TOKEN_FILE, token, { mode: 0o600 });
      fs.chmodSync(TOKEN_FILE, 0o600);
    } catch (err) {
      log(`failed to write token file: ${(err as Error).message}`);
    }
    log(`listening on http://${HOST}:${PORT}`);
  });

  server.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE') {
      log(`port ${PORT} already in use — another VS Code window is hosting the bridge`);
    } else {
      log(`server error: ${err.message}`);
    }
  });

  server.listen(PORT, HOST);

  context.subscriptions.push(output, {
    dispose: () => {
      server.close();
      for (const session of sessions.values()) {
        session.child?.kill();
        session.terminal.dispose();
      }
      sessions.clear();
    },
  });
}

export function deactivate(): void {
  // Disposables registered on the context handle teardown.
}

async function handle(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  token: string,
  version: string,
): Promise<void> {
  const url = new URL(req.url ?? '/', `http://${HOST}`);
  const route = `${req.method} ${url.pathname}`;

  if (route === 'GET /health') {
    return sendJson(res, 200, { status: 'ok', version, agents: installedAgents() });
  }

  if (route === 'POST /task') {
    if (!authorized(req, token)) {
      return sendJson(res, 401, { error: 'unauthorized' });
    }
    let body: any;
    try {
      body = await readJsonBody(req);
    } catch (err) {
      return sendJson(res, (err as any).statusCode ?? 400, { error: (err as Error).message });
    }
    const result = await runTask(body);
    return sendJson(res, 200, result);
  }

  if (route === 'POST /cancel') {
    if (!authorized(req, token)) {
      return sendJson(res, 401, { error: 'unauthorized' });
    }
    let body: any;
    try {
      body = await readJsonBody(req);
    } catch (err) {
      return sendJson(res, (err as any).statusCode ?? 400, { error: (err as Error).message });
    }
    return sendJson(res, 200, cancelTask(body?.session_id));
  }

  return sendJson(res, 404, { error: 'not found' });
}

function authorized(req: http.IncomingMessage, token: string): boolean {
  const header = req.headers['authorization'];
  const value = Array.isArray(header) ? header[0] : header;
  const match = /^Bearer (.+)$/.exec(value ?? '');
  if (!match) {
    return false;
  }
  const provided = Buffer.from(match[1]);
  const expected = Buffer.from(token);
  return provided.length === expected.length && crypto.timingSafeEqual(provided, expected);
}

function installedAgents(): string[] {
  const found: string[] = [];
  for (const ext of vscode.extensions.all) {
    const id = ext.id.toLowerCase();
    if (id.startsWith('vscode.') || id.startsWith('ms-vscode.')) {
      continue;
    }
    if (AGENT_TOKENS.some((tokenName) => id.includes(tokenName))) {
      found.push(ext.id);
    }
  }
  return found.sort();
}

function resolveCwd(cwd: unknown): string {
  if (typeof cwd === 'string' && cwd.trim()) {
    return cwd;
  }
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? os.homedir();
}

async function runTask(body: any): Promise<Record<string, unknown>> {
  const prompt = String(body?.prompt ?? '');
  if (!prompt.trim()) {
    return { success: false, output: '', session_id: null, changed_files: [], error: 'prompt is required' };
  }

  const cwd = resolveCwd(body?.cwd);
  const resume = typeof body?.session_id === 'string' && body.session_id.length > 0;
  const sessionId: string = resume ? body.session_id : crypto.randomUUID();
  const approveAll = body?.approve_all !== false; // default: approve all

  const args = ['--print', '--output-format', 'json', resume ? '--resume' : '--session-id', sessionId];
  if (approveAll) {
    // claude's real "always approve" flag; the bridge exposes it as approve_all.
    args.push('--dangerously-skip-permissions');
  }

  const terminal = vscode.window.createTerminal({ name: `Worker-Bridge ${sessionId.slice(0, 8)}`, cwd });
  terminal.show(false);
  terminal.sendText(`# worker-bridge: running claude for session ${sessionId} (stdout captured by the bridge)`, true);

  const session: Session = { terminal };
  sessions.set(sessionId, session);
  try {
    const { stdout, code, killed } = await runClaude(args, cwd, prompt, session);
    if (killed) {
      return { success: false, output: stdout, session_id: sessionId, changed_files: [], error: 'cancelled' };
    }
    const parsed = parseClaudeJson(stdout);
    const changedFiles = await gitChangedFiles(cwd);
    const success = code === 0 && parsed.is_error !== true;
    return {
      success,
      output: parsed.result ?? stdout,
      session_id: parsed.session_id || sessionId,
      changed_files: changedFiles,
      ...(success ? {} : { error: parsed.error ?? `claude exited with code ${code}` }),
    };
  } catch (err) {
    log(`task failed: ${(err as Error).message}`);
    return { success: false, output: '', session_id: sessionId, changed_files: [], error: (err as Error).message };
  } finally {
    sessions.delete(sessionId);
    terminal.dispose();
  }
}

function runClaude(
  args: string[],
  cwd: string,
  prompt: string,
  session: Session,
): Promise<{ stdout: string; code: number | null; killed: boolean }> {
  return new Promise((resolve, reject) => {
    // shell:true resolves `claude` / `claude.cmd` from PATH cross-platform. The
    // prompt is fed on stdin (never interpolated into the command line), so the
    // shell sees only fixed flags and a UUID — no injection surface.
    const child = spawn(CLAUDE_BIN, args, { cwd, shell: true });
    session.child = child;

    let stdout = '';
    let size = 0;
    child.stdout?.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size <= MAX_BODY) {
        stdout += chunk.toString('utf8');
      }
    });
    child.stderr?.on('data', (chunk: Buffer) => {
      log(`claude stderr: ${chunk.toString('utf8').slice(0, 500)}`);
    });
    child.on('error', (err) => reject(err));
    child.on('close', (code) => resolve({ stdout, code, killed: child.killed }));

    child.stdin?.on('error', () => { /* ignore EPIPE when claude exits early */ });
    child.stdin?.write(prompt);
    child.stdin?.end();
  });
}

interface ClaudeJson {
  result?: string;
  session_id?: string;
  is_error?: boolean;
  error?: string;
}

function parseClaudeJson(raw: string): ClaudeJson {
  const text = raw.trim();
  try {
    return JSON.parse(text) as ClaudeJson;
  } catch {
    // Fall back to the outermost {...} in case anything prefixed the JSON.
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(text.slice(start, end + 1)) as ClaudeJson;
      } catch {
        /* give up */
      }
    }
  }
  return {};
}

function gitChangedFiles(cwd: string): Promise<string[]> {
  return new Promise((resolve) => {
    execFile('git', ['-C', cwd, 'status', '--porcelain'], { maxBuffer: 4 * 1024 * 1024 }, (err, stdout) => {
      if (err) {
        resolve([]);
        return;
      }
      const files = stdout
        .split('\n')
        .map((line) => line.slice(3).trim())
        .filter(Boolean);
      resolve(Array.from(new Set(files)));
    });
  });
}

function cancelTask(sessionId: unknown): Record<string, unknown> {
  if (typeof sessionId !== 'string' || !sessions.has(sessionId)) {
    return { success: false, error: 'unknown session' };
  }
  const session = sessions.get(sessionId)!;
  session.child?.kill();
  session.terminal.dispose();
  sessions.delete(sessionId);
  return { success: true };
}

function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY) {
        const err: any = new Error('request body too large');
        err.statusCode = 413;
        req.destroy();
        reject(err);
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim();
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        const err: any = new Error('invalid JSON body');
        err.statusCode = 400;
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(body);
}
