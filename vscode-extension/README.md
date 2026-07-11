# Worker Bridge VS Code

A minimal VS Code extension that lets worker-bridge delegate coding tasks into a
running VS Code window. On activation it starts a **loopback-only** HTTP server on
`127.0.0.1:9394` and writes a fresh bearer token to `~/.worker-bridge-vscode-token`.

The companion adapter is `worker_bridge/adapters/vscode.py`; select it with
`worker: "vscode"` in a `worker_delegate` call.

## HTTP API

All bodies are JSON. `/task` and `/cancel` require
`Authorization: Bearer <token>` where `<token>` is the contents of
`~/.worker-bridge-vscode-token`. `/health` is unauthenticated (loopback only) so the
adapter can detect the window before trusting the token.

| Method | Path      | Body                                             | Response |
| ------ | --------- | ------------------------------------------------ | -------- |
| GET    | `/health` | –                                                | `{ "status": "ok", "version": "<ext version>", "agents": ["<installed AI extension ids>"] }` |
| POST   | `/task`   | `{ "prompt", "cwd"?, "session_id"?, "approve_all"? }` | `{ "success", "output", "session_id", "changed_files" }` |
| POST   | `/cancel` | `{ "session_id" }`                               | `{ "success" }` |

`/task` runs the `claude` CLI (`claude --print --output-format json`) in the
requested `cwd`. Pass `session_id` to resume a prior session (`--resume`);
otherwise a new session id is generated (`--session-id`). `approve_all` defaults
to `true` and maps to claude's `--dangerously-skip-permissions`. The run is shown
in an integrated terminal (created per request and used as the cancel handle);
stdout is captured from the child process and returned as strict JSON.
`changed_files` is `git status --porcelain` for `cwd` (empty when not a git repo).

Override the CLI with the `WORKER_BRIDGE_CLAUDE_BIN` environment variable.

## Build

Requires Node.js 18+ and npm.

```sh
./build.sh
```

This runs `npm install`, compiles `src/extension.ts` to `dist/extension.js` with
`tsc`, and packages a `.vsix` via `@vscode/vsce`. To do the steps by hand:

```sh
npm install
npm run compile   # tsc -> dist/extension.js
npm run package   # vsce package -> worker-bridge-vscode-<version>.vsix
```

## Install

```sh
code --install-extension worker-bridge-vscode-0.1.0.vsix
```

Then reload/restart VS Code. The extension activates on startup
(`onStartupFinished`); confirm it is live:

```sh
curl http://127.0.0.1:9394/health
```

Only one window can own port 9394; additional windows log
`port 9394 already in use` to the **Worker Bridge** output channel and stay
passive.

## Security notes

- The server binds `127.0.0.1` only — it is not reachable off-host.
- The token file is written `0600` and only after the port bind succeeds, so it
  always matches the live server.
- `approve_all` runs claude with permissions skipped; only enable it for
  workspaces you trust worker-bridge to edit unattended.
