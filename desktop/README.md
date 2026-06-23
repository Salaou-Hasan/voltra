# Voltra Console (desktop)

A native desktop window around the Voltra admin dashboard. It loads a running
server's `/admin` page in the **system webview** — the same dashboard the web
serves, so there is no second UI to maintain.

- **Web** (headless / remote server): open `http://<server>:3001/admin` in a browser.
- **Desktop** (Windows / macOS / Linux): run this app — same dashboard, native window.

## Why it's tiny

It uses the OS webview (WebView2 on Windows, WKWebView on macOS, WebKitGTK on
Linux) instead of bundling a browser like Electron:

| | Electron | Voltra Console |
|---|---|---|
| Binary | ~100 MB | ~0.5 MB |
| RAM | 150–250 MB | 30–60 MB |

## Build

```
cd desktop
cargo build --release      # -> target/release/voltra-console
```

This is a standalone crate (own `[workspace]`), so the server build never pulls
GUI dependencies.

## Run

```
voltra-console                              # http://127.0.0.1:3001/admin
voltra-console http://my-server:3001/admin  # explicit server
VOLTRA_ADMIN_URL=http://host:3001/admin voltra-console
```

## Runtime requirement

- **Windows**: WebView2 runtime (preinstalled on Windows 11; auto-installed by recent Edge on Windows 10).
- **Linux**: `libwebkit2gtk-4.1-0` (`sudo apt install libwebkit2gtk-4.1-0`).
- **macOS**: none — WKWebView is built in.

## Security note

The console talks to the admin API, which is full read/write/execute control of
the database. Point it at a server you trust, over loopback or a tunnel — never
expose port 3001 to the public internet without `VOLTRA_API_KEY` set.
