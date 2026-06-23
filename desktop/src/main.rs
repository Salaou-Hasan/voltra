// Voltra Console — a native window around the admin dashboard.
//
// It does NOT reimplement the dashboard: it points the OS webview at a running
// server's /admin page, so it loads the exact same HTML the web serves and all
// relative API calls resolve to that server (same origin). One dashboard, two
// shells. Uses the system webview (WebView2 / WebKitGTK / WKWebView) — no
// bundled browser, so memory stays low.
//
// Target server (first match wins):
//   1. CLI arg:        voltra-console http://my-server:3001/admin
//   2. env:            VOLTRA_ADMIN_URL
//   3. default:        http://127.0.0.1:3001/admin

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tao::{
    dpi::LogicalSize,
    event::{Event, WindowEvent},
    event_loop::{ControlFlow, EventLoop},
    window::WindowBuilder,
};
use wry::WebViewBuilder;

fn target_url() -> String {
    std::env::args()
        .nth(1)
        .or_else(|| std::env::var("VOLTRA_ADMIN_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:3001/admin".to_string())
}

fn main() -> wry::Result<()> {
    let url = target_url();

    let event_loop = EventLoop::new();
    let window = WindowBuilder::new()
        .with_title("Voltra Console")
        .with_inner_size(LogicalSize::new(1280.0, 832.0))
        .with_min_inner_size(LogicalSize::new(900.0, 600.0))
        .build(&event_loop)
        .expect("create window");

    let _webview = WebViewBuilder::new().with_url(&url).build(&window)?;

    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;
        if let Event::WindowEvent {
            event: WindowEvent::CloseRequested,
            ..
        } = event
        {
            *control_flow = ControlFlow::Exit;
        }
    });
}
