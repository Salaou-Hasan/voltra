# Voltra — VS Code extension

Syntax highlighting and a recognizable file icon for Voltra `.vol` reducer files.

## Install (no marketplace needed)

Copy this folder into your VS Code extensions directory, then reload:

```powershell
# Windows
Copy-Item -Recurse editors\vscode "$HOME\.vscode\extensions\voltra-0.1.0"
```

```bash
# macOS / Linux
cp -r editors/vscode ~/.vscode/extensions/voltra-0.1.0
```

Reload VS Code (`Ctrl+Shift+P` → "Developer: Reload Window"). `.vol` files now show the Voltra bolt icon and get highlighting.

## If `.vol` files still look like Rust

A scaffolded project's `.vscode/settings.json` may force `"*.vol": "rust"`. Remove that line so the Voltra language takes over:

```json
"files.associations": { "*.vol": "rust" }   // delete this
```
