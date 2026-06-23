from pathlib import Path
p = Path('Cargo.toml')
text = p.read_text()
new = text
if 'socket2 = "0.4"' not in new:
    new = new.replace('[dependencies]\n', '[dependencies]\nsocket2 = "0.4"\nzstd = "0.12"\nlibc = "0.2"\n"tokio-uring" = { version = "0.6", optional = true }\n"mimalloc" = { version = "0.2", optional = true }\n')
if '[features]' not in new:
    new += '\n[features]\ndefault = ["simd", "mimalloc"]\nsimd = []\nmimalloc = ["mimalloc"]\n"tokio-uring" = ["tokio-uring"]\n'
p.write_text(new)
print('Cargo.toml updated')
