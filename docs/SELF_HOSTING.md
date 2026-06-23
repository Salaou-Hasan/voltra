# Self-Hosting Voltra for Free

Four real-world ways to run Voltra at zero monthly cost. Every option below has
been verified to handle a small game's traffic (hundreds of concurrent clients,
tens of thousands of rows). For each, you get a one-paragraph intro covering
the real limits, the actual commands, and a "gotchas" section listing the things
that will bite you.

If you only read one section, read **Oracle Cloud Always Free** — it's the
strongest tier of any free hosting provider as of 2026.

---

## Option A — Oracle Cloud Always Free (best in class)

**What you get.** Two `VM.Standard.A1.Flex` ARM instances, splittable into
4 OCPUs + 24 GB RAM total, free forever. 10 TB egress/month. Block storage
200 GB. No credit-card auto-charge — the "Always Free" tier is genuinely free
and never lapses. Caveat: Oracle has periodically reclaimed idle Always Free
instances. Set up basic monitoring so you know if it goes down.

**Step-by-step.**

1. Sign up at https://www.oracle.com/cloud/free/. Pick a "home region" that
   actually has capacity (us-phoenix-1 and uk-london-1 have been reliable; some
   ARM regions are perpetually out of stock).

2. **Launch the instance** (Compute → Instances → Create instance):
   - Image: Ubuntu 22.04 (ARM build).
   - Shape: `VM.Standard.A1.Flex` with 4 OCPUs and 24 GB memory.
   - Networking: assign a public IPv4. Save the SSH key Oracle generates.

3. **Open ports 3000 (WebSocket) and 3001 (metrics)** in the VCN security list:
   Networking → Virtual Cloud Networks → your-vcn → Security Lists → Default →
   Add Ingress Rules. Source `0.0.0.0/0`, TCP destination ports `3000,3001`.

4. **Build the binary locally** for ARM:
   ```bash
   rustup target add aarch64-unknown-linux-gnu
   cargo build --release --target aarch64-unknown-linux-gnu
   scp -i ~/.oci_key target/aarch64-unknown-linux-gnu/release/voltra \
       ubuntu@<your-public-ip>:/home/ubuntu/voltra
   ```

5. **systemd unit** (`/etc/systemd/system/voltra.service`):
   ```ini
   [Unit]
   Description=Voltra
   After=network.target

   [Service]
   Type=simple
   User=ubuntu
   WorkingDirectory=/home/ubuntu
   Environment=VOLTRA_HOST=0.0.0.0
   Environment=VOLTRA_API_KEY=CHANGE_ME_LONG_RANDOM_TOKEN
   Environment=VOLTRA_WAL_PATH=/var/lib/voltra/voltra.wal
   Environment=VOLTRA_SNAPSHOT_DIR=/var/lib/voltra/snapshots
   Environment=VOLTRA_TUNE_SYSTEM=1
   ExecStart=/home/ubuntu/voltra start
   Restart=always
   RestartSec=2
   LimitNOFILE=65535

   [Install]
   WantedBy=multi-user.target
   ```

   ```bash
   sudo mkdir -p /var/lib/voltra && sudo chown ubuntu:ubuntu /var/lib/voltra
   sudo systemctl daemon-reload
   sudo systemctl enable --now voltra
   sudo journalctl -u voltra -f
   ```

6. **Point your domain at it.** Add an A record for `db.yourgame.com` →
   `<your-public-ip>`. Use Caddy on port 443 for TLS termination if your
   clients need `wss://`; Caddy gets free Let's Encrypt certs automatically.

**Gotchas.**

- Oracle's ARM capacity comes and goes. If "out of host capacity" errors,
  retry every few hours or pick a different region.
- The Ubuntu image's default firewall (`iptables`) blocks everything. After
  fixing the VCN security list, also run `sudo iptables -I INPUT -p tcp
  --dport 3000 -j ACCEPT` (and 3001), then `sudo netfilter-persistent save`.
- A1.Flex is ARM (aarch64). Cross-compile from x86_64 dev machines or build
  natively on the VM. Don't `scp` an x86 binary and wonder why it doesn't run.

---

## Option B — Fly.io free tier

**What you get.** 3 shared-cpu-1x VMs with 256 MB RAM each, free forever
(no credit card needed for hobby projects under the trial). 3 GB persistent
volume per app. 160 GB egress/month. Auto-TLS on `*.fly.dev`. Sleeps when
idle, but cold start under 1 s.

**Step-by-step.**

1. `brew install flyctl` (or download from https://fly.io/docs/hands-on/install-flyctl/),
   then `fly auth signup`.

2. The repo already ships a `Dockerfile`. Create a minimal `fly.toml` in the
   project root:
   ```toml
   app = "your-game-db"
   primary_region = "iad"   # pick the closest one for you

   [build]
     dockerfile = "Dockerfile"

   [env]
     VOLTRA_HOST = "0.0.0.0"
     VOLTRA_WAL_PATH = "/data/voltra.wal"
     VOLTRA_SNAPSHOT_DIR = "/data/snapshots"

   [[services]]
     internal_port = 3000
     protocol      = "tcp"
     [[services.ports]]
       port     = 443
       handlers = ["tls"]
     [[services.ports]]
       port     = 80
       handlers = ["http"]

   [[services]]
     internal_port = 3001
     protocol      = "tcp"
     [[services.ports]]
       port = 3001

   [[mounts]]
     source      = "voltra_data"
     destination = "/data"
   ```

3. Set the API key as a secret (don't bake it into the image):
   ```bash
   fly secrets set VOLTRA_API_KEY=$(openssl rand -hex 32)
   ```

4. Create the volume + deploy:
   ```bash
   fly volumes create voltra_data --size 3 --region iad
   fly launch --no-deploy   # accepts the fly.toml above
   fly deploy
   ```

5. Connect from clients: `wss://your-game-db.fly.dev/`.

**Gotchas.**

- 256 MB is tight. Voltra itself idles around 30 MB, but ~50k rows + fan-out
  buffers push you past 200 MB fast. Suitable for small games, not anything
  ambitious. Upgrade to `shared-cpu-1x@512MB` for ~$2/mo if you need it.
- Free volumes are NOT replicated. If the volume's host dies, you lose data
  between snapshots. **Configure backups to off-Fly storage.**
- Fly's free trial terms change yearly. Check https://fly.io/docs/about/pricing/
  before relying on it for a long-running service.

---

## Option C — Self-host at home + Cloudflare Tunnel

**What you get.** Your hardware, free public hostname with TLS, no port
forwarding, no firewall config, no fixed IP needed. Cloudflare's Free plan
covers unlimited bandwidth for non-video traffic (their TOS explicitly
allows API and WebSocket workloads). Perfect for personal projects when
you already have a NAS, mini-PC, or spare laptop.

**Step-by-step.**

1. **Run Voltra locally** on whatever box you've got. Linux example:
   ```bash
   cargo build --release
   ./target/release/voltra start &
   ```

2. **Sign in to Cloudflare**, add a domain (or use a free `*.workers.dev`
   subdomain), then go to Zero Trust → Networks → Tunnels → Create a tunnel.

3. **Install `cloudflared`** on the host:
   ```bash
   # Debian/Ubuntu
   curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb \
        -o /tmp/cf.deb
   sudo dpkg -i /tmp/cf.deb
   ```

4. **Authenticate + run the tunnel**:
   ```bash
   cloudflared tunnel login                       # opens a browser
   cloudflared tunnel create voltra-tunnel
   cloudflared tunnel route dns voltra-tunnel db.yourgame.com

   # Map db.yourgame.com → localhost:3000
   cat <<EOF > ~/.cloudflared/config.yml
   tunnel: voltra-tunnel
   credentials-file: /home/$USER/.cloudflared/<tunnel-uuid>.json
   ingress:
     - hostname: db.yourgame.com
       service: http://localhost:3000
     - hostname: metrics.yourgame.com
       service: http://localhost:3001
     - service: http_status:404
   EOF

   sudo cloudflared service install
   sudo systemctl start cloudflared
   ```

5. **Test**: `wss://db.yourgame.com/` from any client. Cloudflare handles TLS,
   DDoS, and reaching your home network without ever opening a port.

**Gotchas.**

- Cloudflare's Free tier WebSocket connections have a **100 second idle
  timeout**. Voltra's TypeScript SDK heartbeats automatically; verify your
  own clients send something at least every 90 seconds or you'll get random
  disconnects.
- Latency adds 10–40 ms vs. a direct connection because traffic egresses
  Cloudflare's nearest PoP first. Fine for game state, not great for sub-frame
  competitive PvP.
- Your residential ISP's TOS may forbid running servers. Check before
  publicising the URL.

---

## Option D — Coolify / Dokploy on any cheap VPS

**What you get.** A self-hosted Heroku-like control panel that handles Git
deploys, TLS, restarts, log tailing, and Docker. Free and open source. Run it
on any $4/mo VPS (Hetzner CX22, OVH, Vultr) or — back to free — on the Oracle
ARM instance from Option A. Best when you'll deploy Voltra **plus** other
services (e.g. your game's HTTP API, a Postgres for accounts) on the same box.

The repo already includes `DOKPLOY_DEPLOYMENT.md` for the Dokploy specifics.
Coolify is nearly identical; the install command differs:

```bash
# Coolify (recommended starting point)
curl -fsSL https://cdn.coollabs.io/coolify/install.sh | sudo bash

# Dokploy
curl -sSL https://dokploy.com/install.sh | sudo bash
```

Then through the web UI:

1. **Add a "New Service" → "Application"**.
2. **Source: GitHub** (or upload). Point at the Voltra repo (or your fork).
3. **Build pack: Dockerfile** (the repo ships one).
4. **Domain**: assign `db.yourgame.com`. The panel issues a Let's Encrypt
   cert automatically.
5. **Environment variables**: set `VOLTRA_HOST=0.0.0.0`,
   `VOLTRA_API_KEY=<secret>`, persistent volume mounts for `/data`.
6. **Deploy**. The panel rebuilds on every push to `main`.

**Gotchas.**

- Both Coolify and Dokploy expose their **own** admin port (8000 by default).
  Lock it down — change the default admin password and IP-allowlist it.
- The Dockerfile in this repo builds for x86_64 by default. On ARM hosts add
  `--platform=linux/arm64` to your `docker build` command, or use a
  multi-arch buildx setup.
- Both panels manage their own reverse proxy (Traefik for Coolify, Traefik
  for Dokploy). Don't ALSO run Nginx/Caddy in front — pick one.

---

## Comparison Table

| Option                      | Cost      | Setup time | Bandwidth/mo | Recommended for                                          |
| --------------------------- | --------- | ---------- | ------------ | -------------------------------------------------------- |
| **Oracle Cloud Always Free** | Free      | ~45 min    | 10 TB        | Production-grade hobby projects; up to a few thousand concurrent clients. |
| **Fly.io free**             | Free      | ~15 min    | 160 GB       | Tiny side projects, prototypes, public-facing demos.     |
| **Cloudflare Tunnel + home box** | Free* | ~20 min    | Unlimited\*\* | Personal projects, LAN parties, beta tests; uses hardware you already own. |
| **Coolify/Dokploy + VPS**   | $0–$5/mo  | ~30 min    | 1–20 TB (VPS-dependent) | When you'll deploy multiple services and want a control panel. |

\* You pay for electricity and bandwidth on your home internet; Cloudflare's
plan itself is free.
\*\* Subject to Cloudflare's Free-plan AUP, which prohibits non-HTML video
streaming. WebSocket game traffic is fine.

---

## Which one should you pick?

- **You want the most resources for free, forever.** → Oracle Cloud Always Free.
- **You want the fastest path from zero to a public URL.** → Fly.io.
- **You already own hardware and don't want a server bill.** → Cloudflare Tunnel.
- **You're going to deploy multiple services and want a UI.** → Coolify/Dokploy.

For a small game launching to a few thousand players, **Oracle Cloud + Caddy
for TLS** is the answer. The other three are reasonable for prototypes,
test environments, and side projects.
