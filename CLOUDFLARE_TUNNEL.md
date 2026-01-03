# Cloudflare Tunnel Setup for Streamlit App

This guide explains how to expose your Streamlit app to the internet using Cloudflare Tunnel, allowing access from any device (iPhone, iPad, etc.) without opening ports on your router.

## Quick Start

1. **Install Cloudflare Tunnel:**
   ```bash
   ./setup_cloudflare_tunnel.sh
   ```

2. **Run Streamlit with Tunnel:**
   ```bash
   ./run_streamlit_with_tunnel.sh
   ```

3. **Access your app:**
   - The script will display a Cloudflare Tunnel URL (e.g., `https://random-name.trycloudflare.com`)
   - Open this URL in any browser on any device
   - The URL is valid until you stop the tunnel

## How It Works

Cloudflare Tunnel creates a secure connection between your local Streamlit app and Cloudflare's edge network:
- **No router configuration needed** - works behind NAT/firewalls
- **HTTPS by default** - secure connection automatically
- **No port forwarding** - no need to open ports on your router
- **Temporary URLs** - each tunnel session gets a unique URL

## Security Considerations

⚠️ **Important Security Notes:**

1. **Password Protection**: The Streamlit app has password protection enabled via `.streamlit/secrets.toml`. Make sure you have set a strong password.

2. **Temporary URLs**: The tunnel URLs are temporary and change each time you restart the tunnel. This provides some security, but:
   - Don't share the URL publicly
   - The URL expires when you stop the tunnel
   - Consider using a named tunnel for a permanent URL (see Advanced Setup below)

3. **Data Access**: Anyone with the URL can access your app (if they know the password). Only share URLs with trusted individuals.

4. **Local Network**: Your local machine must be running for the tunnel to work.

## Manual Installation

If the setup script doesn't work, you can install `cloudflared` manually:

### Linux
```bash
# Download latest release
wget https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared
chmod +x cloudflared
sudo mv cloudflared /usr/local/bin/
```

### macOS
```bash
brew install cloudflared
```

### Windows
Download from: https://github.com/cloudflare/cloudflared/releases

## Advanced Setup: Named Tunnel (Permanent URL)

For a permanent URL that doesn't change, you can set up a named tunnel:

1. **Create a Cloudflare account** (free): https://dash.cloudflare.com/sign-up

2. **Authenticate:**
   ```bash
   cloudflared tunnel login
   ```

3. **Create a named tunnel:**
   ```bash
   cloudflared tunnel create streamlit-app
   ```

4. **Create a config file** (`~/.cloudflared/config.yml`):
   ```yaml
   tunnel: streamlit-app
   ingress:
     - hostname: your-subdomain.yourdomain.com
       service: http://localhost:8500
     - service: http_status:404
   ```

5. **Run the tunnel:**
   ```bash
   cloudflared tunnel run streamlit-app
   ```

For more details, see: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/

## Troubleshooting

### Tunnel URL not appearing
- Make sure Streamlit started successfully (check for errors)
- Wait a few seconds for the tunnel to establish
- Check that port 8500 is not already in use

### Can't access from external device
- Verify the URL is correct (copy-paste it)
- Make sure the tunnel is still running
- Check that Streamlit is running (visit http://localhost:8500 locally)

### Connection refused
- Ensure Streamlit is running on port 8500
- Check firewall settings (though Cloudflare Tunnel should bypass most firewalls)

## Stopping the Services

Press `Ctrl+C` in the terminal where the script is running. This will stop both Streamlit and the Cloudflare Tunnel.

## Alternative: Run Services Separately

If you prefer to run them separately:

**Terminal 1 - Streamlit:**
```bash
./run_streamlit.sh
```

**Terminal 2 - Tunnel:**
```bash
cloudflared tunnel --url http://localhost:8500
```

