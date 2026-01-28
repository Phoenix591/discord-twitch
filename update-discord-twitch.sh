#!/bin/bash
# Stop on error
set -e

# ==========================================
# CONFIGURATION
# ==========================================
REPO_URL="https://github.com/Phoenix591/discord-twitch"
S3_BUCKET_BASE="s3://phoenix591/discord-twitch"

# Artifacts
S3_ARTIFACT="$S3_BUCKET_BASE/discord-twitch.tar.xz"
S3_STREAMERS="$S3_BUCKET_BASE/streamers.cfg"
S3_SECRET="$S3_BUCKET_BASE/secret.cfg"

# Local Paths
INSTALL_DIR="/usr/local/discord-twitch"
CONFIG_DIR="/etc/discord-twitch"
SERVICE_DIR="/etc/systemd/system"
BACKUP_DIR="${INSTALL_DIR}/backups/$(date +%Y%m%d_%H%M%S)"
DOWNLOAD_USER="nobody"

# Self-Location for self-update check
CURRENT_SCRIPT=$(realpath "$0")

# 1. ROOT CHECK
if [ "$EUID" -ne 0 ]; then
  echo "❌ Please run as root (sudo)."
  exit 1
fi

echo "🚀 Starting Full Update (S3 Source)..."

# 2. PREPARE SANDBOX
TEMP_DIR=$(mktemp -d)
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

chown $DOWNLOAD_USER "$TEMP_DIR"
chmod 700 "$TEMP_DIR"

# Ensure Config Directory Exists
install -d -m 755 "$CONFIG_DIR"

# 3. IDENTIFY TARGET VERSION (Using Git Remote Tags as 'Source of Truth')
echo "🔍 Checking for latest tag..."
LATEST_TAG_REF=$(cd "$TEMP_DIR" && sudo -u $DOWNLOAD_USER HOME="$TEMP_DIR" git ls-remote --tags --sort='v:refname' $REPO_URL.git | tail -n1 | awk '{print $2}')
LATEST_TAG=${LATEST_TAG_REF##*/}

if [ -z "$LATEST_TAG" ]; then
    echo "❌ No tags found. Aborting."
    exit 1
fi

# Compare with installed version
CURRENT_VERSION=$(cat "$INSTALL_DIR/version.txt" 2>/dev/null || echo 'none')

# Force download if versions mismatch OR if explicit forced update is needed
# (You can remove the version check if you want it to ALWAYS download/reinstall)
echo "🏷️  Latest: $LATEST_TAG | Current: $CURRENT_VERSION"

# 4. DOWNLOAD ARTIFACTS (From S3)
echo "☁️  Fetching application tarball from S3..."
aws s3 cp "$S3_ARTIFACT" "$TEMP_DIR/update.tar.xz" --quiet

echo "☁️  Fetching configs from S3..."
aws s3 cp "$S3_STREAMERS" "$TEMP_DIR/streamers.cfg" --quiet
aws s3 cp "$S3_SECRET" "$TEMP_DIR/secret.cfg" --quiet

# Extract Application (As 'nobody' for safety)
mkdir -p "$TEMP_DIR/extracted"
chown $DOWNLOAD_USER "$TEMP_DIR/extracted"
# Extract tar.xz
sudo -u $DOWNLOAD_USER tar -xJf "$TEMP_DIR/update.tar.xz" -C "$TEMP_DIR/extracted"

# Find the source directory (Since we used --prefix in git archive, it will be inside a subdir)
SOURCE_DIR=$(find "$TEMP_DIR/extracted" -mindepth 1 -maxdepth 1 -type d | head -n 1)

if [ -z "$SOURCE_DIR" ]; then
    echo "❌ Error: Could not find extracted source directory."
    exit 1
fi

# ==========================================
# SELF-UPDATE LOGIC
# ==========================================
NEW_SCRIPT="$SOURCE_DIR/update-discord-twitch.sh"

if [ -f "$NEW_SCRIPT" ]; then
    if ! cmp -s "$CURRENT_SCRIPT" "$NEW_SCRIPT"; then
        echo "✨ New updater version detected. Updating self..."
        install -d -m 755 "$BACKUP_DIR"
        cp "$CURRENT_SCRIPT" "$BACKUP_DIR/update-discord-twitch.sh.bak"
        install -o root -g root -m 755 "$NEW_SCRIPT" "$CURRENT_SCRIPT"
        echo "🔁 Handing over control to new script..."
        rm -rf "$TEMP_DIR"
        exec "$CURRENT_SCRIPT"
    fi
fi
# ==========================================

# 5. BACKUP APP FILES
echo "🗄️  Creating backup..."
install -d -m 755 "$BACKUP_DIR"
[ -f "$INSTALL_DIR/bot.py" ] && cp "$INSTALL_DIR/bot.py" "$BACKUP_DIR/"
[ -f "$CONFIG_DIR/streamers.cfg" ] && cp "$CONFIG_DIR/streamers.cfg" "$BACKUP_DIR/"
[ -f "$CONFIG_DIR/secret.cfg" ] && cp "$CONFIG_DIR/secret.cfg" "$BACKUP_DIR/"
[ -f "$SERVICE_DIR/discord-twitch.service" ] && cp "$SERVICE_DIR/discord-twitch.service" "$BACKUP_DIR/"

echo "🔄 Installing..."

# 6. INSTALL SERVICE FILES
SERVICE_CHANGED=0
find "$SOURCE_DIR" -name "*.service" | while read -r service_file; do
    fname=$(basename "$service_file")
    target="$SERVICE_DIR/$fname"
    if ! cmp -s "$service_file" "$target"; then
        echo "   ⚙️  Updating service: $fname"
        install -o root -g root -m 644 "$service_file" "$target"
        SERVICE_CHANGED=1
    fi
done

# 7. INSTALL APP FILES
# Map new source structure (src/discord_twitch/bot.py) to legacy install location
NEW_BOT_SRC="$SOURCE_DIR/src/discord_twitch/bot.py"

if [ -f "$NEW_BOT_SRC" ]; then
    echo "   📄 Updating file: bot.py"
    install -o root -g root -m 755 "$NEW_BOT_SRC" "$INSTALL_DIR/bot.py"
else
    echo "❌ Error: Could not find bot.py in new source structure ($NEW_BOT_SRC)"
    exit 1
fi

# 8. INSTALL CONFIGS (To /etc/discord-twitch)
S3_CFG_CHANGED=0
S3_SECRET_CHANGED=0

# Streamers
if [ -f "$TEMP_DIR/streamers.cfg" ]; then
    if ! cmp -s "$TEMP_DIR/streamers.cfg" "$CONFIG_DIR/streamers.cfg"; then
        echo "   ☁️  Updating streamers.cfg"
        install -o root -g root -m 644 "$TEMP_DIR/streamers.cfg" "$CONFIG_DIR/streamers.cfg"
        S3_CFG_CHANGED=1
    fi
fi

# Secrets (600 Perms)
if [ -f "$TEMP_DIR/secret.cfg" ]; then
    if [ ! -f "$CONFIG_DIR/secret.cfg" ] || ! cmp -s "$TEMP_DIR/secret.cfg" "$CONFIG_DIR/secret.cfg"; then
        echo "   🔒 Updating secret.cfg"
        install -o root -g root -m 600 "$TEMP_DIR/secret.cfg" "$CONFIG_DIR/secret.cfg"
        S3_SECRET_CHANGED=1
    fi
fi

# 9. RELOAD SYSTEMD
if [ $SERVICE_CHANGED -eq 1 ]; then
    echo "refreshing systemd..."
    systemctl daemon-reload
fi

# 10. CONDITIONAL RESTART
if systemctl is-active --quiet discord-twitch; then
    # Restart if Service, Streamers, Secrets, or Version changed
    if [ $SERVICE_CHANGED -eq 1 ] || [ $S3_CFG_CHANGED -eq 1 ] || [ $S3_SECRET_CHANGED -eq 1 ] || [ "$LATEST_TAG" != "$CURRENT_VERSION" ]; then
        echo "♻️  Changes detected. Queuing Restart..."
        systemctl restart --no-block discord-twitch
        echo "$LATEST_TAG" > "$INSTALL_DIR/version.txt"
    else
        echo "✅ No changes detected. Bot continues running."
    fi
else
    echo "✅ Updated ($LATEST_TAG). No restart required."
    echo "$LATEST_TAG" > "$INSTALL_DIR/version.txt"
fi
