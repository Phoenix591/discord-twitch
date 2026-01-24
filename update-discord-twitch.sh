#!/bin/bash
# Stop on error
set -e

# ==========================================
# CONFIGURATION
# ==========================================
REPO_URL="https://github.com/Phoenix591/discord-twitch"
# UPDATED: Use S3 URI format
S3_CONFIG_URI="s3://phoenix591/discord-twitch/streamers.cfg"

INSTALL_DIR="/usr/local/discord-twitch"
SERVICE_DIR="/etc/systemd/system"
BACKUP_DIR="${INSTALL_DIR}/backups/$(date +%Y%m%d_%H%M%S)"
DOWNLOAD_USER="nobody"

# 1. ROOT CHECK
if [ "$EUID" -ne 0 ]; then
  echo "❌ Please run as root (sudo)."
  exit 1
fi

echo "🚀 Starting Full Update..."

# 2. PREPARE SANDBOX
TEMP_DIR=$(mktemp -d)
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Setup permissions for the unprivileged git download
chown $DOWNLOAD_USER "$TEMP_DIR"
chmod 700 "$TEMP_DIR"

# 3. IDENTIFY VERSION (As 'nobody')
echo "🔍 Checking for latest tag..."
LATEST_TAG_REF=$(cd "$TEMP_DIR" && sudo -u $DOWNLOAD_USER HOME="$TEMP_DIR" git ls-remote --tags --sort='v:refname' $REPO_URL.git | tail -n1 | awk '{print $2}')
LATEST_TAG=${LATEST_TAG_REF##*/}

if [ -z "$LATEST_TAG" ]; then
    echo "❌ No tags found. Aborting."
    exit 1
fi

echo "🏷️  Downloading version: $LATEST_TAG"

# 4. DOWNLOAD FILES

# A. Download Repo Zip (As 'nobody' - Untrusted Internet Source)
ZIP_URL="$REPO_URL/archive/refs/tags/$LATEST_TAG.zip"
sudo -u $DOWNLOAD_USER curl -fL -o "$TEMP_DIR/update.zip" "$ZIP_URL"
sudo -u $DOWNLOAD_USER unzip -q "$TEMP_DIR/update.zip" -d "$TEMP_DIR/extracted"
SOURCE_DIR=$(find "$TEMP_DIR/extracted" -mindepth 1 -maxdepth 1 -type d | head -n 1)

# B. Download Streamers Config (As ROOT - Trusted Private Source)
# We run as root to ensure access to /root/.aws/ credentials
echo "☁️  Fetching streamers.cfg from S3..."
aws s3 cp "$S3_CONFIG_URI" "$TEMP_DIR/streamers.cfg" --quiet

# 5. BACKUP
echo "🗄️  Creating backup..."
install -d -m 755 "$BACKUP_DIR"
[ -f "$INSTALL_DIR/bot.py" ] && cp "$INSTALL_DIR/bot.py" "$BACKUP_DIR/"
[ -f "$INSTALL_DIR/streamers.cfg" ] && cp "$INSTALL_DIR/streamers.cfg" "$BACKUP_DIR/"
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
    else
        echo "   (Skipping identical service: $fname)"
    fi
done

# 7. INSTALL APP FILES (From Git)
find "$SOURCE_DIR" -type f \
    -not -name "*.service" \
    -not -name ".git*" \
    | while read -r file; do
    
    fname=$(basename "$file")
    case "$fname" in
        *.py|*.sh) PERM=755 ;;
        *)         PERM=644 ;;
    esac

    echo "   📄 Updating file: $fname"
    install -o root -g root -m $PERM "$file" "$INSTALL_DIR/$fname"
done

# 8. INSTALL STREAMERS CONFIG (From S3)
S3_CFG_CHANGED=0
# Ensure we downloaded something valid before comparing
if [ -f "$TEMP_DIR/streamers.cfg" ]; then
    if ! cmp -s "$TEMP_DIR/streamers.cfg" "$INSTALL_DIR/streamers.cfg"; then
        echo "   ☁️  Updating streamers.cfg from S3"
        install -o root -g root -m 644 "$TEMP_DIR/streamers.cfg" "$INSTALL_DIR/streamers.cfg"
        S3_CFG_CHANGED=1
    else
        echo "   (Streamers list is unchanged)"
    fi
fi

# 9. RELOAD SYSTEMD
if [ $SERVICE_CHANGED -eq 1 ]; then
    echo "refreshing systemd..."
    systemctl daemon-reload
fi

# 10. CONDITIONAL RESTART
if systemctl is-active --quiet discord-twitch; then
    # Check if we updated code, config, OR git version
    CURRENT_VERSION=$(cat "$INSTALL_DIR/version.txt" 2>/dev/null || echo 'none')
    
    if [ $SERVICE_CHANGED -eq 1 ] || [ $S3_CFG_CHANGED -eq 1 ] || [ "$LATEST_TAG" != "$CURRENT_VERSION" ]; then
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
