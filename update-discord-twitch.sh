#!/bin/bash
set -e

# ==========================================
# CONFIGURATION
# ==========================================
REPO_URL="https://github.com/Phoenix591/discord-twitch"
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

chown $DOWNLOAD_USER "$TEMP_DIR"
chmod 700 "$TEMP_DIR"

# 3. IDENTIFY VERSION (As 'nobody')
echo "🔍 Checking for latest tag..."
# Check tags via git ls-remote (no clone needed)
LATEST_TAG_REF=$(cd "$TEMP_DIR" && sudo -u $DOWNLOAD_USER HOME="$TEMP_DIR" git ls-remote --tags --sort='v:refname' $REPO_URL.git | tail -n1 | awk '{print $2}')
LATEST_TAG=${LATEST_TAG_REF##*/}

if [ -z "$LATEST_TAG" ]; then
    echo "❌ No tags found. Aborting."
    exit 1
fi

echo "🏷️  Downloading version: $LATEST_TAG"

# 4. DOWNLOAD & EXTRACT ZIP (As 'nobody')
ZIP_URL="$REPO_URL/archive/refs/tags/$LATEST_TAG.zip"

sudo -u $DOWNLOAD_USER curl -fL -o "$TEMP_DIR/update.zip" "$ZIP_URL"
sudo -u $DOWNLOAD_USER unzip -q "$TEMP_DIR/update.zip" -d "$TEMP_DIR/extracted"

# Find the specific folder name created by unzip
SOURCE_DIR=$(find "$TEMP_DIR/extracted" -mindepth 1 -maxdepth 1 -type d | head -n 1)

# 5. BACKUP
echo "🗄️  Creating backup..."
install -d -m 755 "$BACKUP_DIR"
[ -f "$INSTALL_DIR/bot.py" ] && cp "$INSTALL_DIR/bot.py" "$BACKUP_DIR/"
[ -f "$SERVICE_DIR/discord-twitch.service" ] && cp "$SERVICE_DIR/discord-twitch.service" "$BACKUP_DIR/"

echo "🔄 Installing..."

# 6. INSTALL SERVICE FILES
# Install any .service files to systemd directory
SERVICE_CHANGED=0
find "$SOURCE_DIR" -name "*.service" | while read -r service_file; do
    fname=$(basename "$service_file")
    target="$SERVICE_DIR/$fname"

    # Check for changes (cmp returns 1 if different, ! handles the logic)
    if ! cmp -s "$service_file" "$target"; then
        echo "   ⚙️  Updating service: $fname"
        install -o root -g root -m 644 "$service_file" "$target"
        SERVICE_CHANGED=1
    else
        echo "   (Skipping identical service: $fname)"
    fi
done

# 7. INSTALL APP FILES
# Install everything else to the bot directory
find "$SOURCE_DIR" -type f \
    -not -name "*.service" \
    -not -name ".git*" \
    | while read -r file; do

    fname=$(basename "$file")

    # Determine permissions based on extension
    case "$fname" in
        *.py|*.sh)
            PERM=755
            ;;
        *)
            PERM=644
            ;;
    esac

    echo "   📄 Updating file: $fname ($PERM)"
    install -o root -g root -m $PERM "$file" "$INSTALL_DIR/$fname"
done

# 8. RELOAD SYSTEMD
if [ $SERVICE_CHANGED -eq 1 ]; then
    echo "refreshing systemd..."
    systemctl daemon-reload
fi

# 9. CONDITIONAL RESTART
if systemctl is-active --quiet discord-twitch; then
    echo "♻️  Bot active. Restarting..."
    systemctl restart discord-twitch
    echo "✅ Updated & Restarted ($LATEST_TAG)"
else
    echo "✅ Updated ($LATEST_TAG). No restart required."
fi
