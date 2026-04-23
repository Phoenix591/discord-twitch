#!/usr/bin/env python3
import boto3
import argparse
import sys

DYNAMODB_REGION = "us-east-1"
TABLE_NAME = "discord-twitch-streamers"


def main():
    parser = argparse.ArgumentParser(
        description="Manage your Discord-Twitch Bot Tracked Streamers"
    )

    # We make these nargs="?" so they aren't strictly required by argparse if someone just wants to --list
    parser.add_argument(
        "platform",
        nargs="?",
        choices=["twitch", "youtube"],
        help="The platform (twitch or youtube)",
    )
    parser.add_argument("id", nargs="?", help="The Channel ID or Twitch User ID")
    parser.add_argument(
        "name", nargs="?", default="", help="The display name (Required when adding)"
    )

    parser.add_argument(
        "--remove",
        action="store_true",
        help="Remove the streamer instead of adding them",
    )
    parser.add_argument(
        "--list", action="store_true", help="List all currently tracked streamers"
    )

    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    table = dynamodb.Table(TABLE_NAME)

    # --- LIST LOGIC ---
    if args.list:
        try:
            response = table.scan()
            items = response.get("Items", [])

            if not items:
                print("📭 No streamers are currently being tracked.")
            else:
                print(f"📋 Currently tracking {len(items)} streamers:\n")

                # Sort into platforms and alphabetize by name
                tw = sorted(
                    [i for i in items if i.get("platform") == "twitch"],
                    key=lambda x: x.get("display_name", "").lower(),
                )
                yt = sorted(
                    [i for i in items if i.get("platform") == "youtube"],
                    key=lambda x: x.get("display_name", "").lower(),
                )

                if tw:
                    print("🟣 Twitch:")
                    for s in tw:
                        print(
                            f"  - {s.get('display_name', 'Unknown')} (ID: {s.get('channel_id')})"
                        )

                if yt:
                    if tw:
                        print("")  # Spacer
                    print("🔴 YouTube:")
                    for s in yt:
                        print(
                            f"  - {s.get('display_name', 'Unknown')} (ID: {s.get('channel_id')})"
                        )

        except Exception as e:
            print(f"❌ Failed to fetch list: {e}")

        return  # Exit gracefully after listing

    # --- ENFORCE POSITIONAL ARGS FOR ADD/REMOVE ---
    if not args.platform or not args.id:
        parser.error(
            "The 'platform' and 'id' arguments are required unless using --list"
        )

    # --- REMOVE LOGIC ---
    if args.remove:
        try:
            table.delete_item(Key={"platform": args.platform, "channel_id": args.id})
            print(f"🗑️  Removed {args.platform} streamer with ID {args.id}")
        except Exception as e:
            print(f"❌ Failed to remove item: {e}")

    # --- ADD LOGIC ---
    else:
        if not args.name:
            print("❌ Error: You must provide a display name when adding a streamer.")
            print(f'Usage: {sys.argv[0]} {args.platform} {args.id} "Streamer Name"')
            sys.exit(1)

        try:
            table.put_item(
                Item={
                    "platform": args.platform,
                    "channel_id": args.id,
                    "display_name": args.name,
                }
            )
            print(f"✅ Added {args.platform} streamer: {args.name} ({args.id})")
        except Exception as e:
            print(f"❌ Failed to add item: {e}")


if __name__ == "__main__":
    main()
