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
    parser.add_argument(
        "platform",
        choices=["twitch", "youtube"],
        help="The platform (twitch or youtube)",
    )
    parser.add_argument("id", help="The Channel ID or Twitch User ID")
    parser.add_argument(
        "name", nargs="?", default="", help="The display name (Required when adding)"
    )
    parser.add_argument(
        "--remove",
        action="store_true",
        help="Remove the streamer instead of adding them",
    )

    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    table = dynamodb.Table(TABLE_NAME)

    if args.remove:
        try:
            table.delete_item(Key={"platform": args.platform, "channel_id": args.id})
            print(f"🗑️  Removed {args.platform} streamer with ID {args.id}")
        except Exception as e:
            print(f"❌ Failed to remove item: {e}")

    else:
        if not args.name:
            print("❌ Error: You must provide a display name when adding a streamer.")
            print(
                f'Usage: python3 {sys.argv[0]} {args.platform} {args.id} "Streamer Name"'
            )
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
