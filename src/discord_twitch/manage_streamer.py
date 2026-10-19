#!/usr/bin/env python3
import boto3
import argparse
import sys

from botocore.exceptions import ClientError

from discord_twitch.config import dynamodb_settings, load_parser
from discord_twitch.dynamo import scan_all


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

    try:
        config, _path = load_parser()
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    region, _state_table, table_name = dynamodb_settings(config)
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    # --- LIST LOGIC ---
    if args.list:
        try:
            items = scan_all(table)

            if not items:
                print(
                    f"📭 No streamers are currently being tracked in {table_name} ({region})."
                )
            else:
                print(
                    f"📋 Currently tracking {len(items)} streamers in {table_name} ({region}):\n"
                )

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
            sys.exit(1)

        return  # Exit gracefully after listing

    # --- ENFORCE POSITIONAL ARGS FOR ADD/REMOVE ---
    if not args.platform or not args.id:
        parser.error(
            "The 'platform' and 'id' arguments are required unless using --list"
        )

    # --- REMOVE LOGIC ---
    if args.remove:
        try:
            table.delete_item(
                Key={"platform": args.platform, "channel_id": args.id},
                ConditionExpression="attribute_exists(channel_id)",
            )
            print(f"🗑️  Removed {args.platform} streamer with ID {args.id}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                print(f"❌ No {args.platform} streamer with ID {args.id} to remove")
                sys.exit(1)
            print(f"❌ Failed to remove item: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Failed to remove item: {e}")
            sys.exit(1)

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
            sys.exit(1)


if __name__ == "__main__":
    main()
