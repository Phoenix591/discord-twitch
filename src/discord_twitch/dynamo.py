"""DynamoDB helpers shared by the bot and manage_streamer."""


def scan_all(table):
    """Return every item. A single scan() stops at 1 MB."""
    items = []
    kwargs = {}
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        kwargs["ExclusiveStartKey"] = last_key
