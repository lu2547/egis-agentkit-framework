"""Compare published configuration without treating revision counters as content."""


def same_publication_content(left, right):
    if left is None or right is None:
        return False
    if left.get("definition_type") == "agent":
        fields = ("agent_name", "agent_display_name", "payload")
    elif left.get("definition_type") == "team":
        fields = ("team_name", "team_display_name", "payload")
    else:
        # Import jobs and uploader provenance identify operations, not configuration.
        fields = (
            "display_name", "description", "category", "avatar", "avatar_checksum",
            "dependency_only",
        )
        return left.get("checksum_sha256") == right.get("checksum_sha256") and all(
            left.get("payload", {}).get(field) == right.get("payload", {}).get(field)
            for field in fields
        )
    return all(left.get(field) == right.get(field) for field in fields)
