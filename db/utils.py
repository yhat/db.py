import os


def profile_path(profile_id, profile):
    """Create full path to given provide for the current user."""
    user = os.path.expanduser("~")
    return os.path.join(user, profile_id + profile)
