"""Health check utility for the Snapper server.

Polls the server health endpoint with configurable retries and delays.
Used for verifying server startup in scripts and CI pipelines.
"""

import time
import urllib.error
import urllib.request

DEFAULT_URL = "http://localhost:8000/api/health"
DEFAULT_MAX_RETRIES = 15
DEFAULT_DELAY = 2


def check_health(
    url: str = DEFAULT_URL,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay: float = DEFAULT_DELAY,
    timeout: float = 5,
) -> bool:
    """Check if server health endpoint is reachable.

    Args:
        url: The health endpoint URL to check.
        max_retries: Maximum number of retry attempts.
        delay: Delay in seconds between retry attempts.
        timeout: Request timeout in seconds.

    Returns:
        True if health check passes, False otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                if response.status == 200:
                    print(f"Health check passed (attempt {attempt})")
                    return True
        except (urllib.error.URLError, ConnectionResetError, TimeoutError):
            pass
        print(f"Attempt {attempt}/{max_retries}: waiting for server...")
        time.sleep(delay)
    print("Health check failed after all retries")
    return False


def main() -> int:
    """Entry point for server check script.

    Returns:
        Exit code: 0 if health check passes, 1 otherwise.
    """
    success = check_health()
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
