"""
Sellers.json Checker
====================

An asynchronous tool for verifying the presence of `sellers.json` files
on a list of domains. The script reads domains from an input file, probes
several common URL variants for each domain, and writes successful hits
to an output file in one of two user-selected formats.

Output formats:
    1. Full resolved URL to the `sellers.json` file (e.g. https://example.com/sellers.json)
    2. Original domain/URL exactly as provided in the input file

Author: (your name)
"""

import asyncio
import socket
import sys
from urllib.parse import urlparse

import aiohttp
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
INPUT_FILE = "input.txt"          # Path to the file containing one domain per line
OUTPUT_FILE = "output.txt"        # Path where results will be written

CONCURRENCY = 50                  # Number of concurrent worker coroutines
TIMEOUT = 10                      # Per-request timeout in seconds
MAX_RETRIES = 2                   # Retry attempts on transient HTTP errors (429, 5xx)
RETRY_DELAY = 5                   # Delay between retries in seconds
BATCH_SIZE = 5                    # Buffer size before flushing results to disk

# Realistic browser headers to avoid trivial bot-filtering on some hosts.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Event loop exception handler
# ---------------------------------------------------------------------------
def _silent_exception_handler(loop, context):
    """
    Suppress noisy but expected network errors that occur in background tasks
    (DNS failures, dropped connections, timeouts). Other exceptions still
    propagate to the default handler so real bugs remain visible.
    """
    exc = context.get("exception")
    msg = context.get("message", "")
    if isinstance(exc, (socket.gaierror, OSError, aiohttp.ClientError, asyncio.TimeoutError)):
        return
    if "shielded future" in msg or "getaddrinfo" in msg or "_resolve_host" in msg:
        return
    loop.default_exception_handler(context)


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------
def normalize(raw: str) -> str:
    """
    Normalize an input line to a bare hostname suitable for URL construction.

    Steps:
        - Strip whitespace and lower-case.
        - Prepend a scheme so urlparse() can extract the netloc reliably.
        - Drop a leading ``www.`` so we can later try both ``www`` and
          non-``www`` variants explicitly.

    Returns an empty string if the input cannot be normalized.
    """
    raw = raw.strip().lower()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "http://" + raw
    host = urlparse(raw).netloc
    return host[4:] if host.startswith("www.") else host


# ---------------------------------------------------------------------------
# Output format selection (interactive prompt)
# ---------------------------------------------------------------------------
def prompt_output_format() -> int:
    """
    Ask the user how successful hits should be written to the output file.

    Returns:
        1 -- write the full resolved URL to sellers.json
        2 -- write the original domain/URL as provided in input.txt
    """
    print("Select output format:")
    print("  1. Full sellers.json URL (e.g. https://example.com/sellers.json)")
    print("  2. Original URL/domain from input.txt")
    while True:
        choice = input("Enter 1 or 2: ").strip()
        if choice in ("1", "2"):
            return int(choice)
        print("Invalid choice. Please enter 1 or 2.")


# ---------------------------------------------------------------------------
# HTTP probing
# ---------------------------------------------------------------------------
async def check(session: aiohttp.ClientSession, url: str) -> str | None:
    """
    Issue a single GET request and decide whether the response represents
    a genuine ``sellers.json`` resource.

    A response is considered valid only if:
        - HTTP status is 200.
        - Content-Type is NOT HTML or XML (these usually indicate a generic
          error page rather than a real JSON document).

    Transient failures (HTTP 429 or 5xx) are retried up to ``MAX_RETRIES``
    times with a fixed ``RETRY_DELAY``. All other exceptions are swallowed
    and treated as "not found".

    Returns the final (possibly redirected) URL on success, otherwise None.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(url, allow_redirects=True) as r:
                if r.status == 429 or 500 <= r.status < 600:
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    return None
                if r.status != 200:
                    return None
                ctype = r.headers.get("Content-Type", "").lower()
                if "text/html" in ctype or "xml" in ctype:
                    return None
                return str(r.url)
        except Exception:
            return None
    return None


async def resolve(session: aiohttp.ClientSession, domain: str) -> str | None:
    """
    Try a small set of common URL variants for the given domain and return
    the first one that successfully serves a sellers.json file.

    Variants probed (in order):
        https://{domain}/sellers.json
        https://www.{domain}/sellers.json
        http://{domain}/sellers.json
        http://www.{domain}/sellers.json
    """
    for url in (
        f"https://{domain}/sellers.json",
        f"https://www.{domain}/sellers.json",
        f"http://{domain}/sellers.json",
        f"http://www.{domain}/sellers.json",
    ):
        found = await check(session, url)
        if found:
            return found
    return None


# ---------------------------------------------------------------------------
# Concurrency primitives: worker and writer
# ---------------------------------------------------------------------------
async def worker(
    session: aiohttp.ClientSession,
    domains: asyncio.Queue,
    results: asyncio.Queue,
    pbar: tqdm,
    output_format: int,
):
    """
    Pull domains from the input queue, probe them, and push successful
    results to the output queue.

    Each input queue item is a tuple ``(original, normalized)`` so that
    the worker can emit either the resolved sellers.json URL or the
    original input line, depending on ``output_format``.

    A ``None`` sentinel in the queue signals the worker to exit.
    """
    while True:
        item = await domains.get()
        try:
            if item is None:
                break
            original, normalized = item
            resolved = await resolve(session, normalized)
            if resolved:
                # Choose the value to emit based on the user's selection.
                if output_format == 1:
                    await results.put(resolved)
                else:
                    await results.put(original)
            pbar.update(1)
        finally:
            domains.task_done()


async def writer(results: asyncio.Queue, out_path: str) -> int:
    """
    Consume results from the queue and write them to disk in batches.

    Batching reduces I/O syscalls when many domains resolve successfully
    in quick succession. A ``None`` sentinel signals end-of-stream, at
    which point any remaining buffered items are flushed.

    Returns the total number of lines written.
    """
    count = 0
    buffer: list[str] = []
    with open(out_path, "w", encoding="utf-8") as out:
        while True:
            item = await results.get()
            try:
                if item is None:
                    if buffer:
                        out.write("\n".join(buffer) + "\n")
                        count += len(buffer)
                        buffer.clear()
                    break
                buffer.append(item)
                if len(buffer) >= BATCH_SIZE:
                    out.write("\n".join(buffer) + "\n")
                    count += len(buffer)
                    buffer.clear()
            finally:
                results.task_done()
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    # Ask the user up-front what format to write results in.
    output_format = prompt_output_format()

    # Install the silent exception handler on the running event loop.
    asyncio.get_running_loop().set_exception_handler(_silent_exception_handler)

    # Read and de-duplicate the input list, preserving the original line
    # for each unique normalized domain so we can echo it back later if
    # the user picked output format 2.
    try:
        with open(INPUT_FILE, encoding="utf-8") as f:
            raw_lines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Input file '{INPUT_FILE}' not found.")
        sys.exit(1)

    pairs: dict[str, str] = {}
    for line in raw_lines:
        normalized = normalize(line)
        if normalized and normalized not in pairs:
            pairs[normalized] = line  # First occurrence wins for the original spelling.

    domains = sorted(pairs.items(), key=lambda kv: kv[0])
    if not domains:
        print("No valid domains found in input file.")
        return

    # Build the work queues.
    domain_q: asyncio.Queue = asyncio.Queue()
    result_q: asyncio.Queue = asyncio.Queue()

    # Each queue entry is (original_line, normalized_host).
    for normalized, original in domains:
        domain_q.put_nowait((original, normalized))
    # One sentinel per worker so they all terminate cleanly.
    for _ in range(CONCURRENCY):
        domain_q.put_nowait(None)

    # Configure the aiohttp session.
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    resolver = aiohttp.AsyncResolver()
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False, resolver=resolver)

    pbar = tqdm(total=len(domains), desc="Checking", unit="domain")
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=HEADERS
    ) as session:
        writer_task = asyncio.create_task(writer(result_q, OUTPUT_FILE))
        workers = [
            asyncio.create_task(worker(session, domain_q, result_q, pbar, output_format))
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)
        # Tell the writer to flush and exit.
        await result_q.put(None)
        found_count = await writer_task
    pbar.close()

    print(
        f"Checked {len(domains)} domains, found {found_count} sellers.json files. "
        f"Results saved to '{OUTPUT_FILE}'."
    )


if __name__ == "__main__":
    asyncio.run(main())