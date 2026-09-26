# Sellers.json Checker

A fast, asynchronous Python tool for verifying the presence of [`sellers.json`](https://iabtechlab.com/sellers-json/) files on a list of domains.

The script reads domains from an input file, probes the four most common URL variants for each domain (`http`/`https` × bare/`www.`), and writes the successful hits to an output file in one of two user-selected formats.

---

## Features

- **Asynchronous and concurrent** — uses `aiohttp` and `asyncio` with a configurable pool of worker coroutines (default: 50).
- **Smart validation** — accepts only HTTP 200 responses whose `Content-Type` is not HTML or XML, avoiding false positives from generic error pages.
- **Retry logic** — automatically retries on transient errors (HTTP 429, 5xx).
- **Progress bar** — real-time progress via `tqdm`.
- **Two output formats** — choose between the resolved `sellers.json` URL or the original input line.
- **Resilient** — DNS and connection errors are silently treated as misses; the run never aborts on a single bad domain.

---

## Requirements

- Python **3.10+** (uses the `str | None` syntax)
- `aiohttp`
- `tqdm`

Install dependencies:

```bash
pip install aiohttp tqdm
```

---

## Usage

1. Create an `input.txt` file in the same directory as the script. Put one domain or URL per line:

    ```
    example.com
    https://anotherdomain.com
    www.somesite.org
    ```

2. Run the script:

    ```bash
    python find_sellers.py
    ```

3. When prompted, choose the output format:

    ```
    Select output format:
      1. Full sellers.json URL (e.g. https://example.com/sellers.json)
      2. Original URL/domain from input.txt
    Enter 1 or 2:
    ```

4. The results will be written to `output.txt`.

---

## Output formats

### Format 1 — Full sellers.json URL

The resolved (and possibly redirected) URL pointing directly to the `sellers.json` file:

```
https://example.com/sellers.json
https://www.anotherdomain.com/sellers.json
http://somesite.org/sellers.json
```

### Format 2 — Original input

The line exactly as it appeared in `input.txt`, for domains where a `sellers.json` was successfully found:

```
example.com
https://anotherdomain.com
www.somesite.org
```

This is useful when you want to filter your original list down to only the domains that publish a `sellers.json` file.

---

## Configuration

All tunable parameters live at the top of the script:

| Constant       | Default | Description                                                  |
|----------------|---------|--------------------------------------------------------------|
| `INPUT_FILE`   | `input.txt`  | Path to the file containing domains to check.           |
| `OUTPUT_FILE`  | `output.txt` | Path where matched results are written.                 |
| `CONCURRENCY`  | `50`    | Number of concurrent worker coroutines.                      |
| `TIMEOUT`      | `10`    | Per-request timeout in seconds.                              |
| `MAX_RETRIES`  | `2`     | Retry attempts on transient HTTP errors (429, 5xx).          |
| `RETRY_DELAY`  | `5`     | Delay between retries in seconds.                            |
| `BATCH_SIZE`   | `5`     | Number of results buffered in memory before a disk write.    |

Increase `CONCURRENCY` for larger lists; decrease it if you start hitting your network or DNS limits.

---

## How it works

For each input line the script:

1. **Normalizes** the input to a bare hostname (strips scheme, whitespace, `www.` prefix).
2. **Probes** four URL variants in order until one succeeds:
   - `https://{domain}/sellers.json`
   - `https://www.{domain}/sellers.json`
   - `http://{domain}/sellers.json`
   - `http://www.{domain}/sellers.json`
3. **Validates** the response: it must be HTTP 200 with a non-HTML, non-XML `Content-Type`.
4. **Emits** either the resolved URL or the original input line, depending on the selected format.

A single writer coroutine consumes the results queue and flushes batches to disk to minimize I/O overhead.

---

## License

MIT — feel free to adapt for your own needs.