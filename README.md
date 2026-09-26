# Python sellers.json Validator & Ingestion Engine

> A high-throughput, asynchronous engine for probing, validating, and auditing IAB Tech Lab `sellers.json` supply-chain endpoints across enterprise domain inventories.

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![AsyncIO: Powered](https://img.shields.io/badge/Concurrency-AsyncIO%20%2F%20aiohttp-45A049?style=for-the-badge)](https://docs.aiohttp.org/)
[![Status: Production--Ready](https://img.shields.io/badge/Status-Production--Ready-success?style=for-the-badge)]()
[![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000.svg?style=for-the-badge)](https://github.com/psf/black)

---

## Table of Contents

- [Title and Description](#python-sellersjson-validator--ingestion-engine)
- [Table of Contents](#table-of-contents)
- [Features](#features)
- [Tech Stack & Architecture](#tech-stack--architecture)
  - [Core Technologies & Dependencies](#core-technologies--dependencies)
  - [Project Structure](#project-structure)
  - [Key Design Decisions](#key-design-decisions)
  - [Architecture & Data Pipeline](#architecture--data-pipeline)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Testing](#testing)
  - [Running Unit & Integration Tests](#running-unit--integration-tests)
  - [Static Analysis & Linters](#static-analysis--linters)
- [Deployment](#deployment)
  - [Production Guidelines](#production-guidelines)
  - [Containerization (Docker & Docker Compose)](#containerization-docker--docker-compose)
  - [CI/CD Pipeline Integration](#cicd-pipeline-integration)
- [Usage](#usage)
  - [Quickstart: CLI Workflow](#quickstart-cli-workflow)
  - [Output Formats](#output-formats)
  - [Programmatic & Advanced Integration](#programmatic--advanced-integration)
- [Configuration](#configuration)
  - [Core Tuning Parameters](#core-tuning-parameters)
  - [Environment Variables & Overrides](#environment-variables--overrides)
- [License](#license)
- [Support the Project](#support-the-project)

---

## Features

- **Asynchronous Coroutine Worker Pool**  
  Engineered around Python's native `asyncio` event loop and non-blocking `aiohttp.ClientSession`. Orchestrates pools of concurrent worker coroutines (default: 50 concurrent workers) processing thousands of domains per minute without thread overhead or socket starvation.

- **Non-Blocking Asynchronous DNS Resolution**  
  Utilizes `aiohttp.AsyncResolver` backed by `aiodns` / `c-ares` for non-blocking parallel DNS queries, preventing thread pool deadlocks common in high-volume `getaddrinfo` invocations.

- **Deterministic 4-Stage Fallback Probe Matrix**  
  Probes all canonical URL variants per domain sequentially until validation criteria are met:
  1. `https://{domain}/sellers.json`
  2. `https://www.{domain}/sellers.json`
  3. `http://{domain}/sellers.json`
  4. `http://{domain}/sellers.json`

- **Intelligent Anti-False-Positive Validation Engine**  
  Enforces strict RFC and programmatic validation. Rejects HTTP 200 soft-404 error pages, parking pages, and captive portals by validating against text/html and XML MIME types while verifying actual JSON-serving endpoints.

- **Built-in Exponential Backoff & Transient Error Retries**  
  Resilient retry handler intercepts HTTP 429 (Rate Limited / Too Many Requests) and HTTP 5xx transient server responses, scheduling configurable delays before dropping the candidate.

- **Deduplication and Hostname Canonicalization**  
  Normalizes irregular input schemes (`http://`, `https://`, leading `www.`, mixed case, URI paths, and trailing whitespace) while maintaining a strict lookup map to retain original input formatting.

- **Buffered Non-Blocking Disk Flush Pipeline**  
  Decouples network probing from disk I/O through a dedicated asynchronous writer consumer queue. Flushes result buffers in discrete batches (default: 5 records) to drastically reduce OS write syscalls and eliminate disk lock contention.

- **Real-Time Visual Progress Telemetry**  
  Seamless `tqdm` integration provides real-time throughput metrics (domains/sec), remaining batch time, and completion percentage without polluting standard output streams.

- **Dual Ingestion & Extraction Modes**  
  Outputs either the final canonical, fully resolved target URL (post-redirect) or extracts matching original domain names for frictionless down-pipeline filtering.

- **Fail-Safe Exception Isolation**  
  Suppresses low-level network noise (`socket.gaierror`, `asyncio.TimeoutError`, connection drops) via custom event-loop exception handlers while keeping syntax and logic faults audible.

---

## Tech Stack & Architecture

### Core Technologies & Dependencies

| Layer / Component | Technology | Rationale |
|---|---|---|
| **Runtime Environment** | Python `3.10+` | Uses modern PEP 604 union types (`str \| None`), pattern matching, and optimized `asyncio` loop performance. |
| **HTTP Client Library** | `aiohttp >= 3.8.0` | Asynchronous connection pooling, HTTP keep-alive, streaming responses, and custom connection limits. |
| **DNS Subsystem** | `aiodns >= 3.0.0` / `pycares` | Asynchronous DNS resolution preventing POSIX thread-blocking on heavy DNS lookups. |
| **Telemetry & UI** | `tqdm >= 4.65.0` | Terminal progress visualization with minimal computational overhead. |
| **Parsing & URL Engine** | Python `urllib.parse` | Fast standard-library URL parsing and RFC-compliant host decomposition. |

### Project Structure

```
python-sellers-json-validator/
├── find_sellers.py         # Main asynchronous verification and worker pipeline
├── input.txt               # Input target repository (line-separated domains)
├── output.txt              # Validated endpoint output destination
├── cmd_commands.txt        # Shell execution notes and quick reference
├── LICENSE                 # Apache License 2.0 terms
└── README.md               # Technical project documentation
```

<details>
<summary>Click to view deep module breakdown and source file responsibilities</summary>

| File Path | Responsibility | Primary Routines / Artifacts |
|---|---|---|
| `find_sellers.py` | Core engine entry point and orchestrator. | `normalize()`, `prompt_output_format()`, `check()`, `resolve()`, `worker()`, `writer()`, `main()`, `_silent_exception_handler()` |
| `input.txt` | Raw input domain targets. | Supports raw domains, hostnames with protocol prefixes, and subdomains. |
| `output.txt` | Target artifact containing successful resolutions. | Emitted via buffered batch writes based on format selection (Format 1 or 2). |
| `cmd_commands.txt` | Command-line execution cheatsheet. | Quick CLI bootstrap instructions. |
| `LICENSE` | Legal governance file. | Apache License, Version 2.0. |

</details>

### Key Design Decisions

1. **Producer-Consumer Queue Decoupling (`asyncio.Queue`)**  
   The scanner separates network I/O from disk I/O. Coroutine workers fetch targets from `domain_q`, probe endpoints, and append matching responses to `result_q`. A dedicated `writer` coroutine drains `result_q`, amortizing disk write overhead through batch buffering.
2. **Deterministic Fallback Hierarchy**  
   Because modern publishers default to SSL, the probe sequence tests `https://` variants before `http://`. By checking bare domains before `www.` subdomains, redundant redirects are minimized.
3. **MIME-Type & Soft-404 Disqualification**  
   Many misconfigured web servers or CDNs return HTTP status 200 for missing pages with an HTML landing page. The validator inspects `Content-Type`, immediately discarding responses containing `text/html` or `xml`.
4. **Global Connector Optimization**  
   A single shared `aiohttp.TCPConnector` with an asynchronous DNS resolver (`AsyncResolver`) and connection pooling guarantees efficient socket reuse and prevents operating-system-level file-descriptor exhaustion.

### Architecture & Data Pipeline

<details open>
<summary><b>System Architecture Diagram</b> (Click to collapse)</summary>

```mermaid
flowchart TD
    subgraph Initialization ["1. Pipeline Initialization & Normalization"]
        A[input.txt] -->|Read & Strip| B[Deduplication Map]
        B -->|Host Normalization: strip www, schemes| C[Canonical Host Pairs]
        C -->|Enqueue Tuples: raw, normalized| D[(domain_q: asyncio.Queue)]
    end

    subgraph WorkerPool ["2. Asynchronous Worker Pool (CONCURRENCY=50)"]
        D -->|Worker 1..N Dequeue| W[Worker Coroutines]
        W -->|Probe: https://domain/sellers.json| P1{Status 200 & Valid Content-Type?}
        P1 -- No -->|Probe: https://www.domain/sellers.json| P2{Status 200 & Valid Content-Type?}
        P2 -- No -->|Probe: http://domain/sellers.json| P3{Status 200 & Valid Content-Type?}
        P3 -- No -->|Probe: http://www.domain/sellers.json| P4{Status 200 & Valid Content-Type?}
        
        P1 -- Yes --> Match[Validated sellers.json Endpoint]
        P2 -- Yes --> Match
        P3 -- Yes --> Match
        P4 -- Yes --> Match
        P4 -- No --> Miss[Discard / Silent Miss]
    end

    subgraph ResultSink ["3. Result Processing & Buffered Disk Flush"]
        Match -->|Format 1: Resolved URL / Format 2: Original Input| R[(result_q: asyncio.Queue)]
        R -->|Drain & Buffer| Writer[Writer Coroutine]
        Writer -->|Buffer >= BATCH_SIZE| Flush[Atomic File Write]
        Flush --> Out[output.txt]
    end
```

</details>

---

## Getting Started

### Prerequisites

- **Python**: Version `3.10` or higher (`str | None` syntax requires >= 3.10)
- **Virtual Environment**: `venv` or `conda` (recommended)
- **C Compiler (Optional)**: Required if compiling `pycares` from source for `aiodns`

Check your Python environment:

```bash
python3 --version
```

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/adops-tool/python-sellers-json-validator.git
   cd python-sellers-json-validator
   ```

2. **Initialize and activate an isolated virtual environment:**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install runtime dependencies:**

   ```bash
   pip install --upgrade pip
   pip install aiohttp aiodns tqdm
   ```

> [!NOTE]
> Installing `aiodns` enables asynchronous DNS resolution via `c-ares`. If omitted, Python will fall back to default socket getaddrinfo calls, which can degrade scanning throughput under high concurrency.

<details>
<summary>Troubleshooting & Alternative Installation Methods</summary>

#### Compiling with Custom wheels or Locked Dependencies
To lock exact dependency trees for auditability, generate a `requirements.txt`:

```bash
cat <<EOF > requirements.txt
aiohttp>=3.9.0
aiodns>=3.1.1
pycares>=4.4.0
tqdm>=4.66.1
EOF

pip install -r requirements.txt
```

#### Fixing `c-ares` / `aiodns` compilation errors on Alpine or Minimal Linux
If compiling `aiodns` fails due to missing C development headers:

```bash
# Debian / Ubuntu
sudo apt-get update && sudo apt-get install -y gcc python3-dev libcares-dev

# Alpine Linux
apk add --no-cache gcc musl-dev python3-dev c-ares-dev
```

#### Running on Windows
If experiencing `asyncio` event loop issues on Windows platforms, ensure the default ProactorEventLoop is active (default in Python 3.8+).

</details>

---

## Testing

The testing suite validates input normalization, network probe resilience, transient HTTP status retry algorithms, and queue ingestion.

### Running Unit & Integration Tests

Execute the suite using `pytest` and `pytest-asyncio`:

```bash
# Install test harness
pip install pytest pytest-asyncio aioresponses pytest-mock

# Run all test suites
pytest -v
```

<details>
<summary>Click to view synthetic unit test example (`test_sellers_validator.py`)</summary>

Save the following as `test_sellers_validator.py` to test core engine logic locally:

```python
import pytest
from aioresponses import aioresponses
import aiohttp
from find_sellers import normalize, check

def test_normalization():
    assert normalize("http://example.com") == "example.com"
    assert normalize("https://www.company.org/path/test") == "company.org"
    assert normalize("  SUB.DOMAIN.NET  ") == "sub.domain.net"
    assert normalize("") == ""

@pytest.mark.asyncio
async def test_check_valid_sellers_json():
    target_url = "https://example.com/sellers.json"
    async with aioresponses() as m:
        m.get(
            target_url,
            status=200,
            headers={"Content-Type": "application/json"},
            body='{"seller_id": "pub-1234"}'
        )
        async with aiohttp.ClientSession() as session:
            result = await check(session, target_url)
            assert result == target_url

@pytest.mark.asyncio
async def test_check_rejects_html_soft_404():
    target_url = "https://example.com/sellers.json"
    async with aioresponses() as m:
        m.get(
            target_url,
            status=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            body='<html><body>Not Found</body></html>'
        )
        async with aiohttp.ClientSession() as session:
            result = await check(session, target_url)
            assert result is None
```

</details>

### Static Analysis & Linters

Maintain clean code quality, PEP 8 compliance, and type adherence:

```bash
# Static typing checks
pip install mypy
mypy --ignore-missing-imports find_sellers.py

# Linting and stylistic conformance
pip install flake8 black
flake8 find_sellers.py --max-line-length=100
black --check find_sellers.py
```

---

## Deployment

### Production Guidelines

When deploying this scanner in enterprise ad-tech environments or large-scale inventory validation runs:
- **File Descriptor Limits**: Ensure the operating system permits adequate open file descriptors for the desired concurrency:
  ```bash
  ulimit -n 65535
  ```
- **DNS Rate Limiting**: Ensure your recursive DNS resolver (e.g., local Unbound daemon, internal DNS cache, or public resolver like 1.1.1.1 / 8.8.8.8) is sized to withstand burst query traffic.
- **Network Bandwidth & Edge Proxies**: For large enterprise runs (>100,000 domains), execute inside a cloud instance (AWS EC2, GCP Compute Engine) close to tier-1 transit providers to avoid local ISP connection tracking table exhaustion.

### Containerization (Docker & Docker Compose)

Deploy the validator as a lightweight, reproducible container.

<details open>
<summary><b>Dockerfile Specification</b> (Click to collapse)</summary>

```dockerfile
FROM python:3.11-slim-bullseye

# Install security updates and build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libcares-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency specifications and source
COPY find_sellers.py input.txt ./

# Install runtime dependencies
RUN pip install --no-cache-dir aiohttp aiodns tqdm

# Run with unbuffered output
ENV PYTHONUNBUFFERED=1

CMD ["python", "find_sellers.py"]
```

</details>

<details>
<summary>Docker Compose Orchestration (`docker-compose.yml`)</summary>

```yaml
version: '3.8'

services:
  sellers-validator:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: sellers-json-validator
    volumes:
      - ./input.txt:/app/input.txt:ro
      - ./output.txt:/app/output.txt:rw
    stdin_open: true
    tty: true
    restart: "no"
```

To run with Docker Compose:

```bash
docker compose build
docker compose run --rm sellers-validator
```

</details>

### CI/CD Pipeline Integration

Integrate automated supply chain audits into GitHub Actions workflows to continuously verify partner inventory validity.

<details>
<summary>GitHub Actions Workflow (`.github/workflows/audit.yml`)</summary>

```yaml
name: Partner sellers.json Verification

on:
  schedule:
    - cron: '0 2 * * 1' # Run every Monday at 02:00 UTC
  workflow_dispatch:

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install aiohttp aiodns tqdm

      - name: Execute sellers.json audit
        run: |
          # Automatically pipe format selection (1 = Resolved URLs)
          echo "1" | python find_sellers.py

      - name: Archive validation results
        uses: actions/upload-artifact@v4
        with:
          name: sellers-json-output
          path: output.txt
          retention-days: 14
```

</details>

---

## Usage

### Quickstart: CLI Workflow

1. **Populate `input.txt`**:
   Add target domains or URLs (one per line). Raw domains, fully qualified protocols, and `www.` subdomains are automatically normalized:
   ```text
   example.com
   https://rubiconproject.com
   www.appnexus.com
   openx.com/sellers.json
   invalid-domain-entry-test.org
   ```

2. **Execute the script**:
   ```bash
   python find_sellers.py
   ```

3. **Select the output mode when prompted**:
   ```text
   Select output format:
     1. Full sellers.json URL (e.g. https://example.com/sellers.json)
     2. Original URL/domain from input.txt
   Enter 1 or 2: 1
   ```

4. **Observe real-time telemetry**:
   ```text
   Checking: 100%|██████████████████████████████| 1290/1290 [00:18<00:00, 71.42domain/s]
   Checked 1290 domains, found 55 sellers.json files. Results saved to 'output.txt'.
   ```

### Output Formats

The validator provides two output format modes:

#### Format 1: Canonical Resolved URL
Saves the fully resolved (and redirected) target endpoint that verified as an authentic `sellers.json` location:
```text
https://example.com/sellers.json
https://www.rubiconproject.com/sellers.json
https://adnxs.com/sellers.json
```

#### Format 2: Original Input Representation
Writes the exact line string originally supplied in `input.txt`. This mode is designed for list filtering and data hygiene pipelines:
```text
example.com
https://rubiconproject.com
www.appnexus.com
```

> [!TIP]
> Use **Format 2** when your objective is to sanitize an ad-tech CRM list, DSP allowlist, or publisher directory to keep only domains that actively publish a `sellers.json` file.

<details>
<summary>Advanced Usage: Headless & Non-Interactive Ingestion</summary>

For automated or scripted execution where standard input cannot prompt a user, pipe the format choice directly into the interpreter:

```bash
# Non-interactive execution selecting Format 1
python find_sellers.py <<EOF
1
EOF

# Non-interactive execution selecting Format 2 via printf
printf "2\n" | python find_sellers.py
```

</details>

<details>
<summary>Advanced Usage: Embedding as an Asynchronous Python Module</summary>

You can import and consume components of `find_sellers.py` directly inside your own application services:

```python
import asyncio
import aiohttp
from find_sellers import resolve, normalize

async def audit_domain(domain: str):
    clean_domain = normalize(domain)
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(ssl=False)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        endpoint = await resolve(session, clean_domain)
        if endpoint:
            print(f"[FOUND] {domain} -> {endpoint}")
        else:
            print(f"[MISS]  {domain} does not serve a valid sellers.json")

if __name__ == "__main__":
    asyncio.run(audit_domain("rubiconproject.com"))
```

</details>

<details>
<summary>Edge Cases & Defensive Behaviors Handled</summary>

| Edge Case Scenario | Engine Mitigation Strategy |
|---|---|
| **Soft-404 / Captive Portal** | Rejects responses with MIME-type `text/html` or `xml`, even when responding with status code `200 OK`. |
| **Infinite Redirect Loops** | Managed by `aiohttp` default redirect hops cap (`max_redirects=10`). |
| **Slowloris / Hanging Sockets** | Enforces a strict total per-request `TIMEOUT` (default: 10s) covering connect, read, and socket handling. |
| **Aggressive Cloud WAFs** | Outgoing requests include realistic browser `User-Agent` and `Accept: application/json` headers to avoid naive bot fingerprint filters. |
| **Unresolvable Hostnames** | Suppressed via `_silent_exception_handler` to avoid terminal clutter while counting the target as an unvalidated miss. |

</details>

---

## Configuration

All runtime variables and tuning parameters are defined at the top of `find_sellers.py`:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `INPUT_FILE` | `str` | `"input.txt"` | File path containing the list of target domains to inspect. |
| `OUTPUT_FILE` | `str` | `"output.txt"` | Destination file path where validated outputs are persisted. |
| `CONCURRENCY` | `int` | `50` | Maximum number of concurrent worker coroutines and connection pool size. |
| `TIMEOUT` | `int` | `10` | Total maximum time in seconds allocated per HTTP attempt. |
| `MAX_RETRIES` | `int` | `2` | Maximum retry attempts upon receiving HTTP 429 or 5xx server responses. |
| `RETRY_DELAY` | `int` | `5` | Backoff wait time (in seconds) between retry attempts. |
| `BATCH_SIZE` | `int` | `5` | In-memory buffer threshold before flushing results to disk. |
| `HEADERS` | `dict` | *Realistic Chrome UA* | Default HTTP header payload transmitted across probe attempts. |

> [!IMPORTANT]
> If tuning `CONCURRENCY` above `200`, verify your system's `ulimit -n` setting and ensure your local DNS server does not throttle outbound UDP lookup queries.

<details>
<summary>Configuration Table: Advanced Tuning & Environment Variable Mapping</summary>

To dynamically configure parameters without modifying source files, apply the following environment variable mapping pattern:

```python
import os

INPUT_FILE = os.getenv("VALIDATOR_INPUT_FILE", "input.txt")
OUTPUT_FILE = os.getenv("VALIDATOR_OUTPUT_FILE", "output.txt")
CONCURRENCY = int(os.getenv("VALIDATOR_CONCURRENCY", "50"))
TIMEOUT = int(os.getenv("VALIDATOR_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("VALIDATOR_MAX_RETRIES", "2"))
RETRY_DELAY = int(os.getenv("VALIDATOR_RETRY_DELAY", "5"))
BATCH_SIZE = int(os.getenv("VALIDATOR_BATCH_SIZE", "5"))
```

#### Example `.env` file schema:
```ini
# Validator Runtime Environment Settings
VALIDATOR_INPUT_FILE=targets/domains.txt
VALIDATOR_OUTPUT_FILE=dist/validated_sellers.json
VALIDATOR_CONCURRENCY=100
VALIDATOR_TIMEOUT=15
VALIDATOR_MAX_RETRIES=3
VALIDATOR_RETRY_DELAY=2
VALIDATOR_BATCH_SIZE=25
```

</details>

---

## License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for details.

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## Support the Project

[![Patreon](https://img.shields.io/badge/Patreon-OstinFCT-f96854?style=flat-square&logo=patreon)](https://www.patreon.com/OstinFCT)
[![Ko-fi](https://img.shields.io/badge/Ko--fi-fctostin-29abe0?style=flat-square&logo=ko-fi)](https://ko-fi.com/fctostin)
[![Boosty](https://img.shields.io/badge/Boosty-Support-f15f2c?style=flat-square)](https://boosty.to/ostinfct)
[![YouTube](https://img.shields.io/badge/YouTube-FCT--Ostin-red?style=flat-square&logo=youtube)](https://www.youtube.com/@FCT-Ostin)
[![Telegram](https://img.shields.io/badge/Telegram-FCTostin-2ca5e0?style=flat-square&logo=telegram)](https://t.me/FCTostin)

If you find this tool useful, consider leaving a star on GitHub or supporting the author directly.
