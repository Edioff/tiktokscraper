# TikTok Scraper

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Performance](https://img.shields.io/badge/Speed-667%20comments%2Fmin-brightgreen?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

> High-performance TikTok comment scraper capable of extracting **667 comments per minute** using a hybrid API + scraping approach.

## Overview

Extracts comments from TikTok videos at scale. Uses a hybrid approach combining TikTok's internal API endpoints with web scraping techniques to achieve high throughput while maintaining session stability.

## Features

- **667 comments/minute** extraction speed
- **Hybrid approach** — API endpoints + web scraping fallback
- **Session management** — Automatic cookie refresh and rotation
- **Structured output** — JSON and CSV export
- **Rate limiting** — Configurable delays to avoid detection
- **Pagination** — Handles cursor-based pagination automatically

## Output Format

Exports to both JSON and CSV with fields including:
- Comment text and timestamp
- Author username and ID
- Like count
- Reply count
- Parent comment reference (for threaded replies)

## Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)

- **Python 3.10+**
- **requests/httpx** — HTTP client with session management
- **JSON/CSV** — Dual output format

## Installation

```bash
git clone https://github.com/Edioff/tiktokscraper.git
cd tiktokscraper
pip install -r requirements.txt
```

## Usage

```bash
python tiktokscraper.py
```

Configure target videos and output settings in the script.

## Performance

| Metric | Value |
|--------|-------|
| Extraction speed | **667 comments/minute** |
| Output formats | JSON, CSV |
| Pagination | Automatic cursor-based |
| Session recovery | Automatic |

## Notes

- For educational and research purposes
- Respect TikTok's Terms of Service and rate limits
- Requires valid session cookies for API access

## Author

**Johan Cruz** — Data Engineer & Web Scraping Specialist
- GitHub: [@Edioff](https://github.com/Edioff)
- Available for freelance projects

## License

MIT
