import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import subprocess
import json
import csv
import time
import os
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar variables de entorno desde .env
env_file_path = Path(__file__).parent / ".env"
if env_file_path.exists():
    with open(env_file_path) as env_file:
        for env_line in env_file:
            if '=' in env_line:
                env_key, env_value = env_line.strip().split('=', 1)
                os.environ[env_key] = env_value

# Configuración del proxy
PROXY_USER = os.environ.get("PROXY_USER")
PROXY_PASS = os.environ.get("PROXY_PASS")
PROXY_HOST = os.environ.get("PROXY_HOST")
PROXY_PORT = os.environ.get("PROXY_PORT")
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Configuración de paginación
COMMENTS_PER_BATCH = 50
BATCHES_BEFORE_TOKEN_REFRESH = 15
MAX_RETRIES = 3
MAX_WORKERS = 3
MAX_COMMENTS = 10000
SAVE_EVERY_N_BATCHES = 5

# Directorio para cache
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# Lock para print thread-safe
print_lock = threading.Lock()

# Videos de prueba
TEST_VIDEOS = [
    ("7120798207600299310", "keemokazi"),
]


def safe_print(message):
    with print_lock:
        print(message)


def get_proxy_ip():
    curl_command = ['curl', '-s', '--proxy', PROXY_URL, '--connect-timeout', '10', 'https://api.ipify.org?format=json']
    try:
        curl_result = subprocess.run(curl_command, capture_output=True, timeout=15)
        api_response = json.loads(curl_result.stdout.decode())
        return api_response.get("ip")
    except:
        return "?"


def rotate_proxy():
    try:
        curl_command = ['curl', '-s', '--proxy', PROXY_URL, '--connect-timeout', '10', 'https://api.ipify.org?format=json']
        curl_result = subprocess.run(curl_command, capture_output=True, timeout=15)
        api_response = json.loads(curl_result.stdout.decode())
        return api_response.get("ip", "?")
    except:
        return None


class TokenManager:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.current_token = None
        self.current_ip = None
        self.batch_count = 0
        self.total_tokens_used = 0
        self.lock = threading.Lock()

    def rotate_proxy_and_get_token(self):
        safe_print(f"      [W{self.worker_id}][PROXY] Rotando proxy...")
        new_ip = rotate_proxy()
        if new_ip:
            safe_print(f"      [W{self.worker_id}][PROXY] Nueva IP: {new_ip}")
            self.current_ip = new_ip
        return self.get_fresh_token()

    def get_fresh_token(self):
        curl_command = [
            'curl', '-s', '-c', '-', '--proxy', PROXY_URL, '--connect-timeout', '15',
            'https://www.tiktok.com/api/recommend/item_list/?aid=1988&count=1',
            '-H', f'user-agent: {USER_AGENT}',
        ]
        try:
            curl_result = subprocess.run(curl_command, capture_output=True, timeout=20)
            curl_output = curl_result.stdout.decode('utf-8', errors='replace')
            for cookie_line in curl_output.split('\n'):
                if 'msToken' in cookie_line:
                    cookie_parts = cookie_line.split('\t')
                    if len(cookie_parts) >= 7:
                        with self.lock:
                            self.current_token = cookie_parts[-1].strip()
                            self.batch_count = 0
                            self.total_tokens_used += 1
                        return self.current_token
        except Exception as error:
            safe_print(f"      [W{self.worker_id}] Error obteniendo token: {error}")
        return None

    def get_token(self, force_refresh=False):
        with self.lock:
            needs_refresh = (
                self.current_token is None or
                self.batch_count >= BATCHES_BEFORE_TOKEN_REFRESH or
                force_refresh
            )
        if needs_refresh:
            safe_print(f"      [W{self.worker_id}][TOKEN] Rotando msToken (batches: {self.batch_count})...")
            return self.get_fresh_token()
        return self.current_token

    def increment_batch(self):
        with self.lock:
            self.batch_count += 1


def fetch_comment_batch(video_id: str, cursor: int, mstoken: str) -> dict:
    api_url = f"https://www.tiktok.com/api/comment/list/?aid=1988&aweme_id={video_id}&count={COMMENTS_PER_BATCH}&cursor={cursor}&msToken={mstoken}"
    curl_command = [
        'curl', '-s', '--proxy', PROXY_URL, '--connect-timeout', '15', api_url,
        '-H', f'user-agent: {USER_AGENT}',
        '-H', 'referer: https://www.tiktok.com/',
    ]

    try:
        curl_result = subprocess.run(curl_command, capture_output=True, timeout=20)
        response_body = curl_result.stdout.decode('utf-8', errors='replace')

        if response_body:
            api_response = json.loads(response_body)
            page_comments = api_response.get("comments", [])

            return {
                "success": len(page_comments) > 0,
                "comments": page_comments,
                "has_more": api_response.get("has_more", False),
                "next_cursor": api_response.get("cursor", cursor + COMMENTS_PER_BATCH),
            }
    except:
        pass

    return {"success": False, "comments": [], "has_more": False, "next_cursor": cursor}


def save_cache(worker_id: int, video_id: str, comments: list, seen_cids: set, batch_number: int, cursor: int, is_final: bool = False):
    cache_file = CACHE_DIR / f"worker_{worker_id}_{video_id}.json"

    cache_data = {
        "video_id": video_id,
        "worker_id": worker_id,
        "timestamp": datetime.now().isoformat(),
        "batch_number": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": is_final,
        "seen_cids": list(seen_cids),
        "comments": comments
    }

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False)

    if not is_final:
        safe_print(f"      [W{worker_id}][CACHE] Guardado: {len(comments)} comentarios (batch {batch_number})")


def load_cache(worker_id: int, video_id: str) -> dict:
    cache_file = CACHE_DIR / f"worker_{worker_id}_{video_id}.json"

    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return None


def scrape_video_comments(video_id: str, video_author: str, worker_id: int) -> dict:
    token_manager = TokenManager(worker_id)

    # Intentar cargar cache existente
    cached = load_cache(worker_id, video_id)
    if cached and not cached.get("is_complete", False):
        all_comments = cached.get("comments", [])
        seen_cids = set(cached.get("seen_cids", []))
        cursor = cached.get("cursor", 0)
        batch_number = cached.get("batch_number", 0)
        safe_print(f"\n [W{worker_id}] RESUMIENDO desde cache: {len(all_comments)} comentarios, batch {batch_number}")
    else:
        all_comments = []
        seen_cids = set()  # Set para deduplicar O(1)
        cursor = 0
        batch_number = 0

    consecutive_failures = 0
    proxy_rotations = 0
    duplicates_skipped = 0

    safe_print(f"\n [W{worker_id}] INICIANDO: {video_id} (@{video_author})")
    safe_print(f" [W{worker_id}] {'─'*55}")

    while True:
        batch_number += 1

        mstoken = token_manager.get_token()
        if not mstoken:
            consecutive_failures += 1
            if consecutive_failures >= MAX_RETRIES:
                safe_print(f"      [W{worker_id}] No se pudo obtener token después de {MAX_RETRIES} intentos")
                break
            time.sleep(1)
            continue

        token_short = mstoken[:8] + "..."
        safe_print(f"      [W{worker_id}][BATCH {batch_number:3d}] cursor={cursor:<6} token={token_short} total={len(all_comments):<5}")

        batch_result = fetch_comment_batch(video_id, cursor, mstoken)

        if batch_result["success"]:
            # Deduplicar en O(1) usando set
            new_comments = []
            for comment in batch_result["comments"]:
                cid = comment.get('cid')
                if cid and cid not in seen_cids:
                    seen_cids.add(cid)
                    new_comments.append(comment)
                else:
                    duplicates_skipped += 1

            all_comments.extend(new_comments)
            token_manager.increment_batch()
            consecutive_failures = 0

            # Guardar cache cada N batches
            if batch_number % SAVE_EVERY_N_BATCHES == 0:
                save_cache(worker_id, video_id, all_comments, seen_cids, batch_number, batch_result["next_cursor"])

            # Verificar límite
            if len(all_comments) >= MAX_COMMENTS:
                safe_print(f"      [W{worker_id}][LIMITE] Alcanzado {MAX_COMMENTS} comentarios")
                break

            if not batch_result["has_more"]:
                safe_print(f"      [W{worker_id}][FIN] No hay más comentarios")
                break

            cursor = batch_result["next_cursor"]
            time.sleep(0.3)

        else:
            consecutive_failures += 1
            safe_print(f"      [W{worker_id}][BATCH {batch_number:3d}] ✗ retry {consecutive_failures}/{MAX_RETRIES}")

            if consecutive_failures >= MAX_RETRIES:
                safe_print(f"      [W{worker_id}] Max retries - rotando PROXY + TOKEN...")
                proxy_rotations += 1
                token_manager.rotate_proxy_and_get_token()
                consecutive_failures = 0
                save_cache(worker_id, video_id, all_comments, seen_cids, batch_number, cursor)

            time.sleep(1)

    # Trim a exactamente MAX_COMMENTS
    if len(all_comments) > MAX_COMMENTS:
        all_comments = all_comments[:MAX_COMMENTS]

    # Guardar cache final
    save_cache(worker_id, video_id, all_comments, seen_cids, batch_number, cursor, is_final=True)

    result = {
        "video_id": video_id,
        "author": video_author,
        "worker_id": worker_id,
        "total_comments": len(all_comments),
        "total_batches": batch_number,
        "tokens_used": token_manager.total_tokens_used,
        "proxy_rotations": proxy_rotations,
        "duplicates_skipped": duplicates_skipped,
        "comments": all_comments,
    }

    safe_print(f"\n [W{worker_id}] COMPLETADO: {video_id}")
    safe_print(f" [W{worker_id}] → {result['total_comments']} comentarios | {duplicates_skipped} duplicados omitidos | {result['tokens_used']} tokens")

    return result


def save_to_csv(results: list, filepath: Path):
    """Guarda comentarios en CSV"""
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow([
            'video_id', 'video_author', 'comment_id', 'username', 'nickname',
            'text', 'likes', 'replies', 'create_time', 'create_date'
        ])
        # Data
        for video_result in results:
            video_id = video_result.get('video_id', '')
            author = video_result.get('author', '')
            for comment in video_result.get('comments', []):
                user = comment.get('user', {})
                create_time = comment.get('create_time', 0)
                create_date = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d %H:%M:%S') if create_time else ''

                writer.writerow([
                    video_id,
                    author,
                    comment.get('cid', ''),
                    user.get('unique_id', ''),
                    user.get('nickname', ''),
                    comment.get('text', ''),
                    comment.get('digg_count', 0),
                    comment.get('reply_comment_total', 0),
                    create_time,
                    create_date
                ])


def main():
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 10 + "TIKTOK SCRAPER - DEDUPLICADO + CSV OUTPUT" + " " * 10 + "║")
    print("║" + " " * 8 + f"Workers: {MAX_WORKERS} | Max {MAX_COMMENTS} comments | Dedup: ON" + " " * 11 + "║")
    print("╚" + "═" * 68 + "╝")

    print(f"\n Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Proxy: {PROXY_HOST}:{PROXY_PORT}")
    print(f" Videos a procesar: {len(TEST_VIDEOS)}")

    print(f"\n Obteniendo IP del proxy...", end=" ", flush=True)
    initial_proxy_ip = get_proxy_ip()
    print(f"IP: {initial_proxy_ip}")

    print(f"\n{'═'*70}")
    print("INICIANDO SCRAPING...")
    print(f"{'═'*70}")

    all_results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_video = {
            executor.submit(scrape_video_comments, video_id, author, idx): (video_id, author)
            for idx, (video_id, author) in enumerate(TEST_VIDEOS, 1)
        }

        for future in as_completed(future_to_video):
            video_id, author = future_to_video[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as exc:
                safe_print(f" [ERROR] {video_id}: {exc}")
                all_results.append({
                    "video_id": video_id,
                    "author": author,
                    "total_comments": 0,
                    "comments": [],
                    "error": str(exc)
                })

    elapsed_time = time.time() - start_time

    # Resumen
    print(f"\n{'═'*70}")
    print("RESUMEN FINAL")
    print(f"{'═'*70}")

    total_comments = sum(r["total_comments"] for r in all_results)
    total_batches = sum(r.get("total_batches", 0) for r in all_results)
    total_tokens = sum(r.get("tokens_used", 0) for r in all_results)
    total_duplicates = sum(r.get("duplicates_skipped", 0) for r in all_results)

    print(f" Videos procesados: {len(all_results)}")
    print(f" Comentarios únicos: {total_comments}")
    print(f" Duplicados omitidos: {total_duplicates}")
    print(f" Batches totales: {total_batches}")
    print(f" Tokens usados: {total_tokens}")
    print(f" Tiempo total: {elapsed_time:.1f} segundos")
    print(f" Velocidad: {total_comments/max(elapsed_time,1):.1f} comentarios/segundo")

    # Guardar JSON
    json_path = Path(__file__).parent / "RESULTADOS.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "proxy_ip": initial_proxy_ip,
            "config": {
                "max_comments": MAX_COMMENTS,
                "deduplication": True,
            },
            "summary": {
                "total_comments": total_comments,
                "duplicates_skipped": total_duplicates,
                "elapsed_seconds": elapsed_time,
            },
            "videos": all_results
        }, f, ensure_ascii=False, indent=2)

    # Guardar CSV
    csv_path = Path(__file__).parent / "RESULTADOS.csv"
    save_to_csv(all_results, csv_path)

    print(f"\n Guardado JSON: {json_path.name}")
    print(f" Guardado CSV: {csv_path.name}")

    # Muestra
    if all_results and all_results[0].get("comments"):
        print(f"\n{'═'*70}")
        print("MUESTRA (primeros 5):")
        print(f"{'═'*70}")
        for i, c in enumerate(all_results[0]["comments"][:5], 1):
            user = c.get("user", {}).get("unique_id", "?")
            text = c.get("text", "")[:40]
            likes = c.get("digg_count", 0)
            print(f" {i}. @{user}: \"{text}...\" [{likes} likes]")

    print(f"\n{'═'*70}")
    print("COMPLETADO")
    print(f"{'═'*70}")


if __name__ == "__main__":
    main()
