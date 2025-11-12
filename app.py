import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from urllib.parse import quote_plus, urljoin
from typing import Optional, List, Set, Tuple, Dict, Any
from copy import copy as shallow_copy
from threading import Lock

import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, url_for, request, jsonify
from openpyxl import load_workbook
from openpyxl.cell import Cell

# Playwright imports (optional, теперь не обязательно для IPRbooks)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
    PLAYWRIGHT_LOCK = Lock()
except ImportError:
    print("[INFO] Playwright не установлен. Используется requests для IPRbooks поиска.")
    sync_playwright = None
    PlaywrightTimeoutError = Exception
    PLAYWRIGHT_AVAILABLE = False
    PLAYWRIGHT_LOCK = None

BANNED_SUBSTRINGS = (
    "иностран",
    "физкульт", 
    "физическая культура",
    "физкультура",
    "история казахстана",
    "информационно",
    "коммуникативн",
    "философия",
    "шетел тілі",
    "қазақ",
    "орыс",
    "тілі",
    "қазақстан тарихы",
    "ақпараттық",
    "коммуникациялық",
    "технологиялар",
    "дене шынықтыру",
    "жббп",
    "бп жк",
    "бп тк", 
    "бп циклі",
    "кп жк",
    "кп тк",
    "кп циклі",
    "циклі бойынша",
    "по циклу",
    "оод ок",
    "оод вк",
    "оод",
    "бд вк",
    "бд кв",
    "бд",
    "пд вк", 
    "пд кв",
    "пд",
    "эирм",
    "қорытынды аттестаттау",
    "итоговая аттестация",
    "final certification",
    "академиялық департамент",
    "директор академического департамента",
    "нужно так",
)
SUBJECT_EXCLUDE_SUBSTRINGS = (
    "учебная нагрузка",
    "учебн",
    "семинар",
    "практичес",
    "лаборатор",
    "самостоятель",
    "руководител",
    "организац",
    "декан",
    "заместитель",
    "профессор",
    "кандидат",
    "научно",
    "деятельност",
    "работ",
)
MAX_WORKERS = 8
PLAYWRIGHT_LOCK = Lock() if PLAYWRIGHT_AVAILABLE else None

KNOWN_RESOURCE_RULES = [
    {
        "contains": ("физ", "культур"),
        "resources": [
            {
                "title": "Веденина О.А. Самостоятельные занятия физической культурой : учебное пособие для СПО / Веденина О.А.. — Саратов, Москва : Профобразование, Ай Пи Ар Медиа, 2025.",
                "url": "https://www.iprbookshop.ru/147933.html",
                "status": "success",
                "note": "Учебное пособие IPRbooks",
            },
            {
                "title": "Гришина, Ю.И. Физическая культура студента: учеб. пособие. — Ростов н/Д: Феникс, 2019. — 283 c. — (Высшее образование). — ISBN 978-5-222-31286-5.",
                "url": "https://rmebrk.kz/book/1177773",
                "status": "warning",
                "note": "Доступ на rmebrk.kz",
            },
        ],
    },
]

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html", current_year=datetime.now().year)


@app.route("/search")
def search():
    return render_template("search.html", current_year=datetime.now().year)


def extract_subjects_from_up33(up_file_stream) -> List[str]:
    wb = load_workbook(up_file_stream, data_only=True)
    ws = wb.active
    subjects: List[str] = []
    seen: Set[str] = set()
    for r in range(2, ws.max_row + 1):
        c_val = ws.cell(row=r, column=3).value
        d_val = ws.cell(row=r, column=4).value
        c_text = str(c_val).strip() if c_val is not None else ""
        d_text = str(d_val).strip() if d_val is not None else ""
        if not c_text and not d_text:
            continue
        header_tokens = {"наимен", "предмет", "дисциплин"}
        combined_text = " ".join([c_text.lower(), d_text.lower()]).strip()
        if any(token in combined_text for token in header_tokens):
            continue
        merged = " / ".join([text for text in (c_text, d_text) if text])
        
        # Извлекаем только русскую часть (после "/")
        if " / " in merged:
            parts = merged.split(" / ")
            # Берем последнюю часть (обычно русская)
            russian_part = parts[-1].strip()
            if russian_part:
                merged = russian_part
        
        merged_lower = merged.lower()
        if any(bad in merged_lower for bad in SUBJECT_EXCLUDE_SUBSTRINGS):
            continue
        if any(bad in merged_lower for bad in BANNED_SUBSTRINGS):
            continue
        if len(merged_lower) > 160 or len(merged_lower.split()) > 20:
            continue
        norm = merged.strip().lower()
        if merged and norm not in seen:
            seen.add(norm)
            subjects.append(merged)
    return subjects


def find_next_row(ws, start_row: int) -> int:
    r = start_row
    while True:
        a = ws.cell(row=r, column=1).value
        b = ws.cell(row=r, column=2).value
        c = ws.cell(row=r, column=3).value
        d = ws.cell(row=r, column=4).value
        if not any([a, b, c, d]):
            return r
        r += 1


def compute_next_number(ws, start_row: int) -> int:
    max_num = 0
    for r in range(start_row, start_row + 2000):
        v = ws.cell(row=r, column=1).value
        if isinstance(v, (int, float)):
            try:
                iv = int(v)
                if iv > max_num:
                    max_num = iv
            except Exception:
                pass
        elif isinstance(v, str) and v.strip().isdigit():
            iv = int(v.strip())
            if iv > max_num:
                max_num = iv
    return max_num + 1 if max_num >= 0 else 1


def search_urait_multiple_results(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    """Ищет несколько результатов на Юрайт"""
    search_url = f"https://urait.ru/search?words={quote_plus(query)}"
    print(f"[DEBUG] Юрайт поиск множественных результатов: {search_url}")
    results = []
    
    try:
        response = requests.get(search_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Ищем все ссылки на книги
        book_links = soup.select("a[href*='/book/']")
        print(f"[DEBUG] Юрайт найдено ссылок на книги: {len(book_links)}")
        
        seen_urls = set()
        for link in book_links[:max_results * 2]:  # Берем больше чтобы отфильтровать дубли
            href = link.get("href", "")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)
            
            # Получаем название книги
            title = link.get_text(strip=True)
            if not title:
                # Ищем заголовок рядом
                parent = link.find_parent()
                if parent:
                    title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                    if title_elem:
                        title = title_elem.get_text(strip=True)
            
            if not title:
                title = f"Учебник по предмету: {query}"
            
            full_url = urljoin("https://urait.ru/", href) if not href.startswith("http") else href
            
            results.append({
                "title": title,
                "url": full_url,
                "status": "success",
                "note": "Электронная библиотека Юрайт - полный доступ к учебнику",
                "source": "urait"
            })
            
            if len(results) >= max_results:
                break
                
        print(f"[DEBUG] Юрайт собрано результатов: {len(results)}")
        return results
        
    except Exception as e:
        print(f"[DEBUG] Юрайт ошибка множественного поиска: {e}")
        return []


def search_urait_viewer_link(query: str) -> Optional[str]:
    """Оставляем для обратной совместимости"""
    results = search_urait_multiple_results(query, 1)
    return results[0]["url"] if results else None


def get_urait_book_title(book_url: str, fallback_subject: str) -> str:
    """Получает название книги с страницы Юрайт"""
    try:
        response = requests.get(book_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Ищем заголовок книги
        title_element = soup.select_one("h1.book-title, .book-header__title, h1")
        if title_element:
            title = title_element.get_text().strip()
            if title and len(title) > 5:  # Проверяем что заголовок не пустой
                return title
        
        # Если не нашли заголовок, ищем в meta тегах
        meta_title = soup.select_one("meta[property='og:title']")
        if meta_title and meta_title.get("content"):
            return meta_title["content"].strip()
            
    except Exception:
        pass
    
    # Если ничего не нашли, возвращаем предмет с пометкой
    return f"Учебник по предмету: {fallback_subject}"


def iprbookshop_search_url(query: str) -> str:
    return "https://www.iprbookshop.ru/586.html?title=" + quote_plus(query)


def fetch_iprbookshop_reader(subject: str) -> Optional[Dict[str, Any]]:
    """Поиск на IPRbooks через requests для совместимости с Vercel"""
    print(f"[DEBUG] IPRbooks поиск через requests: '{subject}'")
    
    try:
        # Используем requests вместо Playwright для Vercel
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Поиск на IPRbooks
        search_url = "https://www.iprbookshop.ru/586.html"
        search_data = {
            'pagetitle': subject,
            'submit': 'Применить'
        }
        
        response = session.post(search_url, data=search_data, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ищем результаты поиска
        book_elements = soup.select('div.row.row-book')
        print(f"[DEBUG] IPRbooks найдено элементов книг: {len(book_elements)}")
        
        if not book_elements:
            print("[DEBUG] IPRbooks: результаты не найдены")
            return None
        
        # Применяем скоринг для выбора лучшей книги
        best_book = None
        best_score = -100
        best_element = None
        
        for book_elem in book_elements:
            try:
                title_link = book_elem.select_one('h4 a')
                if not title_link:
                    continue
                
                title_text = title_link.get_text(strip=True)
                title_lower = title_text.lower()
                subject_lower = subject.lower()
                
                # Scoring system for book relevance
                score = 0
                
                # Exact match gets highest score
                if subject_lower in title_lower:
                    score += 3
                
                # Partial keyword matching
                subject_words = subject_lower.split()
                for word in subject_words:
                    if len(word) > 2 and word in title_lower:
                        score += 2
                
                # Penalty for completely unrelated topics
                unrelated_words = ["программирование", "информатика", "математика", "физика", "химия"]
                if any(word in title_lower for word in unrelated_words) and not any(word in subject_lower for word in unrelated_words):
                    score = -10
                
                # Strong penalty for automation books when not searching for automation
                automation_words = ["автоматизация", "автоматизированный", "automation", "автомат", "машиностроение"]
                automation_in_title = any(word in title_lower for word in automation_words)
                automation_in_subject = any(word in subject_lower for word in ["автомат", "машин", "производств"])
                
                if automation_in_title and not automation_in_subject:
                    score = -20  # Very strong penalty
                
                if score > best_score:
                    best_score = score
                    best_book = title_text
                    best_element = book_elem
                    
            except Exception as e:
                print(f"[DEBUG] IPRbooks ошибка обработки элемента: {e}")
                continue
        
        # Если не нашли хорошую книгу, берем первую
        if best_element is None or best_score < 0:
            print("[DEBUG] IPRbooks: не найдено подходящих книг, берем первую")
            best_element = book_elements[0]
            title_link = best_element.select_one('h4 a')
            if title_link:
                best_book = title_link.get_text(strip=True)
        
        if not best_element:
            return None
        
        # Получаем ссылку на книгу
        title_link = best_element.select_one('h4 a')
        if not title_link:
            return None
        
        detail_href = title_link.get('href')
        if not detail_href or detail_href.startswith('javascript'):
            return None
        
        detail_url = urljoin("https://www.iprbookshop.ru/", detail_href)
        
        # Пытаемся получить прямую ссылку для чтения
        reader_url = detail_url
        try:
            detail_response = session.get(detail_url, timeout=5)
            detail_response.raise_for_status()
            detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
            
            # Ищем кнопку "Читать"
            read_button = detail_soup.select_one('a.btn-read')
            if read_button and read_button.get('href'):
                reader_href = read_button.get('href')
                reader_url = urljoin(detail_url, reader_href)
                print(f"[DEBUG] IPRbooks: найдена прямая ссылка для чтения: {reader_url}")
            
            # Получаем информацию о публикации
            note_parts = []
            pub_data_elements = detail_soup.select('div.pub-data')
            for elem in pub_data_elements:
                text = elem.get_text(strip=True)
                if text:
                    note_parts.append(text)
        
        except Exception as e:
            print(f"[DEBUG] IPRbooks: не удалось получить детали книги: {e}")
            note_parts = ["Электронная библиотека с полным доступом к тексту"]
        
        return {
            "url": reader_url,
            "title": best_book or f"Учебник по предмету: {subject}",
            "status": "success",
            "note": "; ".join(note_parts) if note_parts else "Читать на IPRbooks",
            "source": "iprbookshop",
            "multiple": False
        }
        
    except Exception as e:
        print(f"[DEBUG] IPRbooks ошибка поиска: {e}")
        return None


# Функция get_first_iprbookshop_result больше не нужна, так как основная функция теперь использует requests


def get_multiple_iprbookshop_results(page, subject: str, results_count: int, max_results: int = 10) -> List[Dict[str, Any]]:
    """Получает несколько результатов с IPRbooks для API следующего варианта"""
    print(f"[DEBUG] IPRbooks: собираем {max_results} результатов из {results_count}")
    
    base_url = "https://www.iprbookshop.ru/"
    books = page.locator("div.row.row-book")
    results = []
    
    for i in range(min(results_count, max_results)):
        try:
            book = books.nth(i)
            title_element = book.locator("h4 a")
            if not title_element.count():
                continue
                
            title_text = title_element.inner_text().strip()
            detail_href = title_element.get_attribute("href")
            
            if not detail_href or detail_href.startswith("javascript"):
                continue
                
            detail_url = urljoin(base_url, detail_href)
            
            results.append({
                "title": title_text,
                "url": detail_url,
                "status": "warning", 
                "note": f"IPRbooks - вариант {i+1}. Требуется проверка доступности",
                "source": "iprbookshop"
            })
            
        except Exception as e:
            print(f"[DEBUG] IPRbooks ошибка получения результата {i}: {e}")
            continue
    
    return results


def search_iprbookshop_multiple_results(subject: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """Ищет несколько результатов на IPRbooks через requests"""
    print(f"[DEBUG] IPRbooks множественный поиск через requests: '{subject}'")
    
    try:
        # Используем requests вместо Playwright
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Поиск на IPRbooks
        search_url = "https://www.iprbookshop.ru/586.html"
        search_data = {
            'pagetitle': subject,
            'submit': 'Применить'
        }
        
        response = session.post(search_url, data=search_data, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ищем результаты поиска
        book_elements = soup.select('div.row.row-book')
        print(f"[DEBUG] IPRbooks найдено элементов книг: {len(book_elements)}")
        
        results = []
        
        for i, book_elem in enumerate(book_elements[:max_results]):
            try:
                # Ищем ссылку на книгу
                title_link = book_elem.select_one('h4 a')
                if not title_link:
                    continue
                
                title_text = title_link.get_text(strip=True)
                detail_href = title_link.get('href')
                
                if not detail_href or detail_href.startswith('javascript'):
                    continue
                
                detail_url = urljoin("https://www.iprbookshop.ru/", detail_href)
                
                # Пытаемся получить прямую ссылку для чтения
                reader_url = detail_url
                try:
                    detail_response = session.get(detail_url, timeout=5)
                    detail_response.raise_for_status()
                    detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                    
                    # Ищем кнопку "Читать"
                    read_button = detail_soup.select_one('a.btn-read')
                    if read_button and read_button.get('href'):
                        reader_href = read_button.get('href')
                        reader_url = urljoin(detail_url, reader_href)
                        print(f"[DEBUG] IPRbooks: найдена прямая ссылка для чтения: {reader_url}")
                
                except Exception as e:
                    print(f"[DEBUG] IPRbooks: не удалось получить прямую ссылку для {i}: {e}")
                
                results.append({
                    "title": title_text,
                    "url": reader_url,
                    "status": "warning", 
                    "note": f"IPRbooks - вариант {i+1}. Требуется проверка доступности",
                    "source": "iprbookshop"
                })
                
            except Exception as e:
                print(f"[DEBUG] IPRbooks ошибка получения результата {i}: {e}")
                continue
        
        print(f"[DEBUG] IPRbooks собрано результатов: {len(results)}")
        return results
        
    except Exception as e:
        print(f"[DEBUG] IPRbooks ошибка множественного поиска: {e}")
        return []


def fetch_links_for_subject(subject: str) -> Tuple[str, Dict[str, Any]]:
    info: Dict[str, Any] = {
        "links": [],
        "primary_link": None,
        "status": "warning",
        "note": "",
        "resources": [],
    }

    norm = normalize_subject(subject)
    for rule in KNOWN_RESOURCE_RULES:
        if all(token in norm for token in rule.get("contains", [])):
            resources: List[Dict[str, Any]] = []
            for res in rule.get("resources", []):
                if not res.get("url"):
                    continue
                resources.append(
                    {
                        "title": res.get("title") or res.get("url"),
                        "url": res.get("url"),
                        "status": res.get("status", "success"),
                        "note": res.get("note", ""),
                    }
                )
            if resources:
                info["resources"] = resources
                info["links"] = [res["url"] for res in resources]
                info["primary_link"] = resources[0]["url"]
                info["status"] = resources[0].get("status", "success")
                info["note"] = resources[0].get("note", "")
                if info["status"] == "warning" and not info["note"]:
                    info["note"] = "Проверьте предложенный ресурс"
            return subject, info

    try:
        print(f"[DEBUG] Ищем на Юрайт: '{subject}'")
        urait_results = search_urait_multiple_results(subject, 1)  # Показываем только первый результат
        if urait_results:
            print(f"[DEBUG] Юрайт найден первый результат")
            result = urait_results[0]
            info["links"].append(result["url"])
            info["resources"].append(result)
            
            info["primary_link"] = result["url"]
            info["status"] = "success"
            info["note"] = "Найдено на Юрайт"
        else:
            print(f"[DEBUG] Юрайт не найден для: '{subject}'")
    except Exception as exc:
        print(f"[DEBUG] Ошибка поиска Юрайт: {exc}")
        info["status"] = "error"
        info["note"] = f"Ошибка urait: {exc}"

    ipr_result = fetch_iprbookshop_reader(subject)
    if ipr_result:
        # Всегда одиночный результат IPRbooks
        ipr_resource = {
            "title": ipr_result.get("title", f"Учебник по предмету: {subject}"),
            "url": ipr_result["url"],
            "status": ipr_result.get("status", "success"),
            "note": f"IPRbooks - {ipr_result.get('note', 'Электронная библиотека с полным доступом к тексту')}",
            "source": "iprbookshop"
        }
        info["links"].append(ipr_result["url"])
        info["resources"].append(ipr_resource)
        info["primary_link"] = info["primary_link"] or ipr_result["url"]
        info["status"] = ipr_result.get("status", "success")
        info["note"] = info.get("note") or "Найдено на IPRbooks"

    seen_links: Set[str] = set()
    unique_links: List[str] = []
    unique_resources: List[Dict[str, Any]] = []
    for res in info["resources"]:
        url = res.get("url")
        if not url or url in seen_links:
            continue
        seen_links.add(url)
        unique_links.append(url)
        unique_resources.append(res)

    info["links"] = unique_links
    info["resources"] = unique_resources

    if info["status"] == "error" and not unique_links:
        info["note"] = info.get("note") or "Ссылки не найдены"

    return subject, info


def copy_style(src: Cell, dst: Cell) -> None:
    if src is None or dst is None:
        return
    if not src.has_style:
        return
    try:
        val = src.font
        dst.font = shallow_copy(val)
    except Exception:
        pass
    try:
        val = src.border
        dst.border = shallow_copy(val)
    except Exception:
        pass
    try:
        val = src.fill
        dst.fill = shallow_copy(val)
    except Exception:
        pass
    try:
        dst.number_format = src.number_format
    except Exception:
        pass
    try:
        val = src.protection
        dst.protection = shallow_copy(val)
    except Exception:
        pass
    try:
        val = src.alignment
        dst.alignment = shallow_copy(val)
    except Exception:
        pass


def normalize_subject(value: str) -> str:
    return value.strip().lower()


@app.route("/process", methods=["POST"])
def process():
    return process_streaming()


@app.route("/get_next_resource", methods=["POST"])
def get_next_resource():
    """Получает следующий вариант ресурса для предмета"""
    data = request.get_json()
    subject = data.get("subject")
    current_url = data.get("current_url")
    source = data.get("source", "")
    
    print(f"[DEBUG] Запрос следующего ресурса для '{subject}', текущий: {current_url}")
    
    if source == "urait":
        results = search_urait_multiple_results(subject, 10)
        # Находим текущий индекс и возвращаем следующий
        current_index = -1
        for i, result in enumerate(results):
            if result["url"] == current_url:
                current_index = i
                break
        
        next_index = current_index + 1
        if next_index < len(results):
            return jsonify({"status": "success", "resource": results[next_index]})
        else:
            return jsonify({"status": "no_more", "message": "Больше вариантов нет"})
    
    elif source == "iprbookshop":
        # Для IPRbooks запускаем поиск заново через Playwright
        print(f"[DEBUG] IPRbooks: ищем следующий вариант для '{subject}'")
        try:
            ipr_results = search_iprbookshop_multiple_results(subject, 10)
            if not ipr_results:
                return jsonify({"status": "no_more", "message": "Результаты не найдены"})
            
            # Находим текущий индекс по ID публикации
            current_index = -1
            current_publication_id = None
            
            # Извлекаем ID из текущего URL (например: publicationId=149958)
            if "publicationId=" in current_url:
                current_publication_id = current_url.split("publicationId=")[1].split("&")[0]
                print(f"[DEBUG] IPRbooks: ищем по ID публикации: {current_publication_id}")
            
            for i, result in enumerate(ipr_results):
                # Проверяем по ID публикации или частичному совпадению URL
                if current_publication_id and current_publication_id in result["url"]:
                    current_index = i
                    print(f"[DEBUG] IPRbooks: найден текущий индекс по ID: {i}")
                    break
                elif current_url in result["url"] or result["url"] in current_url:
                    current_index = i
                    print(f"[DEBUG] IPRbooks: найден текущий индекс по URL: {i}")
                    break
            
            next_index = current_index + 1
            if next_index < len(ipr_results):
                return jsonify({"status": "success", "resource": ipr_results[next_index]})
            else:
                return jsonify({"status": "no_more", "message": "Больше вариантов нет"})
                
        except Exception as e:
            print(f"[DEBUG] IPRbooks ошибка поиска следующего: {e}")
            return jsonify({"status": "error", "message": f"Ошибка поиска: {e}"})
    
    return jsonify({"status": "error", "message": "Неизвестный источник"})


@app.route("/process_streaming", methods=["POST"])
def process_streaming():
    ood_file = request.files.get("ood_file")
    up_file = request.files.get("up_file")
    if not ood_file or not up_file:
        return jsonify({"status": "error", "message": "Загрузите оба файла: ООД и 33-УП."}), 400

    subjects = extract_subjects_from_up33(up_file)
    if not subjects:
        return jsonify({"status": "error", "message": "В 33-УП не найдено подходящих предметов."}), 400

    ood_file.stream.seek(0)
    wb = load_workbook(ood_file, data_only=False)
    ws = wb.active

    start_row = 136
    existing_subjects: Set[str] = set()
    existing_links: Set[Tuple[str, str]] = set()

    # Check existing subjects from ALL rows (1 to max_row) to avoid duplicates
    for r in range(1, ws.max_row + 1):
        subj_val = ws.cell(row=r, column=2).value
        if subj_val:
            existing_subjects.add(normalize_subject(str(subj_val)))

    for r in range(start_row, ws.max_row + 1):
        subj_val = ws.cell(row=r, column=2).value
        link_val = ws.cell(row=r, column=4).value
        if subj_val and link_val:
            existing_links.add((normalize_subject(str(subj_val)), str(link_val).strip()))

    pending_subjects: List[str] = []
    skipped_subjects: List[Dict[str, str]] = []
    for subject in subjects:
        norm = normalize_subject(subject)
        if not norm:
            continue
        if norm in existing_subjects:
            skipped_subjects.append({"subject": subject, "reason": "Уже заполнено в ООД"})
            continue
        pending_subjects.append(subject)
        existing_subjects.add(norm)

    if not pending_subjects:
        return jsonify({"status": "ok", "results": [], "skipped": skipped_subjects})

    def generate():
        import json
        
        # Send initial subjects list
        for subject in pending_subjects:
            yield f"data: {json.dumps({'type': 'subject_start', 'subject': subject})}\n\n"
        
        link_results: Dict[str, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_subject = {executor.submit(fetch_links_for_subject, subject): subject for subject in pending_subjects}
            for future in future_to_subject:
                subject_key = future_to_subject[future]
                try:
                    subject, info = future.result()
                    link_results[subject] = info
                    # Escape JSON properly to avoid parse errors
                    json_data = json.dumps({'type': 'subject_done', 'subject': subject, 'info': info}, ensure_ascii=False)
                    yield f"data: {json_data}\n\n"
                except Exception as exc:
                    fallback_info = {
                        "links": [],
                        "primary_link": None,
                        "status": "error",
                        "note": f"Ошибка поиска: {exc}",
                        "resources": []
                    }
                    link_results[subject_key] = fallback_info
                    json_data = json.dumps({'type': 'subject_done', 'subject': subject_key, 'info': fallback_info}, ensure_ascii=False)
                    yield f"data: {json_data}\n\n"

        # Generate final file
        results_payload = process_excel_file(ws, pending_subjects, link_results, start_row)
        
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        final_data = {
            "type": "complete",
            "status": "ok",
            "results": results_payload,
            "skipped": skipped_subjects,
            "file_data": base64.b64encode(output.getvalue()).decode("utf-8"),
            "filename": "updated_ood.xlsx",
        }
        yield f"data: {json.dumps(final_data)}\n\n"
    
    from flask import Response
    return Response(generate(), mimetype='text/event-stream')


def process_excel_file(ws, pending_subjects, link_results, start_row):
    row_ptr = find_next_row(ws, start_row)
    next_num = compute_next_number(ws, start_row)

    template_a = ws.cell(row=start_row, column=1)
    template_b = ws.cell(row=start_row, column=2)
    template_d = ws.cell(row=start_row, column=4)

    results_payload: List[Dict[str, Any]] = []
    existing_links: Set[Tuple[str, str]] = set()
    
    # Get existing links from rows 136+
    for r in range(start_row, ws.max_row + 1):
        subj_val = ws.cell(row=r, column=2).value
        link_val = ws.cell(row=r, column=4).value
        if subj_val and link_val:
            existing_links.add((normalize_subject(str(subj_val)), str(link_val).strip()))
    
    for subject in pending_subjects:
        row_ptr = find_next_row(ws, row_ptr)
        a_cell = ws.cell(row=row_ptr, column=1)
        b_cell = ws.cell(row=row_ptr, column=2)
        copy_style(template_a, a_cell)
        copy_style(template_b, b_cell)
        a_cell.value = next_num
        b_cell.value = subject

        next_num += 1
        row_ptr += 1

        subject_info = link_results.get(subject, {"links": []})
        links = subject_info.get("links", [])
        primary_link = subject_info.get("primary_link")
        status = subject_info.get("status", "warning")
        note = subject_info.get("note", "")

        norm_subject = normalize_subject(subject)
        for link in links:
            key = (norm_subject, link)
            if key in existing_links:
                continue
            row_ptr = find_next_row(ws, row_ptr)
            b = ws.cell(row=row_ptr, column=2)
            d = ws.cell(row=row_ptr, column=4)
            copy_style(template_b, b)
            copy_style(template_d, d)
            b.value = subject
            d.value = link
            existing_links.add(key)
            row_ptr += 1

        if status == "error" and not links:
            note = note or "Ссылки не найдены"
        elif status == "warning" and not note:
            note = "Требуется выбрать книгу вручную"

        results_payload.append(
            {
                "subject": subject,
                "status": status,
                "primary_link": primary_link,
                "links": links,
                "resources": subject_info.get("resources", []),
                "note": note,
            }
        )

    return results_payload


if __name__ == "__main__":
    app.run(debug=True)
