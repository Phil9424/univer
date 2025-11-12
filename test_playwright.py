#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестовый скрипт для поиска книг в IPRSmart через Playwright
Запускает браузер в видимом режиме для отладки
"""

import sys
from urllib.parse import urljoin

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("Установите Playwright: pip install playwright")
    print("Затем установите браузеры: playwright install")
    sys.exit(1)


def test_iprsmart_search(subject: str):
    """Тестирует поиск книги в IPRSmart с видимым браузером"""
    print(f"[ПОИСК] Ищем книгу по запросу: '{subject}'")
    
    base_url = "https://www.iprbookshop.ru/"
    search_url = urljoin(base_url, "586.html")
    
    with sync_playwright() as pw:
        # Запускаем браузер в видимом режиме (headless=False)
        browser = pw.chromium.launch(headless=False, slow_mo=1000)  # slow_mo для наглядности
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(15000)
        
        try:
            print(f"[ПЕРЕХОД] Переходим на страницу поиска: {search_url}")
            page.goto(search_url, wait_until="domcontentloaded")
            
            # Проверяем, не перенаправило ли на авторизацию
            if "auth" in page.url.lower():
                print("[ОШИБКА] Перенаправлено на страницу авторизации")
                return None
            
            print("[ПОИСК] Ищем поле для ввода запроса...")
            search_input = page.locator("input#pagetitle")
            if not search_input.count():
                print("[ОШИБКА] Поле поиска не найдено")
                return None
            
            print(f"[ВВОД] Вводим запрос: '{subject}'")
            search_input.fill(subject)
            
            print("[КЛИК] Нажимаем кнопку 'Применить'...")
            try:
                page.click("input[type='submit'][value='Применить']", timeout=5000)
            except PlaywrightTimeoutError:
                print("[ОШИБКА] Кнопка 'Применить' не найдена или не нажимается")
                return None
            
            print("[ОЖИДАНИЕ] Ждем загрузки результатов...")
            try:
                page.wait_for_selector("#ajaxContentBooks", timeout=10000)
            except PlaywrightTimeoutError:
                print("[ПРЕДУПРЕЖДЕНИЕ] Результаты не загрузились за 10 секунд, продолжаем...")
                page.wait_for_timeout(2000)
            
            # Подсчитываем результаты
            try:
                results_count = page.eval_on_selector_all("div.row.row-book", "nodes => nodes.length") or 0
                print(f"[РЕЗУЛЬТАТ] Найдено результатов: {results_count}")
            except Exception as e:
                print(f"[ОШИБКА] Ошибка подсчета результатов: {e}")
                results_count = 0
            
            if results_count == 0:
                print("[ОШИБКА] Книги не найдены")
                input("Нажмите Enter для закрытия браузера...")
                return None
            
            print("[АНАЛИЗ] Анализируем первые 5 результатов...")
            books = page.locator("div.row.row-book")
            best_book = None
            best_score = 0
            subject_lower = subject.lower()
            subject_words = subject_lower.split()
            
            for i in range(min(results_count, 5)):
                book = books.nth(i)
                title_element = book.locator("h4 a")
                if title_element.count():
                    title_text = title_element.inner_text().strip()
                    title_lower = title_text.lower()
                    
                    # Простая оценка: считаем совпадающие слова
                    score = sum(1 for word in subject_words if word in title_lower)
                    print(f"  [КНИГА {i+1}] {title_text[:80]}... (оценка: {score})")
                    
                    if score > best_score:
                        best_score = score
                        best_book = book
            
            if best_book is None:
                best_book = books.first
                print("[ВЫБОР] Выбираем первую книгу (лучшая оценка не найдена)")
            else:
                print(f"[ВЫБОР] Выбрана книга с лучшей оценкой: {best_score}")
            
            # Получаем детали выбранной книги
            title_element = best_book.locator("h4 a")
            detail_href = title_element.get_attribute("href")
            title_text = title_element.inner_text().strip() if title_element.count() else subject
            
            if not detail_href or detail_href.startswith("javascript"):
                print("[ОШИБКА] Ссылка на детали книги недоступна")
                input("Нажмите Enter для закрытия браузера...")
                return None
            
            detail_url = urljoin(base_url, detail_href)
            print(f"[ПЕРЕХОД] Переходим на страницу книги: {detail_url}")
            page.goto(detail_url, wait_until="domcontentloaded")
            
            if "auth" in page.url.lower():
                print("[ОШИБКА] Перенаправлено на авторизацию при переходе к книге")
                input("Нажмите Enter для закрытия браузера...")
                return None
            
            # Ищем кнопку "Читать"
            print("[ПОИСК] Ищем кнопку 'НАЧАТЬ ЧТЕНИЕ'...")
            reader_locator = page.locator("a.btn-read").first
            try:
                reader_href = reader_locator.get_attribute("href")
            except PlaywrightTimeoutError:
                reader_href = None
            
            if not reader_href:
                print("[ОШИБКА] Кнопка 'НАЧАТЬ ЧТЕНИЕ' не найдена")
                input("Нажмите Enter для закрытия браузера...")
                return None
            
            reader_url = urljoin(detail_url, reader_href)
            print(f"[УСПЕХ] Найдена ссылка для чтения: {reader_url}")
            
            # Получаем дополнительную информацию
            note_parts = []
            try:
                note_parts = page.eval_on_selector_all(
                    "div.pub-data",
                    "nodes => nodes.slice(0, 4).map(n => n.innerText.trim()).filter(Boolean)",
                ) or []
            except Exception:
                note_parts = []
            
            if not note_parts:
                note_parts = ["Читать на IPRbooks"]
            
            result = {
                "title": title_text,
                "url": reader_url,
                "status": "success",
                "note": "; ".join(note_parts),
            }
            
            print("\n[РЕЗУЛЬТАТ]:")
            print(f"  Название: {result['title']}")
            print(f"  Ссылка: {result['url']}")
            print(f"  Примечание: {result['note']}")
            
            input("\nНажмите Enter для закрытия браузера...")
            return result
            
        except Exception as e:
            print(f"[ОШИБКА] Произошла ошибка: {e}")
            input("Нажмите Enter для закрытия браузера...")
            return None
        
        finally:
            browser.close()


def main():
    """Главная функция для тестирования"""
    print("[ЗАПУСК] Тестовый скрипт поиска в IPRSmart")
    print("=" * 50)
    
    # Можно задать предмет для поиска
    if len(sys.argv) > 1:
        subject = " ".join(sys.argv[1:])
    else:
        subject = input("Введите название предмета для поиска: ").strip()
    
    if not subject:
        print("[ОШИБКА] Название предмета не может быть пустым")
        return
    
    result = test_iprsmart_search(subject)
    
    if result:
        print("\n[УСПЕХ] Поиск завершен успешно!")
    else:
        print("\n[НЕУДАЧА] Поиск не дал результатов")


if __name__ == "__main__":
    main()
