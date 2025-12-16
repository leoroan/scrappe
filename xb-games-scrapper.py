# v3
import requests
from bs4 import BeautifulSoup
import csv
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional, List
from urllib.parse import urljoin, urlparse, parse_qs

# --- 1. Definición del Objeto de Datos ---
@dataclass
class GameDeal:
    product_id: str
    title: str
    original_price: float
    current_price: float
    discount_percentage: float
    offer_text: Optional[str]
    url: str
    image_url: str  # ← NUEVO CAMPO

    def to_csv_row(self):
        return [
            self.product_id,
            self.title,
            self.original_price,
            self.current_price,
            f"{self.discount_percentage:.2f}%",
            self.offer_text,
            self.url,
            self.image_url  # ← NUEVO EN CSV
        ]

# --- 2. Lógica de Parsing y Limpieza ---
class GameParser:

    @staticmethod
    def clean_price(price_str: str) -> float:
        """Limpia strings como 'ARS$ 4.799,60+' o '$ 39.999,50'."""
        if not price_str or "gratis" in price_str.lower():
            return 0.0

        clean = price_str.replace('+', '').replace('ARS$', '').replace('$', '').strip()
        clean = re.sub(r'[^\d.,]', '', clean)
        clean = clean.replace('.', '')
        clean = clean.replace(',', '.')

        try:
            return float(clean)
        except ValueError:
            return 0.0

    @staticmethod
    def clean_image_url(raw_url: str) -> str:
        """
        Opcional: limpia la URL de imagen para quitar parámetros innecesarios
        y obtener una URL base más estable.
        Ej: ?q=90&w=512&... → solo deja el path base.
        """
        if not raw_url:
            return ""
        parsed = urlparse(raw_url)
        # Conservamos solo el esquema, netloc y path
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return clean

    @staticmethod
    def fix_url(raw_href: str) -> str:
        if "microsoft.com" in raw_href:
            path = raw_href.split("microsoft.com")[-1]
        else:
            path = raw_href
        new_path = path.replace("/p/", "/games/store/")
        return f"https://www.xbox.com{new_path}"

    @staticmethod
    def fetch_deep_price(url: str) -> tuple[float, float]:
        print(f"   >>> Inspeccionando detalle: {url}")
        try:
            time.sleep(0.5)
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                return 0.0, 0.0

            soup = BeautifulSoup(r.text, 'html.parser')
            price_tag = soup.find('span', class_=re.compile(r'Price-module__boldText.*Price-module__listedDiscountPrice'))
            original_tag = soup.find('span', class_=re.compile(r'Price-module__lineThroughText'))

            curr = GameParser.clean_price(price_tag.text) if price_tag else 0.0
            orig = GameParser.clean_price(original_tag.text) if original_tag else curr

            return orig, curr
        except Exception as e:
            print(f"   >>> Error en deep scraping: {e}")
            return 0.0, 0.0

    @staticmethod
    def parse_card(card_soup) -> Optional[GameDeal]:
        try:
            container = card_soup.find('div', class_='card')
            if not container:
                return None

            pid = container.get('data-bi-pid', 'N/A')
            title = container.get('data-bi-prdname', 'Unknown')

            link_tag = container.find('h3', class_='base').find('a')
            raw_href = link_tag['href'] if link_tag else ""
            final_url = GameParser.fix_url(raw_href)

            # --- EXTRACCIÓN DE IMAGEN ---
            img_tag = container.find('img', class_='card-img')
            raw_img_url = img_tag.get('src', '') if img_tag else ''
            clean_img_url = GameParser.clean_image_url(raw_img_url)

            badge = container.find('span', class_='product-cards-savings-badge')
            offer_text = badge.text.strip() if badge else ""

            price_orig_tag = container.find('span', class_='text-line-through')
            price_curr_tag = container.find('span', class_='font-weight-semibold')

            orig_price = GameParser.clean_price(price_orig_tag.text) if price_orig_tag else 0.0
            curr_price = GameParser.clean_price(price_curr_tag.text) if price_curr_tag else 0.0

            if orig_price == 0 and curr_price > 0:
                orig_price = curr_price

            if curr_price == 0.0:
                deep_orig, deep_curr = GameParser.fetch_deep_price(final_url)
                if deep_curr > 0:
                    curr_price = deep_curr
                    orig_price = deep_orig

            discount_pct = 0.0
            if orig_price > 0:
                discount_pct = ((orig_price - curr_price) / orig_price) * 100

            return GameDeal(
                product_id=pid,
                title=title,
                original_price=orig_price,
                current_price=curr_price,
                discount_percentage=discount_pct,
                offer_text=offer_text,
                url=final_url,
                image_url=clean_img_url  # ← AÑADIDO
            )

        except Exception as e:
            # print(f"Error parseando item: {e}")
            return None

# --- 3. Scraper Principal (sin cambios aquí) ---
class MicrosoftStoreScraper:
    BASE_URL = "https://www.microsoft.com/es-ar/store/{filter_mode}/games/pc"

    def __init__(self, filter_mode="deals"):
        self.base_url = self.BASE_URL.format(filter_mode=filter_mode)
        self.games: List[GameDeal] = []
        self.total_items = 0

    def get_total_count(self, soup):
        status_div = soup.find('div', id=re.compile(r'status-container-\d+'))
        if status_div:
            text = status_div.get_text()
            match = re.search(r'de\s+(\d+)', text)
            if match:
                return int(match.group(1))
        return 0

    def run(self):
        print(f"Iniciando escaneo en: {self.base_url}")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        skip = 0

        while True:
            target_url = f"{self.base_url}?skipItems={skip}"
            print(f"Scrapeando bloque (skip={skip})...")

            r = requests.get(target_url, headers=headers)
            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, 'html.parser')

            if skip == 0:
                self.total_items = self.get_total_count(soup)
                print(f"--- TOTAL DETECTADO: {self.total_items} items ---")

            cards = soup.find_all('li', class_='col mb-4 px-2')
            if not cards:
                print("No se encontraron más items.")
                break

            print(f"Procesando {len(cards)} tarjetas...")

            for card in cards:
                game = GameParser.parse_card(card)
                if game:
                    self.games.append(game)

            skip += len(cards)
            if skip >= self.total_items:
                print("Se han procesado todos los items indicados por la página.")
                break

            time.sleep(1)

    def export_csv(self, filename="xbox_scrapped_full.csv"):
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "ID", "Title", "Original Price", "Current Price",
                "Discount %", "Offer", "URL", "Image URL"  # ← NUEVA COLUMNA
            ])
            for g in self.games:
                writer.writerow(g.to_csv_row())
        print(f"CSV guardado: {filename} con {len(self.games)} filas.")

# --- Ejecución ---
if __name__ == "__main__":
    scraper = MicrosoftStoreScraper(filter_mode="deals")
    scraper.run()
    scraper.export_csv()
