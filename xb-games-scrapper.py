# v5
import requests
from bs4 import BeautifulSoup
import re
import time
from dataclasses import dataclass
from typing import Optional, List
from urllib.parse import urlparse
import json
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURACIÓN ---
SPREADSHEET_ID = "11hC5cJWSJEgl9G2sITMUyS6ohiLbU80_No3JBfqVwAI"
SHEET_NAME = "xb"
META_SHEET = "_meta"


def get_gsheet_client():
    if "GOOGLE_CREDENTIALS" in os.environ:
        creds_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    else:
        # Fallback local
        with open("credentials.json") as f:
            creds_info = json.load(f)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


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
    image_url: str
    is_new: bool  # Badge Negro
    is_deal: bool  # Badge Amarillo (NUEVO)
    category_scraped: str

    def to_csv_row(self):
        return [
            self.product_id,
            self.title,
            self.original_price,
            self.current_price,
            f"{self.discount_percentage:.2f}%",
            self.offer_text,
            "SÍ" if self.is_deal else "NO",  # Columna Oferta
            "SÍ" if self.is_new else "NO",  # Columna Nuevo
            self.category_scraped,
            self.url,
            self.image_url,
        ]


# --- 2. Lógica de Parsing y Limpieza ---
class GameParser:
    @staticmethod
    def clean_price(price_str: str) -> float:
        if not price_str or "gratis" in price_str.lower():
            return 0.0
        clean = price_str.replace("+", "").replace("ARS$", "").replace("$", "").strip()
        clean = re.sub(r"[^\d.,]", "", clean)
        clean = clean.replace(".", "").replace(",", ".")
        try:
            return float(clean)
        except ValueError:
            return 0.0

    @staticmethod
    def clean_image_url(raw_url: str) -> str:
        if not raw_url:
            return ""
        parsed = urlparse(raw_url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    @staticmethod
    def fix_url(raw_href: str) -> str:
        if "microsoft.com" in raw_href:
            path = raw_href.split("microsoft.com")[-1]
        else:
            path = raw_href
        new_path = path.replace("/p/", "/games/store/")
        return f"https://www.xbox.com{new_path}"

    @staticmethod
    def parse_card(card_soup, category_name) -> Optional[GameDeal]:
        try:
            container = card_soup.find("div", class_="card")
            if not container:
                return None

            pid = container.get("data-bi-pid", "N/A")
            title = container.get("data-bi-prdname", "Unknown")

            link_tag = container.find("h3", class_="base").find("a")
            raw_href = link_tag["href"] if link_tag else ""
            final_url = GameParser.fix_url(raw_href)

            # --- EXTRACCIÓN DE IMAGEN ---
            img_tag = container.find("img", class_="card-img")
            clean_img_url = (
                GameParser.clean_image_url(img_tag.get("src", "")) if img_tag else ""
            )

            # --- TEXTO DE OFERTA GENÉRICO ---
            # A veces el texto está aunque no tenga el color amarillo
            badge_span = container.find("span", class_="product-cards-savings-badge")
            offer_text = badge_span.text.strip() if badge_span else ""

            # --- LOGICA BOOLEANA DE BADGES (Colores) ---

            # A. DETECTAR SI ES OFERTA (Badge Amarillo)
            # Buscamos clases que contengan 'bg-yellow' dentro del contenedor
            is_deal = False
            yellow_badge = container.find(
                "span", class_=lambda x: x and "bg-yellow" in x
            )
            if yellow_badge:
                is_deal = True

            # B. DETECTAR SI ES NUEVO (Badge Negro)
            is_new = False
            black_badge = container.find("span", class_=lambda x: x and "bg-black" in x)
            # Verificamos texto por seguridad, aunque el color suele ser suficiente
            if black_badge and "nuevo" in black_badge.get_text().lower():
                is_new = True

            # --- PRECIOS ---
            price_orig_tag = container.find("span", class_="text-line-through")
            price_curr_tag = container.find("span", class_="font-weight-semibold")

            orig_price = (
                GameParser.clean_price(price_orig_tag.text) if price_orig_tag else 0.0
            )
            curr_price = (
                GameParser.clean_price(price_curr_tag.text) if price_curr_tag else 0.0
            )

            # Ajuste lógico: Si hay precio actual pero no original, el original es el actual
            if orig_price == 0.0 and curr_price > 0:
                orig_price = curr_price

            # Calcular descuento matemático
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
                image_url=clean_img_url,
                is_new=is_new,  # Estado Nuevo
                is_deal=is_deal,  # Estado Oferta
                category_scraped=category_name,
            )

        except Exception as e:
            # print(f"Error parseando item: {e}")
            return None


# --- 3. Scraper Principal ---
class MicrosoftStoreScraper:
    BASE_URL_TEMPLATE = "https://www.microsoft.com/es-ar/store/{filter_mode}/games/pc"

    # ACTUALIZADO: Quitamos 'isdeal=true' para traer todo el universo filtrado por precio
    QUERY_PARAMS = "?price=0.01To10000"

    def __init__(self, filter_types: List[str]):
        self.filter_types = filter_types
        self.games: List[GameDeal] = []
        self.scraped_ids = set()

    def get_total_count(self, soup):
        status_div = soup.find("div", id=re.compile(r"status-container-\d+"))
        if status_div:
            text = status_div.get_text()
            match = re.search(r"de\s+(\d+)", text)
            if match:
                return int(match.group(1))
        return 0

    def run(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        for category in self.filter_types:
            print(f"\n>>> INICIANDO CATEGORÍA: {category.upper()}")

            base_category_url = self.BASE_URL_TEMPLATE.format(filter_mode=category)
            skip = 0

            while True:
                target_url = f"{base_category_url}{self.QUERY_PARAMS}&skipItems={skip}"
                print(f"   Scanning: {category} | Skip: {skip}")

                try:
                    r = requests.get(target_url, headers=headers)
                    if r.status_code != 200:
                        print(f"   Error status {r.status_code}")
                        break

                    soup = BeautifulSoup(r.text, "html.parser")

                    total_items = self.get_total_count(soup) if skip == 0 else 9999

                    cards = soup.find_all("li", class_="col mb-4 px-2")
                    if not cards:
                        print("   No se encontraron más items en esta página.")
                        break

                    # print(f"   Procesando {len(cards)} tarjetas...")

                    for card in cards:
                        game = GameParser.parse_card(card, category)

                        if game:
                            if game.product_id not in self.scraped_ids:
                                self.games.append(game)
                                self.scraped_ids.add(game.product_id)

                                # Log simple visual
                                # Muestra [OFERTA] o [NUEVO] al lado del nombre
                                flags = []
                                if game.is_deal:
                                    flags.append("OFERTA")
                                if game.is_new:
                                    flags.append("NUEVO")
                                flag_str = f"[{'|'.join(flags)}]" if flags else ""

                                print(f"    + {game.title[:30]}... {flag_str}")
                            else:
                                pass

                    skip += len(cards)
                    if skip >= total_items or len(cards) == 0:
                        break

                    # Pausa mínima ya que no hacemos deep scraping
                    time.sleep(0.5)
                except Exception as e:
                    print(f"Error crítico en loop: {e}")
                    break

    def export_to_sheet(self):
        try:
            gc = get_gsheet_client()

            sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
            meta = gc.open_by_key(SPREADSHEET_ID).worksheet(META_SHEET)

            # Encabezados actualizados
            rows = [
                [
                    "ID",
                    "Title",
                    "Original Price",
                    "Current Price",
                    "Discount %",
                    "Offer",
                    "Es Oferta",
                    "Es Nuevo",
                    "Categoría",
                    "URL",
                    "Image URL",
                ]
            ]

            # ORDENAMIENTO INTELIGENTE:
            # 1. Primero los que son Oferta (True > False)
            # 2. Luego por mayor porcentaje de descuento
            sorted_games = sorted(
                self.games,
                key=lambda x: (x.is_deal, x.discount_percentage),
                reverse=True,
            )

            for g in sorted_games:
                rows.append(g.to_csv_row())

            sheet.clear()
            sheet.update(range_name="A1", values=rows)

            now = datetime.utcnow().isoformat() + "Z"
            meta.update(range_name="B2", values=[[now]])
            meta.update(range_name="B3", values=[[0]])

            print(
                f"\n✔ ÉXITO: Sheet '{SHEET_NAME}' actualizada con {len(self.games)} filas únicas."
            )
        except Exception as e:
            print(f"Error al exportar a Sheets: {e}")


# --- Ejecución ---
if __name__ == "__main__":
    categories_to_scrape = [
        "top-paid",
        "best-rated",
        "most-popular",
        "new-and-rising",
    ]

    scraper = MicrosoftStoreScraper(filter_types=categories_to_scrape)
    scraper.run()
    scraper.export_to_sheet()
