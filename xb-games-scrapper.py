#v4
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
        with open('credentials.json') as f:
            creds_info = json.load(f)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
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
    is_new: bool            # ← NUEVO CAMPO (Booleano)
    launch_date: Optional[datetime] # ← NUEVO CAMPO (Objeto fecha)
    category_scraped: str   # ← Extra: Para saber de qué lista vino (top-paid, etc)

    def to_csv_row(self):
        # Formateamos la fecha para el excel
        date_str = self.launch_date.strftime("%d/%m/%Y") if self.launch_date else ""
        
        return [
            self.product_id,
            self.title,
            self.original_price,
            self.current_price,
            f"{self.discount_percentage:.2f}%",
            self.offer_text,
            "SÍ" if self.is_new else "NO", # Formato leíble
            date_str,
            self.category_scraped,
            self.url,
            self.image_url
        ]

# --- 2. Lógica de Parsing y Limpieza ---
class GameParser:

    @staticmethod
    def clean_price(price_str: str) -> float:
        if not price_str or "gratis" in price_str.lower():
            return 0.0
        clean = price_str.replace('+', '').replace('ARS$', '').replace('$', '').strip()
        clean = re.sub(r'[^\d.,]', '', clean)
        clean = clean.replace('.', '').replace(',', '.')
        try:
            return float(clean)
        except ValueError:
            return 0.0

    @staticmethod
    def clean_image_url(raw_url: str) -> str:
        if not raw_url: return ""
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
    def fetch_deep_details(url: str):
        """
        Entra a la ficha del producto para buscar:
        1. Precios (si no estaban afuera).
        2. Fecha de lanzamiento.
        """
        # print(f"    >>> Deep Scraping: {url}") # Comentado para menos ruido
        try:
            time.sleep(0.4) # Pequeña pausa para no saturar
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                return 0.0, 0.0, None

            soup = BeautifulSoup(r.text, 'html.parser')
            
            # A. PRECIOS (Backup)
            price_tag = soup.find('span', class_=re.compile(r'Price-module__boldText.*Price-module__listedDiscountPrice'))
            original_tag = soup.find('span', class_=re.compile(r'Price-module__lineThroughText'))
            
            curr = GameParser.clean_price(price_tag.text) if price_tag else 0.0
            orig = GameParser.clean_price(original_tag.text) if original_tag else curr

            # B. FECHA DE LANZAMIENTO
            # Buscamos por el texto "Fecha de lanzamiento" para ser más robustos que la clase CSS hash
            launch_dt = None
            # Estrategia: Buscar el H3 o div que diga "Fecha de lanzamiento" y tomar el siguiente div
            label_tag = soup.find(string=re.compile("Fecha de lanzamiento"))
            if label_tag:
                # Subir al padre (h3 generalmente) y buscar el hermano (div con el dato)
                parent = label_tag.parent
                if parent:
                    # A veces es el siguiente hermano directo
                    date_div = parent.find_next_sibling('div')
                    if date_div:
                        date_text = date_div.get_text().strip()
                        try:
                            # Formato esperado: 20/1/2023
                            launch_dt = datetime.strptime(date_text, "%d/%m/%Y")
                        except ValueError:
                            pass # No se pudo parsear

            return orig, curr, launch_dt

        except Exception as e:
            print(f"    >>> Error deep details: {e}")
            return 0.0, 0.0, None

    @staticmethod
    def parse_card(card_soup, category_name) -> Optional[GameDeal]:
        try:
            container = card_soup.find('div', class_='card')
            if not container: return None

            pid = container.get('data-bi-pid', 'N/A')
            title = container.get('data-bi-prdname', 'Unknown')

            link_tag = container.find('h3', class_='base').find('a')
            raw_href = link_tag['href'] if link_tag else ""
            final_url = GameParser.fix_url(raw_href)

            # --- EXTRACCIÓN DE IMAGEN ---
            img_tag = container.find('img', class_='card-img')
            clean_img_url = GameParser.clean_image_url(img_tag.get('src', '')) if img_tag else ''

            # --- BADGE DE OFERTA ---
            badge = container.find('span', class_='product-cards-savings-badge')
            offer_text = badge.text.strip() if badge else ""

            # --- BADGE DE "NUEVO" ---
            # Buscamos el span con clase badge bg-black que contenga "Nuevo"
            is_new = False
            new_badge = container.find('span', class_=lambda x: x and 'badge' in x and 'bg-black' in x)
            if new_badge and "nuevo" in new_badge.get_text().lower():
                is_new = True

            # --- PRECIOS SUPERFICIALES ---
            price_orig_tag = container.find('span', class_='text-line-through')
            price_curr_tag = container.find('span', class_='font-weight-semibold')
            
            orig_price = GameParser.clean_price(price_orig_tag.text) if price_orig_tag else 0.0
            curr_price = GameParser.clean_price(price_curr_tag.text) if price_curr_tag else 0.0

            # --- DEEP SCRAPING (Ahora obligatorio para la fecha) ---
            # Entramos SIEMPRE para buscar la fecha, y de paso corregimos precios si faltan
            deep_orig, deep_curr, launch_date = GameParser.fetch_deep_details(final_url)

            # Prioridad de precios: Deep > Superficial (si superficial es 0)
            if curr_price == 0.0 and deep_curr > 0:
                curr_price = deep_curr
            if orig_price == 0.0:
                orig_price = deep_orig if deep_orig > 0 else curr_price

            # Calcular descuento
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
                is_new=is_new,          # Nuevo
                launch_date=launch_date,# Nuevo
                category_scraped=category_name # Nuevo
            )

        except Exception as e:
            # print(f"Error parseando item: {e}")
            return None

# --- 3. Scraper Principal ---
class MicrosoftStoreScraper:
    # URL base con placeholder para el TIPO
    BASE_URL_TEMPLATE = "https://www.microsoft.com/es-ar/store/{filter_mode}/games/pc"
    
    # Query params constantes
    QUERY_PARAMS = "?isdeal=true&price=0.01To10000"

    def __init__(self, filter_types: List[str]):
        self.filter_types = filter_types
        self.games: List[GameDeal] = []
        # Para evitar duplicados si un juego aparece en varias listas
        self.scraped_ids = set() 

    def get_total_count(self, soup):
        status_div = soup.find('div', id=re.compile(r'status-container-\d+'))
        if status_div:
            text = status_div.get_text()
            match = re.search(r'de\s+(\d+)', text)
            if match:
                return int(match.group(1))
        return 0

    def run(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        for category in self.filter_types:
            print(f"\n>>> INICIANDO CATEGORÍA: {category.upper()}")
            
            # Construcción de la URL completa
            base_category_url = self.BASE_URL_TEMPLATE.format(filter_mode=category)
            skip = 0
            
            while True:
                # Armamos la URL con paginación y filtros
                target_url = f"{base_category_url}{self.QUERY_PARAMS}&skipItems={skip}"
                print(f"   Scanning: {category} | Skip: {skip}")

                try:
                    r = requests.get(target_url, headers=headers)
                    if r.status_code != 200:
                        print(f"   Error status {r.status_code}")
                        break

                    soup = BeautifulSoup(r.text, 'html.parser')

                    # Obtener total solo en la primera página de la categoría
                    total_items = self.get_total_count(soup) if skip == 0 else 9999
                    
                    cards = soup.find_all('li', class_='col mb-4 px-2')
                    if not cards:
                        print("   No se encontraron más items en esta página.")
                        break

                    print(f"   Procesando {len(cards)} tarjetas (esto puede tardar por la fecha)...")

                    for card in cards:
                        # Parseamos
                        game = GameParser.parse_card(card, category)
                        
                        if game:
                            # Verificamos duplicados por ID
                            if game.product_id not in self.scraped_ids:
                                self.games.append(game)
                                self.scraped_ids.add(game.product_id)
                                # Feedback visual pequeño (opcional)
                                d_str = game.launch_date.strftime("%Y-%m") if game.launch_date else "???"
                                print(f"    + {game.title[:30]}... ({d_str}) {'[NUEVO]' if game.is_new else ''}")
                            else:
                                pass # Ya lo tenemos de otra categoría

                    skip += len(cards)
                    # Si la página devolvió menos items de los esperados o superamos el total
                    if skip >= total_items or len(cards) == 0:
                        break
                    
                    time.sleep(1) # Respeto a la API
                except Exception as e:
                    print(f"Error crítico en loop: {e}")
                    break

    def export_to_sheet(self):
        try:
            gc = get_gsheet_client()
            
            sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
            meta = gc.open_by_key(SPREADSHEET_ID).worksheet(META_SHEET)
            
            # Encabezados actualizados
            rows = [[
                "ID", "Title", "Original Price", "Current Price",
                "Discount %", "Offer", "Es Nuevo", "Lanzamiento", 
                "Categoría", "URL", "Image URL"
            ]]
            
            # Ordenamos por fecha de lanzamiento (descendente) antes de subir
            # Ponemos una fecha muy vieja si es None para que queden al final
            sorted_games = sorted(
                self.games, 
                key=lambda x: x.launch_date if x.launch_date else datetime(1900,1,1), 
                reverse=True
            )

            for g in sorted_games:
                rows.append(g.to_csv_row())
            
            sheet.clear()
            sheet.update(range_name="A1", values=rows)

            now = datetime.utcnow().isoformat() + "Z"
            meta.update(range_name="B2", values=[[now]])
            meta.update(range_name="B3", values=[[0]]) # Reset offset si lo usas
            
            print(f"\n✔ ÉXITO: Sheet '{SHEET_NAME}' actualizada con {len(self.games)} filas únicas.")
        except Exception as e:
            print(f"Error al exportar a Sheets: {e}")

# --- Ejecución ---
if __name__ == "__main__":
    # Definimos las categorías que pediste
    categories_to_scrape = [
        "top-paid",
        "best-rated",
        "most-popular",
        "new-and-rising",
    ]
    
    scraper = MicrosoftStoreScraper(filter_types=categories_to_scrape)
    scraper.run()
    scraper.export_to_sheet()
