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
        # Asegúrate de tener tu archivo credentials.json en la misma carpeta
        try:
            with open('credentials.json') as f:
                creds_info = json.load(f)
        except FileNotFoundError:
            print("⚠ No se encontró credentials.json ni variables de entorno.")
            return None

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
    category_scraped: str   
    scrape_method: str # 'card' o 'deep' para saber de dónde salió el precio

    def to_csv_row(self):
        return [
            self.product_id,
            self.title,
            self.original_price,
            self.current_price,
            f"{self.discount_percentage:.2f}%" if self.discount_percentage > 0 else "",
            self.offer_text,
            "SÍ" if self.discount_percentage > 0 else "NO", # Es oferta simple check
            self.category_scraped,
            self.url,
            self.image_url,
            self.scrape_method
        ]

# --- 2. Lógica de Parsing ---
class GameParser:

    @staticmethod
    def clean_price(price_str: str) -> float:
        """Convierte strings como 'ARS$ 1.500,00' a float 1500.00"""
        if not price_str: return 0.0
        # Limpieza básica
        txt = price_str.lower().replace('ars$', '').replace('$', '').replace('+', '').replace('desde', '').strip()
        
        # Si dice gratis, devolvemos 0 explícito
        if "gratis" in txt or "free" in txt:
            return 0.0

        # Limpiar caracteres no numéricos excepto coma y punto
        txt = re.sub(r'[^\d.,]', '', txt)
        
        # Formato Argentina: 1.000,00 -> Eliminar punto miles, reemplazar coma decimal por punto
        txt = txt.replace('.', '').replace(',', '.')
        
        try:
            return float(txt)
        except ValueError:
            return 0.0

    @staticmethod
    def fix_url(raw_href: str) -> str:
        """Normaliza la URL a xbox.com/es-ar/games/store/..."""
        if not raw_href: return ""
        if "microsoft.com" in raw_href or "xbox.com" in raw_href:
            return raw_href
        
        # Usualmente vienen como /es-ar/p/titulo/id
        return f"https://www.microsoft.com{raw_href}"

    @staticmethod
    def fetch_deep_price(url: str) -> tuple[float, float]:
        """
        Entra a la página del producto para buscar el botón de compra
        cuando la tarjeta dice 'Incluido con Game Pass'.
        """
        print(f"        >>> 🔎 Deep Scraping: {url}")
        try:
            time.sleep(1.0) # Espera de cortesía y carga
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "es-AR,es;q=0.9"
            }
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                return 0.0, 0.0

            soup = BeautifulSoup(r.text, 'html.parser')

            # --- ESTRATEGIA 1: Buscar por las clases hasheadas del botón ---
            # Buscamos el contenedor de precios dentro del botón de compra
            
            # Precio Actual (Clase común en ambos casos provistos: AcquisitionButtons-module__listedPrice___PS6Zm)
            curr_tag = soup.find('span', class_=re.compile(r'AcquisitionButtons-module__listedPrice'))
            
            # Precio Original (Puede variar: Price-module__brandOriginalPrice o Price-module__originalPrice)
            orig_tag = soup.find('span', class_=re.compile(r'Price-module__.*OriginalPrice'))

            curr_val = GameParser.clean_price(curr_tag.text) if curr_tag else 0.0
            orig_val = GameParser.clean_price(orig_tag.text) if orig_tag else 0.0

            # --- ESTRATEGIA 2: Fallback usando Aria-Label del botón ---
            # Si falló lo anterior, buscamos el botón que contenga "Comprar" y parseamos su texto
            if curr_val == 0.0:
                button = soup.find('button', attrs={'aria-label': re.compile(r'Comprar.*Precio original', re.IGNORECASE)})
                if button:
                    aria_text = button.get('aria-label', '')
                    # Regex para extraer: "Precio original: ARS$ 35.990,00; en oferta por ARS$ 28.792,00"
                    precios = re.findall(r'ARS\$\s?[\d.,]+', aria_text)
                    if len(precios) >= 2:
                        orig_val = GameParser.clean_price(precios[0])
                        curr_val = GameParser.clean_price(precios[1])
                    elif len(precios) == 1:
                        curr_val = GameParser.clean_price(precios[0])
                        orig_val = curr_val

            # Ajuste final si solo encontramos precio actual
            if orig_val == 0.0 and curr_val > 0:
                orig_val = curr_val

            return orig_val, curr_val

        except Exception as e:
            print(f"        ❌ Error en deep scraping: {e}")
            return 0.0, 0.0

    @staticmethod
    def parse_card(card_soup, category_name) -> Optional[GameDeal]:
        try:
            # 1. Verificar si es una tarjeta de producto válida
            pid = card_soup.find('div', class_='card').get('data-bi-pid')
            if not pid: return None
            
            # 2. Título y URL
            title_tag = card_soup.find('h3', class_='base').find('a')
            title = title_tag.text.strip()
            raw_href = title_tag['href']
            final_url = GameParser.fix_url(raw_href)

            # 3. FILTRO: GRATIS
            # Buscamos en el body de la tarjeta si dice "Gratis"
            card_body = card_soup.find('div', class_='card-body')
            if card_body and "gratis" in card_body.get_text().lower():
                # print(f"    - Saltando GRATIS: {title}")
                return None

            # 4. Imagen
            img_tag = card_soup.find('img', class_='card-img')
            img_url = img_tag['src'] if img_tag else ""
            # Limpiar query params de la imagen si se desea
            if "?" in img_url: img_url = img_url.split("?")[0]

            # 5. Oferta / Badge Amarillo
            offer_text = ""
            yellow_badge = card_soup.find('span', class_=lambda x: x and 'bg-yellow' in x)
            if yellow_badge:
                offer_text = yellow_badge.text.strip()

            # --- LOGICA DE PRECIOS ---
            orig_price = 0.0
            curr_price = 0.0
            scrape_method = "card"

            # Detectar si es "Incluido con Game Pass"
            # El usuario indicó buscar: <span class="font-weight-semibold">Incluido<sup...>
            # Y el texto "Game Pass"
            
            is_game_pass_card = False
            price_container = card_soup.find('p', {'aria-hidden': 'true'})
            
            if price_container:
                text_content = price_container.get_text().lower()
                if "incluido" in text_content or "game pass" in text_content:
                    is_game_pass_card = True
            
            # Chequeo adicional por el badge gris de Game Pass
            if card_soup.find('span', string=re.compile("Game Pass")):
                is_game_pass_card = True

            if is_game_pass_card:
                # >>> ACTIVAR DEEP SCRAPING <<<
                orig_price, curr_price = GameParser.fetch_deep_price(final_url)
                scrape_method = "deep"
            else:
                # >>> SCRAPING NORMAL DE TARJETA <<<
                # Precio Original (Tachado)
                orig_tag = card_soup.find('span', class_='text-line-through')
                if orig_tag: 
                    orig_price = GameParser.clean_price(orig_tag.text)
                
                # Precio Actual (Semibold)
                curr_tag = card_soup.find('span', class_='font-weight-semibold')
                if curr_tag:
                    curr_price = GameParser.clean_price(curr_tag.text)

            # Lógica final de precios
            if curr_price == 0.0 and orig_price == 0.0:
                return None # No pudimos sacar precio, descartar o revisar

            if orig_price == 0.0 and curr_price > 0:
                orig_price = curr_price

            discount_pct = 0.0
            if orig_price > 0 and curr_price < orig_price:
                discount_pct = ((orig_price - curr_price) / orig_price) * 100

            return GameDeal(
                product_id=pid,
                title=title,
                original_price=orig_price,
                current_price=curr_price,
                discount_percentage=discount_pct,
                offer_text=offer_text,
                url=final_url,
                image_url=img_url,
                category_scraped=category_name,
                scrape_method=scrape_method
            )

        except Exception as e:
            # print(f"Error parseando item: {e}")
            return None

# --- 3. Scraper Principal ---
class MicrosoftStoreScraper:
    BASE_URL_TEMPLATE = "https://www.microsoft.com/es-ar/store/{filter_mode}/games/pc"
    # Añadimos skipItems={} para formato
    
    def __init__(self, filter_types: List[str]):
        self.filter_types = filter_types
        self.games: List[GameDeal] = []
        self.scraped_ids = set() 

    def run(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        for category in self.filter_types:
            print(f"\n>>> 🎮 CATEGORÍA: {category.upper()}")
            
            skip = 0
            while True:
                target_url = f"{self.BASE_URL_TEMPLATE.format(filter_mode=category)}?skipItems={skip}"
                print(f"    Scanning Page (Skip {skip})...")

                try:
                    r = requests.get(target_url, headers=headers)
                    if r.status_code != 200:
                        print(f"    Error {r.status_code} - Fin de categoría.")
                        break

                    soup = BeautifulSoup(r.text, 'html.parser')
                    
                    # Encontrar todas las tarjetas (li con clase col mb-4 px-2)
                    cards = soup.find_all('li', class_='col mb-4 px-2')
                    
                    if not cards:
                        print("    No se encontraron más juegos.")
                        break

                    new_items_count = 0
                    for card in cards:
                        game = GameParser.parse_card(card, category)
                        
                        if game and game.product_id not in self.scraped_ids:
                            self.games.append(game)
                            self.scraped_ids.add(game.product_id)
                            new_items_count += 1
                            
                            # Log visual
                            symbol = "💲" if game.scrape_method == 'deep' else "📄"
                            print(f"    + {symbol} {game.title[:30]}... ${game.current_price:,.2f}")

                    if new_items_count == 0 and skip > 0:
                        print("    Todos los items de esta página ya estaban scrapeados (o eran gratis).")
                        # Opcional: break si confiamos en que el orden es estático, 
                        # pero mejor seguir por si aparecen nuevos más abajo.
                    
                    skip += 90 # Microsoft suele paginar de a 90
                    time.sleep(1) # Pausa amigable

                except Exception as e:
                    print(f"Error crítico en loop: {e}")
                    break

    def export_to_sheet(self):
        if not self.games:
            print("No hay datos para exportar.")
            return

        print("\n>>> 💾 Exportando a Google Sheets...")
        try:
            gc = get_gsheet_client()
            if not gc: return

            sh = gc.open_by_key(SPREADSHEET_ID)
            
            # Preparar datos
            rows = [[
                "ID", "Title", "Original Price", "Current Price",
                "Discount %", "Offer Text", "Es Oferta", 
                "Categoría", "URL", "Image URL", "Metodo"
            ]]
            
            # Ordenar: Primero las ofertas, luego por mayor descuento
            sorted_games = sorted(
                self.games, 
                key=lambda x: (x.discount_percentage, x.title), 
                reverse=True
            )

            for g in sorted_games:
                rows.append(g.to_csv_row())

            # Escribir en hoja principal
            ws = sh.worksheet(SHEET_NAME)
            ws.clear()
            ws.update(range_name="A1", values=rows)
            
            # Actualizar Metadata
            try:
                meta = sh.worksheet(META_SHEET)
                meta.update(range_name="B2", values=[[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]])
                meta.update(range_name="B3", values=[[len(self.games)]])
            except:
                print("Nota: No se actualizó la hoja _meta (quizás no existe).")

            print(f"✔ ÉXITO: {len(self.games)} juegos exportados.")

        except Exception as e:
            print(f"Error al exportar a Sheets: {e}")

# --- Ejecución ---
if __name__ == "__main__":
    # Las categorías que pediste
    categories = [
        "top-paid",
        "best-rated",
        "most-popular",
        "new-and-rising",
        "deals"
    ]
    
    scraper = MicrosoftStoreScraper(filter_types=categories)
    scraper.run()
    scraper.export_to_sheet()