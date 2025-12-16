#!/usr/bin/env python3
"""
Script que imprime el timestamp cada minuto
Para pruebas de GitHub Actions
"""

import time
from datetime import datetime
import sys

def main():
    try:
        # Obtener timestamp actual
        current_time = datetime.now()
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")

        print("✅ Me llamaste!")
        
        # Imprimir timestamp
        print(f"🚀 Timestamp: {timestamp}")
        print(f"📅 Fecha: {current_time.date()}")
        print(f"⏰ Hora: {current_time.time().strftime('%H:%M:%S')}")
        
        # También podemos escribir a un archivo para tracking
        with open("timestamps.log", "a") as f:
            f.write(f"{timestamp}\n")
            
        print("✅ Timestamp guardado exitosamente")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
