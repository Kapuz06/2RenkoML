import os
import time
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Binance API istemcisi (API anahtarları gerekli değil, halka açık veriler için)
client = Client()

# İlgilenilen kripto para çiftleri (Futures için uygun semboller)
symbols = [
    'BTCUSDT', 'ETHUSDT'
]

# Verilerin kaydedileceği temel dizin
base_path = r'C:\Users\asus\OneDrive\Masaüstü\trade\veriler'

# Son 10 günlük veri aralığı
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=45)

def load_futures_data(symbol, start_time, end_time, output_file):
    """
    Belirtilen futures sembolü için 1 dakikalık veri çekme ve dosyaya yazma fonksiyonu.
    Veriyi parça parça çekerek tek bir dosyada toplar.
    """
    temp_start_time = start_time
    first_chunk = True  # İlk veri çekiminde başlık satırlarını yazmak için flag

    while True:
        try:
            # Futures Kline verilerini çek
            klines = client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                startTime=int(temp_start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=1000  # Maksimum 1000 adet veri
            )

            if not klines:
                break  # Veri kalmadı, döngüden çık

            # Kline verilerini DataFrame'e dönüştürme
            df = pd.DataFrame(klines, columns=[
                'OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume',
                'CloseTime', 'QuoteAssetVolume', 'NumberOfTrades',
                'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore'
            ])

            # OpenTime'ı datetime nesnesine çevirip formatlayalım
            df['OpenTime'] = pd.to_datetime(df['OpenTime'], unit='ms')
            df['OpenTime'] = df['OpenTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # -----------------------------------------------------------
            

            # -----------------------------------------------------------
            # CSV'ye kaydedeceğimiz sütunları düzenleyelim
            df = df[[
                'OpenTime',
                'Open', 'High', 'Low', 'Close', 'Volume'
            ]]

            # CSV'ye yazma (ilk chunk'ta header'ları da yazıyoruz)
            if first_chunk:
                df.to_csv(output_file, index=False, mode='w', header=True)
                first_chunk = False
            else:
                df.to_csv(output_file, index=False, mode='a', header=False)

            print(f"{symbol}: {len(df)} satır çekildi ve {output_file} dosyasına eklendi.")

            # Yeni başlangıç zamanı (son çekilen kline'ın OpenTime + 1 dakika)
            last_kline = klines[-1]
            last_open_time = last_kline[0]  # ms cinsinden
            temp_start_time = datetime.utcfromtimestamp(last_open_time / 1000) + timedelta(minutes=1)

            # Eğer çekilen veri sayısı 1000'in altındaysa veri bitmiş demektir
            if len(klines) < 1000:
                break

            # API rate limit'e takılmamak için kısa bir bekleme
            time.sleep(0.5)

        except BinanceAPIException as e:
            print(f'Binance API Hatası ({symbol}): {e.message}')
            print('10 saniye bekleniyor...')
            time.sleep(10)
            continue
        except Exception as e:
            print(f'Beklenmeyen Hata ({symbol}): {e}')
            print('10 saniye bekleniyor...')
            time.sleep(10)
            continue

# Her bir sembol için döngü
for symbol in symbols:
    print(f'İşleniyor: {symbol}')

    # Sembol için klasör oluşturma
    symbol_path = os.path.join(base_path, symbol)
    os.makedirs(symbol_path, exist_ok=True)

    # Çıktı dosyasının tam yolu
    output_file = os.path.join(symbol_path, f'{symbol}.txt')

    # 1 dakikalık veriyi çekip CSV'ye kaydet
    load_futures_data(symbol, start_time, end_time, output_file)

    print(f'{symbol} verileri {output_file} dosyasına başarıyla kaydedildi.\n')

print('Tüm veriler başarıyla çekildi ve kaydedildi.')
