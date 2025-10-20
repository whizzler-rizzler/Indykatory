#!/usr/bin/env python3
"""
STANDALONE INDICATORS SERVICE
Pobiera świece 5-minutowe i oblicza wskaźniki volatility w czasie rzeczywistym.
Bot główny odczytuje gotowe wskaźniki z SQLite (<1s latency).
"""

import asyncio
import aiohttp
import sqlite3
import json
import time
import logging
import math
import os
from collections import deque
from typing import Optional, Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("indicators-service")
dbg = logging.getLogger("debug")
dbg.setLevel(logging.DEBUG)

# ====== CONFIG ======
SYMBOL = "BTCUSDT"
CANDLE_INTERVAL_MIN = 5  # 5-minutowe świece
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
BINANCE_REST_URL = "https://data.binance.com/api/v3/klines"  # Historical candles
BINANCE_TICKER_URL = "https://data.binance.com/api/v3/ticker/price?symbol=BTCUSDT"  # Current price co 10s!
KRAKEN_REST_URL = "https://api.kraken.com/0/public/OHLC"  # Fallback jeśli Binance zablokowany
DB_PATH = "logs/indicators_cache.db"  # SQLite fallback
UPDATE_INTERVAL_S = 10  # Aktualizacja wskaźników co 10s (real-time monitoring!)
CANDLE_REFRESH_INTERVAL_S = 60  # Pobieranie nowych świec co 60s (nie przeciążać serwera)
CANDLE_BUFFER_SIZE = 100  # Ile świec 5-min przechowywać (500 min = 8h 20min)

# STANDARDOWE OKRESY (FIXED) - 6 wskaźników
OHLC_PERIODS = [50, 25]  # OHLC dla 50 i 25 świec
ATR_LENGTHS = [14, 28]   # ATR dla 14 i 28 okresów
C2C_PERIODS = [50, 25]   # Close-to-Close dla 50 i 25 świec

# PostgreSQL (Railway) - auto-detect
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway provides this
USE_POSTGRES = DATABASE_URL is not None

# ====== DATABASE STORAGE (PostgreSQL or SQLite) ======
class IndicatorsStorage:
    """Database storage dla wskaźników (PostgreSQL on Railway, SQLite local)"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.use_postgres = USE_POSTGRES
        self.db_path = db_path or DB_PATH
        self._init_db()
    
    def _init_db(self):
        """Inicjalizuj bazę danych (PostgreSQL lub SQLite)"""
        if self.use_postgres:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS indicators (
                    symbol TEXT PRIMARY KEY,
                    interval TEXT,
                    data TEXT,
                    updated_at BIGINT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles_history (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT,
                    interval TEXT,
                    time BIGINT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    created_at BIGINT
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            db_url_display = DATABASE_URL[:50] if DATABASE_URL else "None"
            log.info(f"✅ PostgreSQL initialized: {db_url_display}...")
        else:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS indicators (
                    symbol TEXT PRIMARY KEY,
                    interval TEXT,
                    data TEXT,
                    updated_at INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS candles_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    interval TEXT,
                    time INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    created_at INTEGER
                )
            """)
            conn.commit()
            conn.close()
            log.info(f"✅ SQLite initialized: {self.db_path}")
    
    def save_indicators(self, symbol: str, interval: str, indicators: Dict):
        """Zapisz wskaźniki do bazy"""
        if self.use_postgres:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO indicators (symbol, interval, data, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    interval = EXCLUDED.interval,
                    data = EXCLUDED.data,
                    updated_at = EXCLUDED.updated_at
            """, (symbol, interval, json.dumps(indicators), int(time.time())))
            conn.commit()
            cur.close()
            conn.close()
        else:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                INSERT OR REPLACE INTO indicators (symbol, interval, data, updated_at)
                VALUES (?, ?, ?, ?)
            """, (symbol, interval, json.dumps(indicators), int(time.time())))
            conn.commit()
            conn.close()
    
    def save_candle_history(self, symbol: str, interval: str, candle: Dict):
        """Zapisz świecę do historii"""
        if self.use_postgres:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO candles_history (symbol, interval, time, open, high, low, close, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol, interval, int(candle['time']), candle['open'], candle['high'], 
                  candle['low'], candle['close'], int(time.time())))
            conn.commit()
            cur.close()
            conn.close()
        else:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                INSERT INTO candles_history (symbol, interval, time, open, high, low, close, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, interval, candle['time'], candle['open'], candle['high'], 
                  candle['low'], candle['close'], int(time.time())))
            conn.commit()
            conn.close()
    
    def get_indicators(self, symbol: str) -> Optional[Dict]:
        """Odczytaj wskaźniki z bazy"""
        if self.use_postgres:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                SELECT data, updated_at FROM indicators WHERE symbol = %s
            """, (symbol,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                data = json.loads(row[0])
                data['updated_at'] = row[1]
                return data
            return None
        else:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("""
                SELECT data, updated_at FROM indicators WHERE symbol = ?
            """, (symbol,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                data = json.loads(row[0])
                data['updated_at'] = row[1]
                return data
            return None

# ====== CANDLE AGGREGATOR ======
class CandleAggregator:
    """Agreguje świece 1-minutowe do 5-minutowych"""
    
    def __init__(self, buffer_size: int = 50):
        self.candles = deque(maxlen=buffer_size)
    
    def add_candle(self, candle: Dict):
        """Dodaj świecę do bufora"""
        self.candles.append(candle)
    
    def get_recent_candles(self, n: int) -> List[Dict]:
        """Zwróć ostatnie N świec"""
        return list(self.candles)[-n:] if len(self.candles) >= n else list(self.candles)

# ====== INDICATOR ENGINE ======
class IndicatorEngine:
    """Oblicza wskaźniki volatility"""
    
    @staticmethod
    def calculate_close_to_close_volatility(candles: List[Dict]) -> float:
        """Close-to-Close Volatility (Annualized) - TradingView Compatible"""
        if len(candles) < 2:
            return 0.0
        
        log_returns = []
        for i in range(1, len(candles)):
            prev_close = candles[i-1]['close']
            curr_close = candles[i]['close']
            if prev_close > 0:
                log_ret = math.log(curr_close / prev_close)
                log_returns.append(log_ret)
        
        if not log_returns:
            return 0.0
        
        n = len(log_returns)
        
        # Sample standard deviation (n-1 for unbiased estimator)
        mean = sum(log_returns) / n
        variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1) if n > 1 else 0
        std_dev = math.sqrt(variance)
        
        # TradingView annualizacja: sqrt(252) dla daily-based (nie intraday 24/7!)
        ANNUAL_PERIODS = 252  # Trading days per year (standard market convention)
        
        volatility_pct = std_dev * math.sqrt(ANNUAL_PERIODS) * 100
        return volatility_pct
    
    @staticmethod
    def calculate_ohlc_volatility(candles: List[Dict]) -> float:
        """Garman-Klass OHLC Volatility (Annualized) - TradingView Compatible"""
        if len(candles) < 2:
            return 0.0
        
        n = len(candles)
        sum_variance = 0.0
        
        for c in candles:
            if c['high'] > 0 and c['low'] > 0 and c['open'] > 0 and c['close'] > 0:
                # High/Low component
                hl_ratio = math.log(c['high'] / c['low'])
                hl_component = 0.5 * (hl_ratio ** 2)
                
                # Close/Open component
                co_ratio = math.log(c['close'] / c['open'])
                co_component = (2 * math.log(2) - 1) * (co_ratio ** 2)  # ≈ 0.3863 * co_ratio²
                
                # Combine
                period_variance = hl_component - co_component
                sum_variance += period_variance
        
        if sum_variance <= 0:
            return 0.0
        
        # Sample variance: divide by (n-1) for unbiased estimator
        avg_variance = sum_variance / (n - 1) if n > 1 else sum_variance
        
        # TradingView crypto 24/7 annualizacja dla intraday range-based volatility:
        # Używa calendar days (365) zamiast trading days (252) bo crypto działa 24/7
        # Plus scaling dla 5-min candles (288 okresów/dzień)
        CALENDAR_DAYS = 365  # Crypto trades 24/7/365
        PERIODS_PER_DAY = 288  # 5-min candles: 24h * 60min / 5min
        
        annualized_variance = avg_variance * CALENDAR_DAYS * PERIODS_PER_DAY
        volatility_pct = math.sqrt(annualized_variance) * 100
        
        return volatility_pct
    
    @staticmethod
    def calculate_atr(candles: List[Dict], period: int = 14) -> float:
        """ATR (Average True Range) - TradingView compatible with RMA smoothing"""
        if len(candles) < period + 1:
            return 0.0
        
        # Calculate True Range for each candle
        true_ranges = []
        for i in range(1, len(candles)):
            high = candles[i]['high']
            low = candles[i]['low']
            prev_close = candles[i-1]['close']
            
            # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        if len(true_ranges) < period:
            return 0.0
        
        # TradingView uses RMA (Running Moving Average) for ATR
        # RMA is similar to EMA: alpha = 1/period
        # First ATR = SMA of first 'period' values
        # Then: ATR = (previous_ATR * (period-1) + current_TR) / period
        
        # Initial ATR (SMA of first 'period' TR values)
        atr = sum(true_ranges[:period]) / period
        
        # Apply RMA for remaining values
        for i in range(period, len(true_ranges)):
            atr = (atr * (period - 1) + true_ranges[i]) / period
        
        return atr
    
    @classmethod
    def calculate_all(cls, candles: List[Dict]) -> Dict:
        """Oblicz wszystkie 6 wskaźników (FIXED PERIODS)"""
        
        # OHLC - 2 wartości (50 i 25 świec)
        ohlc_50 = round(cls.calculate_ohlc_volatility(candles[-50:] if len(candles) >= 50 else candles), 4)
        ohlc_25 = round(cls.calculate_ohlc_volatility(candles[-25:] if len(candles) >= 25 else candles), 4)
        
        # ATR - 2 wartości (14 i 28 length)
        atr_14 = round(cls.calculate_atr(candles, period=14), 2)
        atr_28 = round(cls.calculate_atr(candles, period=28), 2)
        
        # Close-to-Close - 2 wartości (50 i 25 świec)
        c2c_50 = round(cls.calculate_close_to_close_volatility(candles[-50:] if len(candles) >= 50 else candles), 4)
        c2c_25 = round(cls.calculate_close_to_close_volatility(candles[-25:] if len(candles) >= 25 else candles), 4)
        
        return {
            'ohlc_50': ohlc_50,
            'ohlc_25': ohlc_25,
            'atr_14': atr_14,
            'atr_28': atr_28,
            'c2c_50': c2c_50,
            'c2c_25': c2c_25,
            'candles_count': len(candles),
        }

# ====== LIGHTER WEBSOCKET FALLBACK ======
class LighterCandleBuilder:
    """Buduje świece 5-minutowe z ticków Lighter WebSocket"""
    
    def __init__(self, interval_min: int = 5):
        self.interval_min = interval_min
        self.current_interval_start = 0
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
    
    def update(self, price: float) -> Optional[Dict]:
        """Aktualizuj świecę, zwróć zamkniętą świecę jeśli interwał się skończył"""
        current_interval_start = int(time.time() / (self.interval_min * 60)) * (self.interval_min * 60)
        
        # Nowy interwał - zamknij poprzednią świecę
        if current_interval_start != self.current_interval_start:
            completed_candle = None
            if self.open_price is not None:
                completed_candle = {
                    'time': self.current_interval_start,
                    'open': self.open_price,
                    'high': self.high_price,
                    'low': self.low_price,
                    'close': self.close_price,
                }
            
            # Reset dla nowego interwału
            self.current_interval_start = current_interval_start
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            
            return completed_candle
        else:
            # Aktualizuj bieżącą świecę
            if self.open_price is None:
                self.open_price = price
            self.high_price = max(self.high_price or price, price)
            self.low_price = min(self.low_price or price, price)
            self.close_price = price
            
            return None

# ====== DATA FETCHER ======
class DataFetcher:
    """Pobiera świece - PRIMARY: Binance, FALLBACK: Lighter WebSocket"""
    
    def __init__(self, symbol: str, interval_min: int):
        self.symbol = symbol
        self.interval_min = interval_min
        self.ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval_min}m"
        self.lighter_ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        self.lighter_candle_builder = LighterCandleBuilder(interval_min)
    
    async def fetch_kraken_rest(self, limit: int = 50) -> List[Dict]:
        """Pobierz świece z Kraken API (publiczny, bez geo-blokady)"""
        try:
            # Kraken używa różnych symboli: BTC=XBT, USDT=USDT
            kraken_pair = "XBTUSDT"  # BTC/USDT na Kraken
            params = {
                'pair': kraken_pair,
                'interval': self.interval_min,  # 5 = 5 min
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(KRAKEN_REST_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        log.error(f"❌ Kraken REST error: status={resp.status}")
                        return []
                    
                    data = await resp.json()
                    
                    if data.get('error') and len(data['error']) > 0:
                        log.error(f"❌ Kraken API error: {data['error']}")
                        return []
                    
                    result = data.get('result', {})
                    ohlc_data = result.get(kraken_pair, [])
                    
                    if not ohlc_data:
                        log.error(f"❌ No Kraken data for {kraken_pair}")
                        return []
                    
                    # Kraken format: [timestamp, open, high, low, close, vwap, volume, count]
                    candles = []
                    for item in ohlc_data[-limit:]:  # Ostatnie N świec
                        candles.append({
                            'time': int(item[0]),
                            'open': float(item[1]),
                            'high': float(item[2]),
                            'low': float(item[3]),
                            'close': float(item[4]),
                        })
                    
                    log.info(f"✅ Kraken REST: pobrano {len(candles)} świec {self.interval_min}m")
                    return candles
        
        except Exception as e:
            log.error(f"❌ Kraken REST error: {e}")
            return []
    
    async def fetch_rest(self, limit: int = 50) -> List[Dict]:
        """Pobierz świece z REST API - PRIMARY: BINANCE, FALLBACK: Kraken"""
        # BINANCE PRIMARY - zawsze próbuj najpierw!
        try:
            params = {
                'symbol': self.symbol,
                'interval': f'{self.interval_min}m',
                'limit': limit
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_REST_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        candles = []
                        for kline in data:
                            candles.append({
                                'time': int(kline[0]) / 1000,
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                            })
                        
                        log.info(f"✅ Binance REST (PRIMARY): pobrano {len(candles)} świec {self.interval_min}m")
                        return candles
                    else:
                        log.warning(f"⚠️ Binance REST status={resp.status} - trying Kraken fallback")
        
        except Exception as e:
            log.warning(f"⚠️ Binance REST error: {e} - trying Kraken fallback")
        
        # KRAKEN FALLBACK - tylko jeśli Binance nie działa
        candles = await self.fetch_kraken_rest(limit)
        if candles:
            return candles
        
        return []
    
    async def fetch_current_price(self) -> Optional[float]:
        """Pobierz CURRENT PRICE z Binance ticker (do aktualizacji wskaźników co 10s!)"""
        # BINANCE PRIMARY
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_TICKER_URL, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data["price"])
                        return price
        except Exception as e:
            pass  # Silent fail - spróbuj Kraken
        
        # KRAKEN FALLBACK
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSDT", 
                                      timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data["result"]["XBTUSDT"]["c"][0])  # Last trade price
                        return price
        except Exception as e:
            log.error(f"❌ Failed to fetch current price: {e}")
        
        return None
    
    async def stream_websocket(self, aggregator: CandleAggregator, storage: IndicatorsStorage, engine: IndicatorEngine):
        """Stream świec z WebSocket (primary source - Binance)"""
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url) as ws:
                        log.info(f"✅ Binance WebSocket connected: {self.ws_url}")
                        
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                
                                if 'k' in data:
                                    kline = data['k']
                                    is_closed = kline['x']
                                    
                                    if is_closed:
                                        candle = {
                                            'time': int(kline['t']) / 1000,
                                            'open': float(kline['o']),
                                            'high': float(kline['h']),
                                            'low': float(kline['l']),
                                            'close': float(kline['c']),
                                        }
                                        
                                        aggregator.add_candle(candle)
                                        storage.save_candle_history(self.symbol, f"{self.interval_min}m", candle)
                                        
                                        # Oblicz 6 wskaźników używając wszystkich świec
                                        all_candles = list(aggregator.candles)
                                        indicators = engine.calculate_all(all_candles)
                                        storage.save_indicators(self.symbol, f"{self.interval_min}m", indicators)
                                        
                                        log.info(f"📊 [Binance] OHLC: 50={indicators['ohlc_50']:.2f}% 25={indicators['ohlc_25']:.2f}% | "
                                                f"ATR: 14={indicators['atr_14']:.2f} 28={indicators['atr_28']:.2f} | "
                                                f"C2C: 50={indicators['c2c_50']:.2f}% 25={indicators['c2c_25']:.2f}%")
                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                log.warning("⚠️ Binance WebSocket closed, reconnecting...")
                                break
            
            except Exception as e:
                log.error(f"❌ Binance WebSocket error: {e}")
            
            await asyncio.sleep(5)
    
    async def stream_lighter_websocket(self, aggregator: CandleAggregator, storage: IndicatorsStorage, engine: IndicatorEngine):
        """Stream z Lighter WebSocket (fallback - buduj świece z ticków)"""
        WS_MARKET_ID = "1"  # BTC market
        
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.lighter_ws_url) as ws:
                        log.info(f"✅ Lighter WebSocket connected (FALLBACK): {self.lighter_ws_url}")
                        
                        subscribed = False
                        
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    
                                    # Wait for 'connected' before subscribing
                                    if not subscribed and data.get("type") == "connected":
                                        await ws.send_json({
                                            "type": "subscribe",
                                            "channel": f"order_book/{WS_MARKET_ID}"
                                        })
                                        log.info("📡 Subscribed to Lighter order_book/%s", WS_MARKET_ID)
                                        subscribed = True
                                    
                                    # Check for orderbook data
                                    channel = data.get("channel", "")
                                    if "order_book" not in channel:
                                        continue
                                    
                                    # Get orderbook (NEW FORMAT)
                                    book = data.get("order_book", {})
                                    bids = book.get("bids", [])
                                    asks = book.get("asks", [])
                                    
                                    if not bids or not asks:
                                        continue
                                    
                                    # Calculate mid price from best bid/ask (Lighter format: list of dicts)
                                    best_bid = float(bids[0]["price"]) if bids else None
                                    best_ask = float(asks[0]["price"]) if asks else None
                                    
                                    if best_bid and best_ask:
                                        mid_price = (best_bid + best_ask) / 2.0
                                        
                                        # Update candle builder
                                        completed_candle = self.lighter_candle_builder.update(mid_price)
                                        
                                        if completed_candle:
                                            aggregator.add_candle(completed_candle)
                                            storage.save_candle_history(self.symbol, f"{self.interval_min}m", completed_candle)
                                            
                                            # Oblicz 6 wskaźników używając wszystkich świec
                                            all_candles = list(aggregator.candles)
                                            indicators = engine.calculate_all(all_candles)
                                            storage.save_indicators(self.symbol, f"{self.interval_min}m", indicators)
                                            
                                            log.info(f"📊 [Lighter] OHLC: 50={indicators['ohlc_50']:.2f}% 25={indicators['ohlc_25']:.2f}% | "
                                                    f"ATR: 14={indicators['atr_14']:.2f} 28={indicators['atr_28']:.2f} | "
                                                    f"C2C: 50={indicators['c2c_50']:.2f}% 25={indicators['c2c_25']:.2f}%")
                                
                                except Exception as e:
                                    dbg.debug(f"Lighter parse error: {e}")
                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                log.warning("⚠️ Lighter WebSocket closed, reconnecting...")
                                break
            
            except Exception as e:
                log.error(f"❌ Lighter WebSocket error: {e}")
            
            await asyncio.sleep(5)

# ====== CANDLE REFRESH LOOP ======
async def candle_refresh_loop(fetcher: DataFetcher, aggregator: CandleAggregator, storage: IndicatorsStorage, symbol: str, interval_min: int):
    """Pobieraj nowe świece co CANDLE_REFRESH_INTERVAL_S sekund z REST API"""
    log.info(f"🔄 Starting candle refresh loop: every {CANDLE_REFRESH_INTERVAL_S}s")
    
    while True:
        try:
            await asyncio.sleep(CANDLE_REFRESH_INTERVAL_S)
            
            # Pobierz tylko najnowsze świece (limit 20 - ostatnia godzina + 40min)
            candles = await fetcher.fetch_rest(limit=20)
            
            if candles:
                # Dodaj tylko nowe świece (sprawdź timestamp)
                existing_times = {c['time'] for c in aggregator.candles}
                new_candles = [c for c in candles if c['time'] not in existing_times]
                
                for candle in new_candles:
                    aggregator.add_candle(candle)
                    storage.save_candle_history(symbol, f"{interval_min}m", candle)
                
                if new_candles:
                    log.info(f"🔄 [REFRESH] Added {len(new_candles)} new candles (total: {len(aggregator.candles)})")
        
        except Exception as e:
            log.error(f"❌ Candle refresh error: {e}")
            await asyncio.sleep(CANDLE_REFRESH_INTERVAL_S)

# ====== CONTINUOUS UPDATE LOOP ======
async def continuous_update_loop(fetcher: DataFetcher, aggregator: CandleAggregator, storage: IndicatorsStorage, engine: IndicatorEngine, symbol: str, interval: str):
    """Aktualizuj wskaźniki co 10s używając CURRENT PRICE z Binance ticker!"""
    log.info(f"⏰ Starting continuous update loop: every {UPDATE_INTERVAL_S}s with LIVE PRICE")
    
    while True:
        try:
            await asyncio.sleep(UPDATE_INTERVAL_S)
            
            # Pobierz CURRENT PRICE z Binance ticker
            current_price = await fetcher.fetch_current_price()
            
            if current_price:
                # Aktualizuj ostatnią świecę z current price (symuluje real-time close)
                all_candles = list(aggregator.candles)
                
                if len(all_candles) >= 2:
                    # Zaktualizuj close/high/low ostatniej świecy na podstawie current price
                    last_candle = all_candles[-1].copy()
                    last_candle['close'] = current_price
                    last_candle['high'] = max(last_candle['high'], current_price)
                    last_candle['low'] = min(last_candle['low'], current_price)
                    
                    # Zastąp ostatnią świecę zaktualizowaną wersją
                    updated_candles = all_candles[:-1] + [last_candle]
                    
                    # Przelicz 6 wskaźników z LIVE PRICE
                    indicators = engine.calculate_all(updated_candles)
                    storage.save_indicators(symbol, interval, indicators)
                    log.info(f"💹 [LIVE UPDATE] Price={current_price:.2f} | "
                            f"OHLC: 50={indicators['ohlc_50']:.2f}% 25={indicators['ohlc_25']:.2f}% | "
                            f"ATR: 14={indicators['atr_14']:.2f} 28={indicators['atr_28']:.2f} | "
                            f"C2C: 50={indicators['c2c_50']:.2f}% 25={indicators['c2c_25']:.2f}%")
        
        except Exception as e:
            log.error(f"❌ Update loop error: {e}")
            await asyncio.sleep(UPDATE_INTERVAL_S)

# ====== MAIN SERVICE ======
async def run_service():
    """Uruchom serwis wskaźników"""
    log.info("🚀 Starting Indicators Service")
    log.info(f"📊 Symbol: {SYMBOL} | Interval: {CANDLE_INTERVAL_MIN}m | Buffer: {CANDLE_BUFFER_SIZE} świec")
    log.info(f"⏱️  Update frequency: every {UPDATE_INTERVAL_S} seconds")
    
    storage = IndicatorsStorage(DB_PATH)
    aggregator = CandleAggregator(buffer_size=CANDLE_BUFFER_SIZE)
    engine = IndicatorEngine()
    fetcher = DataFetcher(SYMBOL, CANDLE_INTERVAL_MIN)
    
    # Pobierz początkową historię świec (BINANCE PRIMARY!)
    log.info("⏳ Fetching initial candles from BINANCE REST...")
    try:
        candles = await asyncio.wait_for(fetcher.fetch_rest(limit=CANDLE_BUFFER_SIZE), timeout=10.0)
        for candle in candles:
            aggregator.add_candle(candle)
        
        if candles:
            indicators = engine.calculate_all(list(aggregator.candles))
            storage.save_indicators(SYMBOL, f"{CANDLE_INTERVAL_MIN}m", indicators)
            log.info(f"✅ Initial indicators (6 values): "
                    f"OHLC: 50={indicators['ohlc_50']:.2f}% 25={indicators['ohlc_25']:.2f}% | "
                    f"ATR: 14={indicators['atr_14']:.2f} 28={indicators['atr_28']:.2f} | "
                    f"C2C: 50={indicators['c2c_50']:.2f}% 25={indicators['c2c_25']:.2f}%")
            
            # Uruchom background loops
            log.info(f"✅ Starting BINANCE monitoring (candles every {CANDLE_REFRESH_INTERVAL_S}s, price every {UPDATE_INTERVAL_S}s)")
            
            # Start candle refresh loop (pobiera nowe świece co 60s)
            refresh_task = asyncio.create_task(
                candle_refresh_loop(fetcher, aggregator, storage, SYMBOL, CANDLE_INTERVAL_MIN)
            )
            
            # Start indicator update loop (przelicza wskaźniki co 10s z LIVE PRICE)
            update_task = asyncio.create_task(
                continuous_update_loop(fetcher, aggregator, storage, engine, SYMBOL, f"{CANDLE_INTERVAL_MIN}m")
            )
            
            # Keep running forever
            await asyncio.gather(refresh_task, update_task)
        else:
            raise Exception("No candles from data source")
    
    except Exception as e:
        log.error(f"❌ Failed to start service: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(run_service())
