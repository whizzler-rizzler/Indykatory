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
BINANCE_REST_URL = "https://api.binance.com/api/v3/klines"
DB_PATH = "logs/indicators_cache.db"  # SQLite fallback
UPDATE_INTERVAL_S = 60  # Aktualizacja wskaźników co 60s
CANDLE_BUFFER_SIZE = 50  # Ile świec 5-min przechowywać
VOLATILITY_WINDOW = 10  # Ile świec używać do obliczeń

# PostgreSQL (Railway) - auto-detect
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway provides this
USE_POSTGRES = DATABASE_URL is not None

# ====== DATABASE STORAGE (PostgreSQL or SQLite) ======
class IndicatorsStorage:
    """Database storage dla wskaźników (PostgreSQL on Railway, SQLite local)"""
    
    def __init__(self, db_path: str = None):
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
            log.info(f"✅ PostgreSQL initialized: {DATABASE_URL[:50]}...")
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
        """Close-to-Close Volatility = StdDev(log returns) * sqrt(periods) * 100"""
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
        
        mean_ret = sum(log_returns) / len(log_returns)
        variance = sum((r - mean_ret) ** 2 for r in log_returns) / len(log_returns)
        std_dev = math.sqrt(variance)
        
        volatility_pct = std_dev * math.sqrt(len(log_returns)) * 100
        return volatility_pct
    
    @staticmethod
    def calculate_ohlc_volatility(candles: List[Dict]) -> float:
        """OHLC Volatility = Średnia((High - Low) / Close) * 100"""
        if not candles:
            return 0.0
        
        hl_ratios = []
        for c in candles:
            if c['close'] > 0:
                hl_ratio = (c['high'] - c['low']) / c['close']
                hl_ratios.append(hl_ratio)
        
        if not hl_ratios:
            return 0.0
        
        avg_hl_ratio = sum(hl_ratios) / len(hl_ratios)
        return avg_hl_ratio * 100
    
    @staticmethod
    def calculate_chaikin_volatility(candles: List[Dict], ema_period: int = 10) -> float:
        """Chaikin Volatility = ((EMA_HL - EMA_HL_prev) / EMA_HL_prev) * 100"""
        if len(candles) < ema_period + 1:
            return 0.0
        
        hl_values = [c['high'] - c['low'] for c in candles]
        
        multiplier = 2 / (ema_period + 1)
        ema = hl_values[0]
        ema_values = [ema]
        
        for hl in hl_values[1:]:
            ema = hl * multiplier + ema * (1 - multiplier)
            ema_values.append(ema)
        
        if len(ema_values) < 2 or ema_values[-2] == 0:
            return 0.0
        
        chaikin = ((ema_values[-1] - ema_values[-2]) / ema_values[-2]) * 100
        return abs(chaikin)
    
    @classmethod
    def calculate_all(cls, candles: List[Dict]) -> Dict:
        """Oblicz wszystkie wskaźniki"""
        return {
            'close_to_close': round(cls.calculate_close_to_close_volatility(candles), 4),
            'ohlc': round(cls.calculate_ohlc_volatility(candles), 4),
            'chaikin': round(cls.calculate_chaikin_volatility(candles), 4),
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
    
    async def fetch_rest(self, limit: int = 50) -> List[Dict]:
        """Pobierz świece z REST API (fallback)"""
        try:
            params = {
                'symbol': self.symbol,
                'interval': f'{self.interval_min}m',
                'limit': limit
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_REST_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        log.error(f"❌ Binance REST error: status={resp.status}")
                        return []
                    
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
                    
                    log.info(f"✅ REST: pobrano {len(candles)} świec {self.interval_min}m")
                    return candles
        
        except Exception as e:
            log.error(f"❌ REST fetch error: {e}")
            return []
    
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
                                        
                                        recent_candles = aggregator.get_recent_candles(VOLATILITY_WINDOW)
                                        indicators = engine.calculate_all(recent_candles)
                                        storage.save_indicators(self.symbol, f"{self.interval_min}m", indicators)
                                        
                                        log.info(f"📊 [Binance] Indicators: C2C={indicators['close_to_close']:.2f}% "
                                                f"OHLC={indicators['ohlc']:.2f}% Chaikin={indicators['chaikin']:.2f}")
                            
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
                                            
                                            recent_candles = aggregator.get_recent_candles(VOLATILITY_WINDOW)
                                            indicators = engine.calculate_all(recent_candles)
                                            storage.save_indicators(self.symbol, f"{self.interval_min}m", indicators)
                                            
                                            log.info(f"📊 [Lighter] Indicators: C2C={indicators['close_to_close']:.2f}% "
                                                    f"OHLC={indicators['ohlc']:.2f}% Chaikin={indicators['chaikin']:.2f}")
                                
                                except Exception as e:
                                    dbg.debug(f"Lighter parse error: {e}")
                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                log.warning("⚠️ Lighter WebSocket closed, reconnecting...")
                                break
            
            except Exception as e:
                log.error(f"❌ Lighter WebSocket error: {e}")
            
            await asyncio.sleep(5)

# ====== MAIN SERVICE ======
async def run_service():
    """Uruchom serwis wskaźników"""
    log.info("🚀 Starting Indicators Service")
    log.info(f"📊 Symbol: {SYMBOL} | Interval: {CANDLE_INTERVAL_MIN}m | Buffer: {CANDLE_BUFFER_SIZE} świec")
    
    storage = IndicatorsStorage(DB_PATH)
    aggregator = CandleAggregator(buffer_size=CANDLE_BUFFER_SIZE)
    engine = IndicatorEngine()
    fetcher = DataFetcher(SYMBOL, CANDLE_INTERVAL_MIN)
    
    # Próbuj pobrać historię z Binance REST (optional, szybko fail)
    log.info("⏳ Trying Binance REST for historical candles...")
    try:
        candles = await asyncio.wait_for(fetcher.fetch_rest(limit=CANDLE_BUFFER_SIZE), timeout=5.0)
        for candle in candles:
            aggregator.add_candle(candle)
        
        if candles:
            indicators = engine.calculate_all(aggregator.get_recent_candles(VOLATILITY_WINDOW))
            storage.save_indicators(SYMBOL, f"{CANDLE_INTERVAL_MIN}m", indicators)
            log.info(f"✅ Initial indicators from Binance: C2C={indicators['close_to_close']:.2f}% "
                    f"OHLC={indicators['ohlc']:.2f}% Chaikin={indicators['chaikin']:.2f}")
            
            # Binance działa - użyj Binance WebSocket
            log.info("✅ Using Binance WebSocket (PRIMARY)")
            await fetcher.stream_websocket(aggregator, storage, engine)
        else:
            raise Exception("No candles from Binance")
    
    except Exception as e:
        log.warning(f"⚠️ Binance unavailable ({e}) - switching to LIGHTER FALLBACK")
        log.info("✅ Using Lighter WebSocket (FALLBACK) - budowanie świec z ticków")
        log.info("⏳ Pierwsze wskaźniki pojawią się za ~5 minut (czas na zbudowanie świecy)")
        
        # Lighter fallback - zawsze działa!
        await fetcher.stream_lighter_websocket(aggregator, storage, engine)

if __name__ == "__main__":
    asyncio.run(run_service())
