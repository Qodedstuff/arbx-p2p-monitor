const express = require('express');
const cors = require('cors');
const path = require('path');
const Database = require('better-sqlite3');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Database Setup ───────────────────────────────────────────────────────────
const db = new Database(path.join(__dirname, 'arb_data.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    exchange TEXT NOT NULL,
    currency TEXT NOT NULL,
    lowest_sell REAL,
    highest_buy REAL,
    spread_pct REAL
  );

  CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    threshold REAL DEFAULT 1.5,
    telegram_webhook TEXT,
    enabled INTEGER DEFAULT 1
  );

  INSERT OR IGNORE INTO alerts (id, threshold, enabled) VALUES (1, 1.5, 1);
`);

// ─── Cache ────────────────────────────────────────────────────────────────────
const cache = {};
function getCached(key, ttlMs = 8000) {
  const entry = cache[key];
  if (entry && Date.now() - entry.ts < ttlMs) return entry.data;
  return null;
}
function setCache(key, data) {
  cache[key] = { ts: Date.now(), data };
}

// ─── Bybit P2P ────────────────────────────────────────────────────────────────
async function fetchBybit(side, currency = 'USDT') {
  const cacheKey = `bybit_${side}_${currency}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  try {
    const res = await axios.post(
      'https://api2.bybit.com/fiat/otc/item/online',
      {
        userId: '',
        tokenId: currency,
        currencyId: 'NGN',
        payment: [],
        side: side, // "1" = BUY ads (user sells), "0" = SELL ads (user buys)
        size: '20',
        page: '1',
        amount: '',
        authMaker: false,
        canTrade: false
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Mozilla/5.0'
        },
        timeout: 10000
      }
    );

    const items = res.data?.result?.items || [];
    const filtered = items.filter(item => {
      const payments = item.payments || [];
      const hasBankTransfer = payments.some(p =>
        p.toLowerCase().includes('bank') || p === '75' || p === '14'
      );
      const completionRate = parseFloat(item.recentExecuteRate || item.completionRate || 0);
      const orderCount = parseInt(item.recentOrderNum || item.orderNum || 0);
      const isOnline = item.isOnline === true || item.isOnline === 1;
      const minAmount = parseFloat(item.minAmount || 0);
      const isVerified = item.authStatus === true || item.nickName;

      return (
        hasBankTransfer &&
        completionRate >= 95 &&
        orderCount >= 100 &&
        isOnline &&
        minAmount >= 50000
      );
    });

    const prices = filtered.map(i => parseFloat(i.price)).filter(p => p > 0);
    setCache(cacheKey, prices);
    return prices;
  } catch (err) {
    console.error('Bybit fetch error:', err.message);
    return [];
  }
}

// ─── Gate.io P2P ──────────────────────────────────────────────────────────────
async function fetchGate(tradeType, currency = 'USDT') {
  const cacheKey = `gate_${tradeType}_${currency}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  try {
    const res = await axios.get(
      'https://www.gate.io/api/dex/c2c/v1/ads/list',
      {
        params: {
          currency_pair: `${currency}_NGN`,
          trade_type: tradeType, // "buy" or "sell"
          page: 1,
          limit: 20
        },
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Accept': 'application/json'
        },
        timeout: 10000
      }
    );

    const items = res.data?.data?.list || res.data?.list || [];
    const filtered = items.filter(item => {
      const hasBankTransfer = (item.payment_methods || []).some(p =>
        (p.name || p).toLowerCase().includes('bank')
      );
      const completionRate = parseFloat(item.completion_rate || item.completionRate || 0);
      const orderCount = parseInt(item.order_count || item.orderCount || 0);
      const isOnline = item.online === true || item.is_online === true || item.online === 1;
      const minAmount = parseFloat(item.min_amount || item.minAmount || 0);

      return (
        hasBankTransfer &&
        completionRate >= 95 &&
        orderCount >= 100 &&
        isOnline &&
        minAmount >= 50000
      );
    });

    const prices = filtered.map(i => parseFloat(i.price || i.unit_price)).filter(p => p > 0);
    setCache(cacheKey, prices);
    return prices;
  } catch (err) {
    console.error('Gate fetch error:', err.message);
    return [];
  }
}

// ─── Arbitrage Calculator ─────────────────────────────────────────────────────
function calcSpread(sellPrices, buyPrices) {
  const lowestSell = sellPrices.length ? Math.min(...sellPrices) : null;
  const highestBuy = buyPrices.length ? Math.max(...buyPrices) : null;
  const spreadPct =
    lowestSell && highestBuy
      ? (((highestBuy - lowestSell) / lowestSell) * 100).toFixed(3)
      : null;
  return { lowestSell, highestBuy, spreadPct: spreadPct ? parseFloat(spreadPct) : null };
}

function calcCrossArb(buyExchangeSell, sellExchangeBuy) {
  if (!buyExchangeSell || !sellExchangeBuy) return null;
  return parseFloat((((sellExchangeBuy - buyExchangeSell) / buyExchangeSell) * 100).toFixed(3));
}

// ─── Main Data Fetch ──────────────────────────────────────────────────────────
async function fetchAllData(currency = 'USDT') {
  const [bybitSell, bybitBuy, gateSell, gateBuy] = await Promise.all([
    fetchBybit('0', currency), // SELL ads = user buys here
    fetchBybit('1', currency), // BUY ads = user sells here
    fetchGate('sell', currency),
    fetchGate('buy', currency)
  ]);

  const bybit = calcSpread(bybitSell, bybitBuy);
  const gate = calcSpread(gateSell, gateBuy);

  const bybitToGate = calcCrossArb(bybit.lowestSell, gate.highestBuy);
  const gateToBybit = calcCrossArb(gate.lowestSell, bybit.highestBuy);

  const opportunities = [
    { path: 'Bybit → Gate', pct: bybitToGate, buyPrice: bybit.lowestSell, sellPrice: gate.highestBuy },
    { path: 'Gate → Bybit', pct: gateToBybit, buyPrice: gate.lowestSell, sellPrice: bybit.highestBuy }
  ].filter(o => o.pct !== null).sort((a, b) => b.pct - a.pct);

  return {
    timestamp: Date.now(),
    currency,
    bybit,
    gate,
    opportunities,
    raw: {
      bybitSellPrices: bybitSell.slice(0, 5),
      bybitBuyPrices: bybitBuy.slice(0, 5),
      gateSellPrices: gateSell.slice(0, 5),
      gateBuyPrices: gateBuy.slice(0, 5)
    }
  };
}

// ─── Store Snapshot ───────────────────────────────────────────────────────────
function storeSnapshot(data) {
  const stmt = db.prepare(`
    INSERT INTO price_snapshots (timestamp, exchange, currency, lowest_sell, highest_buy, spread_pct)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  const ts = data.timestamp;
  const cur = data.currency;

  if (data.bybit.lowestSell) stmt.run(ts, 'bybit', cur, data.bybit.lowestSell, data.bybit.highestBuy, data.bybit.spreadPct);
  if (data.gate.lowestSell) stmt.run(ts, 'gate', cur, data.gate.lowestSell, data.gate.highestBuy, data.gate.spreadPct);
}

// ─── Alert Check ─────────────────────────────────────────────────────────────
async function checkAlerts(data) {
  const alertConfig = db.prepare('SELECT * FROM alerts WHERE id = 1').get();
  if (!alertConfig || !alertConfig.enabled) return;

  const threshold = alertConfig.threshold;
  const triggered = data.opportunities.filter(o => o.pct >= threshold);

  if (triggered.length === 0) return;

  const best = triggered[0];
  console.log(`🚨 ALERT: ${best.path} = ${best.pct}% (threshold: ${threshold}%)`);

  if (alertConfig.telegram_webhook) {
    try {
      await axios.post(alertConfig.telegram_webhook, {
        text: `🚨 P2P ARB ALERT\n${best.path}: ${best.pct}%\nBuy @ ${best.buyPrice} | Sell @ ${best.sellPrice}\nCurrency: ${data.currency}`
      }, { timeout: 5000 });
    } catch (e) {
      console.error('Telegram webhook error:', e.message);
    }
  }
}

// ─── Background Worker ────────────────────────────────────────────────────────
let lastData = { USDT: null, USDC: null };

async function runWorker() {
  console.log(`[${new Date().toISOString()}] Running worker...`);
  for (const currency of ['USDT', 'USDC']) {
    try {
      const data = await fetchAllData(currency);
      lastData[currency] = data;
      storeSnapshot(data);
      await checkAlerts(data);
    } catch (e) {
      console.error(`Worker error for ${currency}:`, e.message);
    }
  }
}

// Run every 60 seconds
cron.schedule('* * * * *', runWorker);
// Run immediately on start
runWorker();

// ─── API Routes ───────────────────────────────────────────────────────────────

// Live prices
app.get('/api/live/:currency?', async (req, res) => {
  const currency = (req.params.currency || 'USDT').toUpperCase();
  try {
    const data = lastData[currency] || await fetchAllData(currency);
    res.json({ success: true, data });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Force refresh
app.post('/api/refresh/:currency?', async (req, res) => {
  const currency = (req.params.currency || 'USDT').toUpperCase();
  // Clear caches
  Object.keys(cache).forEach(k => { if (k.includes(currency.toLowerCase())) delete cache[k]; });
  try {
    const data = await fetchAllData(currency);
    lastData[currency] = data;
    storeSnapshot(data);
    res.json({ success: true, data });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Historical data
app.get('/api/history', (req, res) => {
  const { exchange, currency = 'USDT', hours = 24 } = req.query;
  const since = Date.now() - hours * 3600 * 1000;

  let query = `SELECT * FROM price_snapshots WHERE timestamp > ? AND currency = ?`;
  const params = [since, currency];

  if (exchange) {
    query += ` AND exchange = ?`;
    params.push(exchange);
  }

  query += ` ORDER BY timestamp ASC`;

  const rows = db.prepare(query).all(...params);
  res.json({ success: true, data: rows });
});

// Time-based analytics
app.get('/api/analytics/:currency?', (req, res) => {
  const currency = (req.params.currency || 'USDT').toUpperCase();
  const since = Date.now() - 24 * 3600 * 1000;

  const rows = db.prepare(`
    SELECT exchange, timestamp, lowest_sell, highest_buy, spread_pct
    FROM price_snapshots
    WHERE timestamp > ? AND currency = ?
    ORDER BY timestamp ASC
  `).all(since, currency);

  // Group by hour
  const hourly = {};
  for (let h = 0; h < 24; h++) {
    hourly[h] = { bybit_sell: [], bybit_buy: [], gate_sell: [], gate_buy: [] };
  }

  rows.forEach(row => {
    const hour = new Date(row.timestamp).getHours();
    if (row.exchange === 'bybit') {
      if (row.lowest_sell) hourly[hour].bybit_sell.push(row.lowest_sell);
      if (row.highest_buy) hourly[hour].bybit_buy.push(row.highest_buy);
    } else {
      if (row.lowest_sell) hourly[hour].gate_sell.push(row.lowest_sell);
      if (row.highest_buy) hourly[hour].gate_buy.push(row.highest_buy);
    }
  });

  const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const volatility = arr => {
    if (arr.length < 2) return null;
    const mean = avg(arr);
    const variance = arr.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / arr.length;
    return Math.sqrt(variance);
  };

  const hourlyStats = Object.entries(hourly).map(([h, d]) => ({
    hour: parseInt(h),
    bybit: {
      avgSell: avg(d.bybit_sell),
      avgBuy: avg(d.bybit_buy),
      volatilitySell: volatility(d.bybit_sell)
    },
    gate: {
      avgSell: avg(d.gate_sell),
      avgBuy: avg(d.gate_buy),
      volatilityBuy: volatility(d.gate_buy)
    },
    dataPoints: d.bybit_sell.length + d.gate_sell.length
  }));

  // Find best hours
  const withData = hourlyStats.filter(h => h.dataPoints > 0);
  const bestBuyHour = withData.reduce((best, h) => {
    const sell = h.bybit.avgSell || h.gate.avgSell;
    const bestSell = best?.bybit?.avgSell || best?.gate?.avgSell;
    if (!sell) return best;
    if (!bestSell || sell < bestSell) return h;
    return best;
  }, null);

  const bestSellHour = withData.reduce((best, h) => {
    const buy = h.bybit.avgBuy || h.gate.avgBuy;
    const bestBuy = best?.bybit?.avgBuy || best?.gate?.avgBuy;
    if (!buy) return best;
    if (!bestBuy || buy > bestBuy) return h;
    return best;
  }, null);

  res.json({
    success: true,
    data: {
      hourlyStats,
      insights: {
        bestBuyHour: bestBuyHour?.hour ?? null,
        bestSellHour: bestSellHour?.hour ?? null,
        totalDataPoints: rows.length
      }
    }
  });
});

// Alert config
app.get('/api/alerts', (req, res) => {
  const config = db.prepare('SELECT * FROM alerts WHERE id = 1').get();
  res.json({ success: true, data: config });
});

app.put('/api/alerts', (req, res) => {
  const { threshold, telegram_webhook, enabled } = req.body;
  db.prepare(`
    UPDATE alerts SET threshold = ?, telegram_webhook = ?, enabled = ? WHERE id = 1
  `).run(threshold ?? 1.5, telegram_webhook ?? null, enabled ? 1 : 0);
  res.json({ success: true });
});

// Health
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

app.listen(PORT, () => {
  console.log(`🚀 P2P Arb Monitor running at http://localhost:${PORT}`);
});
