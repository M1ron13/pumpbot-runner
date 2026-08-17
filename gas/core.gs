/**
 * Pump Detector — ядро детекции (чистые функции, без Apps Script API).
 *
 * Паритет формул с Python-версией (../pump_bot.py): те же пороги, та же
 * статистика, тот же анти-спам. Этот файл гоняется тестами на Node
 * (../gas/test_core.mjs), поэтому здесь нет ни UrlFetchApp, ни DriveApp.
 *
 * Отличие рантайма: Apps Script умеет триггер не чаще 1 минуты, поэтому
 * сэмплы минутные, а не 10-секундные. Окна (15м / 5м) считаются по времени
 * сэмплов, а не по их количеству — математика от каденции не зависит.
 */

/** Все пороги в одном месте — аналог config.json Python-версии. */
function defaultConfig() {
  return {
    universe: {
      quoteSuffix: 'USDT',
      min24hVolumeUsd: 10000000,
      excludeSymbols: ['BTCUSDT', 'ETHUSDT'],
      minHistoryMinutes: 120
    },
    detection: {
      windowMainSec: 900,
      windowFastSec: 300,
      // сэмпл годится как «начало окна», только если он не старше окна × tolerance:
      // Apps Script умеет пропустить триггер, и без этого допуска рост за 40 минут
      // посчитался бы как рост за 15 и дал ложный сигнал
      windowToleranceMult: 2.0,
      minObservations: 30,
      sigmaFloorPct: 0.25,
      zscoreThreshold: 4.0,
      minAbsMoveMainPct: 6.0,
      volumeMultMain: 4.0,
      fastMovePct: 7.0,
      volumeMultFast: 5.0
    },
    btcFilter: {
      btcSymbol: 'BTCUSDT',
      btcMove15mPct: 2.0,
      thresholdMultiplier: 1.3
    },
    alerts: {
      cooldownMin: 60,
      maxPerDay: 40,
      dayWindowSec: 86400,
      chartUrlTemplate: 'https://www.tradingview.com/chart/?symbol={exchange}%3A{symbol}.P',
      volumeDecimals: 1,
      fundingDecimals: 3,
      priceSignificantDigits: 6
    },
    runtime: {
      ticksKeep: 20,          // 20 минутных сэмплов — с запасом на 15-минутное окно
      historyKeep: 360,       // 6 часов наблюдений для mu/sigma и vol_baseline
      staleDropSec: 21600,    // символ пропал из тикеров на 6 часов — выкидываем из состояния
      stateFileName: 'pumpbot-state.json.gz',
      fundingRefreshSec: 300,
      startupPing: true
    },
    telegram: {
      apiBase: 'https://api.telegram.org',
      parseMode: 'HTML',
      disableWebPagePreview: true
    },
    sources: {
      binanceTickersUrl: 'https://fapi.binance.com/fapi/v1/ticker/24hr',
      binancePremiumIndexUrl: 'https://fapi.binance.com/fapi/v1/premiumIndex',
      bybitTickersUrl: 'https://api.bybit.com/v5/market/tickers?category=linear'
    }
  };
}

function emptyState() {
  return { v: 1, saved: 0, symbols: {}, cool: {}, day: [], fundingAt: 0 };
}

// --------------------------------------------------------------------------- //
// мелкая математика
// --------------------------------------------------------------------------- //

function meanOf(values) {
  var sum = 0;
  for (var i = 0; i < values.length; i++) sum += values[i];
  return sum / values.length;
}

function stdevOf(values, mu) {
  if (values.length < 2) return 0;
  var acc = 0;
  for (var i = 0; i < values.length; i++) acc += Math.pow(values[i] - mu, 2);
  return Math.sqrt(Math.max(0, acc / (values.length - 1)));
}

/** Ближайший сэмпл не позже targetTs, но не старше maxAgeTs. null — истории не хватает. */
function sampleAtOrBefore(ticks, targetTs, minTs) {
  for (var i = ticks.length - 1; i >= 0; i--) {
    if (ticks[i][0] <= targetTs) {
      return ticks[i][0] >= minTs ? ticks[i] : null;
    }
  }
  return null;
}

function windowEdges(ticks, windowSec, tolerance) {
  if (!ticks.length) return null;
  var last = ticks[ticks.length - 1];
  var past = sampleAtOrBefore(ticks, last[0] - windowSec, last[0] - windowSec * tolerance);
  if (!past || past === last || past[1] <= 0) return null;
  return { last: last, past: past };
}

function movePct(ticks, windowSec, tolerance) {
  var e = windowEdges(ticks, windowSec, tolerance);
  return e ? (e.last[1] / e.past[1] - 1) * 100 : null;
}

/** Интервальный объём как дельта кумулятивного 24h-объёма; отрицательная — null. */
function intervalVolume(ticks, windowSec, tolerance) {
  var e = windowEdges(ticks, windowSec, tolerance);
  if (!e) return null;
  var delta = e.last[2] - e.past[2];
  return delta < 0 ? null : delta;
}

// --------------------------------------------------------------------------- //
// форматирование алерта
// --------------------------------------------------------------------------- //

function fmtVolume(value, decimals) {
  if (value === null || value === undefined) return 'n/a';
  if (value >= 1e9) return '$' + (value / 1e9).toFixed(decimals) + 'B';
  if (value >= 1e6) return '$' + (value / 1e6).toFixed(decimals) + 'M';
  return '$' + Math.round(value / 1e3) + 'K';
}

function fmtPrice(price, significant) {
  if (!(price > 0)) return '0';
  var exponent = Math.floor(Math.log(price) / Math.LN10);
  var decimals = Math.max(0, Math.min(20, significant - 1 - exponent));
  var text = price.toFixed(decimals);
  if (text.indexOf('.') >= 0) text = text.replace(/0+$/, '').replace(/\.$/, '');
  return text || '0';
}

function fmtFunding(rate, decimals) {
  if (rate === null || rate === undefined) return 'n/a';
  return (rate * 100).toFixed(decimals) + '%';
}

function fmtClockUtc(tsSec) {
  var d = new Date(tsSec * 1000);
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  return pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds());
}

function renderAlert(alert, cfg) {
  var al = cfg.alerts;
  var sig = alert.primary;
  var quote = cfg.universe.quoteSuffix;
  var base = sig.symbol.slice(0, sig.symbol.length - quote.length) || sig.symbol;
  var chart = al.chartUrlTemplate
    .replace('{exchange}', sig.exchange)
    .replace('{symbol}', sig.symbol);

  var lines = ['🔴 <b>PUMP: ' + base + '/' + quote + '</b> [' + alert.exchanges.join(' + ') + ']'];
  if (sig.triggers.indexOf('FAST') >= 0 && sig.move5m !== null) {
    lines.push('⚡ <b>FAST +' + sig.move5m.toFixed(1) + '%/5м — вертикаль</b>');
  }
  var moveLine = '📈 +' + sig.move15m.toFixed(1) + '% / 15м';
  if (sig.move5m !== null) moveLine += '  (+' + sig.move5m.toFixed(1) + '% / 5м)';
  lines.push(moveLine);
  lines.push('⚡ z-score: ' + sig.zscore.toFixed(1) + ' | Vol: ' + sig.volMult.toFixed(1) + 'x avg');
  lines.push('💰 Цена: ' + fmtPrice(sig.price, al.priceSignificantDigits) +
             ' | 24h Vol: ' + fmtVolume(sig.volume24h, al.volumeDecimals));
  lines.push('💸 Funding: ' + fmtFunding(sig.funding, al.fundingDecimals));
  lines.push('🕐 ' + fmtClockUtc(alert.ts) + ' UTC');
  lines.push('📊 График (ссылка: ' + chart + ')');
  return lines.join('\n');
}

// --------------------------------------------------------------------------- //
// ядро: один проход детектора
// --------------------------------------------------------------------------- //

/**
 * @param {Object} state    состояние из предыдущего прогона (emptyState() при первом)
 * @param {Array}  snapshots [{exchange, symbol, price, volume24h, funding}]
 * @param {number} nowSec   текущее время в секундах
 * @param {Object} cfg      defaultConfig()
 * @return {{alerts: Array, state: Object, stats: Object}}
 */
function stepDetector(state, snapshots, nowSec, cfg) {
  var det = cfg.detection;
  var uni = cfg.universe;
  var rt = cfg.runtime;
  var tol = det.windowToleranceMult;

  // -- 1. принять снимки ---------------------------------------------------- //
  var seen = {};
  for (var i = 0; i < snapshots.length; i++) {
    var snap = snapshots[i];
    if (!snap || !snap.symbol) continue;
    if (snap.symbol.slice(-uni.quoteSuffix.length) !== uni.quoteSuffix) continue;
    if (!(snap.price > 0)) continue;

    var key = snap.exchange + ':' + snap.symbol;
    var st = state.symbols[key];
    if (!st) {
      st = { fs: nowSec, t: [], m: [], vo: [], f: null };
      state.symbols[key] = st;
    }
    st.t.push([nowSec, snap.price, snap.volume24h]);
    if (st.t.length > rt.ticksKeep) st.t = st.t.slice(-rt.ticksKeep);
    if (snap.funding !== null && snap.funding !== undefined) st.f = snap.funding;
    seen[key] = true;
  }

  // -- 2. BTC-фильтр режима рынка ------------------------------------------ //
  var btcMult = 1.0;
  var btcMoves = [];
  ['BINANCE', 'BYBIT'].forEach(function (ex) {
    var st = state.symbols[ex + ':' + cfg.btcFilter.btcSymbol];
    if (!st) return;
    var mv = movePct(st.t, det.windowMainSec, tol);
    if (mv !== null) btcMoves.push(mv);
  });
  if (btcMoves.length && Math.max.apply(null, btcMoves) >= cfg.btcFilter.btcMove15mPct) {
    btcMult = cfg.btcFilter.thresholdMultiplier;
  }

  // -- 3. статистика и триггеры -------------------------------------------- //
  var minHistorySec = uni.minHistoryMinutes * 60;
  var signals = [];
  var checked = 0;

  Object.keys(state.symbols).forEach(function (key) {
    var st = state.symbols[key];
    var parts = key.split(':');
    var exchange = parts[0];
    var symbol = parts[1];
    var last = st.t.length ? st.t[st.t.length - 1] : null;

    // символ давно не появляется в тикерах — выкидываем, чтобы состояние не пухло
    if (!last || nowSec - last[0] > rt.staleDropSec) {
      delete state.symbols[key];
      return;
    }
    if (!seen[key]) return;  // на этом проходе биржа по нему не ответила

    var move15m = movePct(st.t, det.windowMainSec, tol);
    var vol15m = intervalVolume(st.t, det.windowMainSec, tol);

    // статистика — по истории ДО текущего наблюдения, иначе сам памп
    // поднимает mu/sigma и глушит свой же z-score
    var histMoves = st.m.slice();
    var histVols = st.vo.slice();
    if (move15m !== null) {
      st.m.push(move15m);
      if (st.m.length > rt.historyKeep) st.m = st.m.slice(-rt.historyKeep);
    }
    if (vol15m !== null) {
      st.vo.push(vol15m);
      if (st.vo.length > rt.historyKeep) st.vo = st.vo.slice(-rt.historyKeep);
    }

    if (uni.excludeSymbols.indexOf(symbol) >= 0) return;
    if (last[2] < uni.min24hVolumeUsd) return;
    if (nowSec - st.fs < minHistorySec) return;
    if (move15m === null || vol15m === null) return;
    if (histMoves.length < det.minObservations || histVols.length < det.minObservations) return;

    var mu = meanOf(histMoves);
    var sigma = Math.max(stdevOf(histMoves, mu), det.sigmaFloorPct);
    var volBaseline = meanOf(histVols);
    if (!(volBaseline > 0)) return;
    checked++;

    var zscore = (move15m - mu) / sigma;
    var volMult = vol15m / volBaseline;
    var move5m = movePct(st.t, det.windowFastSec, tol);
    var vol5m = intervalVolume(st.t, det.windowFastSec, tol);

    var triggers = [];
    if (zscore >= det.zscoreThreshold * btcMult &&
        move15m >= det.minAbsMoveMainPct * btcMult &&
        vol15m >= det.volumeMultMain * volBaseline) {
      triggers.push('MAIN');
    }
    var fastBaseline = volBaseline * (det.windowFastSec / det.windowMainSec);
    if (move5m !== null && vol5m !== null &&
        move5m >= det.fastMovePct * btcMult &&
        vol5m >= det.volumeMultFast * fastBaseline) {
      triggers.push('FAST');
    }
    if (!triggers.length) return;

    signals.push({
      exchange: exchange,
      symbol: symbol,
      triggers: triggers,
      move15m: move15m,
      move5m: move5m,
      zscore: zscore,
      volMult: volMult,
      price: last[1],
      volume24h: last[2],
      funding: st.f === undefined ? null : st.f,
      ts: nowSec
    });
  });

  // -- 4. дедуп между биржами + анти-спам ---------------------------------- //
  var bySymbol = {};
  signals.forEach(function (sig) {
    (bySymbol[sig.symbol] = bySymbol[sig.symbol] || []).push(sig);
  });

  var merged = Object.keys(bySymbol).map(function (symbol) {
    var group = bySymbol[symbol];
    var primary = group.reduce(function (a, b) { return b.zscore > a.zscore ? b : a; });
    var triggers = ['MAIN', 'FAST'].filter(function (t) {
      return group.some(function (s) { return s.triggers.indexOf(t) >= 0; });
    });
    primary.triggers = triggers;
    var exchanges = group.map(function (s) { return s.exchange; })
      .filter(function (v, idx, arr) { return arr.indexOf(v) === idx; })
      .sort();
    return { symbol: symbol, exchanges: exchanges, primary: primary, ts: nowSec };
  }).sort(function (a, b) { return b.primary.zscore - a.primary.zscore; });

  var cooldownSec = cfg.alerts.cooldownMin * 60;
  state.day = (state.day || []).filter(function (ts) { return nowSec - ts <= cfg.alerts.dayWindowSec; });

  var accepted = [];
  merged.forEach(function (alert) {
    var lastTs = state.cool[alert.symbol];
    if (lastTs !== undefined && nowSec - lastTs < cooldownSec) return;
    if (state.day.length >= cfg.alerts.maxPerDay) return;
    state.cool[alert.symbol] = nowSec;
    state.day.push(nowSec);
    accepted.push(alert);
  });

  // чистим cooldown-записи старше суток, чтобы состояние не росло
  Object.keys(state.cool).forEach(function (symbol) {
    if (nowSec - state.cool[symbol] > cfg.alerts.dayWindowSec) delete state.cool[symbol];
  });

  state.saved = nowSec;
  return {
    alerts: accepted,
    state: state,
    stats: { symbols: Object.keys(state.symbols).length, checked: checked, btcMult: btcMult }
  };
}

// --------------------------------------------------------------------------- //
// парсеры ответов бирж (чистые — тестируются без сети)
// --------------------------------------------------------------------------- //

function parseBinanceTickers(data) {
  var out = [];
  if (!data || !data.length) return out;
  for (var i = 0; i < data.length; i++) {
    var it = data[i];
    var price = parseFloat(it.lastPrice);
    var vol = parseFloat(it.quoteVolume);
    if (!it.symbol || !isFinite(price) || !isFinite(vol)) continue;
    out.push({ exchange: 'BINANCE', symbol: it.symbol, price: price, volume24h: vol, funding: null });
  }
  return out;
}

function parseBybitTickers(data) {
  var out = [];
  var list = data && data.result && data.result.list;
  if (!list) return out;
  for (var i = 0; i < list.length; i++) {
    var it = list[i];
    var price = parseFloat(it.lastPrice);
    var vol = parseFloat(it.turnover24h);
    if (!it.symbol || !isFinite(price) || !isFinite(vol)) continue;
    var funding = parseFloat(it.fundingRate);
    out.push({
      exchange: 'BYBIT',
      symbol: it.symbol,
      price: price,
      volume24h: vol,
      funding: isFinite(funding) ? funding : null
    });
  }
  return out;
}

function parseBinanceFunding(data) {
  var out = {};
  if (!data || !data.length) return out;
  for (var i = 0; i < data.length; i++) {
    var rate = parseFloat(data[i].lastFundingRate);
    if (data[i].symbol && isFinite(rate)) out[data[i].symbol] = rate;
  }
  return out;
}

// экспорт для Node-тестов; в Apps Script этот блок игнорируется
if (typeof module !== 'undefined') {
  module.exports = {
    defaultConfig: defaultConfig,
    emptyState: emptyState,
    stepDetector: stepDetector,
    renderAlert: renderAlert,
    movePct: movePct,
    intervalVolume: intervalVolume,
    fmtVolume: fmtVolume,
    fmtPrice: fmtPrice,
    parseBinanceTickers: parseBinanceTickers,
    parseBybitTickers: parseBybitTickers,
    parseBinanceFunding: parseBinanceFunding
  };
}
