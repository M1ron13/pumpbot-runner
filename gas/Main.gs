/**
 * Pump Detector — обвязка Apps Script: сеть, состояние в Drive, Telegram, триггер.
 * Логика детекции — в core.gs (тот же файл гоняется тестами на Node).
 *
 * Что запускать руками в редакторе:
 *   setup()        — один раз: сохранить креды в Script Properties, поставить
 *                    минутный триггер, отправить тестовое сообщение
 *   health()       — сводка состояния в лог и в Telegram
 *   resetState()   — стереть накопленную историю (начнёт набирать заново)
 *   stopBot()      — снять триггер (бот замолчит)
 */

var PROP_TOKEN = 'TG_TOKEN';
var PROP_CHAT = 'TG_CHAT_ID';
var TRIGGER_FN = 'detectorTick';
var TRIGGER_EVERY_MINUTES = 1;   // минимум, который умеет Apps Script

// --------------------------------------------------------------------------- //
// точка входа: вызывается триггером раз в минуту
// --------------------------------------------------------------------------- //

function detectorTick() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    console.warn('предыдущий прогон ещё идёт — пропускаю');
    return;
  }
  try {
    var cfg = defaultConfig();
    var nowSec = Math.floor(Date.now() / 1000);
    var state = loadState_(cfg);

    var snapshots = fetchSnapshots_(cfg);
    if (!snapshots.length) {
      console.warn('пустой снимок рынка — итерация пропущена');
      return;
    }
    applyFunding_(cfg, state, snapshots, nowSec);

    var result = stepDetector(state, snapshots, nowSec, cfg);
    result.alerts.forEach(function (alert) {
      var sig = alert.primary;
      console.log('АЛЕРТ ' + sig.symbol + ' [' + alert.exchanges.join('+') + '] ' +
                  sig.triggers.join('+') + ': ' + sig.move15m.toFixed(2) + '%/15м z=' +
                  sig.zscore.toFixed(2) + ' vol=' + sig.volMult.toFixed(2) + 'x');
      sendTelegram_(cfg, renderAlert(alert, cfg));
    });

    saveState_(cfg, result.state);
  } catch (err) {
    console.error('ошибка итерации: ' + (err && err.stack ? err.stack : err));
  } finally {
    lock.releaseLock();
  }
}

// --------------------------------------------------------------------------- //
// сеть
// --------------------------------------------------------------------------- //

function fetchSnapshots_(cfg) {
  var requests = [
    { url: cfg.sources.binanceTickersUrl, muteHttpExceptions: true },
    { url: cfg.sources.bybitTickersUrl, muteHttpExceptions: true }
  ];
  var responses;
  try {
    responses = UrlFetchApp.fetchAll(requests);   // оба запроса параллельно
  } catch (err) {
    console.warn('запрос тикеров не удался: ' + err);
    return [];
  }
  var out = [];
  out = out.concat(safeParse_('binance/tickers', responses[0], parseBinanceTickers));
  out = out.concat(safeParse_('bybit/tickers', responses[1], parseBybitTickers));
  return out;
}

function safeParse_(name, response, parser) {
  try {
    var code = response.getResponseCode();
    if (code !== 200) {
      console.warn(name + ': HTTP ' + code);
      return [];
    }
    return parser(JSON.parse(response.getContentText()));
  } catch (err) {
    console.warn(name + ': разбор ответа не удался: ' + err);
    return [];
  }
}

/** Funding: у Bybit приходит с тикерами, у Binance — отдельным запросом раз в N сек. */
function applyFunding_(cfg, state, snapshots, nowSec) {
  if (nowSec - (state.fundingAt || 0) < cfg.runtime.fundingRefreshSec) return;
  try {
    var resp = UrlFetchApp.fetch(cfg.sources.binancePremiumIndexUrl, { muteHttpExceptions: true });
    if (resp.getResponseCode() !== 200) {
      console.warn('binance/premiumIndex: HTTP ' + resp.getResponseCode());
      return;
    }
    var rates = parseBinanceFunding(JSON.parse(resp.getContentText()));
    for (var i = 0; i < snapshots.length; i++) {
      if (snapshots[i].exchange === 'BINANCE' && rates[snapshots[i].symbol] !== undefined) {
        snapshots[i].funding = rates[snapshots[i].symbol];
      }
    }
    state.fundingAt = nowSec;
  } catch (err) {
    console.warn('funding не обновлён: ' + err);
  }
}

// --------------------------------------------------------------------------- //
// состояние: один gzip-JSON в Drive
// --------------------------------------------------------------------------- //

function stateFile_(cfg, createIfMissing) {
  var it = DriveApp.getFilesByName(cfg.runtime.stateFileName);
  if (it.hasNext()) return it.next();
  if (!createIfMissing) return null;
  return DriveApp.createFile(Utilities.newBlob('', 'application/gzip', cfg.runtime.stateFileName));
}

function loadState_(cfg) {
  try {
    var file = stateFile_(cfg, false);
    if (!file) return emptyState();
    var blob = file.getBlob();
    if (!blob.getBytes().length) return emptyState();
    var json = Utilities.ungzip(blob.setContentType('application/x-gzip')).getDataAsString('UTF-8');
    var state = JSON.parse(json);
    if (!state || state.v !== 1) return emptyState();
    state.symbols = state.symbols || {};
    state.cool = state.cool || {};
    state.day = state.day || [];
    return state;
  } catch (err) {
    console.warn('состояние не прочитано, начинаю с чистого листа: ' + err);
    return emptyState();
  }
}

function saveState_(cfg, state) {
  try {
    var gz = Utilities.gzip(Utilities.newBlob(JSON.stringify(state), 'application/json', 'state.json'));
    var file = stateFile_(cfg, true);
    file.setContent('');                        // сбросить старое содержимое
    file.setTrashed(false);
    Drive_updateBlob_(file, gz, cfg);
  } catch (err) {
    console.error('состояние не сохранено: ' + err);
  }
}

/** DriveApp не умеет заменять blob напрямую — пересоздаём файл под тем же именем. */
function Drive_updateBlob_(file, blob, cfg) {
  var name = cfg.runtime.stateFileName;
  file.setTrashed(true);
  DriveApp.createFile(blob.setName(name));
}

// --------------------------------------------------------------------------- //
// Telegram
// --------------------------------------------------------------------------- //

function creds_() {
  var props = PropertiesService.getScriptProperties();
  var token = props.getProperty(PROP_TOKEN);
  var chat = props.getProperty(PROP_CHAT);
  if ((!token || !chat) && typeof CREDS !== 'undefined') {
    token = token || CREDS.token;
    chat = chat || CREDS.chatId;
  }
  return { token: token, chatId: chat };
}

function sendTelegram_(cfg, text) {
  var c = creds_();
  if (!c.token || !c.chatId) {
    console.error('Telegram не настроен: запусти setup() или заполни Script Properties');
    return false;
  }
  try {
    var resp = UrlFetchApp.fetch(cfg.telegram.apiBase + '/bot' + c.token + '/sendMessage', {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify({
        chat_id: c.chatId,
        text: text,
        parse_mode: cfg.telegram.parseMode,
        disable_web_page_preview: cfg.telegram.disableWebPagePreview
      })
    });
    if (resp.getResponseCode() !== 200) {
      console.error('Telegram HTTP ' + resp.getResponseCode() + ': ' + resp.getContentText().slice(0, 300));
      return false;
    }
    return true;
  } catch (err) {
    console.error('Telegram не отправлен: ' + err);
    return false;
  }
}

// --------------------------------------------------------------------------- //
// обслуживание (запускать из редактора)
// --------------------------------------------------------------------------- //

function setup() {
  var cfg = defaultConfig();

  if (typeof CREDS !== 'undefined' && CREDS.token && CREDS.chatId) {
    PropertiesService.getScriptProperties().setProperties({
      TG_TOKEN: CREDS.token,
      TG_CHAT_ID: String(CREDS.chatId)
    });
  }
  var c = creds_();
  if (!c.token || !c.chatId) {
    throw new Error('нет кредов: заполни Config.gs (CREDS) или Script Properties TG_TOKEN / TG_CHAT_ID');
  }

  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === TRIGGER_FN) ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger(TRIGGER_FN).timeBased().everyMinutes(TRIGGER_EVERY_MINUTES).create();
  console.log('триггер ' + TRIGGER_FN + ' поставлен: каждые ' + TRIGGER_EVERY_MINUTES + ' мин');

  detectorTick();   // первый прогон сразу, чтобы проверить сеть и права

  if (cfg.runtime.startupPing) {
    sendTelegram_(cfg,
      '🟢 <b>Pump Detector</b> — работает в облаке Google (24/7)\n' +
      'Опрос Binance + Bybit раз в минуту. Первые алерты — после ' +
      cfg.universe.minHistoryMinutes + ' мин накопления истории.\n' +
      'Пороги: z ≥ ' + cfg.detection.zscoreThreshold + ' · рост ≥ ' +
      cfg.detection.minAbsMoveMainPct + '% / 15м · объём ≥ ' + cfg.detection.volumeMultMain + '× среднего.');
  }
  console.log('setup завершён');
}

function stopBot() {
  var removed = 0;
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === TRIGGER_FN) { ScriptApp.deleteTrigger(t); removed++; }
  });
  console.log('снято триггеров: ' + removed);
}

function health() {
  var cfg = defaultConfig();
  var state = loadState_(cfg);
  var nowSec = Math.floor(Date.now() / 1000);
  var symbols = Object.keys(state.symbols).length;
  var ready = 0;
  var minHistorySec = cfg.universe.minHistoryMinutes * 60;
  Object.keys(state.symbols).forEach(function (k) {
    var st = state.symbols[k];
    if (nowSec - st.fs >= minHistorySec && st.m.length >= cfg.detection.minObservations) ready++;
  });
  var alerts24h = (state.day || []).filter(function (ts) { return nowSec - ts <= cfg.alerts.dayWindowSec; }).length;
  var ageMin = state.saved ? Math.round((nowSec - state.saved) / 60) : -1;
  var text = 'символов в состоянии: ' + symbols + ' · готовы к проверке: ' + ready +
             ' · алертов за 24ч: ' + alerts24h + ' · снимку минут: ' + ageMin;
  console.log(text);
  sendTelegram_(cfg, '🩺 <b>Pump Detector</b>\n' + text);
  return text;
}

function resetState() {
  var cfg = defaultConfig();
  var file = stateFile_(cfg, false);
  if (file) file.setTrashed(true);
  console.log('состояние стёрто');
}
