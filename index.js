// ===================== ticket-annul-bot / index.js =====================
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import TelegramBot from 'node-telegram-bot-api';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import http from 'http';

// ---------- 0) Открываем порт СРАЗУ (healthcheck для Render) ----------
const port = process.env.PORT || 10000;
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok');
}).listen(port, () => {
  console.log('🌐 Healthcheck server listening on port', port);
});

// ---------- 1) ENV ----------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

const {
  BOT_TOKEN,
  SHEET_ID,
  APPROVERS = '',
  REQUIRED_APPROVALS = '1',
} = process.env;

// ---------- 2) Глобальные (заполняются в init) ----------
let bot;                 // TelegramBot
let doc;                 // GoogleSpreadsheet
const ticketsState = new Map();
const pendingComments = new Map();

const APPROVER_LIST = APPROVERS.split(',').map(s => s.trim()).filter(Boolean);
const APPROVER_SET  = new Set(APPROVER_LIST);
const REQUIRED      = Number(REQUIRED_APPROVALS) || 1;
const PING_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 часа

const fullName = u => [u.first_name, u.last_name].filter(Boolean).join(' ') || 'сотрудник';
const mentionByProfile = u => (u.username ? `@${u.username}` : `<a href="tg://user?id=${u.id}">${fullName(u)}</a>`);
const mentionApproversLine = () => (APPROVER_LIST.length ? `Утверждающие: ${APPROVER_LIST.map(u => `@${u}`).join(', ')}` : '');
const needFooterLine = () => (REQUIRED === 1 ? 'Нужно одобрение: 1' : `Нужно одобрений: ${REQUIRED}`);
const nowHelsinkiString = () => new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Helsinki' });
const monthSheetName = () => {
  const dt = new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Helsinki' }));
  return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
};

const REQUIRED_HEADERS = [
  'Тикет','Тип нарушения','Основание для аннулирования','Сумма',
  'Оператор','Статус согласования','Кто подтвердил','Дата внесения'
];

// ---------- 3) Таблицы ----------
async function ensureHeaders(sheet) {
  await sheet.loadHeaderRow();
  const current = Array.isArray(sheet.headerValues) ? sheet.headerValues : [];
  if (!current || current.length === 0 || current.every(c => !c || !String(c).trim())) {
    await sheet.setHeaderRow(REQUIRED_HEADERS);
    console.log(`🛠 Шапка листа "${sheet.title}" восстановлена.`);
    return;
  }
  const have = new Set(current);
  const missing = REQUIRED_HEADERS.filter(h => !have.has(h));
  if (missing.length) {
    await sheet.setHeaderRow([...current, ...missing]);
    console.log(`🛠 Добавлены колонки: ${missing.join(', ')}`);
  }
}

async function getOrCreateMonthlySheet() {
  const title = monthSheetName();
  await doc.loadInfo();
  let sh = doc.sheetsByTitle[title];
  if (!sh) {
    console.log(`ℹ️ Создаём лист "${title}"`);
    sh = await doc.addSheet({ title, headerValues: REQUIRED_HEADERS });
  } else {
    await ensureHeaders(sh);
  }
  return sh;
}

function makeCardText(st, { progress = null, footer = '' } = {}) {
  const lines = [
    '<b>🧾 Аннулирование штрафа</b>','',
    `<b>Тикет:</b> ${st.ticket}`,
    `<b>Тип нарушения:</b> ${st.violation}`,
    `<b>Основание:</b> ${st.reason}`,
    (st.amount ? `<b>Сумма:</b> ${st.amount}` : ''),
    (st.operator ? `<b>Оператор:</b> ${st.operator}` : ''),
    '',
    (progress != null ? `<b>Статус:</b> ${progress}` : ''),
    (footer ? `${footer}` : '')
  ].filter(Boolean);
  return lines.join('\n');
}

// ---------- 4) Парсинг ----------
function parsePayload(text) {
  if (!text) return null;
  const grab = (label) => (text.match(new RegExp(`${label}:\\s*([^\\n]+)`, 'i')) || [,''])[1].trim();
  const ticket    = grab('Тикет');
  const violation = grab('Нарушение');
  const reason    = grab('Причина');
  const amount    = grab('Сумма');
  const operator  = grab('Оператор');
  if (!ticket || !violation || !reason) return null;
  return { ticket, violation, reason, amount, operator };
}

// ---------- 5) INIT (асинхронно, порт уже открыт) ----------
async function init() {
  try {
    if (!BOT_TOKEN) throw new Error('BOT_TOKEN отсутствует');
    if (!SHEET_ID) throw new Error('SHEET_ID отсутствует');

    // creds из GOOGLE_CREDS (Render) или локального credentials.json
    let rawCreds;
    try {
      if (process.env.GOOGLE_CREDS && process.env.GOOGLE_CREDS.trim().startsWith('{')) {
        rawCreds = JSON.parse(process.env.GOOGLE_CREDS);
      } else {
        rawCreds = JSON.parse(fs.readFileSync(path.join(__dirname, 'credentials.json'), 'utf8'));
      }
    } catch (e) {
      console.error('❌ Не удалось прочитать GOOGLE_CREDS/credentials.json:', e?.message || e);
      throw e;
    }

    const auth = new JWT({
      email: rawCreds.client_email,
      key: rawCreds.private_key,
      scopes: [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
      ],
    });

    doc = new GoogleSpreadsheet(SHEET_ID, auth);
    await doc.loadInfo();
    console.log('✅ Подключено к Google Sheet:', doc.title);

    bot = new TelegramBot(BOT_TOKEN, { polling: true });
    console.log('🤖 Бот запущен');

    // ===== Хэндлеры =====
    bot.onText(/^\/(?:анн|ann|a)(?:@[\w_]+)?(?:\s+|$)/i, async (msg) => {
      const chatId = msg.chat.id;
      const userName = msg.from.first_name || msg.from.username || 'коллега';
      const template = [
        `Привет, ${userName}! 👋`,
        `Вот шаблон для аннулирования — просто заполни поля и отправь его сюда:`,
        '',
        '#аннулировать','Тикет:','Нарушение:','Причина:','Сумма:','Оператор:'
      ].join('\n');
      await bot.sendMessage(chatId, template);
    });

    bot.onText(/#аннулировать([\s\S]*)/i, async (msg, match) => {
      const chatId = msg.chat.id;
      const data = parsePayload((match?.[1] || '').trim());
      if (!data) {
        await bot.sendMessage(chatId,
          '⚠️ Не удалось распознать формат. Используй:\n#аннулировать\nТикет:\nНарушение:\nПричина:\nСумма:\nОператор:'
        );
        return;
      }
      const summary = makeCardText(data, { footer: `${mentionApproversLine()}\n${needFooterLine()}` });
      const sent = await bot.sendMessage(chatId, summary, {
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [[
          { text: '✅ Одобрить', callback_data: 'approve' },
          { text: '❌ Отклонить', callback_data: 'reject' }
        ]]}
      });
      ticketsState.set(sent.message_id, {
        chatId, ...data, approvals: new Map(), voters: new Set(), resolved: false, rejected: false
      });
      // Пинг через 2 часа
      setTimeout(async () => {
        const st = ticketsState.get(sent.message_id);
        if (!st || st.resolved) return;
        const pending = APPROVER_LIST.filter(u => !Array.from(st.approvals.values()).some(p => p.username === u));
        if (pending.length === 0) return;
        await bot.sendMessage(chatId, makeCardText(st, {
          footer: `⏰ <i>Напоминание:</i> не хватает одобрения. Прошу ${pending.map(u => `@${u}`).join(', ') || 'утверждающих'} подтвердить.`
        }), { parse_mode: 'HTML' });
      }, PING_TIMEOUT_MS);
    });

    bot.on('callback_query', async (query) => {
      const msgId = query.message?.message_id;
      const chatId = query.message?.chat.id;
      if (!msgId || !chatId) return;

      const st = ticketsState.get(msgId);
      if (!st || st.resolved) return;

      const user = query.from;
      const userId = user.id;
      const username = user.username || '';
      const prof = { id: userId, username, name: fullName(user) };

      if (APPROVER_SET.size && !APPROVER_SET.has(username)) {
        await bot.answerCallbackQuery(query.id, { text: 'Нет прав на согласование', show_alert: true });
        return;
      }
      if (st.voters.has(userId)) {
        await bot.answerCallbackQuery(query.id, { text: 'Вы уже голосовали', show_alert: true });
        return;
      }

      if (query.data === 'approve') {
        st.voters.add(userId);
        st.approvals.set(userId, prof);
        await bot.answerCallbackQuery(query.id, { text: 'Одобрение учтено' });

        const approvedList = Array.from(st.approvals.values()).map(p => mentionByProfile(p)).join(', ') || '—';
        const progress = `${st.approvals.size}/${REQUIRED}`;
        await bot.editMessageText(
          makeCardText(st, { progress, footer: `${mentionApproversLine()}\n<b>Одобрили:</b> ${approvedList}` }),
          { chat_id: chatId, message_id: msgId, parse_mode: 'HTML' }
        );

        if (st.approvals.size >= REQUIRED) {
          st.resolved = true;
          try {
            const sheet = await getOrCreateMonthlySheet();
            await ensureHeaders(sheet);
            await sheet.addRow({
              'Тикет': st.ticket,
              'Тип нарушения': st.violation,
              'Основание для аннулирования': st.reason,
              'Сумма': st.amount || '',
              'Оператор': st.operator || '',
              'Статус согласования': 'Одобрено',
              'Кто подтвердил': Array.from(st.approvals.values()).map(a => a.username || a.name).join(', '),
              'Дата внесения': nowHelsinkiString()
            });

            const approverNames = Array.from(st.approvals.values())
              .map(a => (a.username ? `@${a.username}` : a.name)).join(', ');
            const others = APPROVER_LIST
              .filter(u => !Array.from(st.approvals.values()).some(a => a.username === u))
              .map(u => `@${u}`)
              .join(', ');

            await bot.editMessageText(
              `✅ Тикет ${st.ticket} согласован (${approverNames}). Записано в лист «${monthSheetName()}».` +
              (others ? `\nℹ️ Для информации: ${others}` : ''),
              { chat_id: chatId, message_id: msgId, parse_mode: 'HTML' }
            );
          } catch (e) {
            console.error('❌ addRow error:', e);
            await bot.sendMessage(chatId, `⚠️ Ошибка записи в таблицу: ${e.message || e}`);
          }
        }
      }

      if (query.data === 'reject') {
        st.voters.add(userId);
        st.rejected = true;
        st.resolved = true;

        await bot.answerCallbackQuery(query.id, { text: 'Укажите причину отказа' });
        const prompt = await bot.sendMessage(
          chatId,
          `❌ ${mentionByProfile(user)}, ответьте на это сообщение комментарием (почему отклонено тикет ${st.ticket}).`,
          { reply_markup: { force_reply: true }, parse_mode: 'HTML' }
        );
        pendingComments.set(`${chatId}:${userId}`, { promptMsgId: prompt.message_id, ticketMsgId: msgId });

        await bot.editMessageText(
          `❌ Тикет ${st.ticket} отклонён. Ожидаю комментарий от ${mentionByProfile(user)}.`,
          { chat_id: chatId, message_id: msgId, parse_mode: 'HTML' }
        );
      }
    });

    bot.on('message', async (msg) => {
      const chatId = msg.chat.id;
      const key = `${chatId}:${msg.from.id}`;
      const wait = pendingComments.get(key);
      if (!wait) return;
      if (!msg.reply_to_message || msg.reply_to_message.message_id !== wait.promptMsgId) return;

      const st = ticketsState.get(wait.ticketMsgId);
      if (!st) {
        pendingComments.delete(key);
        return;
      }

      st.rejectComment = (msg.text || '').trim();
      pendingComments.delete(key);

      await bot.editMessageText(
        `❌ Тикет ${st.ticket} отклонён.\n<b>Комментарий:</b> ${st.rejectComment || '—'}\n<b>От:</b> ${mentionByProfile(msg.from)}`,
        { chat_id: chatId, message_id: wait.ticketMsgId, parse_mode: 'HTML' }
      );

      try { await bot.deleteMessage(chatId, wait.promptMsgId); } catch {}
      try { await bot.deleteMessage(chatId, msg.message_id); } catch {}
    });

    bot.onText(/^\/stats(?:@[\w_]+)?(?:\s+(\d{4}-\d{2}))?$/i, async (msg, match) => {
      const chatId = msg.chat.id;
      const monthTitle = (match && match[1]) ? match[1] : monthSheetName();
      try {
        await doc.loadInfo();
        const sh = doc.sheetsByTitle[monthTitle];
        if (!sh) {
          await bot.sendMessage(chatId, `📊 Лист «${monthTitle}» не найден. Формат вкладки: YYYY-MM (например, 2025-11).`);
          return;
        }
        await ensureHeaders(sh);

        const headers = sh.headerValues || [];
        const idx = (name) => headers.indexOf(name);
        const iType   = idx('Тип нарушения');
        const iStatus = idx('Статус согласования');
        const iAmount = idx('Сумма');

        if (iType === -1 || iStatus === -1) {
          await bot.sendMessage(chatId, `⚠️ Нет колонок «Тип нарушения» или «Статус согласования». Текущая шапка: ${headers.join(' | ') || '—'}`);
          return;
        }

        const rows = await sh.getRows();
        if (!rows.length) {
          await bot.sendMessage(chatId, `📊 На листе «${monthTitle}» пока нет данных.`);
          return;
        }

        const agg = new Map();
        let totalApproved = 0;
        let totalAmount = 0;

        for (const r of rows) {
          const status = (r._rawData[iStatus] || '').toString().trim().toLowerCase();
          if (status !== 'одобрено') continue;
          const type = (r._rawData[iType] || '—').toString().trim();
          const amountVal = (iAmount !== -1)
            ? (() => {
                const raw = (r._rawData[iAmount] ?? '').toString().replace(/\s/g, '').replace(',', '.');
                const v = parseFloat(raw);
                return Number.isFinite(v) ? v : 0;
              })()
            : 0;

          totalApproved += 1;
          totalAmount += amountVal;

          const cur = agg.get(type) || { count: 0, sum: 0 };
          cur.count++;
          cur.sum += amountVal;
          agg.set(type, cur);
        }

        if (totalApproved === 0) {
          await bot.sendMessage(chatId, `📊 За «${monthTitle}» одобренных записей не найдено.`);
          return;
        }

        const fmt = (n) => new Intl.NumberFormat('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 2 }).format(n);
        const lines = Array.from(agg.entries())
          .sort((a, b) => b[1].count - a[1].count)
          .map(([type, v]) => `• ${type}: ${v.count} — сумма: ${fmt(v.sum)}`);
        const reply =
          `<b>📊 Сводка за ${monthTitle}</b>\n` +
          `<b>Одобрено записей:</b> ${totalApproved}\n\n` +
          lines.join('\n') +
          `\n<b>Итого сумма:</b> ${fmt(totalAmount)}`;

        await bot.sendMessage(chatId, reply, { parse_mode: 'HTML' });
      } catch (e) {
        console.error('stats fatal error:', e);
        await bot.sendMessage(chatId, '⚠️ Ошибка чтения сводки.');
      }
    });

    console.log('✅ init завершён успешно');
  } catch (e) {
    console.error('💥 init error:', e?.message || e);
    // Порт открыт, сервис живёт — можно смотреть логи и править переменные
  }
}

// Стартуем init в фоне
setImmediate(init);
// ===================== end of file =====================
