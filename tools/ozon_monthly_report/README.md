# Ozon monthly API data pack

Сборщик исходных данных для ежемесячного управленческого отчёта MP-Unit.

Цель: перестать вручную скачивать 10–15 XLSX/CSV из Ozon. Один запуск сохраняет воспроизводимый набор исходников за месяц, из которого строятся P&L, ROI, заказы, отмены, возвраты и переходящие заказы.

## Что забирается

### Seller API

- `/v1/finance/accrual/types` — справочник начислений;
- `/v1/finance/accrual/by-day` — **основной источник финансовых начислений**;
- `/v4/posting/fbs/list` — FBS-отправления (актуальная версия);
- `/v3/posting/fbo/list` — FBO-отправления (актуальная версия);
- `/v1/returns/list` — возвраты;
- `/v1/finance/products/buyout` — выкупленные Ozon товары.

Старые `/v3/finance/transaction/list`, `/v3/posting/fbs/list` и `/v2/posting/fbo/list` намеренно не используются.

### Performance API — опционально

Если заданы отдельные credentials рекламного кабинета:

- кампании CPC;
- контрольная статистика CPC;
- выгрузки статистики CPC по кампаниям/SKU;
- отчёт заказов «Оплата за заказ» (CPA).

Performance API использует **другой client_id/client_secret**, это не Seller API key.

## Секреты

Seller API credentials уже могут храниться в корне MP-Unit в `stores.secrets.js`. Этот файл находится в `.gitignore`; сборщик его читает, но **никогда не копирует ключи в выгрузку или manifest**.

Альтернатива — переменные окружения:

```bash
export OZON_CLIENT_ID='...'
export OZON_API_KEY='...'
export OZON_STORE_NAME='Косметика'
```

Для рекламы:

```bash
export OZON_PERFORMANCE_CLIENT_ID='...'
export OZON_PERFORMANCE_CLIENT_SECRET='...'
```

Не коммитьте эти значения в Git.

## Первый запуск за август 2026

Из корня проекта:

```bash
python3 tools/ozon_monthly_report/ozon_monthly_data.py \
  --month 2026-08 \
  --store 'Косметика' \
  --as-of 2026-09-04 \
  --with-performance
```

Если имя магазина в `stores.secrets.js` другое, первый раз можно запустить без `--store`: будут обработаны все настроенные магазины.

Без Performance credentials:

```bash
python3 tools/ozon_monthly_report/ozon_monthly_data.py \
  --month 2026-08 \
  --store 'Косметика' \
  --as-of 2026-09-04
```

## Результат

По умолчанию данные появляются в:

```text
data/ozon_monthly/2026-08/
├── manifest.json
├── seller/
│   └── <магазин>/
│       ├── accrual_types.json
│       ├── accruals.jsonl
│       ├── fbs_postings.jsonl
│       ├── fbo_postings.jsonl
│       ├── returns.jsonl
│       └── buyouts.json
└── performance/                 # если включён Performance API
    ├── campaigns_cpc.json
    ├── cpc_campaign_summary.json
    ├── cpc_sku_*.csv|zip
    └── cpa_orders_*.csv|zip
```

`data/` уже исключён из Git.

## Почему возвраты собираются до `--as-of`

Для отчёта за август недостаточно смотреть только 01.08–31.08. Августовский заказ может отмениться/вернуться в сентябре. Поэтому заказы фиксируются за месяц, а возвраты запрашиваются от начала месяца до даты `--as-of`. В итоговом отчёте они сопоставляются обратно с августовскими `posting_number/order_id`.

Это даёт нормальные блоки:

- отменено до отгрузки;
- отменено после отгрузки;
- доставлено в августе;
- доставлено в сентябре;
- ещё в пути;
- возвратилось позднее.

## Бухгалтерская логика

1. Финансовые начисления берём из `/v1/finance/accrual/by-day`.
2. Внутренние комиссии/логистика/эквайринг, уже сидящие в начислении, второй раз не вычитаются.
3. Выкупы Ozon хранятся отдельно и при построении P&L проверяются на двойной учёт.
4. CPC и CPA берутся из Performance API и связываются с рекламируемым SKU/заказом.
5. Закупка и внутренняя стоимость упаковки **не существуют в Ozon API** — их берём из MP-Unit.
6. Штраф за нерекомендованный слот в товарном ROI должен связываться с отправлением; если в отправлении несколько SKU — распределяться пропорционально стоимости строк.

## Код возврата

- `0` — Seller API собран без ошибок;
- `2` — один из блоков Seller API не собрался; детали есть в `manifest.json`.

Performance API не делает Seller-сборку аварийной: рекламная ошибка записывается в manifest, чтобы финансовые данные Ozon всё равно сохранились.
