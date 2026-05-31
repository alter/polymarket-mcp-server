# Stage 66 + Stage 67 — Полная техспецификация для переноса

**Источник:** коммиты `22478b2` (Wave 1 S57-S67), `c9d553b` (Stage 67).
**Данные:** Bybit BTCUSDT, 1m → ресемпл на **8h** (TF=480 минут), **bars_per_year = 525600 / 480 = 1095**.
**Период:** ~6.1 года, HOLDOUT граница = `2025-01-01 UTC`.
**Комиссии:** `FEE = 0.00055` (0.055%), `SLIP = 0.00020` (0.020%) — Bybit Futures taker.
**Начальный капитал:** $10,000.
**Источники OHLCV:** TimescaleDB (docker контейнер `1ad172140528_binance-timescaledb`, БД `bybit`, таблица `klines`, интервал `1m`) → экспорт в parquet в `/tmp/bybit_data/`.

---

## 0. Быстрый FAQ — что это вообще такое

**Stage 66** — production strategy. Это композиция из 3 слоёв поверх baseline M3 TB (Triple Barrier ML exit pipeline):
- **Meta-labeling** (AFML ch.3.5): XGBoost secondary classifier гейтит primary TB exit
- **F&G feature**: alternative.me Fear & Greed Index добавлен фичами в meta-classifier
- **Kelly_half**: fractional Kelly sizing (0.5×) заменяет classical 0.15/σ vol-target

**Stage 67** — НЕ отдельная стратегия, а **аналитический ablation study** чемпиона Stage 66. Показывает сколько Calmar дал каждый слой. Переносить как "стратегию" его не нужно — он только для верификации что слои реально работают.

---

## 1. BASELINE M3 TB PIPELINE (фундамент обеих стратегий)

### 1.1 Pool построения сигналов (~6618 стратегий, функция `build_pool`)

Агрегация кандидат-стратегий на 3 таймфреймах `TF_LIST = [240, 480, 1440]` минут:

1. **Channel breakout** (`linreg`, `keltner`, `bollinger`):
   - periods: `[10, 15, 20, 30, 50, 80]`
   - widths (σ-units): `[1.0, 1.5, 2.0, 2.5]`
   - exit levels: `[0.3, 0.5, 0.7]`
   - trail pct: `[0.0, 0.05, 0.08, 0.12]`
   - flavors: `LO` (long-only), `CS100` (crossunder exit на SMA(100))

2. **Donchian breakout**:
   - entry windows: `[10, 20, 30, 50, 80, 120, 200]`
   - exit windows: `[5, 10, 15, 20, 30, 50]` (только xw<ew)
   - flavors: LO, CS100

3. **MA cross** (SMA / FRAMA / KAMA):
   - windows: `[5, 7, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300]`
   - все пары fast<slow, slow<n/2
   - flavors: LO, CS100

4. **Momentum rotation** (`momentum_rotation_signals`):
   - fast: `[5, 10, 15, 20]`, slow: `[30, 50, 80, 120, 200]`

Каждый net returns пересчитывается на `BASE_TF=480m` через `ds()` (агрегация или репликация).

### 1.2 Walk-Forward: **Nested CV** (`walk_forward_nested_cv`)

**Критично:** используется `nested_cv` вариант, НЕ `original`. Это stealth-улучшение +0.65 Calmar (Stage 67 L1).

```
TRAIN_MONTHS = 18
TEST_MONTHS = 6
STEP_MONTHS = 6
inner_val_frac = 0.25  (75% inner_train / 25% val)
K_GRID = [3, 5, 7, 10, 15, 20]
bpm = (1440 * 30) // 480 = 90 bars/month (при 8h TF)
```

Процесс на каждом WF шаге:
1. Sharpe ranking всех стратегий pool на **inner_train** (первые 75% train).
2. Для каждого K из K_GRID: top-K по Sharpe → `min_variance_subset` weights на inner_train → применить на inner_val → посчитать Calmar.
3. Выбрать K с лучшим **VAL Calmar**.
4. REFIT weights на full train (18 месяцев) с выбранным K.
5. Применить на OOS test window (следующие 6 месяцев).
6. Сдвиг на 6 месяцев и повтор.

Итог: `oos` — временной ряд weighted combo-returns на OOS барах.

### 1.3 Features для ML (функция `build_features` + wavelets)

**Standard features (`sma_cross/ml_features.py`, `build_features(close, high, low, volume)`)** — всего ~20, все lagged на 1 бар:
- Returns: `ret_1, ret_3, ret_5, ret_10, ret_20, ret_50`
- Rolling vol (std 1-bar returns): `vol_10, vol_20, vol_50, vol_100`
- Vol ratio: `vol_ratio_10_50`
- SMA position (close/SMA − 1): `sma_pos_20, sma_pos_50, sma_pos_100, sma_pos_200`
- SMA slope (5-bar): `sma_slope_20, sma_slope_50, sma_slope_100`
- RSI: `rsi_14, rsi_28`
- Drawdown from peak
- Normalized ATR: `natr_14, natr_28`
- ATR ratio: `atr_ratio_7_28`
- Volume: `vol_rel_20, vol_trend_20_50`

**Wavelet features (`wavelet_features`)** — 6 штук, db4 wavelet, level=3 decomposition на окне 64 бара, затем lag 1:
- detail energies (3 шт)
- approximation slope
- high/low frequency ratio
- total energy

**Итого base features:** standard (~20) + wavelet (6) ≈ 26 колонок.

### 1.4 Triple Barrier labels (`triple_barrier_labels`)

```python
triple_barrier_labels(raw, tp_mult=1.5, sl_mult=1.0, max_hold=10, vol_window=20)
```
- Rolling vol на 20 барах от raw returns.
- TP barrier: `1.5 × vol`, SL barrier: `1.0 × vol`, timeout: 10 баров.
- label = 1 если первым касанием был TP; 0 если SL или timeout с убытком.
- `tb_exit_labels = 1 - tb_labels` (инвертируем: "exit, если следующий сегмент скорее-убыток").

### 1.5 ML Exit (`ml_exit_flex`)

XGBoost классификатор в WF-режиме (18m/6m/6m, purge=10):
- `n_estimators=50, max_depth=1, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8`
- `scale_pos_weight = (1-pr)/pr`, `random_state=42`, `n_jobs=1`
- StandardScaler перед fit.
- Вход: `tb_exit_labels` как labels_override + features.
- Выход: `tb_exit` — raw returns с обнулёнными барами где `prob > 0.5` (exit signal).
- Transaction cost `FEE+SLIP` вычитается при переключении состояния (persistent `global_prev_exit` через окна).

### 1.6 Post-ML overlays (порядок критичен!)

```python
tb_overlay = vt_from_raw(raw, tb_exit, close, bpy)  # vol target + regime
tb_tiered, _ = tiered_reentry(tb_overlay)           # drawdown-based sizing
```

**vt_from_raw** (target 15% годовой vol, rolling 60 бар):
```python
mult[t] = clip(0.15 / ann_vol[t-1], 0.05, 1.0)
overlay = post_ml_ret * mult
return gradual_regime_overlay(overlay, close, 150, 300)
```

**gradual_regime_overlay(nr, close, sf=150, ss=300)**:
- SMA(150) = fast, SMA(300) = slow
- multiplier = `1.0` если close > SMA(150), `0.5` если close > SMA(300), иначе `0.25`.

**tiered_reentry(nr, threshold=0.10, power=3, min_alloc=0.2)**:
- Отслеживает running log-equity и peak.
- При drawdown dd от peak: `alloc = min_alloc + (1-min_alloc) × (1 - dd/threshold)^power` для dd ∈ [0, threshold].
- dd ≥ threshold (10%) → min_alloc (20%); dd = 0 → 100%.

**Baseline M3 TB метрики (референс для сравнения):**
- FULL: Calmar=3.36, Sharpe=3.11, MDD=5.6%, Final=2.20×
- DEV: Cal=3.76, HOLDOUT: Cal=2.72

---

## 2. STAGE 66 — CHAMPION `meta_fng_plus_kelly`

**Скрипт:** `scripts/run_stage66_grand_combo.py`
**Отчёт:** `results/stage66_grand_combo_results.md`

### 2.1 Формула pipeline (end-to-end)

```
raw, close, tb_exit = M3_baseline_pipeline(BTCUSDT)           # см. §1

# Слой A: Meta-labeling + F&G features
fng           = alternative_me_daily_FG()                      # §2.2
fng_lag       = fng[t-1]                                       # lag 1 bar
fng_mean_90   = rolling_mean(fng_lag, 90)                      # ~30 дней на 8h
fng_dev       = fng_lag - fng_mean_90

base_features    = hstack([standard_features, wavelet_lag1])   # ~26 cols
meta_features    = hstack([base_features, sign(tb_exit)])      # +1 col = primary_pos
meta_features_fg = hstack([meta_features, fng_lag, fng_dev])   # +2 cols F&G

gated_exit = meta_classifier_wf(meta_features_fg, tb_exit, raw, bpy, horizon=3, purge=9)
    # XGBoost WF: обнуляет tb_exit где meta_prob < 0.5

# Слой B: Fractional Kelly 0.5×
km      = kelly_mult(raw, fraction=0.5, window=180)            # §2.4
overlay = gated_exit.astype(float) * km
overlay = gradual_regime_overlay(overlay, close, 150, 300)
champion, _ = tiered_reentry(overlay, threshold=0.10, power=3, min_alloc=0.2)
```

### 2.2 Fear & Greed Index (Stage 65 feature slice)

- **API:** `https://api.alternative.me/fng/?limit=0` (всё доступно с 2018, daily).
- **Cache:** `scripts/audit_cache/fng_alternative_me.json` (список dict с keys `value`, `timestamp`, `value_classification`).
- **Alignment:** forward-fill на 8h bars — для каждого bar timestamp ищем последнюю F&G запись с `fng_ts <= bar_ts`.
- **Два производных фичи в meta-classifier:**
  - `fng_lag` = `fng[t-1]` (сырой F&G прошлого бара)
  - `fng_dev` = `fng_lag - rolling_mean_90bar(fng_lag)` (отклонение от ~30-дневного среднего)
- **⚠️ Rule-overlay вариант НЕ используется в чемпионе** (`|F&G-50| > 40 → pos × 0.5` — тестировался в Stage 65, edge не дал).

### 2.3 Meta-labeling (Stage 60, AFML ch.3.5)

**Функция `meta_classifier_wf(features, tb_exit, raw, bpy, horizon=3, purge=9)`:**

- **Target** (`meta_label`): для каждого бара где primary в позиции (`|tb_exit[t]| > 1e-9`) → `y[t] = 1 if sum(raw[t+1:t+1+horizon]) > 0 else 0`. Остальные NaN.
- **Horizon:** `META_HORIZON = 3` баров (24h на 8h TF).
- **Walk-forward:** 18m train / 6m test / 6m step (тот же bpm=90).
- **Purge:** 9 баров (= horizon × 3) в конце train window.
- **Guard:** пропуск окна если `pr < 0.02` или `pr > 0.98`, или `<50` валидных сэмплов.
- **Модель:** XGBClassifier:
  ```python
  XGBClassifier(n_estimators=50, max_depth=3, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=(1-pr)/max(pr,0.01),
                use_label_encoder=False, eval_metric="logloss",
                verbosity=0, n_jobs=1, random_state=42)
  # ВНИМАНИЕ: max_depth=3 (а не 1 как в ml_exit_flex)
  ```
- **StandardScaler** на train → transform на test.
- **Apply:** для каждого бара в test window где `|tb_exit[t]| > 1e-9`:
  - `prob = mdl.predict_proba(X[t])[0, 1]`
  - если `prob < 0.5`: `gated[t] = 0.0` (блокируем primary)
  - иначе `gated[t] = tb_exit[t]` (пропускаем)
- **Output:** `gated_exit` (тот же shape что tb_exit), `meta_prob`, `(pass_count, gate_count)`.

### 2.4 Fractional Kelly sizing (Stage 62)

**Функция `kelly_mult(raw, fraction=0.5, window=180)`:**

```python
KELLY_WINDOW = 180   # ~60 дней на 8h
KELLY_MIN = 0.05     # floor
KELLY_MAX = 1.0      # cap (no leverage)

for t in range(window+1, n):
    seg  = raw[t-window:t]
    mu   = mean(seg)
    sig2 = var(seg)
    f_star = mu / sig2
    mult[t] = clip(0.5 * f_star, 0.05, 1.0)
```

**⚠️ Caveat (из Stage 67):** clamping [0.05, 1.0] + `fraction=0.5` на практике делает этот overlay почти бинарным trend-filter (μ>0 → почти full, μ<0 → `KELLY_MIN`). Это НЕ теоретический log-optimal Kelly — это rolling trend regime.

### 2.5 Замена vt_from_raw

В baseline pipeline `vt_from_raw` применяла `0.15/ann_vol` multiplier. В champion `kelly_mult` **заменяет** его:

```python
# baseline:  overlay = tb_exit × (0.15/ann_vol) × regime
# champion:  overlay = gated_exit × kelly_mult × regime
```

`gradual_regime_overlay(..., 150, 300)` и `tiered_reentry(..., 0.10, 3, 0.2)` остаются.

### 2.6 Метрики Champion (Stage 66 результат)

| Период | Calmar | Sharpe | MDD | Final |
|---|---|---|---|---|
| FULL (6.1y) | 6.34 | 3.86 | 4.0% | 2.79× |
| DEV (до 2025-01) | 6.87 | 3.80 | 4.0% | 2.21× |
| HOLDOUT (2025-01+) | 26.93 | 4.35 | 0.7% | 1.26× |

Δ vs baseline M3 TB: **ΔCalmar = +2.98, ΔMDD = −1.63pp**.

### 2.7 ⚠️ Важные caveats для production

1. **HOLDOUT Cal 27 — артефакт**. MDD 0.7% аномально низкая; Calmar = CAGR/MDD раздут near-zero знаменателем. Реалистичный target: **Cal 10–15 при MDD 2–4%** (geomean DEV×HOLDOUT = 13.6).
2. **Kelly_half = binary trend filter** из-за clamping. Если хочешь "настоящий Kelly" — убери `KELLY_MAX=1.0` и разреши leverage (но это изменит риск-профиль).
3. **F&G даёт +0.36 Calmar из +3.63 total** (10% вклада). Если API недоступен — можно выпилить, потеряешь ~6% Calmar.
4. **Nested CV WF обязательна**. На `walk_forward_original` baseline падает с Cal 3.36 → 2.71.
5. **Период обучения TB labels:** `max_hold=10, vol_window=20` — если меняешь TF, пересмотри эти константы (они в 8h-барах).

---

## 3. STAGE 67 — Stepwise Decomposition (ablation чемпиона)

**Скрипт:** `scripts/run_stage67_baseline_analysis.py`
**Отчёт:** `results/stage67_baseline_analysis_results.md`

Stage 67 — это НЕ отдельная стратегия для деплоя. Это верификация: запускается baseline в 5 вариантах (L0..L4) и сравниваются Calmar, чтобы показать **честный вклад каждого слоя**.

### 3.1 Пять конфигов (все на той же baseline M3 TB инфраструктуре)

| Level | Что меняется |
|---|---|
| **L0: Stage 41 original WF** | `walk_forward_original` вместо `walk_forward_nested_cv`, остальное = baseline M3 TB |
| **L1: + Nested CV WF** | = baseline M3 TB (§1). Ничего больше не добавлено. |
| **L2: + Meta-labeling (S60)** | L1 + meta gate на `meta_features = base + primary_pos` (БЕЗ F&G), `vt_from_raw` остаётся |
| **L3: + Kelly_half (S62)** | L2 + замена `vt_from_raw` на `kelly_mult(raw, 0.5, 180)` |
| **L4: + F&G feature = Champion S66** | L3 + добавить `fng_lag, fng_dev` в meta_features → `meta_features_fg` |

### 3.2 Разбивка прироста

| Level | FULL Cal | ΔCal vs prev | FULL MDD | HOLD Cal | HOLD MDD | HOLD Final |
|---|---|---|---|---|---|---|
| L0 | 2.71 | — | 5.7% | 3.05 | 4.3% | 1.17× |
| L1 | 3.36 | +0.65 | 5.6% | 2.72 | 4.9% | 1.18× |
| L2 | 4.31 | +0.95 | 5.6% | 5.65 | 3.4% | 1.25× |
| L3 | 5.98 | +1.68 | 4.0% | 25.13 | 0.7% | 1.25× |
| L4 | 6.34 | +0.36 | 4.0% | 26.93 | 0.7% | 1.26× |

**Total L0→L4:** ΔCal = +3.63 (+134%), ΔMDD = −1.72pp, Final ratio × 1.45.

**Главный драйвер:** Kelly_half (+1.68 Cal, −1.63pp MDD) — работает через sit-out в нисходящих режимах (μ<0 → KELLY_MIN=0.05).

### 3.3 vs BTC Buy & Hold

- B&H FULL 6.1y: 1.63×, Cal 0.15, MDD 77%
- B&H HOLDOUT (15 мес): 0.83× (BTC потерял −17% на Bybit в 2025-2026)
- Champion HOLDOUT: 1.26× (+26% за 15 мес пока B&H падал)
- **Это подтверждает value-prop:** стратегия зарабатывает в bear market.

---

## 4. ЗАВИСИМОСТИ (Python packages)

```
numpy
pyarrow
pywt              # PyWavelets (для wavelet_features)
scikit-learn      # StandardScaler
xgboost           # XGBClassifier (meta + ml_exit)
hmmlearn          # GaussianHMM (только для hmm_regime_overlay, в champion НЕ используется)
requests          # для alternative.me F&G API
matplotlib        # только для графиков
```

Внутренние модули (репо `sma_cross/`):
- `sma_cross.backtest` → `backtest_signal`, `compute_returns`
- `sma_cross.config` → `bars_per_year_for_tf`, `BASE_TF=480`
- `sma_cross.metrics` → `single_metrics` (Calmar/Sharpe/MDD/final_equity/CAGR)
- `sma_cross.resample` → `resample_ohlcv`
- `sma_cross.overlay` → `rolling_std`
- `sma_cross.weights` → `min_variance_subset`, `equal_subset`
- `sma_cross.ml_features` → `build_features`
- `sma_cross.channels`, `sma_cross.channel_signals`, `sma_cross.donchian`, `sma_cross.ma_engines`, `sma_cross.mean_reversion` — нужны ТОЛЬКО для `build_pool`

---

## 5. КРИТИЧЕСКИЕ КОНСТАНТЫ (единая таблица)

| Константа | Значение | Где | Назначение |
|---|---|---|---|
| BASE_TF | 480 | audit_common.py, run_modern_methods.py | 8h таймфрейм |
| bpy | 1095 | bars_per_year_for_tf(480) | для Sharpe/Calmar аннуализации |
| FEE | 0.00055 | run_modern_methods.py | Bybit taker 0.055% |
| SLIP | 0.00020 | run_modern_methods.py | slippage 0.020% |
| INITIAL | 10000 | run_modern_methods.py | стартовый капитал |
| HOLDOUT_TS | 2025-01-01 UTC | audit_common.py | граница train/holdout |
| TRAIN_MONTHS | 18 | WF outer train | |
| TEST_MONTHS | 6 | WF outer test | |
| STEP_MONTHS | 6 | WF шаг | |
| inner_val_frac | 0.25 | walk_forward_nested_cv | 75/25 split внутри train |
| K_GRID | [3,5,7,10,15,20] | ensemble size grid | |
| TB tp_mult | 1.5 | triple_barrier_labels | TP barrier = 1.5 × vol |
| TB sl_mult | 1.0 | triple_barrier_labels | SL barrier = 1.0 × vol |
| TB max_hold | 10 | triple_barrier_labels | timeout 10 баров |
| TB vol_window | 20 | triple_barrier_labels | окно для vol estimate |
| VT rolling window | 60 | vt_from_raw | окно rolling vol |
| VT target vol | 0.15 | vt_from_raw | 15% annualized |
| VT mult clip | [0.05, 1.0] | vt_from_raw | |
| Regime SMA fast | 150 | gradual_regime_overlay | |
| Regime SMA slow | 300 | gradual_regime_overlay | |
| Regime mults | 1.0 / 0.5 / 0.25 | gradual_regime_overlay | above fast / above slow / below |
| Tiered threshold | 0.10 | tiered_reentry | 10% dd → min_alloc |
| Tiered power | 3 | tiered_reentry | |
| Tiered min_alloc | 0.2 | tiered_reentry | 20% floor |
| ML Exit n_estimators | 50 | ml_exit_flex | XGB trees |
| ML Exit max_depth | 1 | ml_exit_flex (для TB labels_override) | |
| ML Exit threshold | 0.5 | ml_exit_flex | prob > 0.5 → exit |
| ML Exit purge | 10 | ml_exit_flex | = TB max_hold |
| Meta n_estimators | 50 | meta_classifier_wf | XGB trees |
| **Meta max_depth** | **3** | meta_classifier_wf | **отличается от ML Exit!** |
| Meta threshold | 0.5 | meta_classifier_wf | prob < 0.5 → block |
| META_HORIZON | 3 | meta_classifier_wf | target = sum next 3 bars > 0 |
| Meta purge | 9 | meta_classifier_wf | = horizon × 3 |
| KELLY_WINDOW | 180 | run_stage62_kelly | ~60 дней на 8h |
| KELLY fraction | 0.5 | champion | half-Kelly |
| KELLY_MIN | 0.05 | | floor multiplier |
| KELLY_MAX | 1.0 | | cap (no leverage) |
| F&G lag | 1 bar | run_stage65_feargreed | |
| F&G dev window | 90 bars | | ~30 дней на 8h |
| F&G extreme threshold | `\|fng-50\|>40` | | rule-overlay (НЕ в champion) |
| Seed (все XGB) | 42 | | детерминизм |

---

## 6. ЧЕК-ЛИСТ ПЕРЕНОСА

Минимальный порядок портирования:

1. **Data:** настроить загрузку 1m OHLCV Bybit BTCUSDT → ресемпл на 8h.
2. **Fees:** FEE=0.055%, SLIP=0.020% применять в `backtest_signal` (при переключении позиции).
3. **Pool:** `build_pool` (TF 240/480/1440, ~6618 стратегий) — тяжёлая штука, нужен multiprocessing.
4. **WF:** `walk_forward_nested_cv` (NOT original).
5. **Features:** `build_features` + `wavelet_features` (pywt db4) → hstack + lag 1.
6. **TB labels:** `triple_barrier_labels(raw, 1.5, 1.0, 10, 20)` → инверсия = `tb_exit_labels`.
7. **ML Exit:** `ml_exit_flex(raw, features, bpy, labels_override=tb_exit_labels)` → `tb_exit`.
8. **F&G:** `fetch_fng()` + `align_fng_to_bars` → `fng_lag, fng_dev`.
9. **Meta features:** `[base_features, primary_pos, fng_lag, fng_dev]`.
10. **Meta gate:** `meta_classifier_wf(meta_features_fg, tb_exit, raw, bpy, 3, 9)` → `gated_exit`.
11. **Kelly sizing:** `kelly_mult(raw, 0.5, 180)` → `overlay = gated_exit × km`.
12. **Regime:** `gradual_regime_overlay(overlay, close, 150, 300)`.
13. **Tiered:** `tiered_reentry(overlay, 0.10, 3, 0.2)` → **final equity stream**.

**Валидация переноса:** должно совпасть с референсом ±0.01 Calmar на каждом L0..L4 конфиге Stage 67.

---

## 7. ИСХОДНЫЕ ФАЙЛЫ (прочитать все)

Обязательно:
- `scripts/run_stage66_grand_combo.py` — champion entry point
- `scripts/run_stage67_baseline_analysis.py` — ablation проверка
- `scripts/run_stage60_meta_labeling.py` — `meta_classifier_wf`, `meta_label`
- `scripts/run_stage62_kelly.py` — `kelly_mult`, `apply_regime`
- `scripts/run_stage65_feargreed.py` — `fetch_fng`, `align_fng_to_bars`
- `scripts/exp_audit_e_bybit.py` — `run_m3_pipeline_bybit`, `build_or_load_pool_bybit`
- `scripts/audit_common.py` — `walk_forward_nested_cv`, константы, кэш
- `scripts/run_modern_methods.py` — `build_pool`, `ml_exit_flex`, `triple_barrier_labels`, `vt_from_raw`, `gradual_regime_overlay`, `tiered_reentry`, `wavelet_features`
- `sma_cross/ml_features.py` — `build_features`
- `sma_cross/metrics.py` — `single_metrics` (Calmar/Sharpe/MDD)
- `sma_cross/resample.py`, `sma_cross/overlay.py`, `sma_cross/weights.py`, `sma_cross/backtest.py`, `sma_cross/config.py` — utilities

Отчёты (для сверки цифр):
- `results/stage66_grand_combo_results.md`
- `results/stage67_baseline_analysis_results.md`

---

## 8. TL;DR (одной фразой)

**Champion = M3 Triple Barrier baseline (Nested CV WF + XGBoost exit на vol-scaled TB labels) → meta-gated через XGBoost secondary classifier с F&G фичами → sized через half-Kelly rolling 60d → regime overlay (SMA 150/300) → tiered dd-based reentry.**

