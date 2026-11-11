# -*- coding: utf-8 -*-
"""Правило виявлення аномалій для потоку 1-хвилинних свічок BTC/USD (ПЗ3, завдання 6).

Формальне правило. Нехай C_t - ціна закриття хвилини t, V_t - обсяг,
r_t = ln(C_t / C_{t-1}) - логарифмічна дохідність.
Базове вікно W_t = {t-60, ..., t-1} (60 попередніх хвилин, поточна не входить).
    sigma_t = max( 1.4826 * MAD_60(r), SIGMA_FLOOR ),   MAD_60(r) = median(|r_i - median(r)|), i in W_t
    z_t     = |r_t| / sigma_t
    q_t     = V_t / median_60(V),  лише якщо median_60(V) > 0
Хвилина t позначається аномальною, якщо  z_t > k  АБО  q_t > k_v.  Оцінка (score) = max(z_t/k, q_t/k_v); аномалія <=> score > 1.
Пласкі хвилини (Volume = 0, O=H=L=C; 16,79 % історії – заповнені пропуски біржі) НЕ входять до базових
статистик: MAD та медіана обчислюються лише за непласкими хвилинами вікна (потрібно не менше MIN_OBS таких),
сама пласка хвилина не оцінюється (r=0, V=0 не можуть бути аномалією).
Вікно вважається коректним, якщо 61 останній запис вкладається у MAX_SPAN секунд
(немає великих пропусків у даних, тому дохідності вікна скінченні й суміжні).
Одна й та сама функція score() використовується і в потоці (foreachBatch), і офлайн.
"""
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

WINDOW = 60            # довжина ковзного вікна, хв
K_GRID = (3, 5, 7, 10, 15, 20, 30, 50, 100)            # сітка k для правила за дохідністю (вимога завдання: 3,5,7,10; решта – розширення)
KV_GRID = (3, 5, 10, 20, 50, 100, 200, 300, 500, 700, 1000, 2000)  # сітка k_v для правила за обсягом
K_SEL, KV_SEL = 10, 500  # обрані пороги (обґрунтування: results/pz3_threshold_selection.csv)
MAD_SCALE = 1.4826     # переведення MAD у оцінку sigma для нормального розподілу
SIGMA_FLOOR = 1e-4     # мінімальна sigma (1 б.п.): захист від MAD=0 на пласкому ринку
MIN_OBS = 20           # мін. кількість непласких хвилин у вікні для коректної оцінки
MAX_SPAN = 3900        # макс. тривалість 61 запису, с (60 хв + 5 хв допуску)


def _rolling_median_mad(a, w=WINDOW, chunk=100_000):
    """Для кожного i>=w: медіана та MAD по a[i-w:i] серед скінченних значень (NaN = пласка хвилина, ігнорується).
    Якщо скінченних значень менше MIN_OBS, повертається NaN. Обробка блоками для економії RAM."""
    n = len(a)
    med = np.full(n, np.nan)
    mad = np.full(n, np.nan)
    if n <= w:
        return med, mad
    win = sliding_window_view(a, w)      # рядок j = a[j:j+w], вікно перед індексом j+w
    m = n - w
    import warnings
    for s in range(0, m, chunk):
        e = min(s + chunk, m)
        blk = win[s:e]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            md = np.nanmedian(blk, axis=1)
            md_ = np.nanmedian(np.abs(blk - md[:, None]), axis=1)
        cnt = np.isfinite(blk).sum(axis=1)
        md[cnt < MIN_OBS] = np.nan
        md_[cnt < MIN_OBS] = np.nan
        med[s + w:e + w] = md
        mad[s + w:e + w] = md_
    return med, mad


def score(df, k=K_SEL, kv=KV_SEL, w=WINDOW):
    """df: колонки ts (unix с), close, volume. Повертає DataFrame з z_ret, vol_ratio та прапорцями flag_k{k}."""
    d = df[["ts", "close", "volume"]].drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    n = len(d)
    ts = d["ts"].to_numpy(np.int64)
    c = d["close"].to_numpy(float)
    v = d["volume"].to_numpy(float)
    r = np.full(n, np.nan)
    if n > 1:
        r[1:] = np.diff(np.log(c))
    flat = (v == 0)                       # пласкі (заповнені) хвилини
    r_eff = np.where(flat, np.nan, r)     # виключені з бази MAD
    v_eff = np.where(flat, np.nan, v)     # виключені з бази медіани обсягу
    _, mad_r = _rolling_median_mad(r_eff, w)
    med_v, _ = _rolling_median_mad(v_eff, w)
    sigma = np.maximum(MAD_SCALE * mad_r, SIGMA_FLOOR)
    span = np.full(n, np.inf)
    if n > w + 1:
        span[w + 1:] = ts[w + 1:] - ts[:-(w + 1)]
    valid = (span <= MAX_SPAN) & np.isfinite(r) & np.isfinite(mad_r) & np.isfinite(med_v)
    z = np.where(valid, np.abs(r) / sigma, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        q = np.where(valid & (med_v > 0), v / med_v, np.nan)
    out = d.copy()
    out["r"] = r
    out["sigma"] = np.where(valid, sigma, np.nan)
    out["med_vol"] = np.where(valid, med_v, np.nan)
    out["z_ret"] = z
    out["vol_ratio"] = q
    out["scored"] = valid
    sc_z = np.nan_to_num(z / k, nan=0.0)
    sc_q = np.nan_to_num(q / kv, nan=0.0)
    out["score"] = np.maximum(sc_z, sc_q)
    out["anomaly"] = out["score"] > 1.0
    out["rule"] = np.where(~out["anomaly"], "", np.where(sc_z > 1, np.where(sc_q > 1, "return+volume", "return"), "volume"))
    return out


class StreamScorer:
    """Стан для foreachBatch: буфер останніх w+1 записів між мікробатчами."""

    def __init__(self, w=WINDOW, k=K_SEL, kv=KV_SEL):
        self.w, self.k, self.kv = w, k, kv
        self.buf = pd.DataFrame(columns=["ts", "close", "volume"])

    def update(self, batch):
        """batch: DataFrame ts, close, volume (лише коректні записи). Повертає оцінки для нових рядків."""
        batch = batch[["ts", "close", "volume"]].drop_duplicates("ts")
        # при повторній доставці (at-least-once) відкидаємо вже оброблені ts
        if len(self.buf):
            batch = batch[~batch["ts"].isin(self.buf["ts"])]
        if batch.empty:
            return batch.assign(r=[], sigma=[], med_vol=[], z_ret=[], vol_ratio=[], scored=[], score=[], anomaly=[], rule=[])
        full = pd.concat([self.buf, batch], ignore_index=True)
        sc = score(full, self.k, self.kv, self.w)
        new_ts = set(batch["ts"].tolist())
        res = sc[sc["ts"].isin(new_ts)].reset_index(drop=True)
        self.buf = full.sort_values("ts").tail(self.w + 1)[["ts", "close", "volume"]].reset_index(drop=True)
        return res
