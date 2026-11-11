#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mapper_ohlc.py - Mapper для побудови добових OHLC зі свічок 1 хв.

Вхід  (stdin) : CSV  Timestamp,Open,High,Low,Close,Volume
Вихід (stdout): доба <TAB> ts_перша <TAB> open <TAB> high <TAB> low
                <TAB> ts_остання <TAB> close <TAB> volume <TAB> кількість

Застосовано in-mapper combining: у межах одного split зберігається часткова
агрегація по добі (перша/остання свічка за Timestamp, max High, min Low, сума Volume),
а в кінці виводиться один запис на добу. Це на два порядки зменшує обсяг shuffle.
Оскільки блок HDFS може розрізати добу навпіл, reducer все одно зливає часткові
записи і сам впорядковує їх за часом.
Значення open/high/low/close передаються як вихідні текстові токени (без втрати точності).
"""
import sys
import time


def main():
    day_name = {}     # номер доби -> рядок YYYY-MM-DD
    acc = {}          # доба -> [ts_first, open_s, ts_last, close_s, high_f, high_s, low_f, low_s, vol, n]
    for line in sys.stdin:
        p = line.strip().split(",")
        if len(p) < 6:
            continue
        try:
            ts, o, h, l, c, v = [float(x) for x in p[:6]]
        except ValueError:
            continue   # заголовок
        if ts != ts or o != o or h != h or l != l or c != c or v != v:
            continue
        d = int(ts) // 86400
        name = day_name.get(d)
        if name is None:
            name = time.strftime("%Y-%m-%d", time.gmtime(d * 86400))
            day_name[d] = name
        a = acc.get(name)
        if a is None:
            acc[name] = [ts, p[1], ts, p[4], h, p[2], l, p[3], v, 1]
            continue
        if ts < a[0]:                 # раніша свічка -> нове Open
            a[0] = ts
            a[1] = p[1]
        if ts > a[2]:                 # пізніша свічка -> нове Close
            a[2] = ts
            a[3] = p[4]
        if h > a[4]:
            a[4] = h
            a[5] = p[2]
        if l < a[6]:
            a[6] = l
            a[7] = p[3]
        a[8] += v
        a[9] += 1
    for name, a in acc.items():
        sys.stdout.write("%s\t%r\t%s\t%s\t%s\t%r\t%s\t%r\t%d\n" %
                         (name, a[0], a[1], a[5], a[7], a[2], a[3], a[8], a[9]))


if __name__ == "__main__":
    main()
