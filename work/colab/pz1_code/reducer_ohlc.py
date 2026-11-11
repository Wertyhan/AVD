#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reducer_ohlc.py - Reducer: зливає часткові записи доби в підсумкову свічку OHLCV.

Вхід  (stdin) : доба <TAB> ts_перша <TAB> open <TAB> high <TAB> low
                <TAB> ts_остання <TAB> close <TAB> volume <TAB> кількість
Вихід (stdout): доба <TAB> open <TAB> high <TAB> low <TAB> close <TAB> volume <TAB> кількість_свічок

Правила (коректний OHLC):
  open   = Open першої за Timestamp свічки доби,
  high   = max(High), low = min(Low),
  close  = Close останньої за Timestamp свічки доби,
  volume = сума Volume.
Hadoop сортує лише за ключем (доба), порядок часткових записів усередині доби
не гарантований, тому reducer явно сортує їх за ts_перша / ts_остання.
"""
import sys
from itertools import groupby
from operator import itemgetter


def parse(stream):
    for line in stream:
        p = line.rstrip("\n").split("\t")
        if len(p) != 9:
            continue
        try:
            yield (p[0], float(p[1]), p[2], float(p[3]), p[3], float(p[4]), p[4],
                   float(p[5]), p[6], float(p[7]), int(p[8]))
        except ValueError:
            continue


def main():
    # кортеж: 0 доба, 1 ts_first, 2 open_s, 3 high_f, 4 high_s, 5 low_f, 6 low_s,
    #         7 ts_last, 8 close_s, 9 volume, 10 n
    for day, grp in groupby(parse(sys.stdin), key=itemgetter(0)):
        parts = list(grp)
        first = sorted(parts, key=itemgetter(1))[0]        # найраніша свічка доби
        last = sorted(parts, key=itemgetter(7))[-1]        # найпізніша свічка доби
        hi = max(parts, key=itemgetter(3))
        lo = min(parts, key=itemgetter(5))
        volume = sum(x[9] for x in parts)
        n = sum(x[10] for x in parts)
        sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%r\t%d\n" %
                         (day, first[2], hi[4], lo[6], last[8], volume, n))


if __name__ == "__main__":
    main()
