#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mapper_year.py - Mapper (Hadoop Streaming) для підрахунку кількості хвилинних
свічок та сумарного обсягу торгів за роками.

Вхід  (stdin) : рядки CSV  Timestamp,Open,High,Low,Close,Volume
Вихід (stdout): рік <TAB> 1 <TAB> Volume

Timestamp - це unix-секунди в UTC, тому рік обчислюється через time.gmtime
(без залежності від часового поясу машини).
Заголовок і некоректні рядки пропускаються. Правило фільтрації таке саме,
як у Pandas (dropna за всіма 6 стовпцями), щоб результати можна було зіставити.
"""
import sys
import time


def main():
    year_by_day = {}          # кеш: номер доби -> рік (щоб не викликати gmtime для кожного рядка)
    for line in sys.stdin:
        p = line.strip().split(",")
        if len(p) < 6:
            continue
        try:
            vals = [float(x) for x in p[:6]]   # на заголовку тут буде ValueError
        except ValueError:
            continue
        if any(v != v for v in vals):          # NaN != NaN
            continue
        day = int(vals[0]) // 86400
        year = year_by_day.get(day)
        if year is None:
            year = time.gmtime(day * 86400).tm_year
            year_by_day[day] = year
        sys.stdout.write("%d\t1\t%s\n" % (year, p[5]))


if __name__ == "__main__":
    main()
