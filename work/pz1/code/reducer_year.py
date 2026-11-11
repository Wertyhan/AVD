#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reducer_year.py - Reducer (і одночасно Combiner) для підрахунку свічок і обсягу за роками.

Вхід  (stdin) : рік <TAB> кількість <TAB> обсяг   (відсортовано за ключем - рік)
Вихід (stdout): рік <TAB> сумарна_кількість <TAB> сумарний_обсяг

Формат виходу збігається з форматом входу, тому цей самий скрипт можна
використати як combiner (часткове згортання на стороні mapper).
"""
import sys
from itertools import groupby
from operator import itemgetter


def parse(stream):
    for line in stream:
        p = line.rstrip("\n").split("\t")
        if len(p) != 3:
            continue
        try:
            yield p[0], int(p[1]), float(p[2])
        except ValueError:
            continue


def main():
    for year, grp in groupby(parse(sys.stdin), key=itemgetter(0)):
        count = 0
        volume = 0.0
        for _, c, v in grp:
            count += c
            volume += v
        sys.stdout.write("%s\t%d\t%r\n" % (year, count, volume))


if __name__ == "__main__":
    main()
