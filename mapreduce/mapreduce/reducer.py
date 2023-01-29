#!/usr/bin/env python
"""reducer.py"""

import sys


def perform_reduce():
    current_pmnt_type = None
    current_tip_sum = 0
    current_tip_count = 1
    pmnt_type = None

    for line in sys.stdin:
        line = line.strip()
        pmnt_type, tip = line.split('\t', 1)

        if current_pmnt_type == pmnt_type:
            current_tip_sum += float(tip)
            current_tip_count += 1

        else:
            if current_pmnt_type:
                print('%s\t%s' % (current_pmnt_type, round(current_tip_sum / current_tip_count, 2)))
            current_pmnt_type= pmnt_type
            current_tip_sum = float(tip)
            current_tip_count = 1

    if current_pmnt_type == pmnt_type:
        print('%s\t%s' % (current_pmnt_type, round(current_tip_sum / current_tip_count, 2)))

if __name__ == '__main__':
    perform_reduce()

