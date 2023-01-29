#!/usr/bin/env python
"""mapper.py"""

import sys

def perform_map():
    payment_type = {
        '1': 'Credit card',
        '2': 'Cash',
        '3': 'No charge',
        '4': 'Dispute',
        '5': 'Unknown',
        '6': 'Voided trip'
    }

    for line in sys.stdin:
        line = line.strip().split(',')
        if line[9] in payment_type:
            print('%s\t%s' % (payment_type[line[9]], line[13]))

if __name__ == '__main__':
    perform_map()