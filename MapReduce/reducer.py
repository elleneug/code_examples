#!/usr/bin/env python

import sys

mp = {'1': 'Credit card',
      '2': 'Cash',
      '3': 'No charge',
      '4': 'Dispute',
      '5': 'Unknown',
      '6': 'Voided trip'}


def perform_reduce():
    current_key = None
    total_tips = 0
    count = 0

    for line in sys.stdin:
        key, value = line.split('\t')

        if key != current_key:
            if current_key:
                avg_tip = total_tips / count
                payment_type, month = current_key.split(',')
                print('{0},{1},{2:.2f}'.format(mp[payment_type], month, avg_tip))
            current_key = key

            total_tips = 0
            count = 0

        total_tips += float(value)
        count += 1

    if current_key:
        avg_tip = total_tips / count
        payment_type, month = current_key.split(',')
        print('{0},{1},{2:.2f}'.format(mp[payment_type], month, avg_tip))


if __name__ == '__main__':
    perform_reduce()
