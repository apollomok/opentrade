#!/usr/bin/env python2
'''
roll yesterday's unfinished orders to today.
scripts/roll_confirmation.py archives/`date -d '1 days ago' "+%Y%m%d"`/store/confirmations store/confirmations
'''

import datetime
import os
from parse_confirmation import *

confirmations = []
orders = {}
now = time.time()
one_day = 24 * 3600


def check_confirmation(raw, exec_id, id, *args):
  if exec_type == kUnconfirmedNew:
    tm, algo_id, qty = args[:3]
    x = now - int(tm) / 1e6
    if x > one_day:
      log('too old orders skipped', x / one_day, 'days ago')
      return
    orders[id] = float(qty)
    confirmations.append(id, raw)
  elif exec_type == kNew:
    confirmations.append(id, raw)
  elif exec_type in (kRiskRejected, kCanceled, kRejected, kExpired, kCalculated,
                     kDoneForDay):
    if id in orders: del orders[id]
  elif exec_type in (kPartiallyFilled, kFilled):
    tm, last_shares, last_px, exec_trans_type = args[:4]
    n = float(last_shares)
    if id in orders:
      if exec_trans_type == kTransCancel: orders[id] += n
      elif exec_trans_type == kTransNew:
        orders[id] -= n
        if orders[id] <= 1e-8:
          del orders[id]


def main():
  src = sys.argv[1]
  dest = sys.argv[2]
  if os.path.exists(dest):
    log(dest, 'already exists, skip rolling', src)
    return
  if not os.path.exists(src):
    log(src, 'not exists, skip rolling')
    return
  fh = open(dest, 'wb')
  rolls = []
  parse(src, check_confirmation)
  rolls = [raw for id, raw in confirmations if id in orders]
  log(len(orders), 'orders rolled')
  n = 0
  for c in rolls:
    n += 1
    seq = struct.unpack('I', n)
    for i in xrange(len(seq)):
      c[i] = seq[i]
    fh.write(c)
  fh.close()


def log(*args):
  args = [datetime.datetime.now()] + args
  print(' '.join([str(x) for x in args]))


if __name__ == '__main__':
  main()
