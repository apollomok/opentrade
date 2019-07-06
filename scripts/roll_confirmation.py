#!/usr/bin/env python2
'''
roll yesterday's unfinished orders to today.
scripts/roll_confirmation.py archives/`date -d '1 days ago' "+%Y%m%d"`/store/confirmations store/confirmations
'''

import datetime
import time
import os
from parse_confirmation import *

confirmations = []
orders = {}
now = time.time()
one_day = 24 * 3600


def check_confirmation(seq, raw, exec_type, id, *args):
  if exec_type == kUnconfirmedNew:
    tm, algo_id, qty = args[:3]
    x = now - int(tm) / 1e6
    if x > one_day:
      log('too old orders skipped', x / one_day, 'days ago')
      return
    orders[id] = float(qty)
    confirmations.append((id, raw))
  elif exec_type == kNew:
    confirmations.append((id, raw))
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
  rolls = [(id, raw) for id, raw in confirmations if id in orders]
  log(len(orders), 'orders rolled')
  seq = 0
  for id, raw in rolls:
    seq += 1
    raw = struct.pack('I', seq) + raw[4:]
    if raw[4] == kUnconfirmedNew:
      # modify qty
      qty = orders[id]
      a = raw[:7]
      n = 7
      while raw[n] != '\0':
        n += 1
      b = raw[7:n]
      b = b.split(' ')
      b[3] = str(qty)
      raw = a + ' '.join(b) + '\0\n'
    fh.write(raw)
  fh.close()


def log(*args):
  args = (datetime.datetime.now(),) + args
  print(' '.join([str(x) for x in args]))


if __name__ == '__main__':
  main()
