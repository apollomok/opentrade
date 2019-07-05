#!/usr/bin/env python2

import datetime
import os
from parse_confirmation import *

confirmations = []
orders = {}


def check_confirmation(raw, exec_id, id, *args):
  if exec_type == kUnconfirmedNew:
    algo_id, qty = args[:2]
    orders[id] = float(qty)
  elif exec_type in (kUnconfirmedCancel, kPendingCancel, kCancelRejected):
    # tm, orig_id = args
    return
  elif exec_type == kRiskRejected:
    if id in orders: del orders[id]
    return
  elif exec_type in (kPartiallyFilled, kFilled):
    tm, last_shares, last_px, exec_trans_type = args[:4]
    n = float(last_shares)
    if id not in orders: return
    if exec_trans_type == kTransCancel: orders[id] += n
    elif exec_trans_type == kTransNew:
      orders[id] -= n
      if orders[id] <= 1e-8:
        del orders[id]
    else:
      return
  elif exec_type in (kCanceled, kRejected, kExpired, kCalculated, kDoneForDay):
    if id in orders: del orders[id]
  confirmations.append(id, raw)


def main():
  src = sys.argv[1]
  dest = sys.argv[2]
  if os.path.exists(dest):
    print(datetime.datetime.now(), dest, 'already exists, skip rolling', src)
    return
  if not os.path.exists(src):
    print(datetime.datetime.now(), src, 'not exists, skip rolling')
    return
  fh = open(dest, 'wb')
  rolls = []
  parse(src, check_confirmation)
  rolls = [raw for id, raw in confirmations if id in orders]
  print(datetime.datetime.now(), len(orders), 'orders rolled')
  n = 0
  for c in rolls:
    n += 1
    seq = struct.unpack('I', n)
    for i in xrange(len(seq)):
      c[i] = seq[i]
    c[-1] = 'R'
    fh.write(c)
  fh.close()


if __name__ == '__main__':
  main()
