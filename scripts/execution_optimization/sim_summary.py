#!/usr/bin/env python
# -*- coding: utf-8 -*-

import optparse
import datetime
import os
from collections import defaultdict
import fnmatch


def main():
  opts = optparse.OptionParser('Usage: sim_summary.py [options] report_files')
  opts.add_option(
      '-p',
      '--prefix',
      default='rpt',
      help='report file prefix, default: "rpt"')
  opts.add_option('-a', '--acc', help='acc following fnmatch format')
  opts.add_option(
      '-e', '--exclude', help='excluded acc following fnmatch format')
  opts, args = opts.parse_args()
  print('|    %-12s' % 'params' + '    |    ' + '    |    '.join(
      ['%-12s' % x
       for x in ('avg cost', 'avg fr', 'total pnl', 'total tvr')]) + '    |')
  print('|' + '|'.join(['-' * 20 for i in range(5)]) + '|')
  for fn in args:
    with open(fn) as fh:
      paramstr = os.path.basename(fn).replace(opts.prefix + '-', '').replace(
          '.txt', '')
      res = {}
      for line in fh:
        if line.startswith('acc:'):
          acc = line.strip().split()[1]
        elif line.startswith('cost:'):
          if opts.acc and not fnmatch.fnmatch(acc, opts.acc): continue
          if opts.exclude and fnmatch.fnmatch(acc, opts.exclude): continue
          toks = line.strip().split()
          cost = float(toks[1])
          fr = float(toks[3])
          pnl = float(toks[9])
          tvr = float(toks[11]) if len(toks) > 11 else 0
          res[acc] = (cost, fr, pnl, tvr)
      res = res.values()
      cost = sum([x[0] for x in res]) / len(res)
      fr = sum([x[1] for x in res]) / len(res)
      pnl = sum([x[2] for x in res]) / 1e6
      tvr = sum([x[3] for x in res])
      print('|    %-12s' % paramstr + '    |    ' + '    |    '.join(
          ['%-12.4f' % x for x in (cost, fr, pnl, tvr)]) + '    |')


if __name__ == '__main__':
  main()
