#!/usr/bin/env python2

import struct
import sys


def main():
  if len(sys.argv) < 3:
    print('usages: txt2bin <intput_text_file> <output_bin_file>')
    return
  infile = open(sys.argv[1])
  out = open(sys.argv[2], 'w+b')
  out.write(infile.readline().strip() + ' binary\n')
  for line in infile:
    out.write(line)
    if line.lower().startswith('@end'): break
  for line in infile:
    toks = line.strip().split()
    hmsm = int(toks[0])
    hms = hmsm / 1000
    ms = (hms / 10000 * 3600 + hms % 10000 / 100 * 60 +
          hms % 100) * 1000 + hmsm % 1000
    sec = int(toks[1])
    t = toks[2]
    px = float(toks[3])
    size = int(toks[4])
    raw = struct.pack('I', ms) + struct.pack('H', sec) + t[0] + struct.pack(
        'd', px) + struct.pack('I', size)
    out.write(raw)
  out.close()


if __name__ == '__main__':
  main()
