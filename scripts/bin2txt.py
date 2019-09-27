#!/usr/bin/env python2

import struct
import sys
import mmap


def main():
  if len(sys.argv) < 3:
    print('usages: bin2txt <intput_bin_file> <output_text_file>')
    return
  infile = open(sys.argv[1], 'r+b')
  out = open(sys.argv[2], 'w+t')
  line = infile.readline()
  offset = len(line)
  out.write(' '.join(line.split()[:2]) + '\n')
  for line in infile:
    out.write(line)
    offset += len(line)
    if line.lower().startswith('@end'): break
  mm = mmap.mmap(infile.fileno(), 0)
  while offset < len(mm):
    ms = struct.unpack('I', mm[offset:offset + 4])[0]
    ms = '%02d%02d%02d%03d' % (ms // 3600000, ms % 3600000 // 60000,
                               ms % 60000 // 1000, ms % 1000)
    offset += 4
    sec = struct.unpack('H', mm[offset:offset + 2])[0]
    offset += 2
    t = mm[offset]
    offset += 1
    px = struct.unpack('d', mm[offset:offset + 8])[0]
    offset += 8
    size = struct.unpack('I', mm[offset:offset + 4])[0]
    offset += 4
    out.write('{} {} {} {} {}\n'.format(ms, sec, t, px, size))
  out.close()


if __name__ == '__main__':
  main()
