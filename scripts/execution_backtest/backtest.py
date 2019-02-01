from opentrade import *
import os

exch = get_exchange(os.environ['MARKET'])
min_size = int(os.environ['MinSize'])
period = int(os.environ['ValidSeconds'])
agg = os.environ.get('Aggression', 'Low')
fw = open('algos.txt', 'wt')


def on_start(self):
  log_info('backtest started')
  self.set_interval(1000)


def on_end(self):
  log_info('backtest done')
  fw.close()


def on_start_of_day(self, date):
  log_info(date, 'started', get_datetime())
  fn = date.strftime('targets/%Y%m%d.txt')
  if not os.path.exists(fn):
    log_info('non-existed target file ' + fn)
    self.skip()
    return
  self.targets = []
  with open(fn) as fh:
    for ln in fh:
      tm, symbol, acc, target = ln.strip().split(',')
      st = SecurityTuple()
      st.sec = exch.get_security(symbol)
      st.acc = get_account(acc)
      tm = int(tm)
      h = tm // 10000000
      m = tm % 10000000 // 100000
      tm = h * 3600000 + m * 60000 + tm % 100000
      self.targets.append((tm, st, int(target)))
  self.next_target_end_tm = 0


def on_end_of_day(self, date):
  self.clear()


def on_interval(self, ms):
  if not self.targets and ms >= self.next_target_end_tm:
    log_info('targets finished')
    self.skip()
    return
  while self.targets:
    tm, st, target = self.targets[0]
    if tm > ms: break
    self.targets = self.targets[1:]
    sec = st.sec
    pos = sec.get_position(st.acc).qty
    if pos == target: continue
    self.cancel_algo(sec, st.acc)
    md = sec.md
    px = 0
    ask = md.ask_price
    bid = md.bid_price
    if ask > 0 and bid > 0: px = (ask + bid) / 2
    qty = target - pos
    st.qty = abs(qty)
    st.side = OrderSide.buy if qty > 0 else OrderSide.sell
    algo = self.start_algo(
        'TWAP', {
            'Security': st,
            'MinSize': min_size,
            'Aggression': agg,
            'ValidSeconds': period
        })
    fw.write('%s,%s,%s,%d,%f,%d,%s,%f,%f,%f\n' %
             (get_datetime().strftime('%Y-%m-%d %H:%M:%S.%f'), sec.symbol, 'B'
              if qty > 0 else 'S', st.qty, px, algo, st.acc, sec.rate,
              sec.multiplier, pos))
    self.next_target_end_tm = ms + period * 1000 + 60000
