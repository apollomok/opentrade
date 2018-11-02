import time
import math


def get_param_defs(constants):
  return (
      ('Security', constants.security_tuple, True),
      ('Price', 0.0, False, 0, 10000000, 7),
      ('ValidSeconds', 300, True, 60),
      ('MinSize', 0, False, 0, 10000000),
      ('MaxPov', 0.0, False, 0, 1, 2),
      ('Aggression', ('Low', 'Medium', 'High', 'Highest'), True),
  )


def is_buy(self):
  return self.security_tuple.side == self.constants.order_side_buy


def on_start(self, params):
  self.security_tuple = st = params['Security']
  self.instrument = self.subscribe(st.security, st.src)
  seconds = params.get('ValidSeconds', 0)
  if seconds < 60:
    return 'Too short ValidSeconds, must be >= 60'
  self.begin_time = time.time()
  self.end_time = self.begin_time + seconds
  self.price = params.get('Price', 0.)
  min_size = params.get('MinSize', 0)
  lot_size = st.security.lot_size
  if min_size <= 0 and lot_size <= 0:
    return 'MinSize required for security without lot size'
  if min_size > 0 and lot_size > 0:
    min_size = math.round(min_size / lot_size) * lot_size
  self.min_size = min_size
  self.max_pov = params.get('MaxPov', 0.0)
  if self.max_pov > 1: self.max_pov = 1
  self.agg = params.get('Aggression')
  timer(self)
  self.log_debug('[' + self.name + ' ' + str(self.id) + '] started')
  self.volume = 0


def on_stop(self):
  self.log_debug('[' + self.name + ' ' + str(self.id) + '] stopped')


def on_market_trade(self, instrument):
  md = instrument.md
  self.log_debug(instrument.security.symbol + ' trade: ' + str(md.get_open()) +
                 ' ' + str(md.get_high()) + ' ' + str(md.get_low()) + ' ' +
                 str(md.get_close()) + ' ' + str(md.get_qty()) + ' ' + str(
                     md.get_vwap()) + ' ' + str(md.get_volume()))
  self.volume += md.get_qty()


def on_market_quote(self, instrument):
  md = instrument.md
  self.log_debug(instrument.security.symbol + ' quote: ' +
                 str(md.get_ask_price()) + ' ' + str(md.get_ask_size()) + ' ' +
                 str(md.get_bid_price()) + ' ' + str(md.get_bid_size()))


def on_confirmation(self, confirmation):
  self.log_debug(confirmation)


def timer(self):
  if not self.is_active(): return
  inst = self.instrument
  st = self.security_tuple
  now = time.time()
  if now > self.end_time:
    self.stop()
    return
  self.set_timeout(lambda: timer(self), 1000)
  if not self.instrument.security.is_in_trade_period(): return

  md = self.instrument.md
  bid = md.get_bid_price()
  ask = md.get_ask_price()
  last_px = md.get_close()
  mid_px = 0.
  if ask > bid and bid > 0:
    mid_px = (ask + bid) / 2
    tick_size = self.instrument.security.get_tick_size(mid_px)
    if tick_size > 0:
      if is_buy(self):
        mid_px = math.ceil(mid_px / tick_size) * tick_size
      else:
        mid_px = math.floor(mid_px / tick_size) * tick_size

  orders = self.instrument.active_orders
  if orders:
    for ord in orders:
      if is_buy(self):
        if ord.price < bid: ord.cancel()
      else:
        if ask > 0 and ord.price > ask: ord.cancel()
    return

  if self.volume > 0 and self.max_pov > 0:
    if inst.get_total_qty() > self.max_pov * self.volume:
      return

  ratio = math.min(1., (now - self.begin_time + 1.) /
                   (self.end_time - self.begin_time))
  expect = st.qty * ratio
  leaves = expect - inst.get_total_exposure()
  if leaves <= 0: return
  total_leaves = st.qty - inst.get_total_exposure()
  lot_size = math.max(1, inst.security.get_lot_size())
  max_qty = total_leaves if inst.security.odd_lot_allowed else math.floor(
      total_leaves / lot_size) * lot_size
  if max_qty <= 0: return
  would_qty = math.ceil(leaves / lot_size) * lot_size
  if would_qty < self.min_size: would_qty = self.min_size
  if would_qty > max_qty: would_qty = max_qty
  type = self.constants.order_type_limit
  price = 0
  if self.agg == 'Low':
    if is_buy(self):
      if bid > 0:
        price = bid
      elif last_px > 0:
        price = last_px
      else:
        return
    else:
      if ask > 0:
        price = ask
      elif last_px > 0:
        price = last_px
      else:
        return
  elif self.agg == 'Medium':
    if mid_px > 0:
      price = mid_px
    else:
      return
  elif self.agg == 'High':
    if is_buy(self):
      if ask > 0:
        c.price = ask
      else:
        return
    else:
      if bid > 0:
        price = bid
      else:
        return
  else:
    type = self.constants.order_type_market
  if price_ > 0 and ((is_buy(self) and price > self.price) or
                     ((not is_buy(self)) and price < self.price)):
    return
  self.place(st.account, would_qty, price, st.side, type)
