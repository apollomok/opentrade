#include "cross_engine.h"

#include "exchange_connectivity.h"
#include "logger.h"

namespace opentrade {

void CrossEngine::Place(CrossOrder* ord) {
  assert(ord->inst);
  Get(ord->sec->id)->Execute(ord);
}

void CrossSecurity::Execute(CrossOrder* ord) {
  auto& md = ord->inst->md();
  auto price = 0.;
  auto ask = md.quote().ask_price;
  auto bid = md.quote().bid_price;
  if (ask > 0 && bid > 0) price = (ask + bid) / 2;
  if (!price) price = md.trade.close;
  if (!price) price = ord->sec->close_price;
  Lock lock(m);
  (ord->IsBuy() ? buys : sells).push_back(ord);
  if (!(buys.size() && sells.size())) return;
  if (!price) return;
  auto& ecm = ExchangeConnectivityManager::Instance();
  while (buys.size() && sells.size()) {
    auto buy = buys.front();
    if (!buy->inst->algo().is_active()) {
      buys.pop_front();
      continue;
    }
    auto a = buy->leaves();
    assert(a > 0);
    while (sells.size() && a > 0) {
      auto sell = sells.front();
      if (!sell->inst->algo().is_active()) {
        sells.pop_front();
        continue;
      }
      auto b = sell->leaves();
      assert(b > 0);
      if (a >= b) {
        a -= b;
        b = 0;
      } else {
        b -= a;
        a = 0;
      }
      auto qty = sell->leaves() - b;
      assert(qty > 0);
      AlgoManager::Instance().Cancel(const_cast<Instrument*>(sell->inst));
      ecm.HandleFilled(sell, qty, price,
                       "CX-" + std::to_string(sell->id) + "-" +
                           std::to_string(sell->count++));
      if (!b) {
        sells.pop_front();
      }
    }
    auto qty = buy->leaves() - a;
    if (qty > 0) {
      AlgoManager::Instance().Cancel(const_cast<Instrument*>(buy->inst));
      ecm.HandleFilled(
          buy, qty, price,
          "CX-" + std::to_string(buy->id) + "-" + std::to_string(buy->count++));
    }
    if (!a) {
      buys.pop_front();
    }
  }
}

void CrossEngine::UpdateTrade(Confirmation::Ptr cm) {
  auto sec = Get(cm->order->sec->id);
  auto& orders = (cm->order->IsBuy() ? sec->buys : sec->sells);
  if (orders.empty()) return;
  CrossSecurity::Lock lock(sec->m);
  for (auto it = orders.begin(); it != orders.end(); ++it) {
    if ((*it)->inst == cm->order->inst) {
      (*it)->filled_in_market += cm->last_shares;
      if ((*it)->leaves() <= 0) {
        orders.erase(it);
      }
      break;
    }
  }
}

}  // namespace opentrade
