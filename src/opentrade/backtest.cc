#ifdef BACKTEST

#include "backtest.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "logger.h"

namespace opentrade {

static thread_local boost::uuids::random_generator kUuidGen;

static inline void Async(std::function<void()> func) {
  kTimers.emplace(0, func);
}

decltype(auto) GetSecurities(std::ifstream& ifs, const std::string& fn) {
  std::string line;
  if (!std::getline(ifs, line)) {
    LOG_FATAL("Invalid file: " << fn);
  }
  char a[256];
  *a = 0;
  char b[256];
  *b = 0;
  std::vector<const Security*> out;
  if (sscanf(line.c_str(), "%s %s", a, b) != 2 || strcasecmp(a, "@begin")) {
    LOG_FATAL("Invalid file: " << fn);
  }
  std::unordered_map<std::string, const Security*> sec_map;
  if (!strcasecmp(b, "bbgid")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      if (*pair.second->bbgid) sec_map[pair.second->bbgid] = pair.second;
    }
  } else if (!strcasecmp(b, "isin")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      if (*pair.second->isin) sec_map[pair.second->isin] = pair.second;
    }
  } else if (!strcasecmp(b, "cusip")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      if (*pair.second->cusip) sec_map[pair.second->cusip] = pair.second;
    }
  } else if (!strcasecmp(b, "sedol")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      if (*pair.second->sedol) sec_map[pair.second->sedol] = pair.second;
    }
  } else if (!strcasecmp(b, "id")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      sec_map[std::to_string(pair.second->id)] = pair.second;
    }
  } else if (!strcasecmp(b, "symbol")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      sec_map[std::string(pair.second->exchange->name) + " " +
              pair.second->symbol] = pair.second;
    }
  } else if (!strcasecmp(b, "local_symbol")) {
    for (auto& pair : SecurityManager::Instance().securities()) {
      if (*pair.second->local_symbol)
        sec_map[std::string(pair.second->exchange->name) + " " +
                pair.second->local_symbol] = pair.second;
    }
  } else {
    LOG_FATAL("Invalid file: " << fn);
  }

  while (std::getline(ifs, line)) {
    if (!strcasecmp(line.c_str(), "@end")) break;
    auto sec = sec_map[line];
    out.push_back(sec);
    if (!sec) {
      LOG_ERROR("Unknown " << a << " on line " << line << " of " << fn);
      continue;
    }
  }
  LOG_INFO(out.size() << " securities in " << fn);

  return out;
}

void Backtest::Start() noexcept {
  if (ticks_file_.empty()) {
    LOG_FATAL("Please set ticks file first");
  }
  std::ifstream ifs(ticks_file_.c_str());
  if (!ifs.good()) {
    LOG_FATAL("Can not open " << ticks_file_);
  }

  auto secs = GetSecurities(ifs, ticks_file_);

  LOG_DEBUG("Start to play back " << ticks_file_);
  std::string line;
  while (std::getline(ifs, line)) {
    uint32_t hms;
    uint32_t i;
    char type;
    double px;
    double qty;
    if (sscanf(line.c_str(), "%u %u %c %lf %lf", &hms, &i, &type, &px, &qty) !=
        5)
      continue;
    if (i >= secs.size()) continue;
    auto nsecond = hms / 10000 * 3600 + hms % 10000 / 100 * 60 + hms % 100;
    auto sec = secs[i];
    if (!sec) continue;
    switch (type) {
      case 'T': {
        Update(sec->id, px, qty);
        if (!qty && sec->type == kForexPair) qty = 1e12;
        if (px > 0 && qty > 0) {
          auto size = qty;
          auto& actives = active_orders_[sec->id];
          if (actives.empty()) return;
          auto it = actives.begin();
          while (it != actives.end() && size > 0) {
            auto& tuple = it->second;
            auto ok = (tuple.is_buy && px <= tuple.px) ||
                      (!tuple.is_buy && px >= tuple.px);
            if (!ok) {
              it++;
              continue;
            }
            auto n = std::min(size, tuple.leaves);
            size -= n;
            tuple.leaves -= n;
            assert(size >= 0);
            assert(tuple.leaves >= 0);
            HandleFill(tuple.id, n, tuple.px,
                       boost::uuids::to_string(kUuidGen()), 0,
                       tuple.leaves > 0);
            if (tuple.leaves <= 0)
              it = actives.erase(it);
            else
              it++;
          }
        }
      } break;
      case 'A':
        if (*sec->exchange->name == 'U') qty *= 100;
        Update(sec->id, px, qty, false);
        break;
      case 'B':
        if (*sec->exchange->name == 'U') qty *= 100;
        Update(sec->id, px, qty, true);
        break;
      default:
        break;
    }
  }
}

std::string Backtest::Place(const Order& ord) noexcept {
  auto id = ord.id;
  if (!ord.sec->IsInTradePeriod()) {
    Async([=]() { HandleNewRejected(id, "Not in trading period"); });
    return {};
  }
  auto qty = ord.qty;
  if (qty <= 0) {
    Async([=]() { HandleNewRejected(id, "invalid OrderQty"); });
    return {};
  }
  if (ord.price < 0 && ord.type != kMarket) {
    Async([=]() { HandleNewRejected(id, "invalid price"); });
    return {};
  }
  if (ord.type == kMarket) {
    auto q = MarketDataManager::Instance().Get(*ord.sec).quote();
    auto qty_q = ord.IsBuy() ? q.ask_size : q.bid_size;
    auto px_q = ord.IsBuy() ? q.ask_price : q.bid_price;
    if (!qty_q && ord.sec->type == kForexPair) qty_q = 1e12;
    if (qty_q > 0 && px_q > 0) {
      Async([=]() { HandleNew(id, ""); });
      if (qty_q > qty) qty_q = qty;
      Async([=]() {
        HandleFill(id, qty_q, px_q, boost::uuids::to_string(kUuidGen()), 0,
                   qty_q != qty);
      });
      if (qty_q == qty) return {};
      qty -= qty_q;
    } else {
      Async([=]() { HandleNewRejected(id, "no quote"); });
      return {};
    }
  } else {
    Async([=]() { HandleNew(id, ""); });
  }
  OrderTuple tuple{id, qty, ord.price, ord.IsBuy()};
  active_orders_[ord.sec->id][id] = tuple;
  return {};
}

std::string Backtest::Cancel(const Order& ord) noexcept {
  auto& actives = active_orders_[ord.sec->id];
  auto it = actives.find(ord.orig_id);
  auto id = ord.id;
  auto orig_id = ord.orig_id;
  if (it == actives.end()) {
    Async([=]() { HandleCancelRejected(id, orig_id, "inactive"); });
  } else {
    Async([=]() { HandleCanceled(id, orig_id, ""); });
  }
  actives.erase(it);
  return {};
}

}  // namespace opentrade

#endif
