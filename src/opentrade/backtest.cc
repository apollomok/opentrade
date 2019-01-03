#ifdef BACKTEST

#include "backtest.h"

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "logger.h"

namespace fs = boost::filesystem;

namespace opentrade {

static boost::uuids::random_generator kUuidGen;

static inline void Async(std::function<void()> func, int ms) {
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

void Backtest::PlayTickFile(const std::string& fn_tmpl,
                            const boost::gregorian::date& date) {
  boost::posix_time::ptime pt(date);
  auto tm = boost::posix_time::to_tm(pt);
  auto tm0 = mktime(&tm);
  kTime = tm0 * 1000000lu;

  localtime_r(&tm0, &tm);
  for (auto& pair : SecurityManager::Instance().exchanges()) {
    pair.second->utc_time_offset = tm.tm_gmtoff;
  }

  char fn[256];
  strftime(fn, sizeof(fn), fn_tmpl.c_str(), &tm);

  std::ifstream ifs(fn);
  if (!ifs.good()) return;

  if (on_start_of_day_) {
    try {
      on_start_of_day_(obj_, kOpentrade.attr("get_datetime")().attr("date")());
    } catch (const bp::error_already_set& err) {
      PrintPyError("on_start_of_day", true);
      return;
    }
  }

  auto secs0 = GetSecurities(ifs, fn);
  std::vector<std::tuple<const Security*, double, double>> secs;
  secs.resize(secs0.size());
  auto date_num = date.year() * 10000 + date.month() * 100 + date.day();
  for (auto i = 0u; i < secs0.size(); ++i) {
    auto sec = secs0[i];
    if (!sec) continue;
    auto& adjs = sec->adjs;
    auto it = std::upper_bound(sec->adjs.begin(), sec->adjs.end(),
                               Security::Adj(date_num));
    if (it == adjs.end())
      secs[i] = std::make_tuple(sec, 1., 1.);
    else
      secs[i] = std::make_tuple(sec, it->px, it->vol);
  }

  LOG_DEBUG("Start to play back " << fn);
  std::string line;
  auto seed = 0u;
  while (std::getline(ifs, line)) {
    uint32_t hmsm;
    uint32_t i;
    char type;
    double px;
    double qty;
    if (sscanf(line.c_str(), "%u %u %c %lf %lf", &hmsm, &i, &type, &px, &qty) !=
        5)
      continue;
    if (i >= secs.size()) continue;
    auto hms = hmsm / 1000;
    auto nsecond = hms / 10000 * 3600 + hms % 10000 / 100 * 60 + hms % 100;
    kTime = (tm0 + nsecond) * 1000000lu + hmsm % 1000 * 1000;
    auto it = kTimers.begin();
    while (it != kTimers.end() && it->first < kTime) {
      it->second();
      kTimers.erase(it);
      it = kTimers.begin();
    }
    auto& st = secs[i];
    auto sec = std::get<0>(st);
    if (!sec) continue;
    px *= std::get<1>(st);
    qty *= std::get<2>(st);
    switch (type) {
      case 'T': {
        Update(sec->id, px, qty);
        if (!qty && sec->type == kForexPair) qty = 1e12;
        if (px > 0 && qty > 0 && rand_r(&seed) % 100 > 50) {
          auto size = qty;
          auto& actives = active_orders_[sec->id];
          if (actives.empty()) continue;
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

  PositionManager::Instance().UpdatePnl();

  if (on_end_of_day_) {
    try {
      on_end_of_day_(obj_, kOpentrade.attr("get_datetime")().attr("date")());
    } catch (const bp::error_already_set& err) {
      PrintPyError("on_end_of_day", true);
    }
  }

  for (auto& pair : *md_) {
    const_cast<Security*>(SecurityManager::Instance().Get(pair.first))
        ->close_price = pair.second.trade.close;
    pair.second = opentrade::MarketData{};
  }
}

void Backtest::Clear() {
  auto& algo_mngr = AlgoManager::Instance();
  algo_mngr.runners_[0].instruments_.clear();
  algo_mngr.runners_[0].md_refs_.clear();
  algo_mngr.runners_[0].instruments_.clear();
  algo_mngr.md_refs_.clear();
  algo_mngr.algos_.clear();
  for (auto& pair : algo_mngr.algos_) {
    pair.second->Stop();
    delete pair.second;
  }
  auto& gb = GlobalOrderBook::Instance();
  for (auto& pair : gb.orders_) delete pair.second;
  gb.orders_.clear();
  gb.exec_ids_.clear();
}

std::string Backtest::Place(const Order& ord) noexcept {
  Async(
      [this, ord]() {
        auto id = ord.id;
        if (!ord.sec->IsInTradePeriod()) {
          HandleNewRejected(id, "Not in trading period");
          return;
        }
        auto qty = ord.qty;
        if (qty <= 0) {
          HandleNewRejected(id, "invalid OrderQty");
          return;
        }
        if (ord.price < 0 && ord.type != kMarket) {
          HandleNewRejected(id, "invalid price");
          return;
        }
        if (ord.type == kMarket) {
          auto q = MarketDataManager::Instance().Get(*ord.sec).quote();
          auto qty_q = ord.IsBuy() ? q.ask_size : q.bid_size;
          auto px_q = ord.IsBuy() ? q.ask_price : q.bid_price;
          if (!qty_q && ord.sec->type == kForexPair) qty_q = 1e9;
          if (qty_q > 0 && px_q > 0) {
            HandleNew(id, "");
            if (qty_q > qty) qty_q = qty;
            HandleFill(id, qty_q, px_q, boost::uuids::to_string(kUuidGen()), 0,
                       qty_q != qty);
            if (qty_q != qty) {
              HandleCanceled(id, id, "");
            }
            return;
          } else {
            HandleNewRejected(id, "no quote");
            return;
          }
        } else {
          HandleNew(id, "");
        }
        OrderTuple tuple{id, qty, ord.price, ord.IsBuy()};
        active_orders_[ord.sec->id][id] = tuple;
      },
      latency_);
  return {};
}

std::string Backtest::Cancel(const Order& ord) noexcept {
  Async(
      [this, ord]() {
        auto& actives = active_orders_[ord.sec->id];
        auto it = actives.find(ord.orig_id);
        auto id = ord.id;
        auto orig_id = ord.orig_id;
        if (it == actives.end()) {
          HandleCancelRejected(id, orig_id, "inactive");
        } else {
          HandleCanceled(id, orig_id, "");
        }
        actives.erase(it);
      },
      latency_);
  return {};
}

void Backtest::Start(const std::string& py, int latency) {
  obj_ = bp::object(bp::ptr(this));
  latency_ = latency;

  MarketDataManager::Instance().Add(this);
  ExchangeConnectivityManager::Instance().Add(this);
  for (auto& pair : SecurityManager::Instance().securities()) {
    pair.second->close_price = 0;
  }
  auto& acc_mngr = AccountManager::Instance();
  auto u = new User();
  u->name = "backtest";
  acc_mngr.users_.emplace(u->id, u);
  acc_mngr.user_of_name_.emplace(u->name, u);
  auto b = new BrokerAccount();
  b->name = "backtest";
  b->adapter = this;
  acc_mngr.broker_accounts_.emplace(b->id, b);
  auto s = new SubAccount();
  s->name = "test";
  auto broker_accs = std::make_shared<SubAccount::BrokerAccountMap>();
  broker_accs->emplace(0, b);
  s->set_broker_accounts(broker_accs);  //  0 is the default exchange
  acc_mngr.sub_accounts_.emplace(s->id, s);
  acc_mngr.sub_account_of_name_.emplace(s->name, s);
  auto sub_accs = std::make_shared<User::SubAccountMap>();
  sub_accs->emplace(s->id, s);
  u->set_sub_accounts(sub_accs);

  bp::import("sys").attr("path").attr("insert")(
      0, fs::path(py).parent_path().string());
  auto fn = fs::path(py).filename().string();
  auto module_name = fn.substr(0, fn.length() - 3);
  bp::object m;
  try {
    m = bp::import(module_name.c_str());
  } catch (const bp::error_already_set& err) {
    PrintPyError("load python", true);
    return;
  }
  LOG_INFO(module_name + " loaded");
  on_start_ = GetCallable(m, "on_start");
  on_start_of_day_ = GetCallable(m, "on_start_of_day");
  on_end_ = GetCallable(m, "on_end");
  on_end_of_day_ = GetCallable(m, "on_end_of_day");
  if (!on_start_) return;
  try {
    on_start_(obj_);
  } catch (const bp::error_already_set& err) {
    PrintPyError("on_start", true);
  }
}

void Backtest::End() {
  if (!on_end_) return;
  try {
    on_end_(obj_);
  } catch (const bp::error_already_set& err) {
    PrintPyError("on_end", true);
  }
}

}  // namespace opentrade

#endif
