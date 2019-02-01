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

static inline void Async(std::function<void()> func, double seconds) {
  kTimers.emplace(kTime + seconds * 1e6, func);
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

inline double Backtest::TryFillBuy(double px, double qty, Orders* m) {
  for (auto it = m->buys.begin();
       it != m->buys.end() && qty > 0 && px <= it->first;) {
    auto& tuple = it->second;
    auto n = std::min(qty, tuple.leaves);
    qty -= n;
    tuple.leaves -= n;
    assert(qty >= 0);
    assert(tuple.leaves >= 0);
    HandleFill(tuple.order->id, n, it->first,
               boost::uuids::to_string(kUuidGen()), 0, tuple.leaves > 0);
    auto algo_id = tuple.order->inst ? tuple.order->inst->algo().id() : 0;
    of_ << std::setprecision(15) << GetNowStr() << ','
        << tuple.order->sec->symbol << ',' << (tuple.order->IsBuy() ? 'B' : 'S')
        << ',' << n << ',' << it->first << ',' << algo_id << '\n';
    if (tuple.leaves <= 0) {
      m->all.erase(tuple.order->id);
      it = m->buys.erase(it);
    } else {
      ++it;
    }
  }
  return qty;
}

inline double Backtest::TryFillSell(double px, double qty, Orders* m) {
  for (auto it = m->sells.rbegin();
       it != m->sells.rend() && qty > 0 && px >= it->first;) {
    auto& tuple = it->second;
    auto n = std::min(qty, tuple.leaves);
    qty -= n;
    tuple.leaves -= n;
    assert(qty >= 0);
    assert(tuple.leaves >= 0);
    HandleFill(tuple.order->id, n, it->first,
               boost::uuids::to_string(kUuidGen()), 0, tuple.leaves > 0);
    auto algo_id = tuple.order->inst ? tuple.order->inst->algo().id() : 0;
    of_ << std::setprecision(15) << GetNowStr() << ','
        << tuple.order->sec->symbol << ',' << (tuple.order->IsBuy() ? 'B' : 'S')
        << ',' << n << ',' << it->first << ',' << algo_id << '\n';
    if (tuple.leaves <= 0) {
      m->all.erase(tuple.order->id);
      m->sells.erase(++it.base());
    } else {
      ++it;
    }
  }
  return qty;
}

inline void Backtest::HandleTick(uint32_t hmsm, char type, double px,
                                 double qty, const SecTuple& st) {
  auto hms = hmsm / 1000;
  auto nsecond = hms / 10000 * 3600 + hms % 10000 / 100 * 60 + hms % 100;
  kTime = (tm0_ + nsecond) * 1000000lu + hmsm % 1000 * 1000;
  for (auto it = kTimers.begin(); it != kTimers.end() && it->first < kTime;) {
    it->second();
    it = kTimers.erase(it);
  }
  auto sec = std::get<0>(st);
  if (!sec) return;
  px *= std::get<1>(st);
  qty *= std::get<2>(st);
  if (!qty && sec->type == kForexPair) qty = 1e9;
  auto m = std::get<3>(st);
  switch (type) {
    case 'T': {
      Update(sec->id, px, qty);
      if (m->all.empty()) return;
      if (px > 0 && qty > 0 &&
          rand_r(&seed_) % 100 / 100. >= (1 - trade_hit_ratio_)) {
        qty = TryFillBuy(px, qty, m);
        TryFillSell(px, qty, m);
      }
    } break;
    case 'A':
      Update(sec->id, px, qty, false);
      TryFillBuy(px, qty, m);
      break;
    case 'B':
      Update(sec->id, px, qty, true);
      TryFillSell(px, qty, m);
      break;
    default:
      break;
  }
}

void Backtest::PlayTickFile(const std::string& fn_tmpl,
                            const boost::gregorian::date& date) {
  skip_ = false;
  boost::posix_time::ptime pt(date);
  auto tm = boost::posix_time::to_tm(pt);
  tm0_ = mktime(&tm);
  kTime = tm0_ * 1000000lu;

  localtime_r(&tm0_, &tm);
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
  std::vector<SecTuple> secs;
  secs.resize(secs0.size());
  auto date_num = date.year() * 10000 + date.month() * 100 + date.day();
  for (auto i = 0u; i < secs0.size(); ++i) {
    auto sec = secs0[i];
    if (!sec) continue;
    auto& adjs = sec->adjs;
    auto it = std::upper_bound(sec->adjs.begin(), sec->adjs.end(),
                               Security::Adj(date_num));
    auto orders = &active_orders_[sec->id];
    if (it == adjs.end())
      secs[i] = std::make_tuple(sec, 1., 1., orders);
    else
      secs[i] = std::make_tuple(sec, it->px, it->vol, orders);
  }

  LOG_DEBUG("Start to play back " << fn);
  std::string line;
  seed_ = 0;
  trade_hit_ratio_ = 0.5;
  auto trade_hit_ratio_str = getenv("TRADE_HIT_RATIO");
  if (trade_hit_ratio_str) {
    trade_hit_ratio_ = atof(trade_hit_ratio_str);
  }
  while (std::getline(ifs, line) && !skip_) {
    uint32_t hmsm;
    uint32_t i;
    char type;
    double px;
    double qty;
    if (sscanf(line.c_str(), "%u %u %c %lf %lf", &hmsm, &i, &type, &px, &qty) !=
        5)
      continue;
    if (i >= secs.size()) continue;
    auto& st = secs[i];
    HandleTick(hmsm, type, px, qty, st);
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
  for (auto& pair : algo_mngr.algos_) {
    pair.second->Stop();
    delete pair.second;
  }
  algo_mngr.runners_[0].dirties_.clear();
  algo_mngr.runners_[0].instruments_.clear();
  algo_mngr.runners_[0].md_refs_.clear();
  algo_mngr.md_refs_.clear();
  algo_mngr.algos_.clear();
  algo_mngr.algo_of_token_.clear();
  algo_mngr.algos_of_sec_acc_.clear();
  auto& gb = GlobalOrderBook::Instance();
  for (auto& pair : gb.orders_) delete pair.second;
  gb.orders_.clear();
  gb.exec_ids_.clear();
  active_orders_.clear();
  kTimers.clear();
}

std::string Backtest::Place(const Order& ord) noexcept {
  Async(
      [this, &ord]() {
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
            auto algo_id = ord.inst ? ord.inst->algo().id() : 0;
            of_ << std::setprecision(15) << GetNowStr() << ','
                << ord.sec->symbol << ',' << (ord.IsBuy() ? 'B' : 'S') << ','
                << qty_q << ',' << px_q << ',' << algo_id << '\n';
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
        OrderTuple tuple{qty, &ord};
        auto& m = active_orders_[ord.sec->id];
        auto it = (ord.IsBuy() ? m.buys : m.sells).emplace(ord.price, tuple);
        m.all.emplace(id, it);
        assert(m.all.size() == m.buys.size() + m.sells.size());
      },
      latency_);
  return {};
}

std::string Backtest::Cancel(const Order& ord) noexcept {
  Async(
      [this, &ord]() {
        auto& m = active_orders_[ord.sec->id];
        auto it = m.all.find(ord.orig_id);
        auto id = ord.id;
        auto orig_id = ord.orig_id;
        if (it == m.all.end()) {
          HandleCancelRejected(id, orig_id, "inactive");
        } else {
          HandleCanceled(id, orig_id, "");
          (ord.IsBuy() ? m.buys : m.sells).erase(it->second);
          m.all.erase(it);
        }
      },
      latency_);
  return {};
}

SubAccount* Backtest::CreateSubAccount(const std::string& name) {
  auto& acc_mngr = AccountManager::Instance();
  auto s = new SubAccount();
  s->name = strdup(name.c_str());
  auto broker_accs = std::make_shared<SubAccount::BrokerAccountMap>();
  broker_accs->emplace(0, acc_mngr.GetBrokerAccount(0));
  s->set_broker_accounts(broker_accs);  //  0 is the default exchange
  acc_mngr.sub_accounts_.emplace(s->id, s);
  acc_mngr.sub_account_of_name_.emplace(s->name, s);
  auto sub_accs = std::make_shared<User::SubAccountMap>();
  sub_accs->emplace(s->id, s);
  const_cast<User*>(acc_mngr.GetUser(0))->set_sub_accounts(sub_accs);
  return s;
}

void Backtest::Start(const std::string& py, double latency) {
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
  CreateSubAccount("test");

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
  of_.close();
}

}  // namespace opentrade

#endif
