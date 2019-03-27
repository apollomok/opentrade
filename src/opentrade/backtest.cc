#ifdef BACKTEST

#include "backtest.h"

#include "logger.h"
#include "simulator.h"

namespace fs = boost::filesystem;

namespace opentrade {

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
struct SecTuple {
  const Security* sec;
  Simulator* sim;
  Simulator::Orders* actives;
};
struct Tick {
  SecTuple* st;
  uint32_t hmsm;
  char type;
  double px;
  double qty;
  bool operator<(const Tick& b) const { return hmsm < b.hmsm; }
};
typedef std::vector<std::shared_ptr<SecTuple>> SecTuples;
typedef std::vector<Tick> Ticks;

bool LoadTickFile(const std::string& fn, Simulator* sim,
                  const boost::gregorian::date& date, SecTuples& sts,
                  Ticks& ticks) {
  std::ifstream ifs(fn);
  if (!ifs.good()) return false;

  LOG_INFO("Loading " << fn);
  auto secs0 = GetSecurities(ifs, fn);
  std::vector<std::tuple<SecTuple*, double, double>> secs;
  secs.resize(secs0.size());
  auto date_num = date.year() * 10000 + date.month() * 100 + date.day();
  for (auto i = 0u; i < secs0.size(); ++i) {
    auto sec = secs0[i];
    if (!sec) continue;
    auto& adjs = sec->adjs;
    auto it = std::upper_bound(sec->adjs.begin(), sec->adjs.end(),
                               Security::Adj(date_num));
    auto st = std::shared_ptr<SecTuple>(
        new SecTuple{sec, sim, &sim->active_orders()[sec->id]});
    sts.push_back(st);
    if (it == adjs.end())
      secs[i] = std::make_tuple(st.get(), 1., 1.);
    else
      secs[i] = std::make_tuple(st.get(), it->px, it->vol);
  }
  std::string line;
  while (std::getline(ifs, line)) {
    Tick t;
    uint32_t i;
    if (sscanf(line.c_str(), "%u %u %c %lf %lf", &t.hmsm, &i, &t.type, &t.px,
               &t.qty) != 5)
      continue;
    if (i >= secs.size()) continue;
    auto& st = secs[i];
    t.st = std::get<0>(st);
    t.px *= std::get<1>(st);
    if (!t.px) continue;
    t.qty *= std::get<2>(st);
    ticks.push_back(t);
  }
  return true;
}

void Backtest::Play(const boost::gregorian::date& date) {
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
  SecTuples sts;
  Ticks ticks;
  for (auto& pair : simulators_) {
    strftime(fn, sizeof(fn), pair.first.c_str(), &tm);
    LoadTickFile(fn, pair.second, date, sts, ticks);
  }
  if (ticks.empty()) return;
  if (simulators_.size() > 1) std::sort(ticks.begin(), ticks.end());

  if (on_start_of_day_) {
    try {
      on_start_of_day_(obj_, kOpentrade.attr("get_datetime")().attr("date")());
    } catch (const bp::error_already_set& err) {
      PrintPyError("on_start_of_day", true);
      return;
    }
  }

  LOG_DEBUG("Start to play back " << fn);
  trade_hit_ratio_ = 0.5;
  auto trade_hit_ratio_str = getenv("TRADE_HIT_RATIO");
  if (trade_hit_ratio_str) {
    trade_hit_ratio_ = atof(trade_hit_ratio_str);
  }
  for (auto& t : ticks) {
    if (skip_) break;
    auto hms = t.hmsm / 1000;
    auto nsecond = hms / 10000 * 3600 + hms % 10000 / 100 * 60 + hms % 100;
    auto tm = (tm0_ + nsecond) * 1000000lu + t.hmsm % 1000 * 1000;
    if (tm < kTime) tm = kTime;
    auto it = kTimers.begin();
    while (it != kTimers.end() && it->first <= tm) {
      if (it->first > kTime) kTime = it->first;
      it->second();
      kTimers.erase(it);
      // do not use it = kTimers.erase(it) in case smaller timer inserted
      it = kTimers.begin();
    }
    if (tm > kTime) kTime = tm;

    t.st->sim->HandleTick(*t.st->sec, t.type, t.px, t.qty, trade_hit_ratio_,
                          t.st->actives);
  }

  PositionManager::Instance().UpdatePnl();

  if (on_end_of_day_) {
    try {
      on_end_of_day_(obj_, kOpentrade.attr("get_datetime")().attr("date")());
    } catch (const bp::error_already_set& err) {
      PrintPyError("on_end_of_day", true);
    }
  }

  for (auto& pair : simulators_) {
    pair.second->ResetData();
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
  for (auto& pair : simulators_) pair.second->active_orders().clear();
  kTimers.clear();
}

void Backtest::AddSimulator(const std::string& fn_tmpl,
                            const std::string& name) {
  auto sim = new Simulator(of_);
  sim->set_name(name);
  Adapter::StrMap params;
  std::stringstream tmp;
  for (auto& pair : SecurityManager::Instance().exchanges()) {
    if (!tmp.str().empty()) tmp << ',';
    tmp << pair.second->name;
  }
  params["markets"] = tmp.str();
  sim->set_config(params);
  simulators_.emplace_back(fn_tmpl, sim);
  auto& acc_mngr = AccountManager::Instance();
  auto b = new BrokerAccount();
  b->name = name.empty() ? "backtest" : name.c_str();
  b->adapter = sim;
  b->id = acc_mngr.broker_accounts_.size();
  acc_mngr.broker_accounts_.emplace(b->id, b);
  CreateSubAccount(name.empty() ? "test" : name.c_str(), b);
  MarketDataManager::Instance().Add(sim);
  ExchangeConnectivityManager::Instance().Add(sim);
}

SubAccount* Backtest::CreateSubAccount(const std::string& name,
                                       const BrokerAccount* broker) {
  auto& acc_mngr = AccountManager::Instance();
  auto s = new SubAccount();
  s->name = strdup(name.c_str());
  auto broker_accs = std::make_shared<SubAccount::BrokerAccountMap>();
  broker_accs->emplace(0, broker ? broker : acc_mngr.GetBrokerAccount(0));
  s->set_broker_accounts(broker_accs);
  s->id = acc_mngr.sub_accounts_.size();
  acc_mngr.sub_accounts_.emplace(s->id, s);
  acc_mngr.sub_account_of_name_.emplace(s->name, s);
  auto sub_accs = std::make_shared<User::SubAccountMap>();
  sub_accs->emplace(s->id, s);
  const_cast<User*>(acc_mngr.GetUser(0))->set_sub_accounts(sub_accs);
  return s;
}

void Backtest::Start(const std::string& py, double latency,
                     const std::string& default_tick_file) {
  obj_ = bp::object(bp::ptr(this));
  latency_ = latency;

  for (auto& pair : SecurityManager::Instance().securities()) {
    pair.second->close_price = 0;
  }
  auto& acc_mngr = AccountManager::Instance();
  auto u = new User();
  u->name = "backtest";
  acc_mngr.users_.emplace(u->id, u);
  acc_mngr.user_of_name_.emplace(u->name, u);

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
  if (simulators_.empty()) AddSimulator(default_tick_file);
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
  if (on_end_) {
    try {
      on_end_(obj_);
    } catch (const bp::error_already_set& err) {
      PrintPyError("on_end", true);
    }
  }
  of_.close();
}

}  // namespace opentrade

#endif  // BACKTEST
