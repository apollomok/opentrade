#include "connection.h"

#include <unistd.h>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <thread>

#include "algo.h"
#include "consolidation.h"
#include "database.h"
#include "exchange_connectivity.h"
#include "indicator_handler.h"
#include "logger.h"
#include "market_data.h"
#include "opentick.h"
#include "position.h"
#include "security.h"
#include "server.h"

namespace fs = boost::filesystem;

namespace opentrade {

static time_t kStartTime = GetTime();
static thread_local boost::uuids::random_generator kUuidGen;
static tbb::concurrent_unordered_map<std::string, const User*> kTokens;
static TaskPool kTaskPool;

std::string sha1(const std::string& str) {
  boost::uuids::detail::sha1 s;
  s.process_bytes(str.c_str(), str.size());
  unsigned int digest[5];
  s.get_digest(digest);
  std::string out;
  for (int i = 0; i < 5; ++i) {
    char tmp[17];
    snprintf(tmp, sizeof(tmp), "%08x", digest[i]);
    out += tmp;
  }
  return out;
}

template <typename T>
inline T Get(const json& j) {
  if (!j.is_number_integer()) {
    std::stringstream os;
    os << "wrong json value "
       << ": " << j << ", expect integer";
    throw std::runtime_error(os.str());
  }
  return j.get<T>();
}

template <>
inline std::string Get(const json& j) {
  if (j.is_null()) return {};
  if (!j.is_string()) {
    std::stringstream os;
    os << "wrong json value "
       << ": " << j << ", expect string";
    throw std::runtime_error(os.str());
  }
  return j.get<std::string>();
}

template <>
inline double Get(const json& j) {
  if (!j.is_number_float()) {
    std::stringstream os;
    os << "wrong json value "
       << ": " << j << ", expect float";
    throw std::runtime_error(os.str());
  }
  return j.get<double>();
}

template <>
inline bool Get(const json& j) {
  if (!j.is_boolean()) {
    std::stringstream os;
    os << "wrong json value "
       << ": " << j << ", expect bool";
    throw std::runtime_error(os.str());
  }
  return j.get<bool>();
}

inline double GetNum(const json& j) {
  if (!j.is_number()) {
    std::stringstream os;
    os << "wrong json value "
       << ": " << j << ", expect number";
    throw std::runtime_error(os.str());
  }
  if (j.is_number_float()) return j.get<double>();
  return j.get<int64_t>();
}

static auto GetSecurity(const json& j) {
  const Security* sec = nullptr;
  if (j.is_number_integer()) {
    auto v = Get<int64_t>(j);
    sec = SecurityManager::Instance().Get(v);
    if (!sec)
      throw std::runtime_error("Unknown security id: " + std::to_string(v));
  } else {
    auto exch = Get<std::string>(j[0]);
    auto symbol = Get<std::string>(j[1]);
    sec = SecurityManager::Instance().Get(exch, symbol);
    if (!sec)
      throw std::runtime_error("Unknown security: [" + exch + ", " + symbol +
                               "]");
  }
  return sec;
}

template <typename T>
static inline T ParseParamScalar(const json& j) {
  if (j.is_number_float()) return j.get<double>();
  if (j.is_number_integer()) return j.get<int64_t>();
  if (j.is_boolean()) return j.get<bool>();
  if (j.is_string()) return j.get<std::string>();
  if (j.is_object()) {
    DataSrc src;
    const Security* sec = nullptr;
    const SubAccount* acc = nullptr;
    OrderSide side = static_cast<OrderSide>(0);
    double qty = 0;
    for (auto& it : j.items()) {
      if (it.key() == "qty") {
        qty = GetNum(it.value());
      } else if (it.key() == "side") {
        auto side_str = Get<std::string>(it.value());
        if (!GetOrderSide(side_str, &side)) {
          throw std::runtime_error("Unknown order side: " + side_str);
        }
      } else if (it.key() == "src") {
        src = Get<std::string>(it.value());
      } else if (it.key() == "sec") {
        sec = GetSecurity(it.value());
      } else if (it.key() == "acc") {
        if (it.value().is_number_integer()) {
          auto v = Get<int64_t>(it.value());
          acc = AccountManager::Instance().GetSubAccount(v);
          if (!acc)
            throw std::runtime_error("Unknown account id: " +
                                     std::to_string(v));
        } else if (it.value().is_string()) {
          auto v = Get<std::string>(it.value());
          acc = AccountManager::Instance().GetSubAccount(v);
          if (!acc) throw std::runtime_error("Unknown account: " + v);
        }
      }
    }
    auto s = SecurityTuple{src, sec, acc, side, qty};
    if (qty <= 0) {
      throw std::runtime_error("Empty quantity");
    }
    if (!side) {
      throw std::runtime_error("Empty side");
    }
    if (!sec) {
      throw std::runtime_error("Empty security");
    }
    if (!acc) {
      throw std::runtime_error("Empty account");
    }
    return s;
  }
  return {};
}

static inline ParamDef::Value ParseParamValue(const json& j) {
  if (j.is_array()) {
    ParamDef::ValueVector v;
    for (auto& it : j.items()) {
      v.push_back(ParseParamScalar<ParamDef::ValueScalar>(it.value()));
    }
    return v;
  }
  return ParseParamScalar<ParamDef::Value>(j);
}

static inline decltype(auto) ParseParams(const json& params) {
  auto m = std::make_shared<Algo::ParamMap>();
  for (auto& it : params.items()) {
    (*m.get())[it.key()] = ParseParamValue(it.value());
  }
  return m;
}

Connection::~Connection() {
  LOG_DEBUG(GetAddress() << ": Connection destructed");
}

Connection::Connection(Transport::Ptr transport,
                       std::shared_ptr<boost::asio::io_service> service)
    : transport_(transport), strand_(*service), timer_(*service) {}

void Connection::PublishMarketStatus() {
  auto& ecs = ExchangeConnectivityManager::Instance().adapters();
  for (auto& it : ecs) {
    auto& name = it.first;
    auto v = it.second->connected();
    auto it2 = ecs_.find(name);
    if (it2 == ecs_.end() || it2->second != v) {
      ecs_[name] = v;
      Send(json{"market", "exchange", name, v});
    }
  }
  auto& mds = MarketDataManager::Instance().adapters();
  for (auto& it : mds) {
    auto& name = it.first;
    auto v = it.second->connected();
    auto it2 = mds_.find(name);
    if (it2 == mds_.end() || it2->second != v) {
      mds_[name] = v;
      Send(json{"market", "data", name, v});
    }
  }
}

static inline void GetMarketData(
    const MarketData& md, const MarketData& md0,
    const std::pair<Security::IdType, DataSrc::IdType>& sec_src, json* j) {
  if (md.tm == md0.tm) return;
  json j3;
  j3["t"] = md.tm;
  if (md.trade.open != md0.trade.open) j3["o"] = md.trade.open;
  if (md.trade.high != md0.trade.high) j3["h"] = md.trade.high;
  if (md.trade.low != md0.trade.low) j3["l"] = md.trade.low;
  if (md.trade.close != md0.trade.close) j3["c"] = md.trade.close;
  if (md.trade.qty != md0.trade.qty) j3["q"] = md.trade.qty;
  if (md.trade.volume != md0.trade.volume) j3["v"] = md.trade.volume;
  if (md.trade.vwap != md0.trade.vwap) j3["V"] = md.trade.vwap;
  for (auto i = 0u; i < 5u; ++i) {
    char name[3] = "a";
    auto& d0 = md0.depth[i];
    auto& d = md.depth[i];
    if (d.ask_price != d0.ask_price) {
      name[1] = '0' + i;
      j3[name] = d.ask_price;
    }
    name[0] = 'A';
    if (d.ask_size != d0.ask_size) {
      name[1] = '0' + i;
      j3[name] = d.ask_size;
    }
    name[0] = 'b';
    if (d.bid_price != d0.bid_price) {
      name[1] = '0' + i;
      j3[name] = d.bid_price;
    }
    name[0] = 'B';
    if (d.bid_size != d0.bid_size) {
      name[1] = '0' + i;
      j3[name] = d.bid_size;
    }
  }
  if (sec_src.second)
    j->push_back(
        json{json{sec_src.first, DataSrc::GetStr(sec_src.second)}, j3});
  else
    j->push_back(json{sec_src.first, j3});
}

void Connection::PublishMarketdata() {
  if (closed_) return;
  auto self = shared_from_this();
  timer_.expires_from_now(boost::posix_time::milliseconds(1000));
  timer_.async_wait(strand_.wrap([self](auto) {
    self->PublishMarketdata();
    self->PublishMarketStatus();
    json j = {"md"};
    for (auto& pair : self->subs_) {
      auto sec_src = pair.first;
      auto& md =
          MarketDataManager::Instance().GetLite(sec_src.first, sec_src.second);
      GetMarketData(md, pair.second.first, sec_src, &j);
      pair.second.first = md;
    }
    if (j.size() > 1) {
      self->Send(j);
    }
    if (!self->sub_pnl_) return;
    for (auto& pair : PositionManager::Instance().sub_positions_) {
      auto sub_account_id = pair.first.first;
      if (!self->user_->GetSubAccount(sub_account_id)) continue;
      auto sec_id = pair.first.second;
      auto& pnl0 = self->single_pnls_[pair.first];
      auto& pos = pair.second;
      auto x = pos.realized_pnl != pnl0.first;
      if (x || pos.unrealized_pnl != pnl0.second) {
        pnl0.first = pos.realized_pnl;
        pnl0.second = pos.unrealized_pnl;
        json j = {
            "pnl",
            sub_account_id,
            sec_id,
            pnl0.second,
        };
        if (x) j.push_back(pnl0.first);
        self->Send(j);
      }
    }
    for (auto& pair : PositionManager::Instance().pnls_) {
      auto id = pair.first;
      if (!self->user_->GetSubAccount(id)) continue;
      auto& pnl0 = self->pnls_[id];
      auto& pnl = pair.second;
      if (pnl.realized != pnl0.first || pnl.unrealized != pnl0.second) {
        pnl0.first = pnl.realized;
        pnl0.second = pnl.unrealized;
        self->Send(json{"Pnl", id, GetTime(), pnl.realized, pnl.unrealized});
      }
    }
  }));
}

template <typename T>
static inline bool JsonifyScala(const T& v, json* j) {
  if (auto p_val = std::get_if<bool>(&v)) {
    j->push_back("bool");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<int64_t>(&v)) {
    j->push_back("int");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<int32_t>(&v)) {
    j->push_back("int");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<double>(&v)) {
    j->push_back("float");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<std::string>(&v)) {
    j->push_back("string");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<const char*>(&v)) {
    j->push_back("string");
    j->push_back(*p_val);
  } else if (auto p_val = std::get_if<SecurityTuple>(&v)) {
    j->push_back("security");
  } else {
    return false;
  }
  return true;
}

static inline void Jsonify(const ParamDef::Value& v, json* j) {
  if (JsonifyScala(v, j)) return;
  if (auto p_val = std::get_if<ParamDef::ValueVector>(&v)) {
    j->push_back("vector");
    json j2;
    for (auto& v2 : *p_val) {
      json j3;
      if (JsonifyScala(v2, &j3)) j2.push_back(j3);
    }
    j->push_back(j2);
  }
}

void Connection::OnMessageAsync(const std::string& msg) {
  if (closed_) return;
  auto self = shared_from_this();
  strand_.post([self, msg]() { self->OnMessageSync(msg); });
}

auto GetSecSrc(const json& j) {
  auto id = 0u;
  auto src = 0u;
  if (j.size() == 2) {
    id = Get<int64_t>(j[0]);
    auto tmp = Get<std::string>(j[1]);
    if (strcasecmp(tmp.c_str(), "default")) src = DataSrc::GetId(tmp.c_str());
  } else {
    if (j.is_string()) {
      auto tmp = Split(Get<std::string>(j), " ");
      if (tmp.size() > 0) id = atoll(tmp[0].c_str());
      if (tmp.size() == 2) {
        if (strcasecmp(tmp[1].c_str(), "default"))
          src = DataSrc::GetId(tmp[1].c_str());
      }
    } else {
      id = Get<int64_t>(j);
    }
  }
  return std::make_pair(id, src);
}

void Connection::OnMessageSync(const std::string& msg,
                               const std::string& token) {
  sent_ = false;
  HandleMessageSync(msg, token);
  if (!sent_ && transport_->stateless) Send(json{"ok"});
}

void Connection::HandleMessageSync(const std::string& msg,
                                   const std::string& token) {
  try {
    static std::string h("h");
    if (msg == h) {
      Send(h);
      return;
    }
    auto j = json::parse(msg);
    auto action = Get<std::string>(j[0]);
    if (action.empty()) {
      json j = {"error", "", "empty action"};
      LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
      Send(j);
      return;
    }
    if (action != "login" && !user_) {
      user_ = FindInMap(kTokens, token);
      if (!user_) {
        Send(json{"error", action, "you must login first", "login"});
        return;
      }
    }
    if (action == "login" || action == "validate_user") {
      OnLogin(action, j);
    } else if (action == "change_password") {
      json tmp{"", "", "", user_->id, {}};
      // somehow, json{{a, b}} always converted to [a, b] rather than [[a, b]],
      // so we use push_back instead
      tmp[4].push_back(json{"password", Get<std::string>(j[1])});
      OnAdminUsers(tmp, action, "modify");
    } else if (action == "bod") {
      json out;
      for (auto& pair : PositionManager::Instance().bods_) {
        auto acc = pair.first.first;
        if (!user_->is_admin && !user_->GetSubAccount(acc)) continue;
        auto sec_id = pair.first.second;
        auto& pos = pair.second;
        json j = {
            "bod",
            acc,
            sec_id,
            pos.qty,
            pos.avg_px,
            pos.realized_pnl,
            pos.broker_account_id,
            pos.tm,
        };
        if (transport_->stateless)
          out.push_back(j);
        else
          Send(j);
      }
      if (transport_->stateless) Send(out);
    } else if (action == "reconnect") {
      auto name = Get<std::string>(j[1]);
      auto m = MarketDataManager::Instance().GetAdapter(name);
      if (m) {
        m->Reconnect();
        return;
      }
      auto e = ExchangeConnectivityManager::Instance().GetAdapter(name);
      if (e) {
        e->Reconnect();
        return;
      }
    } else if (action == "securities") {
      OnSecurities(j);
    } else if (action == "admin") {
      OnAdmin(j);
    } else if (action == "position") {
      OnPosition(j, msg);
    } else if (action == "target") {
      if (j.size() == 1) {
        for (auto& pair : AccountManager::Instance().sub_accounts_) {
          if (!user_->is_admin && !user_->GetSubAccount(pair.first)) continue;
          OnTarget(json{"target", pair.second->name}, "");
        }
      } else {
        OnTarget(j, msg);
      }
    } else if (action == "offline") {
      if (j.size() > 2) {
        auto seq_algo = Get<int64_t>(j[2]);
        LOG_DEBUG(GetAddress() << ": Offline algos requested: " << seq_algo);
        AlgoManager::Instance().LoadStore(seq_algo, this);
        Send(json{"offline_algos", "complete"});
      }
      auto seq_confirmation = Get<int64_t>(j[1]);
      LOG_DEBUG(GetAddress()
                << ": Offline confirmations requested: " << seq_confirmation);
      GlobalOrderBook::Instance().LoadStore(seq_confirmation, this);
      Send(json{"offline_orders", "complete"});
      Send(json{"offline", "complete"});
    } else if (action == "shutdown") {
      if (!user_->is_admin) {
        throw std::runtime_error("admin required");
      }
      int seconds = 3;
      double interval = 1;
      if (j.size() > 1) {
        auto n = GetNum(j[1]);
        if (n > seconds) seconds = n;
      }
      if (j.size() > 2) {
        auto n = GetNum(j[2]);
        if (n > interval && n < seconds) interval = n;
      }
      Server::Stop();
      AlgoManager::Instance().Stop();
      LOG_INFO("Shutting down");
      while (seconds) {
        LOG_INFO(seconds);
        seconds -= interval;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(static_cast<int>(interval * 1000)));
        GlobalOrderBook::Instance().Cancel();
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto& ecs = ExchangeConnectivityManager::Instance().adapters();
      for (auto& it : ecs) {
        it.second->Stop();
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      kDatabaseTaskPool.Stop();
      kWriteTaskPool.Stop();
      if (system(("kill -9 " + std::to_string(getpid())).c_str())) return;
    } else if (action == "cancel") {
      auto id = Get<int64_t>(j[1]);
      auto ord = GlobalOrderBook::Instance().Get(id);
      if (!ord) {
        json j = {"error", "cancel", "invalid order id: " + std::to_string(id)};
        LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
        Send(j);
        return;
      }
      ExchangeConnectivityManager::Instance().Cancel(*ord);
    } else if (action == "order") {
      OnOrder(j, msg);
    } else if (action == "algo") {
      OnAlgo(j, msg);
    } else if (action == "pnl") {
      auto tm0 = 0l;
      if (j.size() >= 2) tm0 = Get<int64_t>(j[1]);
      tm0 = std::max(GetTime() - 24 * 3600, tm0);
      // not conform to REST rule
      for (auto& pair : PositionManager::Instance().pnls_) {
        auto id = pair.first;
        if (!user_->GetSubAccount(id)) continue;
        auto path = kStorePath / ("pnl-" + std::to_string(id));
        auto self = shared_from_this();
        kTaskPool.AddTask([self, tm0, id, path]() {
          std::ifstream f(path.c_str());
          const int LINE_LENGTH = 100;
          char str[LINE_LENGTH];
          json j2;
          while (f.getline(str, LINE_LENGTH)) {
            int tm;
            double realized, unrealized;
            if (3 == sscanf(str, "%d %lf %lf", &tm, &realized, &unrealized)) {
              if (tm <= tm0) continue;
              j2.push_back(json{tm, realized, unrealized});
            }
          }
          self->Send(json{"Pnl", id, j2});
        });
      }
      sub_pnl_ = true;
    } else if (action == "sub") {
      json jout = {"md"};
      for (auto i = 1u; i < j.size(); ++i) {
        auto sec_src = GetSecSrc(j[i]);
        auto& s = subs_[sec_src];
        auto sec = SecurityManager::Instance().Get(sec_src.first);
        if (sec) {
          auto& md = MarketDataManager::Instance().Get(*sec, sec_src.second);
          GetMarketData(md, s.first, sec_src, &jout);
          s.first = md;
          s.second += 1;
        }
      }
      if (jout.size() > 1) {
        Send(jout);
      }
    } else if (action == "unsub") {
      for (auto i = 1u; i < j.size(); ++i) {
        auto it = subs_.find(GetSecSrc(j[i]));
        if (it == subs_.end()) return;
        it->second.second -= 1;
        if (it->second.second <= 0) subs_.erase(it);
      }
    } else if (action == "algoFile") {
      auto fn = Get<std::string>(j[1]);
      auto path = kAlgoPath / fn;
      auto self = shared_from_this();
      sent_ = true;
      kTaskPool.AddTask([self, action, fn, path]() {
        json j = {action, fn};
        std::ifstream is(path.string());
        if (is.good()) {
          std::stringstream buffer;
          buffer << is.rdbuf();
          j.push_back(buffer.str());
        } else {
          j.push_back(nullptr);
          j.push_back("Not found");
        }
        self->Send(j);
      });
    } else if (action == "deleteAlgoFile") {
      auto fn = Get<std::string>(j[1]);
      auto path = kAlgoPath / fn;
      json j = {action, fn};
      try {
        fs::remove(path);
      } catch (const fs::filesystem_error& err) {
        j.push_back(err.what());
      }
      Send(j);
    } else if (action == "saveAlgoFile") {
      auto fn = Get<std::string>(j[1]);
      auto text = Get<std::string>(j[2]);
      auto path = kAlgoPath / fn;
      json j = {action, fn};
      std::ofstream os(path.string());
      if (os.good()) {
        os << text;
      } else {
        j.push_back("Can not write");
      }
      Send(j);
    } else if (action == "OpenTick") {
      auto sec = Get<int64_t>(j[1]);
      auto interval = Get<int64_t>(j[2]);
      auto start = Get<int64_t>(j[3]);
      auto end = Get<int64_t>(j[4]);
      std::string tbl = "bar";
      if (j.size() > 5) tbl = Get<std::string>(j[5]);
      sent_ = true;
      auto self = shared_from_this();
      OpenTick::Instance().Request(
          sec, interval, start, end, tbl,
          [self](opentick::ResultSet res, const std::string& err) {
            if (err.size()) {
              self->Send(json{"error", "OpenTick", err});
              return;
            }
            json out = "[]"_json;
            if (res) {
              try {
                for (auto& p : *res) {
                  if (p.size() != 6) continue;
                  out.push_back(
                      json{std::chrono::system_clock::to_time_t(
                               std::get<opentick::Tm>(p[0])),
                           std::get<double>(p[1]), std::get<double>(p[2]),
                           std::get<double>(p[3]), std::get<double>(p[4]),
                           std::get<double>(p[5])});
                }
              } catch (std::exception& e) {
                self->Send(json{"error", "OpenTick", e.what()});
                return;
              }
            }
            self->Send(out);
          });
    } else {
      Send(json{"error", action, "unknown action"});
    }
  } catch (nlohmann::detail::parse_error& e) {
    LOG_DEBUG(GetAddress() << ": invalid json string: " << msg);
    Send(json{"error", "", "invalid json string", "json", msg});
  } catch (nlohmann::detail::exception& e) {
    LOG_DEBUG(GetAddress() << ": json error: " << e.what() << ", " << msg);
    std::string error = "json error: ";
    error += e.what();
    Send(json{"error", "", error, "json", msg});
  } catch (std::exception& e) {
    LOG_DEBUG(GetAddress() << ": Connection::OnMessage: " << e.what() << ", "
                           << msg);
    Send(json{"error", "", e.what(), "Connection::OnMessage", msg});
  }
}

void Connection::Send(Confirmation::Ptr cm) {
  if (closed_) return;
  if (!user_) return;
  if (!user_->is_admin && !user_->GetSubAccount(cm->order->sub_account->id))
    return;
  auto self = shared_from_this();
  strand_.post([self, cm]() { self->Send(*cm.get(), false); });
}

void Connection::Send(const std::string& msg, const SubAccount* acc) {
  if (closed_) return;
  if (!user_) return;
  if (acc && !user_->GetSubAccount(acc->id)) return;
  auto self = shared_from_this();
  strand_.post([self, msg]() { self->Send(msg); });
}

void Connection::Send(const Algo& algo, const std::string& status,
                      const std::string& body, uint32_t seq) {
  if (closed_) return;
  if (!user_ || user_->id != algo.user().id) return;
  auto self = shared_from_this();
  strand_.post([self, &algo, status, body, seq]() {
    self->Send(algo.id(), GetTime(), algo.token(), algo.name(), status, body,
               seq, false);
  });
}

void Connection::Send(Algo::IdType id, time_t tm, const std::string& token,
                      const std::string& name, const std::string& status,
                      const std::string& body, uint32_t seq, bool offline) {
  Send(json{offline ? "Algo" : "algo", seq, id, tm, token, name, status, body});
}

static inline const char* GetSide(OrderSide c) {
  auto side = "";
  switch (c) {
    case kBuy:
      side = "buy";
      break;
    case kSell:
      side = "sell";
      break;
    case kShort:
      side = "short";
      break;
    default:
      break;
  }
  return side;
}

static inline const char* GetType(OrderType c) {
  auto type = "";
  switch (c) {
    case kLimit:
      type = "limit";
      break;
    case kMarket:
      type = "market";
      break;
    case kStop:
      type = "stop";
      break;
    case kStopLimit:
      type = "stop_limit";
      break;
    case kOTC:
      type = "otc";
      break;
    case kCX:
      type = "cx";
      break;
    default:
      break;
  }
  return type;
}

static inline const char* GetTif(TimeInForce c) {
  auto tif = "";
  switch (c) {
    case kDay:
      tif = "Day";
      break;
    case kImmediateOrCancel:
      tif = "IOC";
      break;
    case kGoodTillCancel:
      tif = "GTC";
      break;
    case kAtTheOpening:
      tif = "OPG";
      break;
    case kFillOrKill:
      tif = "FOK";
      break;
    case kGoodTillCrossing:
      tif = "GTX";
      break;
    default:
      break;
  }
  return tif;
}

void Connection::Send(const Confirmation& cm, bool offline) {
  assert(cm.order);
  auto cmd = offline ? "Order" : "order";
  json j = {
      cmd,
      cm.order->id,
      cm.transaction_time / 1000000,
      cm.seq,
  };
  const char* status = nullptr;
  switch (cm.exec_type) {
    case kUnconfirmedNew:
      status = "unconfirmed";
      j.push_back(status);
      j.push_back(cm.order->sec->id);
      j.push_back(cm.order->algo_id);
      j.push_back(cm.order->user->id);
      j.push_back(cm.order->sub_account->id);
      j.push_back(cm.order->broker_account->id);
      j.push_back(cm.order->qty);
      j.push_back(cm.order->price);
      j.push_back(GetSide(cm.order->side));
      j.push_back(GetType(cm.order->type));
      j.push_back(GetTif(cm.order->tif));
      break;

    case kPendingNew:
      status = "pending";
    case kPendingCancel:
      if (!status) status = "pending_cancel";
    case kNew:
      if (!status) status = "new";
    case kSuspended:
      if (!status) status = "suspended";
    case kCanceled:
      if (!status) status = "cancelled";
      j.push_back(status);
      if (cm.exec_type == kNew) {
        j.push_back(cm.order_id);
      }
      if (!cm.text.empty()) {
        j.push_back(cm.text);
      }
      break;

    case kFilled:
      status = "filled";
    case kPartiallyFilled:
      if (!status) status = "partial";
      j.push_back(status);
      j.push_back(cm.last_shares);
      j.push_back(cm.last_px);
      j.push_back(cm.exec_id);
      if (cm.exec_trans_type == kTransNew)
        j.push_back("new");
      else if (cm.exec_trans_type == kTransCancel)
        j.push_back("cancel");
      else
        return;
      break;

    case kRejected:
      status = "new_rejected";
    case kCancelRejected:
      if (!status) status = "cancel_rejected";
    case kRiskRejected:
      if (!status) status = "risk_rejected";
      j.push_back(status);
      j.push_back(cm.text);
      if (cm.exec_type == kRiskRejected) {
        j.push_back(cm.order->sec->id);
        j.push_back(cm.order->algo_id);
        j.push_back(cm.order->user->id);
        j.push_back(cm.order->sub_account->id);
        j.push_back(cm.order->qty);
        j.push_back(cm.order->price);
        j.push_back(GetSide(cm.order->side));
        j.push_back(GetType(cm.order->type));
        j.push_back(GetTif(cm.order->tif));
        if (cm.order->orig_id) {
          j.push_back(cm.order->orig_id);
        }
      }
      break;

    default:
      return;
      break;
  }
  Send(j);
}

void Connection::OnPosition(const json& j, const std::string& msg) {
  auto sec = GetSecurity(j[1]);
  auto acc_name = Get<std::string>(j[2]);
  auto acc = AccountManager::Instance().GetSubAccount(acc_name);
  if (!acc) {
    json j = {"error", "position", "invalid account name: " + acc_name};
    LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
    Send(j);
    return;
  }

  const Position* p;
  bool broker = j.size() > 3 && Get<bool>(j[3]);
  if (broker) {
    auto broker_acc = acc->GetBrokerAccount(sec->exchange->id);
    if (!broker_acc) {
      json j = {"error", "position",
                "can not find broker for this account and security pair"};
      LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
      Send(j);
      return;
    }
    p = &PositionManager::Instance().Get(*broker_acc, *sec);
  } else {
    p = &PositionManager::Instance().Get(*acc, *sec);
  }
  json out = {
      "position",
      {{"qty", p->qty},
       {"avg_px", p->avg_px},
       {"unrealized_pnl", p->unrealized_pnl},
       {"realized_pnl", p->realized_pnl},
       {"total_bought_qty", p->total_bought_qty},
       {"total_sold_qty", p->total_sold_qty},
       {"total_outstanding_buy_qty", p->total_outstanding_buy_qty},
       {"total_outstanding_sell_qty", p->total_outstanding_sell_qty},
       {"total_outstanding_sell_qty", p->total_outstanding_sell_qty}},
  };
  Send(out);
}

auto LoadTargets(const json& j) {
  auto targets = std::make_shared<PositionManager::Targets>();
  for (auto it = j.begin(); it != j.end(); ++it) {
    targets->emplace(Get<int64_t>((*it)[0]), GetNum((*it)[1]));
  }
  return targets;
}

void Connection::OnTarget(const json& j, const std::string& msg) {
  auto sub_account = Get<std::string>(j[1]);
  auto acc = AccountManager::Instance().GetSubAccount(sub_account);
  if (!acc) {
    json j = {"error", "target", "invalid sub_account: " + sub_account};
    LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
    Send(j);
    return;
  }
  auto& inst = PositionManager::Instance();
  if (j.size() <= 2) {
    auto targets = inst.GetTargets(*acc);
    json out;
    if (targets) {
      for (auto& pair : *targets) {
        out.push_back(json{pair.first, pair.second});
      }
    }
    Send(json{"target", acc->id, acc->name, out});
    return;
  }
  auto j2 = j[2];
  std::ofstream os(
      (kStorePath / ("target-" + std::to_string(acc->id) + ".json")).string());
  inst.SetTargets(*acc, LoadTargets(j2));
  os << j2;
  Send(json{"target", "done"});
  Server::Publish(json{"target", acc->id, acc->name, j[2]}.dump(), acc);
}

void Connection::OnAlgo(const json& j, const std::string& msg) {
  auto action = Get<std::string>(j[1]);
  if (action == "cancel") {
    if (j[2].is_string()) {
      AlgoManager::Instance().Stop(Get<std::string>(j[2]));
      return;
    }
    AlgoManager::Instance().Stop(Get<int64_t>(j[2]));
  } else if (action == "cancel_all") {
    auto sec = GetSecurity(j[2]);
    auto acc_name = Get<std::string>(j[3]);
    auto acc = AccountManager::Instance().GetSubAccount(acc_name);
    if (!acc) {
      Send(json{"error", "algo", "unknown account: " + acc_name});
      return;
    }
    if (!user_->GetSubAccount(acc->id)) {
      Send(json{"error", "algo", "no permission of account: " + acc_name});
      return;
    }
    AlgoManager::Instance().Stop(sec->id, acc->id);
  } else if (action == "modify") {
    auto params = ParseParams(j[3]);
    if (j[2].is_string()) {
      AlgoManager::Instance().Modify(Get<std::string>(j[2]), params);
      return;
    }
    AlgoManager::Instance().Modify(Get<int64_t>(j[2]), params);
  } else if (action == "new" || action == "test") {
    auto algo_name = Get<std::string>(j[2]);
    auto token = Get<std::string>(j[3]);
    auto algo = AlgoManager::Instance().Get(token);
    if (algo) {
      json j = {
          "error",
          "algo",
          "duplicate token: " + token,
      };
      LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
      Send(j);
      return;
    }
    try {
      Algo::ParamMapPtr params;
      if (action == "new") {
        params = ParseParams(j[4]);
        for (auto& pair : *params) {
          if (auto pval = std::get_if<SecurityTuple>(&pair.second)) {
            auto acc = pval->acc;
            if (!user_->GetSubAccount(acc->id)) {
              throw std::runtime_error("No permission to trade with account: " +
                                       std::string(acc->name));
            }
            // in case receive [exch, symbol] sec, convert to sec_id before
            // publish to gui
            const_cast<json&>(j)[4]["Security"]["sec"] = pval->sec->id;
          }
        }
      } else if (token.size()) {
        test_algo_tokens_.insert(token);
      }
      std::stringstream ss;
      ss << j[4];
      if (!AlgoManager::Instance().Spawn(params, algo_name, *user_, ss.str(),
                                         token) &&
          params) {
        throw std::runtime_error("Unknown algo name: " + algo_name);
      }
      Send(json{"algo", "done"});
    } catch (const std::exception& err) {
      LOG_DEBUG(GetAddress() << ": " << err.what() << '\n' << msg);
      Send(json{"error", "algo", "invalid params", err.what()});
    }
  } else {
    Send(json{"error", "algo", "invalid action: " + action});
  }
}

void Connection::OnOrder(const json& j, const std::string& msg) {
  auto sub_account = Get<std::string>(j[2]);
  auto acc = AccountManager::Instance().GetSubAccount(sub_account);
  if (!acc) {
    json j = {"error", "order", "invalid sub_account: " + sub_account};
    LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
    Send(j);
    return;
  }
  auto side_str = Get<std::string>(j[3]);
  auto type_str = Get<std::string>(j[4]);
  auto tif_str = Get<std::string>(j[5]);
  auto qty = GetNum(j[6]);
  auto px = GetNum(j[7]);
  auto stop_price = GetNum(j[8]);
  Contract c;
  c.qty = qty;
  c.price = px;
  c.sec = GetSecurity(j[1]);
  c.stop_price = stop_price;
  c.sub_account = acc;
  if (!GetOrderSide(side_str, &c.side)) {
    json j = {
        "error",
        "order",
        "invalid side: " + side_str,
    };
    LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
    Send(j);
    return;
  }
  if (!strcasecmp(type_str.c_str(), "market"))
    c.type = kMarket;
  else if (!strcasecmp(type_str.c_str(), "stop"))
    c.type = kStop;
  else if (!strcasecmp(type_str.c_str(), "stop limit"))
    c.type = kStopLimit;
  else if (!strcasecmp(type_str.c_str(), "otc"))
    c.type = kOTC;
  if (c.stop_price <= 0 && (c.type == kStop || c.type == kStopLimit)) {
    json j = {
        "error",
        "order",
        "miss stop price for stop order",
    };
    LOG_DEBUG(GetAddress() << ": " << j << '\n' << msg);
    Send(j);
    return;
  }
  if (!strcasecmp(tif_str.c_str(), "GTC"))
    c.tif = kGoodTillCancel;
  else if (!strcasecmp(tif_str.c_str(), "OPG"))
    c.tif = kAtTheOpening;
  else if (!strcasecmp(tif_str.c_str(), "IOC"))
    c.tif = kImmediateOrCancel;
  else if (!strcasecmp(tif_str.c_str(), "FOK"))
    c.tif = kFillOrKill;
  else if (!strcasecmp(tif_str.c_str(), "GTX"))
    c.tif = kGoodTillCrossing;
  auto ord = new Order{};
  (Contract&)* ord = c;
  ord->user = user_;
  ExchangeConnectivityManager::Instance().Place(ord);
  Send(json{"order", "done"});
}

void Connection::OnSecurities(const json& j) {
  LOG_DEBUG(GetAddress() << ": Securities requested");
  const Exchange* exch = nullptr;
  if (j.size() > 1)
    exch = SecurityManager::Instance().GetExchange(Get<std::string>(j[1]));
  std::set<std::string_view> symbols;
  std::vector<std::string> symbols_c;  // to retain memory for std::string_view
  if (j.size() > 2) {
    auto n = j[2].size();
    symbols_c.resize(n);
    for (auto k = 0u; k < n; ++k) {
      symbols_c[k] = Get<std::string>(j[2][k]);
      symbols.insert(std::string_view(symbols_c[k]));
    }
  }
  auto& secs = SecurityManager::Instance().securities();
  json out = {"securities"};
  for (auto& pair : secs) {
    auto s = pair.second;
    if (exch && (s->exchange != exch)) continue;
    if (!symbols.empty() &&
        symbols.find(std::string_view(s->symbol)) == symbols.end())
      continue;
    if (user_->is_admin) {
      json j = {
          "security",
          s->id,
          s->symbol,
          s->exchange->name,
          s->type,
          s->lot_size,
          s->multiplier,
          s->rate,
          s->close_price,
          s->currency,
          s->local_symbol,
          s->adv20,
          s->market_cap,
          std::to_string(s->sector),
          std::to_string(s->industry_group),
          std::to_string(s->industry),
          std::to_string(s->sub_industry),
          s->bbgid,
          s->cusip,
          s->sedol,
          s->isin,
      };
      if (transport_->stateless) {
        out.push_back(j);
      } else {
        Send(j);
      }
    } else {
      json j = {
          "security", s->id,       s->symbol,     s->exchange->name,
          s->type,    s->lot_size, s->multiplier, s->rate,
      };
      if (transport_->stateless) {
        out.push_back(j);
      } else {
        Send(j);
      }
    }
  }
  if (transport_->stateless) {
    Send(out);
    return;
  }
  out = {"securities", "complete"};
  Send(out);
}

bool Connection::Disable(const json& j, AccountBase* acc) {
  if (!acc) {
    Send(json{"error", "", "Unknown account id"});
    return false;
  }
  auto old = acc->disabled_reason.load(boost::memory_order_relaxed);
  if (j.size() == 4) {  // enable
    acc->disabled_reason.store(boost::shared_ptr<std::string>{},
                               boost::memory_order_release);
    return !!old;
  }
  acc->disabled_reason.store(
      boost::shared_ptr<std::string>(new std::string(Get<std::string>(j[4]))),
      boost::memory_order_release);
  return !old;
}

std::string Connection::GetDisabledSubAccounts() {
  json out = {"disabled_sub_accounts"};
  for (auto& pair : AccountManager::Instance().sub_accounts_) {
    auto reason =
        pair.second->disabled_reason.load(boost::memory_order_relaxed);
    if (reason) {
      out.push_back(json{pair.second->id, *reason});
    }
  }
  return out.dump();
}

void Connection::OnLogin(const std::string& action, const json& j) {
  auto name = Get<std::string>(j[1]);
  auto password = sha1(Get<std::string>(j[2]));
  auto user = AccountManager::Instance().GetUser(name);
  std::string state;
  if (!user)
    state = "unknown user";
  else if (password != user->password)
    state = "wrong password";
  else if (user->is_disabled)
    state = "disabled";
  else
    state = "ok";
  if (action == "validate_user") {
    auto token = Get<int64_t>(j[3]);
    Send(json{"user_validation", state == "ok" ? user->id : 0, token});
    return;
  }
  if (state != "ok") {
    Send(json{"connection", state});
    return;
  }
  auto token = boost::uuids::to_string(kUuidGen());
  kTokens[token] = user;
  json out = {
      "connection",
      state,
      {{"session", PositionManager::Instance().session()},
       {"userId", user->id},
       {"startTime", kStartTime},
       {"sessionToken", token},
       {"isAdmin", user->is_admin},
       {"securitiesCheckSum", SecurityManager::Instance().check_sum()}},
  };
  Send(out);
  if (!user_ && !transport_->stateless) {
    user_ = user;
    PublishMarketdata();
    if (user->is_admin) {
      for (auto& pair : AccountManager::Instance().users_) {
        auto tmp = pair.second->sub_accounts();
        for (auto& pair2 : *tmp) {
          Send(json{"user_sub_account", pair.first, pair2.first,
                    pair2.second->name});
        }
      }
      for (auto& pair : AccountManager::Instance().sub_accounts_) {
        Send(json{"sub_account", pair.first, pair.second->name});
      }
    } else {
      auto accs = user->sub_accounts();
      for (auto& pair : *accs) {
        Send(json{"sub_account", pair.first, pair.second->name});
      }
    }
    for (auto& pair : AccountManager::Instance().broker_accounts_) {
      Send(json{"broker_account", pair.first, pair.second->name});
    }
    for (auto& pair : MarketDataManager::Instance().srcs()) {
      if (pair.first == kConsolidationSrc) continue;
      Send(json{"src", DataSrc::GetStr(pair.first)});
    }
    Send(GetDisabledSubAccounts());
    for (auto& pair : AlgoManager::Instance().adapters()) {
      if (pair.first.at(0) == '_') continue;
      if (dynamic_cast<IndicatorHandler*>(pair.second)) continue;
      auto& params = pair.second->GetParamDefs();
      json j = {
          "algo_def",
          pair.first,
      };
      for (auto& p : params) {
        json j2 = {
            p.name,
        };
        Jsonify(p.default_value, &j2);
        j2.push_back(p.required);
        j2.push_back(p.min_value);
        j2.push_back(p.max_value);
        j2.push_back(p.precision);
        j.push_back(j2);
      }
      Send(j);
    }
    json files;
    if (fs::is_directory(kAlgoPath)) {
      for (auto& entry :
           boost::make_iterator_range(fs::directory_iterator(kAlgoPath), {})) {
        auto path = entry.path();
        auto fn = path.filename().string();
        if (fn[0] == '_' || fn[0] == '.') continue;
        if (path.extension() == ".pyc") continue;
        if (path.extension() == ".so") continue;
        files.push_back(fn);
      }
    }
    if (files.size() > 0) Send(json{"algoFiles", files});
  }
}

void Connection::SendTestMsg(const std::string& token, const std::string& msg,
                             bool stopped) {
  if (closed_) return;
  if (test_algo_tokens_.find(token) == test_algo_tokens_.end()) return;
  auto self = shared_from_this();
  strand_.post([self, msg, stopped, token]() {
    self->Send(json{"test_msg", msg});
    if (stopped) {
      self->Send(json{"test_done", token});
    }
  });
}

void Connection::OnAdmin(const json& j) {
  auto name = Get<std::string>(j[1]);
  auto action = Get<std::string>(j[2]);
  if (!user_->is_admin && !(name == "sub accounts" && action == "disable")) {
    throw std::runtime_error("admin required");
  }
  if (!strcasecmp(name.c_str(), "users")) {
    OnAdminUsers(j, name, action);
  } else if (!strcasecmp(name.c_str(), "broker accounts")) {
    OnAdminBrokerAccounts(j, name, action);
  } else if (!strcasecmp(name.c_str(), "sub accounts")) {
    OnAdminSubAccounts(j, name, action);
  } else if (!strcasecmp(name.c_str(), "exchanges")) {
    OnAdminExchanges(j, name, action);
  } else if (!strcasecmp(name.c_str(), "securities")) {
    if (action == "reload") {
      SecurityManager::Instance().LoadFromDatabase();
      Server::Trigger(json{"securities"}.dump());
    }
  } else if (!strcasecmp(name.c_str(), "sub accounts of user")) {
    auto& inst = AccountManager::Instance();
    if (action == "ls") {
      json out;
      json users;
      for (auto& pair : inst.users_) {
        users.push_back(pair.second->name);
        for (auto& pair2 : *pair.second->sub_accounts()) {
          json tmp = {pair.second->name, pair2.second->name};
          out.push_back(tmp);
        }
      }
      json subs;
      for (auto& pair : inst.sub_accounts_) {
        subs.push_back(pair.second->name);
      }
      Send(json{"admin", name, action, {out, users, subs}});
      return;
    }
    auto values = j[3];
    std::string user_name;
    std::string sub_name;
    for (auto i = 0u; i < values.size(); ++i) {
      auto v = values[i];
      auto name = Get<std::string>(v[0]);
      auto value = Get<std::string>(v[1]);
      if (name == "user") {
        user_name = value;
      } else if (name == "sub") {
        sub_name = value;
      }
    }
    auto user = const_cast<User*>(inst.GetUser(user_name));
    if (!user) {
      Send(
          json{"admin", name, action, "Unknown user name '" + user_name + "'"});
      return;
    }
    auto user_id = user->id;
    auto sub = inst.GetSubAccount(sub_name);
    if (!sub) {
      Send(json{"admin", name, action,
                "Unknown sub broker name '" + sub_name + "'"});
      return;
    }
    auto sub_id = sub->id;
    std::stringstream ss;
    if (action == "add") {
      ss << "insert into user_sub_account_map(user_id, sub_account_id) "
            "values("
         << user_id << ", " << sub_id << ")";
    } else if (action == "delete") {
      ss << "delete from user_sub_account_map where user_id=" << user_id
         << " and sub_account_id=" << sub_id;
    }
    auto str = ss.str();
    if (str.empty()) return;
    try {
      auto sql = Database::Session();
      *sql << str;
    } catch (const std::exception& e) {
      Send(json{"admin", name, action, e.what()});
      return;
    }
    auto accs = user->sub_accounts();
    if (action == "add") {
      auto tmp = boost::make_shared<User::SubAccountMap>(*accs);
      tmp->emplace(sub->id, sub);
      user->set_sub_accounts(tmp);
    } else if (action == "delete") {
      auto tmp = boost::make_shared<User::SubAccountMap>();
      for (auto& pair : *accs) {
        if (pair.first == sub_id) continue;
        tmp->emplace(pair.first, pair.second);
      }
      user->set_sub_accounts(tmp);
    }
    Send(json{"admin", name, action});
  } else if (!strcasecmp(name.c_str(), "broker accounts of sub account")) {
    auto& inst = AccountManager::Instance();
    if (action == "ls") {
      json out;
      json subs;
      for (auto& pair : inst.sub_accounts_) {
        subs.push_back(pair.second->name);
        for (auto& pair2 : *pair.second->broker_accounts()) {
          auto e = SecurityManager::Instance().GetExchange(pair2.first);
          assert(e);
          if (!e) continue;
          json tmp = {pair.second->name, e->name, pair2.second->name};
          out.push_back(tmp);
        }
      }
      json exchs;
      for (auto& pair : SecurityManager::Instance().exchanges_) {
        exchs.push_back(pair.second->name);
      }
      json brokers;
      for (auto& pair : inst.broker_accounts_) {
        brokers.push_back(pair.second->name);
      }
      Send(json{"admin", name, action, {out, subs, exchs, brokers}});
      return;
    }
    std::string sub_name;
    std::string exch_name;
    std::string broker_name;
    auto values = j[3];
    for (auto i = 0u; i < values.size(); ++i) {
      auto v = values[i];
      auto name = Get<std::string>(v[0]);
      auto value = Get<std::string>(v[1]);
      if (name == "exchange") {
        exch_name = value;
      } else if (name == "sub") {
        sub_name = value;
      } else if (name == "broker") {
        broker_name = value;
      }
    }
    auto sub = const_cast<SubAccount*>(inst.GetSubAccount(sub_name));
    if (!sub) {
      Send(json{"admin", name, action,
                "Unknown sub broker name '" + sub_name + "'"});
      return;
    }
    auto sub_id = sub->id;
    auto exch = SecurityManager::Instance().GetExchange(exch_name);
    if (!exch) {
      Send(json{"admin", name, action,
                "Unknown exchange name '" + exch_name + "'"});
      return;
    }
    auto exch_id = exch->id;
    auto broker = inst.GetBrokerAccount(broker_name);
    if (!broker) {
      Send(json{"admin", name, action,
                "Unknown broker account name '" + broker_name + "'"});
      return;
    }
    auto broker_id = broker->id;
    std::stringstream ss;
    if (action == "add") {
      ss << "insert into sub_account_broker_account_map(sub_account_id, "
            "exchange_id, broker_account_id) "
            "values("
         << sub_id << ", " << exch_id << ", " << broker_id << ")";
    } else if (action == "delete") {
      ss << "delete from sub_account_broker_account_map where sub_account_id="
         << sub_id << " and exchange_id=" << exch_id
         << " and broker_account_id=" << broker_id;
    }
    auto str = ss.str();
    if (str.empty()) return;
    try {
      auto sql = Database::Session();
      *sql << str;
    } catch (const std::exception& e) {
      Send(json{"admin", name, action, e.what()});
      return;
    }
    auto accs = sub->broker_accounts();
    if (action == "add") {
      auto tmp = boost::make_shared<SubAccount::BrokerAccountMap>(*accs);
      tmp->emplace(exch->id, broker);
      sub->set_broker_accounts(tmp);
    } else if (action == "delete") {
      auto tmp = boost::make_shared<SubAccount::BrokerAccountMap>();
      for (auto& pair : *accs) {
        if (pair.first == exch_id) continue;
        tmp->emplace(pair.first, pair.second);
      }
      sub->set_broker_accounts(tmp);
    }
    Send(json{"admin", name, action});
  }
}

char* StrDup(const std::string& str) {
  auto tmp = strdup(str.c_str());
  std::atomic_thread_fence(std::memory_order_release);
  return tmp;
}

template <typename T, bool is_acc = true>
static inline json UpdateAcc(
    const std::string& name, const std::string& action,
    const std::string& table_name, int64_t id, const json& j, T* acc,
    tbb::concurrent_unordered_map<std::string, T*>* acc_of_name,
    std::function<bool(const std::string& key, const json& v, std::string* err,
                       std::stringstream* ss)>
        func1 = {},
    std::function<bool(const std::string& key, const json& v, T* acc)> func2 =
        {}) {
  if (!acc) {
    return json{"admin", name, action, id, "Unknown " + table_name + " id"};
  }
  auto values = j[4];
  std::stringstream ss;
  ss << "update \"" + table_name + "\" set ";
  for (auto i = 0u; i < values.size(); ++i) {
    auto v = values[i];
    auto key = Get<std::string>(v[0]);
    std::string err;
    if (i) ss << ", ";
    ss << '"' << key << "\"=";
    if (!func1 || !func1(key, v[1], &err, &ss)) {
      if (key == "limits") {
        Limits l;
        err = l.FromString(Get<std::string>(v[1]));
      } else if (key == "name") {
        if (Get<std::string>(v[1]).empty()) err = "name can not be empty";
      }

      if (v[1].is_number()) {
        ss << GetNum(v[1]);
      } else if (v[1].is_boolean()) {
        ss << Get<bool>(v[1]);
      } else if (v[1].is_null()) {
        ss << "null";
      } else {
        auto str = Get<std::string>(v[1]);
        if (key == "password") str = sha1(str);
        ss << "'" << str << "'";
      }
    }
    if (err.size()) return json{"admin", name, action, id, err};
  }
  ss << " where id=" << id;
  try {
    auto sql = Database::Session();
    *sql << ss.str();
  } catch (const std::exception& e) {
    return json{"admin", name, action, id, e.what()};
  }
  for (auto i = 0u; i < values.size(); ++i) {
    auto v = values[i];
    auto key = Get<std::string>(v[0]);
    if (func2 && func2(key, v[1], acc)) continue;
    if constexpr (is_acc) {
      if (key == "is_disabled") {
        acc->is_disabled = v[1].is_null() ? false : Get<bool>(v[1]);
        continue;
      }
    }
    auto str = Get<std::string>(v[1]);
    if (key == "name") {
      acc->name = StrDup(str);
      std::atomic_thread_fence(std::memory_order_release);
      (*acc_of_name)[acc->name] = acc;
    } else if (key == "limits") {
      if constexpr (is_acc) {
        acc->limits.FromString(str);
      }
    }
  }
  return json{"admin", name, action, id};
}

template <typename T, bool is_acc = true>
static inline json AddAcc(
    const std::string& name, const std::string& action,
    const std::string& table_name, const json& j,
    tbb::concurrent_unordered_map<typename T::IdType, T*>* accs,
    tbb::concurrent_unordered_map<std::string, T*>* acc_of_name,
    std::function<bool(const std::string& key, const json& v, T* acc,
                       std::string* err)>
        func1 = {},
    std::function<void(T* acc)> func2 = {}) {
  auto values = j[3];
  auto acc = new T;
  std::stringstream ss;
  ss << "insert into \"" + table_name + "\"(";
  for (auto i = 0u; i < values.size(); ++i) {
    auto v = values[i];
    if (i) ss << ",";
    auto key = Get<std::string>(v[0]);
    ss << '"' << key << '"';
    std::string err;
    if (!func1 || !func1(key, v[1], acc, &err)) {
      if constexpr (is_acc) {
        if (key == "is_disabled") {
          acc->is_disabled = v[1].is_null() ? false : Get<bool>(v[1]);
          continue;
        }
      }
      auto str = Get<std::string>(v[1]);
      if (key == "name") {
        acc->name = StrDup(str);
        if (str.empty()) err = "name can not be empty";
      } else if (key == "limits") {
        if constexpr (is_acc) {
          err = acc->limits.FromString(str);
        }
      }
    }
    if (err.size()) return json{"admin", name, action, err};
  }
  if (Database::is_sqlite()) ss << ", id";
  ss << ") values(";
  for (auto i = 0u; i < values.size(); ++i) {
    auto v = values[i];
    if (i) ss << ",";
    if (v[1].is_number()) {
      ss << GetNum(v[1]);
    } else if (v[1].is_boolean()) {
      ss << Get<bool>(v[1]);
    } else {
      auto str = Get<std::string>(v[1]);
      if (Get<std::string>(v[0]) == "password") str = sha1(str);
      ss << "'" << str << "'";
    }
  }
  if (Database::is_sqlite()) {
    *Database::Session() << "select max(id) from " + table_name,
        soci::into(acc->id);
    acc->id += 1;
    ss << ", " << acc->id;
  }
  ss << ")";
  if (!Database::is_sqlite()) ss << " returning id";
  try {
    auto sql = Database::Session();
    if (Database::is_sqlite())
      *sql << ss.str();
    else
      *sql << ss.str(), soci::into(acc->id);
  } catch (const std::exception& e) {
    if (*acc->name) free(const_cast<char*>(acc->name));
    if (func2) func2(acc);
    delete acc;
    return json{"admin", name, action, e.what()};
  }
  std::atomic_thread_fence(std::memory_order_release);
  accs->emplace(acc->id, acc);
  (*acc_of_name)[acc->name] = acc;
  return json{"admin", name, action, acc->id};
}

void Connection::OnAdminUsers(const json& j, const std::string& name,
                              const std::string& action) {
  auto& inst = AccountManager::Instance();
  if (action == "ls") {
    json users;
    for (auto& pair : inst.users_) {
      auto user = pair.second;
      // 0 is the placeholder of password
      users.push_back(json{user->id, user->name, 0, user->is_disabled,
                           user->is_admin, user->limits.GetString()});
    }
    Send(json{"admin", name, action, users});
  } else if (action == "modify") {
    auto id = GetNum(j[3]);
    auto user = const_cast<User*>(inst.GetUser(id));
    Send(UpdateAcc<User>(
        name, action, "user", id, j, user, &inst.user_of_name_,
        [](const std::string& key, const json& v, std::string* err,
           std::stringstream* ss) -> bool {
          if (key == "password") {
            auto str = Get<std::string>(v);
            if (str.empty()) {
              *err = "password can not be empty";
            } else {
              (*ss) << "'" << StrDup(sha1(str)) << "'";
            }
            return true;
          }
          return false;
        },
        [](const std::string& key, const json& v, User* acc) -> bool {
          if (key == "is_admin") {
            acc->is_admin = v.is_null() ? false : Get<bool>(v);
            return true;
          } else if (key == "password") {
            acc->password = StrDup(sha1(Get<std::string>(v)));
            return true;
          }
          return false;
        }));
  } else if (action == "add") {
    Send(AddAcc<User>(name, action, "user", j, &inst.users_,
                      &inst.user_of_name_,
                      [](const std::string& key, const json& v, User* acc,
                         std::string* err) -> bool {
                        if (key == "is_admin") {
                          acc->is_admin = v.is_null() ? false : Get<bool>(v);
                          return true;
                        } else if (key == "password") {
                          auto str = Get<std::string>(v);
                          acc->password = StrDup(sha1(str));
                          if (str.empty()) *err = "password can not be empty";
                          return true;
                        }
                        return false;
                      },
                      [](User* acc) {
                        if (*acc->password)
                          free(const_cast<char*>(acc->password));
                      }));
  } else if (action == "disable") {
    Disable(j, const_cast<User*>(inst.GetUser(GetNum(j[3]))));
  }
}

void Connection::OnAdminBrokerAccounts(const json& j, const std::string& name,
                                       const std::string& action) {
  auto& inst = AccountManager::Instance();
  if (action == "ls") {
    json accs;
    for (auto& pair : inst.broker_accounts_) {
      auto acc = pair.second;
      accs.push_back(json{acc->id, acc->name, acc->adapter_name,
                          acc->is_disabled, acc->limits.GetString(),
                          acc->GetParamsString()});
    }
    Send(json{"admin", name, action, accs});
  } else if (action == "modify") {
    auto id = GetNum(j[3]);
    auto broker = const_cast<BrokerAccount*>(inst.GetBrokerAccount(id));
    Send(UpdateAcc<BrokerAccount>(
        name, action, "broker_account", id, j, broker,
        &inst.broker_account_of_name_,
        [](const std::string& key, const json& v, std::string* err,
           std::stringstream* ss) -> bool {
          if (key != "params" && key != "adapter") return false;
          auto str = v.is_null() ? "" : Get<std::string>(v);
          if (key == "params") {
            BrokerAccount b;
            *err = b.set_params(str);
          } else if (key == "adapter") {
            auto adapter =
                ExchangeConnectivityManager::Instance().GetAdapter(str);
            if (!adapter) *err = "Unknown adapter name";
          }
          (*ss) << "'" << str << "'";
          return true;
        },
        [](const std::string& key, const json& v, BrokerAccount* acc) -> bool {
          if (key != "params" && key != "adapter") return false;
          auto str = v.is_null() ? "" : Get<std::string>(v);
          if (key == "params") {
            acc->set_params(str);
          } else {
            acc->adapter_name = StrDup(str);
            acc->adapter =
                ExchangeConnectivityManager::Instance().GetAdapter(str);
          }
          return true;
        }));
  } else if (action == "add") {
    Send(AddAcc<BrokerAccount>(
        name, action, "broker_account", j, &inst.broker_accounts_,
        &inst.broker_account_of_name_,
        [](const std::string& key, const json& v, BrokerAccount* acc,
           std::string* err) -> bool {
          if (key != "params" && key != "adapter") return false;
          auto str = v.is_null() ? "" : Get<std::string>(v);
          if (key == "params") {
            *err = acc->set_params(str);
          } else {
            auto adapter =
                ExchangeConnectivityManager::Instance().GetAdapter(str);
            if (!adapter) {
              *err = "Unknown adapter name";
            } else {
              acc->adapter = adapter;
              acc->adapter_name = StrDup(str);
            }
          }
          return true;
        },
        [](BrokerAccount* acc) {
          if (*acc->adapter_name) free(const_cast<char*>(acc->adapter_name));
        }));
  } else if (action == "disable") {
    Disable(j, const_cast<BrokerAccount*>(inst.GetBrokerAccount(GetNum(j[3]))));
  }
}

void Connection::OnAdminSubAccounts(const json& j, const std::string& name,
                                    const std::string& action) {
  auto& inst = AccountManager::Instance();
  if (action == "ls") {
    json accs;
    for (auto& pair : inst.sub_accounts_) {
      auto acc = pair.second;
      accs.push_back(
          json{acc->id, acc->name, acc->is_disabled, acc->limits.GetString()});
    }
    Send(json{"admin", name, action, accs});
  } else if (action == "modify") {
    auto id = GetNum(j[3]);
    auto sub = const_cast<SubAccount*>(inst.GetSubAccount(id));
    Send(UpdateAcc<SubAccount>(name, action, "sub_account", id, j, sub,
                               &inst.sub_account_of_name_));
  } else if (action == "add") {
    Send(AddAcc<SubAccount>(name, action, "broker_account", j,
                            &inst.sub_accounts_, &inst.sub_account_of_name_));
  } else if (action == "disable") {
    auto id = GetNum(j[3]);
    if (user_->is_admin || user_->GetSubAccount(id)) {
      if (Disable(j, const_cast<SubAccount*>(inst.GetSubAccount(id)))) {
        Server::Publish(GetDisabledSubAccounts());
      }
    } else {
      throw std::runtime_error("permission required");
    }
  }
}

void Connection::OnAdminExchanges(const json& j, const std::string& name,
                                  const std::string& action) {
  auto& inst = SecurityManager::Instance();
  if (action == "ls") {
    json exchs;
    for (auto& pair : inst.exchanges_) {
      auto exch = pair.second;
      exchs.push_back(json{
          exch->id,
          exch->name,
          exch->mic,
          exch->country,
          exch->ib_name,
          exch->bb_name,
          exch->tz,
          exch->odd_lot_allowed,
          exch->GetTickSizeTableString(),
          exch->GetTradePeriodString(),
          exch->GetBreakPeriodString(),
          exch->GetHalfDayString(),
          exch->GetHalfDaysString(),
          exch->GetParamsString(),
      });
    }
    Send(json{"admin", name, action, exchs});
  } else if (action == "modify") {
    auto id = GetNum(j[3]);
    auto exch = const_cast<Exchange*>(inst.GetExchange(id));
    Send(UpdateAcc<Exchange, false>(
        name, action, "exchange", id, j, exch, &inst.exchange_of_name_,
        [](const std::string& key, const json& v, std::string* err,
           std::stringstream* ss) -> bool {
          if (key == "odd_lot_allowed") return false;
          auto str = v.is_null() ? "" : Get<std::string>(v);
          Exchange e;
          if (key == "tick_size_table") {
            *err = e.ParseTickSizeTable(str);
          } else if (key == "trade_period") {
            *err = e.ParseTradePeriod(str);
          } else if (key == "break_period") {
            *err = e.ParseBreakPeriod(str);
          } else if (key == "half_day") {
            *err = e.ParseHalfDay(str);
          } else if (key == "half_days") {
            *err = e.ParseHalfDays(str);
          } else if (key == "params") {
            *err = e.set_params(str);
          } else {
            return false;
          }
          (*ss) << "'" << str << "'";
          return true;
        },
        [](const std::string& key, const json& v, Exchange* exch) -> bool {
          if (key == "odd_lot_allowed") {
            exch->odd_lot_allowed = v.is_null() ? false : Get<bool>(v);
            return true;
          }
          auto str = v.is_null() ? "" : Get<std::string>(v);
          if (key == "tick_size_table") {
            exch->ParseTickSizeTable(str);
          } else if (key == "trade_period") {
            exch->ParseTradePeriod(str);
          } else if (key == "break_period") {
            exch->ParseBreakPeriod(str);
          } else if (key == "half_day") {
            exch->ParseHalfDay(str);
          } else if (key == "half_days") {
            exch->ParseHalfDays(str);
          } else if (key == "mic") {
            exch->mic = StrDup(str);
          } else if (key == "country") {
            exch->country = StrDup(str);
          } else if (key == "ib_name") {
            exch->ib_name = StrDup(str);
          } else if (key == "bb_name") {
            exch->bb_name = StrDup(str);
          } else if (key == "params") {
            exch->set_params(str);
          } else if (key == "tz") {
            exch->tz = StrDup(str);
            if (*exch->tz) exch->utc_time_offset = GetUtcTimeOffset(exch->tz);
          } else {
            return false;
          }
          return true;
        }));
  } else if (action == "add") {
    Send(AddAcc<Exchange, false>(
        name, action, "exchange", j, &inst.exchanges_, &inst.exchange_of_name_,
        [](const std::string& key, const json& v, Exchange* exch,
           std::string* err) -> bool {
          if (key == "odd_lot_allowed") {
            exch->odd_lot_allowed = v.is_null() ? false : Get<bool>(v);
            return true;
          }
          auto str = v.is_null() ? "" : Get<std::string>(v);
          if (key == "tick_size_table") {
            *err = exch->ParseTickSizeTable(str);
          } else if (key == "trade_period") {
            *err = exch->ParseTradePeriod(str);
          } else if (key == "break_period") {
            *err = exch->ParseBreakPeriod(str);
          } else if (key == "half_day") {
            *err = exch->ParseHalfDay(str);
          } else if (key == "half_days") {
            *err = exch->ParseHalfDays(str);
          } else if (key == "mic") {
            exch->mic = StrDup(str);
          } else if (key == "country") {
            exch->country = StrDup(str);
          } else if (key == "ib_name") {
            exch->ib_name = StrDup(str);
          } else if (key == "bb_name") {
            exch->bb_name = StrDup(str);
          } else if (key == "tz") {
            exch->tz = StrDup(str);
            if (*exch->tz) exch->utc_time_offset = GetUtcTimeOffset(exch->tz);
          } else if (key == "params") {
            exch->set_params(str);
          } else {
            return false;
          }
          return true;
        },
        [](Exchange* exch) {
          if (*exch->mic) free(const_cast<char*>(exch->mic));
          if (*exch->country) free(const_cast<char*>(exch->country));
          if (*exch->ib_name) free(const_cast<char*>(exch->ib_name));
          if (*exch->bb_name) free(const_cast<char*>(exch->bb_name));
          if (*exch->tz) free(const_cast<char*>(exch->tz));
        }));
  }
}

}  // namespace opentrade
