#include "algo.h"

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <mutex>
#include <sstream>

#include "connection.h"
#include "exchange_connectivity.h"
#include "logger.h"
#include "python.h"
#include "server.h"

namespace fs = boost::filesystem;

namespace opentrade {

static auto kPath = kStorePath / "algos";
static thread_local std::string kError;

inline void AlgoRunner::operator()() {
  assert(std::this_thread::get_id() == tid_);

  for (;;) {
    decltype(dirties_)::value_type key;
    {
      LockGuard lock(mutex_);
      if (dirties_.empty()) return;
      auto it = dirties_.begin();
      key = *it;
      dirties_.erase(it);
    }
    auto md = MarketDataManager::Instance().Get(key.second, key.first);
    auto& pair = instruments_[key];
    auto& md0 = pair.first;
    bool trade_update = md0.trade != md.trade;
    bool quote_update = md0.quote() != md.quote();
    auto& insts = pair.second;
    auto it = insts.begin();
    while (it != insts.end()) {
      auto& algo = (*it)->algo();
      if (!algo.is_active()) {
        it = insts.erase(it);
        md_refs_[key]--;
        assert(md_refs_[key] == insts.size());
        assert(AlgoManager::Instance().md_refs_[key] > 0);
        AlgoManager::Instance().md_refs_[key]--;
        continue;
      }
      if (trade_update) algo.OnMarketTrade(**it, md, md0);
      if (quote_update) algo.OnMarketQuote(**it, md, md0);
      it++;
    }
    md0 = md;
  }
}

inline void AlgoManager::Register(Instrument* inst) {
  auto& runner = runners_[inst->algo().id() % threads_.size()];
  auto key = std::make_pair(inst->src(), inst->sec().id);
  auto& pair = runner.instruments_[key];
  if (pair.second.empty()) {
    pair.first = inst->md();
  }
  assert(std::find(pair.second.begin(), pair.second.end(), inst) ==
         pair.second.end());
  runner.md_refs_[key]++;
  md_refs_[key]++;
  pair.second.push_back(inst);
  assert(std::this_thread::get_id() == runner.tid_);
}

void AlgoManager::Modify(Algo* algo, Algo::ParamMapPtr params) {
  if (!algo || !params) return;
  strands_[algo->id_ % threads_.size()].post(
      [params, algo]() { algo->OnModify(*params.get()); });
}

Algo* AlgoManager::Spawn(Algo::ParamMapPtr params, const std::string& name,
                         const User& user, const std::string& params_raw,
                         const std::string& token) {
  Algo* algo = nullptr;
  if (params) {
    auto adapter = GetAdapter(name);
    if (!adapter) return nullptr;
    algo = static_cast<Algo*>(adapter->Clone());
  } else {
    algo = Python::LoadTest(name, token);
    if (!algo) return nullptr;
  }
  algo->id_ = ++algo_id_counter_;
  algo->user_ = &user;
  algo->token_ = token;
  algos_.emplace(algo->id_, algo);
  if (!token.empty()) algo_of_token_.emplace(token, algo);
  Persist(*algo, "new", params ? params_raw : "{\"test\":true}");
  strands_[algo->id_ % threads_.size()].post([params, algo]() {
    kError = params ? algo->OnStart(*params.get()) : algo->Test();
    if (!kError.empty()) {
      algo->Stop();
    }
    kError.clear();
  });
  return algo;
}

void AlgoManager::Initialize() {
  auto& self = Instance();
  self.of_.open(kPath.c_str(), std::ofstream::app);
  if (!self.of_.good()) {
    LOG_FATAL("Failed to write file: " << kPath.c_str() << ": "
                                       << strerror(errno));
  }
  self.LoadStore();
  self.algo_id_counter_ += 100;
  LOG_INFO("Algo id starts from " << self.algo_id_counter_);
  self.work_.reset(new boost::asio::io_service::work(self.io_service_));
  self.seq_counter_ += 100;
}

void AlgoManager::Update(DataSrc::IdType src, Security::IdType id) {
  auto key = std::make_pair(src, id);
  for (auto i = 0u; i < threads_.size(); ++i) {
    auto& runner = runners_[i];
    if (runner.md_refs_[key] > 0) {
      auto should_run = false;
      {
        AlgoRunner::LockGuard lock(runner.mutex_);
        should_run = runner.dirties_.empty();
        runner.dirties_.insert(key);
      }
      if (should_run) strands_[i].post([&runner]() { runner(); });
    }
  }
}

void AlgoManager::Run(int nthreads) {
#ifdef BACKTEST
  threads_.resize(1);
  strands_.resize(1);
  runners_ = new AlgoRunner[1]{};
  runners_[0].tid_ = std::this_thread::get_id();
#else
  nthreads = std::max(1, nthreads);
  runners_ = new AlgoRunner[nthreads]{};
  LOG_INFO("algo_threads=" << nthreads);
  threads_.reserve(nthreads);
  strands_.reserve(nthreads);
  for (auto i = 0; i < nthreads; ++i) {
    threads_.emplace_back([this]() { this->io_service_.run(); });
    strands_.emplace_back(io_service_);
    runners_[i].tid_ = threads_[i].get_id();
  }
#endif
}

void AlgoManager::Handle(Confirmation::Ptr cm) {
  assert(cm->order->inst);
  auto inst = const_cast<Instrument*>(cm->order->inst);
  static std::mutex kMutex;
  {
    std::lock_guard<std::mutex> lock(kMutex);
    switch (cm->exec_type) {
      case kPartiallyFilled:
      case kFilled:
        if (cm->exec_trans_type == kTransNew) {
          if (cm->order->IsBuy()) {
            inst->outstanding_buy_qty_ -= cm->last_shares;
            inst->bought_qty_ += cm->last_shares;
          } else {
            inst->outstanding_sell_qty_ -= cm->last_shares;
            inst->sold_qty_ += cm->last_shares;
          }
        } else if (cm->exec_trans_type == kTransCancel) {
          if (cm->order->IsBuy())
            inst->bought_qty_ -= cm->last_shares;
          else
            inst->sold_qty_ -= cm->last_shares;
        }
        break;
      case kCanceled:
      case kRejected:
      case kExpired:
      case kCalculated:
      case kDoneForDay:
        if (cm->order->IsBuy())
          inst->outstanding_buy_qty_ -= cm->leaves_qty;
        else
          inst->outstanding_sell_qty_ -= cm->leaves_qty;
        break;
      case kUnconfirmedNew:
      case kUnconfirmedCancel:
      case kPendingCancel:
      case kCancelRejected:
      case kPendingNew:
      case kNew:
      case kSuspended:
      case kRiskRejected:
        break;
      default:
        return;
    }
  }
  strands_[cm->order->algo_id % threads_.size()].post([cm, inst]() {
    assert(cm->order->algo_id == inst->algo().id_);
    switch (cm->exec_type) {
      case kPartiallyFilled:
      case kFilled:
        if (!cm->order->IsLive()) inst->active_orders_.erase(cm->order);
        inst->algo().OnConfirmation(*cm.get());
        break;
      case kCanceled:
      case kRejected:
      case kExpired:
      case kCalculated:
      case kDoneForDay:
        inst->active_orders_.erase(cm->order);
        inst->algo().OnConfirmation(*cm.get());
        break;
      case kUnconfirmedNew:
      case kUnconfirmedCancel:
      case kPendingCancel:
      case kCancelRejected:
      case kPendingNew:
      case kNew:
      case kSuspended:
      case kRiskRejected:
        inst->algo().OnConfirmation(*cm.get());
        break;
      default:
        break;
    }
  });
}

void AlgoManager::Stop() {
  for (auto& pair : algos_) {
    auto algo = pair.second;
    strands_[algo->id_ % threads_.size()].post([algo]() { algo->Stop(); });
  }
}

void AlgoManager::Stop(Algo::IdType id) {
  auto algo = FindInMap(algos_, id);
  if (algo)
    strands_[algo->id_ % threads_.size()].post([algo]() { algo->Stop(); });
}

void AlgoManager::Stop(const std::string& token) {
  auto algo = FindInMap(algo_of_token_, token);
  if (algo)
    strands_[algo->id_ % threads_.size()].post([algo]() { algo->Stop(); });
}

void AlgoManager::Persist(const Algo& algo, const std::string& status,
                          const std::string& body) {
#ifdef BACKTEST
  return;
#endif
  kWriteTaskPool.AddTask([this, &algo, status, body]() {
    std::stringstream ss;
    ss << GetTime() << ' ' << algo.name() << ' ' << status << ' ' << body;
    auto str = ss.str();
    auto seq = ++seq_counter_;
    Server::Publish(algo, status, body, seq);
    of_.write(reinterpret_cast<const char*>(&seq), sizeof(seq));
    uint32_t n = str.size();
    of_.write(reinterpret_cast<const char*>(&n), sizeof(n));
    auto uid = algo.user().id;
    of_.write(reinterpret_cast<const char*>(&uid), sizeof(uid));
    auto aid = algo.id();
    of_.write(reinterpret_cast<const char*>(&aid), sizeof(aid));
    of_ << ss.str() << '\0' << std::endl;
  });
}

void AlgoManager::LoadStore(uint32_t seq0, Connection* conn) {
  if (!fs::file_size(kPath)) return;
  boost::iostreams::mapped_file_source m(kPath.string());
  auto p = m.data();
  auto p_end = p + m.size();
  auto ln = 0;
  while (p + 8 < p_end) {
    ln++;
    auto seq = *reinterpret_cast<const uint32_t*>(p);
    if (!conn) seq_counter_ = seq;
    p += 4;
    auto n = *reinterpret_cast<const uint32_t*>(p);
    if (p + n + 10 + sizeof(User::IdType) > p_end) break;
    p += 4;
    auto user_id = *reinterpret_cast<const User::IdType*>(p);
    p += sizeof(User::IdType);
    auto id = *reinterpret_cast<const uint32_t*>(p);
    if (!conn && id > algo_id_counter_) algo_id_counter_ = id;
    p += 4;
    auto payload = p;
    p += n + 2;  // body + '\0' + '\n'
    if (!conn || seq <= seq0) continue;
    if (!conn->user_->is_admin && conn->user_->id != user_id) continue;
    int32_t tm;
    char name[n];
    char status[n];
    char body[n];
    *body = 0;
    if (sscanf(payload, "%d %s %s %[^\1]s", &tm, name, status, body) < 3) {
      LOG_ERROR("Failed to parse algo line #" << ln);
      continue;
    }
    conn->Send(id, tm, "", name, status, body, seq, true);
  }
  if (!conn && p != p_end) {
    LOG_FATAL("Corrupted algo file: " << kPath.c_str()
                                      << ", please fix it first");
  }
}

Algo::~Algo() {
  for (auto& inst : instruments_) delete inst;
}

Instrument* Algo::Subscribe(const Security& sec, DataSrc src) {
  auto adapter = MarketDataManager::Instance().Subscribe(sec, src);
  assert(adapter);
  auto inst = new Instrument(this, sec, DataSrc(adapter->src()));
  inst->md_ = &MarketDataManager::Instance().Get(sec, adapter->src());
  inst->id_ = ++Instrument::kIdCounter;
  instruments_.insert(inst);
  AlgoManager::Instance().Register(inst);
  return inst;
}

void Algo::Stop() {
  if (is_active_) {
    is_active_ = false;
    for (auto inst : instruments_) {
      for (auto ord : inst->active_orders_) {
        Cancel(*ord);
      }
    }
    AlgoManager::Instance().Persist(
        *this, kError.empty() ? "teminated" : "failed", kError);
    OnStop();
  }
}

void Algo::SetTimeout(std::function<void()> func, int milliseconds) {
  AlgoManager::Instance().SetTimeout(id_, func, milliseconds);
}

void AlgoManager::SetTimeout(Algo::IdType id, std::function<void()> func,
                             int milliseconds) {
  if (milliseconds < 0) milliseconds = 0;
#ifdef BACKTEST
  auto tm = kTime + milliseconds * 1000lu;
  kTimers.emplace(tm, func);
#else
  auto t = new boost::asio::deadline_timer(
      io_service_, boost::posix_time::milliseconds(milliseconds));
  t->async_wait(strands_[id % threads_.size()].wrap([func, t](auto) {
    func();
    delete t;
  }));
#endif
}

Order* Algo::Place(const Contract& contract, Instrument* inst) {
  assert(inst);
  if (!is_active_ || !inst) return nullptr;
  auto ord = new Order{};
  (Contract&)* ord = contract;
  ord->algo_id = id_;
  ord->user = user_;
  ord->inst = inst;
  ord->sec = &inst->sec();
  auto ok = ExchangeConnectivityManager::Instance().Place(ord);
  if (!ok) return nullptr;
  inst->active_orders_.insert(ord);
  if (ord->IsBuy())
    inst->outstanding_buy_qty_ += ord->qty;
  else
    inst->outstanding_sell_qty_ += ord->qty;
  return ord;
}

bool Algo::Cancel(const Order& ord) {
  return ExchangeConnectivityManager::Instance().Cancel(ord);
}

}  // namespace opentrade
