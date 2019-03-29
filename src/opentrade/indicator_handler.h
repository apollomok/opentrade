#ifndef OPENTRADE_INDICATOR_HANDLER_H_
#define OPENTRADE_INDICATOR_HANDLER_H_

#include "algo.h"
#include "logger.h"

namespace opentrade {

struct IndicatorHandler : public Algo {
  virtual Indicator::IdType id() const = 0;
  virtual bool Subscribe(Indicator::IdType id, Instrument* inst,
                         bool listen = false) noexcept = 0;
  virtual void OnStart() noexcept = 0;
};

class IndicatorHandlerManager : public Singleton<IndicatorHandlerManager> {
 public:
  IndicatorHandler* Get(Indicator::IdType id) { return FindInMap(ihs_, id); }
  bool Register(IndicatorHandler* h) {
    auto id = h->id();
    auto it = ihs_.find(id);
    if (it != ihs_.end()) {
      LOG_ERROR("Failed to register #"
                << id << " indicator of adapter " << h->name()
                << ", already registered by " << it->second->name());
      return false;
    }
    ihs_[id] = h;
    return true;
  }

 private:
  // Initialized and started at AlgoManager::Run.
  std::map<size_t, IndicatorHandler*> ihs_;
  friend class Backtest;
};

}  // namespace opentrade

#endif  // OPENTRADE_INDICATOR_HANDLER_H_
