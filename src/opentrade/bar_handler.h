#ifndef OPENTRADE_BAR_HANDLER_H_
#define OPENTRADE_BAR_HANDLER_H_

#include "indicator_handler.h"

namespace opentrade {

static const Indicator::IdType kBar = 0;

template <int interval = 1>
struct BarIndicator : public Indicator {
  MarketData::Trade current;
  MarketData::Trade last;
  bp::object GetPyObject() const override {
    bp::dict out;
    out["last"] = bp::ptr(&last);
    out["current"] = bp::ptr(&current);
    out["interval"] = interval;
    return out;
  }

  void Update(double px, double qty) {
    Lock lock(m_);
    current.Update(px, qty);
  }

  void Roll() {
    last = current;
    {
      Lock lock(m_);
      bzero(&current, sizeof(current));
    }
  }
};

template <int interval = 1, Indicator::IdType ind_id = kBar>
class BarHandler : public IndicatorHandler, public TradeTickHook {
 public:
  typedef BarIndicator<interval> Ind;
  explicit BarHandler(const char* name = "bar") {
    set_name(name);
    create_func_ = []() { return new BarHandler; };
    tm0_ = GetStartOfDayTime() * kMicroInSec;
  }

  void OnStart() noexcept override { StartNext(); }

  Indicator::IdType id() const override { return ind_id; }

  bool Subscribe(Instrument* inst, bool listen) noexcept override {
    auto bar = const_cast<Ind*>(inst->Get<Ind>(ind_id));
    if (bar) {
      if (listen) bar->AddListener(inst);
      return true;
    }
    inst->HookTradeTick(this);
    bar = new Ind{};
    bars_.push_back(bar);
    const_cast<MarketData&>(inst->md()).Set(bar, ind_id);
    if (listen) bar->AddListener(inst);
    return true;
  }

  void OnTrade(Security::IdType id, const MarketData* md, time_t tm, double px,
               double qty) noexcept override {
    auto bar = const_cast<Ind*>(md->Get<Ind>(ind_id));
    if (!bar) return;
    bar->Update(px, qty);
  }

  void StartNext() {
    auto n = kMicroInMin * interval;
    SetTimeout([this]() { OnTimer(); }, (n - (NowInMicro() - tm0_) % n) /
                                            static_cast<double>(kMicroInSec));
  }

  void OnTimer() {
    for (auto bar : bars_) {
      bar->Roll();
      bar->Publish(ind_id);
    }
    StartNext();
  }

 private:
  uint64_t tm0_ = 0;
  std::vector<Ind*> bars_;
};

}  // namespace opentrade

#endif  // OPENTRADE_BAR_HANDLER_H_
