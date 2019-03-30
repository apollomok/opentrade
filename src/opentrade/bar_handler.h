#ifndef OPENTRADE_BAR_HANDLER_H_
#define OPENTRADE_BAR_HANDLER_H_

#include "indicator_handler.h"

namespace opentrade {

static const Indicator::IdType kBar = 0;

struct BarIndicator : public Indicator {
  MarketData::Trade current;
  MarketData::Trade last;
  bp::object GetPyObject() const override {
    bp::dict out;
    out["last"] = bp::ptr(&last);
    out["current"] = bp::ptr(&current);
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

class BarHandler : public IndicatorHandler, public TradeTickHook {
 public:
  BarHandler() {
    set_name("bar");
    create_func_ = []() { return new BarHandler; };
    tm0_ = GetStartOfDayTime() * kMicroInSec;
    StartNext();
  }
  Indicator::IdType id() const override { return kBar; }

  bool Subscribe(Indicator::IdType id, Instrument* inst,
                 bool listen) noexcept override {
    auto bar = const_cast<BarIndicator*>(inst->Get<BarIndicator>(id));
    if (bar) {
      if (listen) bar->AddListener(inst);
      return true;
    }
    inst->HookTradeTick(this);
    bar = new BarIndicator{};
    bars_.push_back(bar);
    const_cast<MarketData&>(inst->md()).Set(bar, id);
    if (listen) bar->AddListener(inst);
    return true;
  }

  void OnTrade(Security::IdType id, const MarketData* md, time_t tm, double px,
               double qty) noexcept override {
    auto bar = const_cast<BarIndicator*>(md->Get<BarIndicator>(kBar));
    if (!bar) return;
    bar->Update(px, qty);
  }

  void StartNext() {
    SetTimeout([this]() { OnTimer(); },
               (kMicroInMin - (NowInMicro() - tm0_) % kMicroInMin) /
                   static_cast<double>(kMicroInSec));
  }

  void OnTimer() {
    for (auto bar : bars_) {
      bar->Roll();
      bar->Publish(kBar);
    }
    StartNext();
  }

 private:
  uint64_t tm0_ = 0;
  std::vector<BarIndicator*> bars_;
};

}  // namespace opentrade

#endif  // OPENTRADE_BAR_HANDLER_H_
