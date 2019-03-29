#ifndef OPENTRADE_BAR_HANDLER_H_
#define OPENTRADE_BAR_HANDLER_H_

#include "indicator_handler.h"

namespace opentrade {

static const Indicator::IdType kBar = 0;

struct Bar : public Indicator {};

class BarHandler : public IndicatorHandler, public TradeTickHook {
 public:
  BarHandler() {
    set_name("__Bar__");
    create_func_ = []() { return new BarHandler; };
  }
  Indicator::IdType id() const override { return kBar; }

  bool Subscribe(Indicator::IdType id, Instrument* inst,
                 bool listen) noexcept override {
    // to-do listen
    auto ind = inst->Get<Bar>(id);
    if (ind) return true;
    inst->HookTradeTick(this);
    const_cast<MarketData&>(inst->md()).Set(new Bar{}, id);
    return true;
  }

  void OnTrade(Security::IdType id, MarketData* md, time_t tm, double px,
               double qty) noexcept override {
    std::cout << tm << ' ' << px << ' ' << qty << std::endl;
  }

  void OnStart() noexcept override {}

 private:
};

}  // namespace opentrade

#endif  // OPENTRADE_BAR_HANDLER_H_
