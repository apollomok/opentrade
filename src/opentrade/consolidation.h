#ifndef OPENTRADE_CONSOLIDATION_H_
#define OPENTRADE_CONSOLIDATION_H_

#include "indicator_handler.h"
#include "market_data.h"

namespace opentrade {

static const Indicator::IdType kConsolidation = 1;
static const char* kConsolidationSrc = "CONS";
static const char* kConsolidationBook = "ConsolidationBook";

struct ConsolidationBook : public Indicator {};

class ConsolidationFeed : public MarketDataAdapter {
 public:
  ConsolidationFeed() {
    set_name("consolidation");
    config_["src"] = kConsolidationSrc;
    create_func_ = []() { return new ConsolidationFeed; };
  }
  void Start() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
};

class ConsolidationHandler : public IndicatorHandler {
 public:
  typedef ConsolidationBook Ind;
  ConsolidationHandler() {
    set_name(kConsolidationBook);
    create_func_ = []() { return new ConsolidationHandler; };
  }
  Indicator::IdType id() const override { return kConsolidation; }
  void OnStart() noexcept override;
  bool Subscribe(Instrument* inst, bool listen) noexcept override;

 private:
};

}  // namespace opentrade

#endif  // OPENTRADE_CONSOLIDATION_H_
