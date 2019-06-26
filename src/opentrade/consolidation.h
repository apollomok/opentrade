#ifndef OPENTRADE_CONSOLIDATION_H_
#define OPENTRADE_CONSOLIDATION_H_

#include "indicator_handler.h"
#include "market_data.h"

namespace opentrade {

static const Indicator::IdType kConsolidation = 1;
static const DataSrc kConsolidationSrc("CONS");
static const char* kConsolidationBook = "ConsolidationBook";

struct ConsolidationBook : public Indicator {
  static const Indicator::IdType kId = kConsolidation;
};

class ConsolidationHandler : public IndicatorHandler {
 public:
  typedef ConsolidationBook Ind;
  ConsolidationHandler() {
    set_name(kConsolidationBook);
    create_func_ = []() { return new ConsolidationHandler; };
  }
  Indicator::IdType id() const override { return Ind::kId; }
  void OnStart() noexcept override;
  bool Subscribe(Instrument* inst, bool listen) noexcept override;
  void OnMarketQuote(const Instrument& inst, const MarketData& md,
                     const MarketData& md0) noexcept override;

 private:
};

}  // namespace opentrade

#endif  // OPENTRADE_CONSOLIDATION_H_
