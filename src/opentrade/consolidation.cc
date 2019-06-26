#include "consolidation.h"

namespace opentrade {

class ConsolidationFeed : public MarketDataAdapter {
 public:
  ConsolidationFeed() {
    connected_ = 1;
    set_name(kConsolidationSrc.str());
    config_["src"] = kConsolidationSrc.str();
    create_func_ = []() { return new ConsolidationFeed; };
  }
  void Start() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
};

void ConsolidationHandler::OnStart() noexcept {
  MarketDataManager::Instance().Add(new ConsolidationFeed);
}

bool ConsolidationHandler::Subscribe(Instrument* inst, bool listen) noexcept {
  assert(kConsolidationSrc == inst->src());
  Async([=]() {
    auto book = const_cast<Ind*>(inst->Get<Ind>());
    if (!book) {
      book = new Ind{};
      const_cast<MarketData&>(inst->md()).Set(book);
      for (auto& p : MarketDataManager::Instance().adapters()) {
        if (kConsolidationSrc == p.second->src()) continue;
        Algo::Subscribe(inst->sec(), DataSrc(p.second->src()), true, inst);
      }
    }
    if (listen) book->AddListener(inst);
  });
  return true;
}

void ConsolidationHandler::OnMarketQuote(const Instrument& inst,
                                         const MarketData& md,
                                         const MarketData& md0) noexcept {
  auto parent = const_cast<Instrument*>(&inst)->parent();
  if (!parent) return;
}

}  // namespace opentrade
