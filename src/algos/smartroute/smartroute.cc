#include "../twap/twap.h"
#include "opentrade/consolidation.h"

namespace opentrade {

struct SmartRoute : public TWAP {
  Instrument* Subscribe() override {
    st_.src = kConsolidationSrc;
    auto inst = TWAP::Subscribe();
    inst->Subscribe(kConsolidation);
    return inst;
  }

  const MarketData& md() override {
    auto book = inst_->Get<ConsolidationBook>();
    assert(book);
    const PriceLevel* p = nullptr;
    ConsolidationBook::Lock lock(book->m);
    if (IsBuy(st_.side)) {
      if (!book->bids.empty()) p = &*book->bids.begin();
    } else {
      if (!book->asks.empty()) p = &*book->asks.begin();
    }
    if (p) {
      // to-do: logic here to choose destionation
      auto dest = p->quotes.begin()->inst;
      st_.acc = AccountManager::Instance().GetSubAccount(dest->src().str());
      return dest->md();
    }
    return inst_->md();
  }
};

}  // namespace opentrade

extern "C" {
opentrade::Adapter* create() { return new opentrade::SmartRoute{}; }
}
