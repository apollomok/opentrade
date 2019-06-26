#include "consolidation.h"

namespace opentrade {

void ConsolidationHandler::OnStart() noexcept {
  MarketDataManager::Instance().Add(new ConsolidationFeed);
}

bool ConsolidationHandler::Subscribe(Instrument* inst, bool listen) noexcept {
  assert(inst->src() == DataSrc(kConsolidationSrc));
  auto book = const_cast<Ind*>(inst->Get<Ind>(id()));
  if (book) {
    if (listen) book->AddListener(inst);
    return true;
  }
  book = new Ind{};
  const_cast<MarketData&>(inst->md()).Set(book, id());
  if (listen) book->AddListener(inst);
  return true;
}

}  // namespace opentrade
