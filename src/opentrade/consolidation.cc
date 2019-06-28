#include "consolidation.h"

namespace opentrade {

static PriceLevel::Quotes kEmptyQuotes;

template <bool reset, typename A>
inline void ConsolidationBook::Erase(PriceLevel::Quotes::const_iterator it,
                                     A* a) {
  if constexpr (reset) quotes[it->inst->src_idx()] = kEmptyQuotes.end();
  auto level = it->parent;
  level->quotes.erase(it);
  if (level->quotes.empty()) a->erase(level->self);
}

template <typename A, typename B>
inline void ConsolidationBook::Insert(double price, int64_t size,
                                      const Instrument* inst, A* a, B* b) {
  auto p = a->emplace(price);
  auto& level = const_cast<PriceLevel&>(*p.first);
  if (p.second) level.self = p.first;
  quotes[inst->src_idx()] = level.Insert(size, inst);
  // remove crossed levels of b, lock allowed
  for (auto it = b->begin(); it != b->end() && A::kPriceCmp(price, it->price);
       ++it) {
    for (auto it2 = it->quotes.begin(); it2 != it->quotes.end(); ++it2) {
      Erase<true>(it2, b);
    }
  }
}

template <typename A, typename B>
inline void ConsolidationBook::Update(double price, int64_t size,
                                      const Instrument* inst, A* a, B* b) {
  auto it = quotes[inst->src_idx()];
  Lock lock(m);
  if (it == kEmptyQuotes.end()) {
    if (price > 0) Insert(price, size, inst, a, b);
  } else {
    if (price > 0) {
      if (price == it->parent->price) {
        it->size = size;
      } else {
        Erase<false>(it, a);
        Insert(price, size, inst, a, b);
      }
    } else {
      Erase<true>(it, a);
    }
  }
}

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
      book->quotes.resize(MarketDataManager::Instance().adapters().size() - 1,
                          kEmptyQuotes.end());
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
  assert(inst.src_idx() < MarketDataManager::Instance().adapters().size() - 1);
  assert(inst.parent());
  auto book = const_cast<Ind*>(inst.parent()->Get<Ind>());
  auto& q0 = md0.quote();
  auto& q = md.quote();
  if (q.ask_price != q0.ask_price || q.ask_size != q0.ask_size)
    book->Update(q.ask_price, q.ask_size, &inst, &book->asks, &book->bids);
  if (q.bid_price != q0.bid_price || q.bid_size != q0.bid_size)
    book->Update(q.bid_price, q.bid_size, &inst, &book->bids, &book->asks);
}

}  // namespace opentrade
