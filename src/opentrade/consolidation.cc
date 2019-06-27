#include "consolidation.h"

namespace opentrade {

static PriceLevel::Quotes kEmptyQuotes;

template <typename A>
inline void ConsolidationBook::Erase(PriceLevel::Quotes::iterator it, A* a) {
  it->parent->quotes.erase(it);
  if (it->parent->quotes.empty()) {
    if constexpr (A::IsAsk()) {
      a->erase(it->parent->parent_a);
    } else {
      a->erase(it->parent->parent_b);
    }
  }
}

template <typename A>
inline void ConsolidationBook::Insert(double price, int64_t size,
                                      Instrument* inst, A* a) {
  auto p = a->emplace(price);
  auto& level = const_cast<PriceLevel&>(*p.first);
  if (p.second) {
    if constexpr (A::IsAsk()) {
      level.parent_a = p.first;
    } else {
      level.parent_b = p.first;
    }
  }
  quotes[inst->src_idx()] = level.Insert(size, inst);
}

template <typename A, typename B>
inline void ConsolidationBook::Update(double price, int64_t size,
                                      Instrument* inst, A* a, B* b) {
  auto it = quotes[inst->src_idx()];
  Lock lock(m);
  if (it == kEmptyQuotes.end()) {
    if (price > 0) {
      Insert(price, size, inst, a);
    }
  } else {
    if (price > 0) {
      if (price == it->parent->price) {
        it->size = size;
      } else {
        Erase(it, a);
        Insert(price, size, inst, a);
      }
    } else {
      Erase(it, a);
      quotes[inst->src_idx()] = kEmptyQuotes.end();
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
  auto parent = const_cast<Instrument*>(&inst)->parent();
  assert(parent);
  assert(inst.src_idx() < MarketDataManager::Instance().adapters().size() - 1);
  auto pinst = const_cast<Instrument*>(&inst);
  auto book = const_cast<Ind*>(pinst->Get<Ind>());
  if (md.quote().ask_price != md0.quote().ask_price ||
      md.quote().ask_size != md0.quote().ask_size) {
    book->Update(md.quote().ask_price, md.quote().ask_size, pinst, &book->asks,
                 &book->bids);
  }
}

}  // namespace opentrade
