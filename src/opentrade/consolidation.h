#ifndef OPENTRADE_CONSOLIDATION_H_
#define OPENTRADE_CONSOLIDATION_H_

#include <tbb/tbb_allocator.h>
#include <set>

#include "indicator_handler.h"
#include "market_data.h"

namespace opentrade {

static const Indicator::IdType kConsolidation = 1;
static const DataSrc kConsolidationSrc("CONS");
static const char* kConsolidationBook = "ConsolidationBook";

template <typename T, template <typename> typename Comp,
          template <typename> typename Alloc = std::allocator>
struct PriceLevels : public std::set<T, Comp<T>, Alloc<T>> {
  typedef Comp<double> PriceComp;
  static constexpr bool IsAsk() { return PriceComp()(0, 1); }
};

struct PriceLevel {
  explicit PriceLevel(double price) : price(price) {}
  double price;
  struct Quote {
    Quote(int64_t size, Instrument* inst, PriceLevel* parent)
        : size(size), inst(inst), parent(parent) {}
    int64_t size;
    Instrument* inst;
    PriceLevel* parent;
  };
  typedef std::list<Quote, tbb::tbb_allocator<Quote>> Quotes;
  Quotes quotes;
  union {
    PriceLevels<PriceLevel, std::less, tbb::tbb_allocator>::iterator parent_a;
    PriceLevels<PriceLevel, std::greater, tbb::tbb_allocator>::iterator
        parent_b;
  };
  bool operator<(const PriceLevel& rhs) const { return price < rhs.price; }
  bool operator>(const PriceLevel& rhs) const { return price > rhs.price; }
  auto Insert(int64_t size, Instrument* inst) {
    quotes.emplace_back(size, inst, this);
    return std::prev(quotes.end());
  }
};

struct ConsolidationBook : public Indicator {
  static const Indicator::IdType kId = kConsolidation;
  typedef std::unique_lock<std::mutex> Lock;
  std::vector<PriceLevel::Quotes::iterator> quotes;
  PriceLevels<PriceLevel, std::less, tbb::tbb_allocator> asks;
  PriceLevels<PriceLevel, std::greater, tbb::tbb_allocator> bids;
  std::mutex m;
  template <typename A, typename B>
  void Update(double price, int64_t size, Instrument* inst, A* a, B* b);
  template <typename A>
  void Erase(PriceLevel::Quotes::iterator it, A* a);
  template <typename A>
  void Insert(double price, int64_t size, Instrument* inst, A* a);
};

struct ConsolidationHandler : public IndicatorHandler {
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
};

}  // namespace opentrade

#endif  // OPENTRADE_CONSOLIDATION_H_
