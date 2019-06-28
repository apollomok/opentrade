#include "3rd/catch.hpp"
#include "3rd/fakeit.hpp"

using fakeit::Mock;

#include "opentrade/consolidation.h"

namespace opentrade {

struct MockFeed : public MarketDataAdapter {
  explicit MockFeed(const std::string& src) { config_["src"] = src; }
  void Start() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
};

struct MockConsolidationHandler : public ConsolidationHandler {
  auto CreateInstrument(const Security& sec, DataSrc src,
                        Instrument* parent = nullptr) {
    return Algo::Subscribe(sec, src, false, parent);
  }
};

struct MockAlgoManager : public AlgoManager {
  struct Strand : public AlgoManager::Strand {
    void post(std::function<void()> func) override { func(); }
  };
  MockAlgoManager() {
    threads_.resize(1);
    runners_ = new AlgoRunner(std::this_thread::get_id());
    strands_ = new Strand{};
  }
};

TEST_CASE("ConsolidationHandler", "[ConsolidationHandler]") {
  auto& md_mngr = MarketDataManager::Reset();
  auto& algo_mngr = AlgoManager::Reset<MockAlgoManager>();
  MockConsolidationHandler handler;
  algo_mngr.Add(&handler);
  handler.Start();

  md_mngr.Add(new MockFeed("A"));
  md_mngr.Add(new MockFeed("B"));
  md_mngr.Add(new MockFeed("C"));
  md_mngr.Add(new MockFeed("D"));

  Security sec;
  sec.exchange = new Exchange;
  auto inst_cons = handler.CreateInstrument(sec, DataSrc("CONS"));
  auto inst_a = handler.CreateInstrument(sec, DataSrc("A"), inst_cons);
  auto inst_b = handler.CreateInstrument(sec, DataSrc("B"), inst_cons);
  auto inst_c = handler.CreateInstrument(sec, DataSrc("C"), inst_cons);
  auto inst_d = handler.CreateInstrument(sec, DataSrc("D"), inst_cons);
  handler.Subscribe(inst_cons, false);
  auto& book = *inst_cons->Get<ConsolidationBook>();
  MarketData md;
  auto& quote = md.depth[0];
  auto& ask = quote.ask_price;
  auto& bid = quote.bid_price;
  auto& asks = book.asks;
  auto& bids = book.bids;

  SECTION("Single Src") {
    auto md0 = md;
    ask = 1, bid = 0.5;
    handler.OnMarketQuote(*inst_a, md, md0);
    REQUIRE((asks.size() == 1 && asks.begin()->price == ask &&
             bids.size() == 1 && bids.begin()->price == bid));
    md0 = md;
    ask = 0;
    handler.OnMarketQuote(*inst_a, md, md0);
    REQUIRE(
        (asks.size() == 0 && bids.size() == 1 && bids.begin()->price == bid));
    md0 = md;
    ask = 0.5;
    handler.OnMarketQuote(*inst_a, md, md0);
    REQUIRE((asks.size() == 1 && asks.begin()->price == ask &&
             bids.size() == 1 && bids.begin()->price == bid));
    md0 = md;
    ask = 0.4;
    handler.OnMarketQuote(*inst_a, md, md0);
    REQUIRE(
        (asks.size() == 1 && asks.begin()->price == ask && bids.size() == 0));
    md0 = md;
    bid = 0.6;
    handler.OnMarketQuote(*inst_a, md, md0);
    REQUIRE(
        (asks.size() == 0 && bids.size() == 1 && bids.begin()->price == bid));
  }
}

}  // namespace opentrade
