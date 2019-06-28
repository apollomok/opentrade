#include "3rd/catch.hpp"
#include "3rd/fakeit.hpp"

using fakeit::Mock;

#include "opentrade/consolidation.h"

namespace opentrade {

struct MockFeed : public MarketDataAdapter {
  MockFeed(const std::string& src) { config_["src"] = src; }
  void Start() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
};

struct MockConsolidationHandler : public ConsolidationHandler {
  auto CreateInstrument(const Security& sec, DataSrc src,
                        Instrument* parent = nullptr) {
    return Algo::Subscribe(sec, src, false, parent);
  }
  void Async(std::function<void()> func) override { func(); }
};

struct MockAlgoManager : public AlgoManager {
  MockAlgoManager() {
    threads_.resize(1);
    runners_ = new AlgoRunner(std::this_thread::get_id());
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

  Security sec;
  sec.exchange = new Exchange;
  auto inst_cons = handler.CreateInstrument(sec, DataSrc("CONS"));
  auto inst_a = handler.CreateInstrument(sec, DataSrc("A"), inst_cons);
  auto inst_b = handler.CreateInstrument(sec, DataSrc("B"), inst_cons);
  handler.Subscribe(inst_cons, false);

  SECTION("1") { REQUIRE(true); }
}
}  // namespace opentrade
