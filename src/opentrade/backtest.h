#ifndef OPENTRADE_BACKTEST_H_
#define OPENTRADE_BACKTEST_H_
#ifdef BACKTEST

#include <unordered_map>

#include "exchange_connectivity.h"
#include "market_data.h"
#include "order.h"
#include "security.h"

namespace opentrade {

class Backtest : public ExchangeConnectivityAdapter,
                 public MarketDataAdapter,
                 public Singleton<Backtest> {
 public:
  Backtest() { connected_ = 1; }
  void Start() noexcept override;
  void Reconnect() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
  std::string Place(const opentrade::Order& ord) noexcept override;
  std::string Cancel(const opentrade::Order& ord) noexcept override;
  void set_ticks_file(const std::string& fn) { ticks_file_ = fn; }

 private:
  struct OrderTuple {
    Order::IdType id = 0;
    double leaves = 0;
    double px = 0;
    bool is_buy = false;
  };
  std::unordered_map<Security::IdType,
                     std::unordered_map<Order::IdType, OrderTuple>>
      active_orders_;
  std::string ticks_file_;
};

}  // namespace opentrade

#endif
#endif  // OPENTRADE_BACKTEST_H_
