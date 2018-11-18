#ifndef OPENTRADE_BACKTEST_H_
#define OPENTRADE_BACKTEST_H_
#ifdef BACKTEST

#include <boost/date_time/gregorian/gregorian.hpp>
#include <unordered_map>

#include "exchange_connectivity.h"
#include "market_data.h"
#include "order.h"
#include "python.h"
#include "security.h"

namespace opentrade {

class Backtest : public ExchangeConnectivityAdapter,
                 public MarketDataAdapter,
                 public Singleton<Backtest> {
 public:
  Backtest() { connected_ = 1; }
  void Start() noexcept override {}
  void Reconnect() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
  std::string Place(const opentrade::Order& ord) noexcept override;
  std::string Cancel(const opentrade::Order& ord) noexcept override;
  void PlayTickFile(const std::string& fn_tmpl,
                    const boost::gregorian::date& date);
  void Start(const std::string& py, int latency);
  void End();
  void Clear();

 private:
  bp::object obj_;
  struct OrderTuple {
    Order::IdType id = 0;
    double leaves = 0;
    double px = 0;
    bool is_buy = false;
  };
  std::unordered_map<Security::IdType,
                     std::unordered_map<Order::IdType, OrderTuple>>
      active_orders_;
  bp::object on_start_;
  bp::object on_start_of_day_;
  bp::object on_end_of_day_;
  bp::object on_end_;
  int latency_ = 0;  // in milliseconds
};

}  // namespace opentrade

#endif
#endif  // OPENTRADE_BACKTEST_H_
