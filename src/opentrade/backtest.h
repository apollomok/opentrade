#ifndef OPENTRADE_BACKTEST_H_
#define OPENTRADE_BACKTEST_H_
#ifdef BACKTEST

#include <boost/date_time/gregorian/gregorian.hpp>
#include <fstream>
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
  Backtest() : of_("trades.txt") { connected_ = 1; }
  void Start() noexcept override {}
  void Reconnect() noexcept override {}
  void Subscribe(const opentrade::Security& sec) noexcept override {}
  std::string Place(const opentrade::Order& ord) noexcept override;
  std::string Cancel(const opentrade::Order& ord) noexcept override;
  void PlayTickFile(const std::string& fn_tmpl,
                    const boost::gregorian::date& date);
  void Start(const std::string& py, double latency);
  SubAccount* CreateSubAccount(const std::string& name);
  void End();
  void Clear();
  void Skip() { skip_ = true; }

 private:
  struct OrderTuple {
    double leaves = 0;
    const Order* order = nullptr;
  };
  struct Orders {
    typedef std::multimap<double, OrderTuple> Map;
    Map buys;
    Map sells;
    std::unordered_map<Order::IdType, Map::iterator> all;
  };
  typedef std::tuple<const Security*, double, double, Orders*> SecTuple;
  void HandleTick(uint32_t hmsm, char type, double px, double qty,
                  const SecTuple& st);
  double TryFillBuy(double px, double qty, Orders* m);
  double TryFillSell(double px, double qty, Orders* m);

 private:
  bp::object obj_;
  std::unordered_map<Security::IdType, Orders> active_orders_;
  bp::object on_start_;
  bp::object on_start_of_day_;
  bp::object on_end_of_day_;
  bp::object on_end_;
  double latency_ = 0;  // in seconds
  time_t tm0_ = 0;
  uint32_t seed_ = 0;
  double trade_hit_ratio_ = 0;
  std::ofstream of_;
  bool skip_;
};

}  // namespace opentrade

#endif
#endif  // OPENTRADE_BACKTEST_H_
