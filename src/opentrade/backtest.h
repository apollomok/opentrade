#ifndef OPENTRADE_BACKTEST_H_
#define OPENTRADE_BACKTEST_H_
#ifdef BACKTEST

#include <boost/date_time/gregorian/gregorian.hpp>
#include <fstream>
#include <memory>

#include "python.h"
#include "security.h"
#include "simulator.h"

namespace opentrade {

class Backtest : public Singleton<Backtest> {
 public:
  Backtest() : of_("trades.txt") {}
  void Play(const boost::gregorian::date& date);
  bool LoadTickFile(const std::string& fn, Simulator* sim,
                    const boost::gregorian::date& date);
  void Start(const std::string& py, double latency,
             const std::string& default_tick_file);
  SubAccount* CreateSubAccount(const std::string& name,
                               const BrokerAccount* broker = nullptr);
  void End();
  void Clear();
  void Skip() { skip_ = true; }
  void AddSimulator(const std::string& fn_tmpl, const std::string& name = "");
  auto latency() { return latency_; }

 private:
  bp::object obj_;
  bp::object on_start_;
  bp::object on_start_of_day_;
  bp::object on_end_of_day_;
  bp::object on_end_;
  double latency_ = 0;  // in seconds
  time_t tm0_ = 0;
  double trade_hit_ratio_ = 0;
  std::ofstream of_;
  bool skip_;
  struct SecTuple {
    const Security* sec;
    Simulator* sim;
    Simulator::Orders* actives;
  };
  std::vector<std::shared_ptr<SecTuple>> sts_;
  struct Tick {
    SecTuple* st;
    uint32_t hmsm;
    char type;
    double px;
    double qty;
    bool operator<(const Tick& b) const { return hmsm < b.hmsm; }
  };
  std::vector<Tick> ticks_;
  std::vector<std::pair<std::string, Simulator*>> simulators_;
};

}  // namespace opentrade

#endif  // BACKTEST
#endif  // OPENTRADE_BACKTEST_H_
