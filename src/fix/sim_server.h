#ifndef FIX_SIM_SERVER_H_
#define FIX_SIM_SERVER_H_

#include <tbb/concurrent_unordered_set.h>
#include <boost/unordered_map.hpp>
#include <ctime>
#include <thread>
#include <unordered_map>
#include <vector>

#include "application.h"
#include "opentrade/adapter.h"
#include "opentrade/logger.h"
#include "opentrade/security.h"

using Security = opentrade::Security;

class SimServer : public opentrade::Application {
 public:
  void fromApp(const FIX::Message& msg,
               const FIX::SessionID& session_id) override;
  void HandleTick(Security::IdType sec, char type, double px, double qty);
  void StartFix(const opentrade::Adapter& adapter);

 protected:
  struct OrderTuple {
    double px = 0;
    double leaves = 0;
    bool is_buy = false;
    FIX::Message resp;
  };
  std::unordered_map<Security::IdType,
                     std::unordered_map<std::string, OrderTuple>>
      active_orders_;
  tbb::concurrent_unordered_set<std::string> used_ids_;
  opentrade::TaskPool tp_;
  int latency_ = 0;  // in microseconds
  uint32_t seed_ = 0;
  double trade_hit_ratio_ = 0.5;
};

#endif  // FIX_SIM_SERVER_H_
