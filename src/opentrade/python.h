#ifndef OPENTRADE_PYTHON_H_
#define OPENTRADE_PYTHON_H_

#include <boost/python.hpp>

#include "algo.h"
#include "connection.h"
#include "logger.h"

namespace bp = boost::python;

namespace opentrade {

struct PyModule {
  bp::object on_start;
  bp::object on_modify;
  bp::object on_stop;
  bp::object on_market_trade;
  bp::object on_market_quote;
  bp::object on_confirmation;
  bp::object test;
  bp::object get_param_defs;
};

class Python : public Algo {
 public:
  static Python* Load(const std::string& fn);
  std::string OnStart(const ParamMap& params) noexcept override;
  void OnModify(const ParamMap& params) noexcept override;
  std::string Test() noexcept override;
  void OnStop() noexcept override;
  void OnMarketTrade(const Instrument& inst, const MarketData& md,
                     const MarketData& md0) noexcept override;
  void OnMarketQuote(const Instrument& inst, const MarketData& md,
                     const MarketData& md0) noexcept override;
  void OnConfirmation(const Confirmation& cm) noexcept override;
  const ParamDefs& GetParamDefs() noexcept override;
  void log_info(const std::string& msg) const { LOG_INFO(msg); }
  void log_debug(const std::string& msg) const { LOG_DEBUG(msg); }
  void log_warn(const std::string& msg) const { LOG_WARN(msg); }
  void log_error(const std::string& msg) const { LOG_ERROR(msg); }

 private:
  PyModule py_;
  ParamDefs def_;
  bp::object obj_;
  bool test_;
};

void InitalizePy();

}  // namespace opentrade

#endif  // OPENTRADE_PYTHON_H_
