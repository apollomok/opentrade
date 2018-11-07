#ifndef OPENTRADE_PYTHON_H_
#define OPENTRADE_PYTHON_H_

#include <boost/python.hpp>

#include "algo.h"

namespace bp = boost::python;

namespace opentrade {

struct PyModule {
  bp::object on_start;
  bp::object on_stop;
  bp::object on_market_trade;
  bp::object on_market_quote;
  bp::object on_confirmation;
  bp::object get_param_defs;
};

class Python : public Algo {
 public:
  static Python* Load(const std::string& fn);
  std::string OnStart(const ParamMap& params) noexcept override;
  void OnStop() noexcept override;
  void OnMarketTrade(const Instrument& inst, const MarketData& md,
                     const MarketData& md0) noexcept override;
  void OnMarketQuote(const Instrument& inst, const MarketData& md,
                     const MarketData& md0) noexcept override;
  void OnConfirmation(const Confirmation& cm) noexcept override;
  const ParamDefs& GetParamDefs() noexcept override;
  void AddObj(const void* native, bp::object obj) { objs_[native] = obj; }
  bp::object GetObj(const void* native) { return objs_[native]; }

 private:
  PyModule py_;
  ParamDefs def_;
  bp::object obj_;
  std::map<const void*, bp::object> objs_;
};

void InitalizePy();

}  // namespace opentrade

#endif  // OPENTRADE_PYTHON_H_
