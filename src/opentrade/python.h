#ifndef OPENTRADE_PYTHON_H_
#define OPENTRADE_PYTHON_H_

#include <boost/python.hpp>

#include "algo.h"
#include "connection.h"

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
  static Python* LoadModule(const std::string& module_name);
  static Python* Load(const std::string& module_name);
  static Python* LoadTest(const std::string& module_name,
                          const std::string& token);
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
  void SetTimeout(bp::object func, int milliseconds);

 private:
  PyModule py_;
  ParamDefs def_;
  bp::object obj_;
  std::string test_token_;
};

void InitalizePy();
void PrintPyError(const char*, bool fatal = false);
bp::object GetCallable(const bp::object& m, const char* name);

inline bp::object kOpentrade;

}  // namespace opentrade

#endif  // OPENTRADE_PYTHON_H_
