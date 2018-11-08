#include "python.h"

#include <Python.h>

namespace opentrade {

static bp::object kOpentrade;
static void PrintPyError(const char *);

struct LockGIL {
  LockGIL() { m.lock(); }
  ~LockGIL() { m.unlock(); }
  static inline std::mutex m;
};

BOOST_PYTHON_MODULE(opentrade) {
  bp::enum_<OrderSide>("OrderSide")
      .value("buy", kBuy)
      .value("sell", kSell)
      .value("short", kShort);

  bp::enum_<OrderType>("OrderType")
      .value("market", kMarket)
      .value("limit", kLimit)
      .value("stop", kStop)
      .value("stop_limit", kStopLimit)
      .value("otc", kOTC);

  bp::enum_<ExecTransType>("ExecTransType")
      .value("new", kTransNew)
      .value("cancel", kTransCancel)
      .value("correct", kTransCorrect)
      .value("status", kTransStatus);

  bp::enum_<OrderStatus>("OrderStatus")
      .value("new", kNew)
      .value("partially_filled", kPartiallyFilled)
      .value("filled", kFilled)
      .value("done_for_day", kDoneForDay)
      .value("canceled", kCanceled)
      .value("replace", kReplaced)
      .value("pending_cancel", kPendingCancel)
      .value("stopped", kStopped)
      .value("rejected", kRejected)
      .value("suspended", kSuspended)
      .value("pending_new", kPendingNew)
      .value("calculated", kCalculated)
      .value("expired", kExpired)
      .value("accept_for_bidding", kAcceptedForBidding)
      .value("pending_replace", kPendingReplace)
      .value("risk_rejected", kRiskRejected)
      .value("unconfirmed_new", kUnconfirmedNew)
      .value("unconfirmed_cancel", kUnconfirmedCancel)
      .value("unconfirmed_replace", kUnconfirmedReplace)
      .value("cancel_rejected", kCancelRejected);

  bp::enum_<TimeInForce>("TimeInForce")
      .value("day", kDay)
      .value("gtc", kGoodTillCancel)
      .value("opg", kAtTheOpening)
      .value("ioc", kImmediateOrCancel)
      .value("fok", kFillOrKill)
      .value("gtx", kGoodTillCrossing)
      .value("gtd", kGoodTillDate);

  bp::class_<DataSrc>("DataSrc", bp::init<const char *>())
      .def("__str__", &DataSrc::str);

  bp::class_<SubAccount>("SubAccount")
      .def_readonly("id", &SubAccount::id)
      .def_readonly("name", &SubAccount::name);

  bp::class_<Exchange>("Exchange")
      .def_readonly("name", &Exchange::name)
      .def_readonly("mic", &Exchange::mic)
      .def_readonly("bb_name", &Exchange::bb_name)
      .def_readonly("ib_name", &Exchange::ib_name)
      .def_readonly("tz", &Exchange::tz)
      .def_readonly("trade_start", &Exchange::trade_start)
      .def_readonly("trade_end", &Exchange::trade_end)
      .def_readonly("break_start", &Exchange::break_start)
      .def_readonly("break_end", &Exchange::break_end)
      .def_readonly("desc", &Exchange::desc)
      .def_readonly("utc_time_offset", &Exchange::utc_time_offset)
      .def_readonly("country", &Exchange::country)
      .def_readonly("odd_lot_allowed", &Exchange::odd_lot_allowed)
      .add_property("now", &Exchange::GetTime);

  auto cls = bp::class_<Security>("Security");
  cls.def_readonly("id", &Security::id)
      .def_readonly("symbol", &Security::symbol)
      .def_readonly("isin", &Security::isin)
      .def_readonly("cusip", &Security::cusip)
      .def_readonly("sedol", &Security::sedol)
      .def_readonly("bbgid", &Security::bbgid)
      .def_readonly("currency", &Security::currency)
      .def_readonly("rate", &Security::rate)
      .def_readonly("adv20", &Security::adv20)
      .def_readonly("market_cap", &Security::market_cap)
      .def_readonly("sector", &Security::sector)
      .def_readonly("industry_group", &Security::industry_group)
      .def_readonly("industry", &Security::industry)
      .def_readonly("sub_industry", &Security::sub_industry)
      .def_readonly("strike_price", &Security::strike_price)
      .def_readonly("maturity_date", &Security::maturity_date)
      .def_readonly("put_or_call", &Security::put_or_call)
      .def_readonly("opt_attribute", &Security::opt_attribute)
      .def_readonly("multiplier", &Security::multiplier)
      .def_readonly("lot_size", &Security::lot_size)
      .def("get_tick_size", &Security::GetTickSize)
      .def_readonly("type", &Security::type)
      .add_property(
          "exchange",
          bp::make_function(+[](const Security &s) { return s.exchange; },
                            bp::return_internal_reference<>()))
      .add_property(
          "underlying",
          bp::make_function(+[](const Security &s) { return s.underlying; },
                            bp::return_internal_reference<>()))
      .add_property("is_in_trade_period", &Security::IsInTradePeriod)
      .def_readonly("local_symbol", &Security::local_symbol);

  bp::class_<SecurityTuple>("SecurityTuple")
      .add_property("src",
                    +[](const SecurityTuple &st) { return std::get<0>(st); })
      .add_property(
          "sec", bp::make_function(
                     +[](const SecurityTuple &st) { return std::get<1>(st); },
                     bp::return_internal_reference<>()))
      .add_property(
          "acc", bp::make_function(
                     +[](const SecurityTuple &st) { return std::get<2>(st); },
                     bp::return_internal_reference<>()))
      .add_property("side",
                    +[](const SecurityTuple &st) { return std::get<3>(st); })
      .add_property("qty",
                    +[](const SecurityTuple &st) { return std::get<4>(st); });

  bp::class_<Contract>("Contract")
      .add_property("is_buy", &Contract::IsBuy)
      .add_property("sec",
                    bp::make_function(+[](const Contract &c) { return c.sec; },
                                      bp::return_internal_reference<>()))
      .add_property(
          "acc",
          bp::make_function(+[](const Contract &c) { return c.sub_account; },
                            bp::return_internal_reference<>()),
          +[](Contract &c, const SubAccount *sub_account) {
            c.sub_account = sub_account;
          })
      .def_readwrite("qty", &Contract::qty)
      .def_readwrite("price", &Contract::price)
      .def_readwrite("stop_price", &Contract::stop_price)
      .def_readwrite("side", &Contract::side)
      .def_readwrite("tif", &Contract::tif)
      .def_readwrite("type", &Contract::type);

  bp::class_<MarketData>("MarketData")
      .def_readonly("tm", &MarketData::tm)
      .add_property("open", +[](const MarketData &md) { return md.trade.open; })
      .add_property("high", +[](const MarketData &md) { return md.trade.high; })
      .add_property("low", +[](const MarketData &md) { return md.trade.low; })
      .add_property("close",
                    +[](const MarketData &md) { return md.trade.close; })
      .add_property("qty", +[](const MarketData &md) { return md.trade.qty; })
      .add_property("vwap", +[](const MarketData &md) { return md.trade.vwap; })
      .add_property("volume",
                    +[](const MarketData &md) { return md.trade.volume; })
      .add_property("ask_price",
                    +[](const MarketData &md) { return md.quote().ask_price; })
      .add_property("bid_price",
                    +[](const MarketData &md) { return md.quote().bid_price; })
      .add_property("ask_size",
                    +[](const MarketData &md) { return md.quote().ask_size; })
      .add_property("bid_size",
                    +[](const MarketData &md) { return md.quote().bid_size; })
      .def("get_ask_price",
           +[](const MarketData &md, size_t i) {
             return md.depth[std::min(i, MarketData::kDepthSize - 1)].ask_price;
           })
      .def("get_bid_price",
           +[](const MarketData &md, size_t i) {
             return md.depth[std::min(i, MarketData::kDepthSize - 1)].bid_price;
           })
      .def("get_ask_size",
           +[](const MarketData &md, size_t i) {
             return md.depth[std::min(i, MarketData::kDepthSize - 1)].ask_size;
           })
      .def("get_bid_size", +[](const MarketData &md, size_t i) {
        return md.depth[std::min(i, MarketData::kDepthSize - 1)].bid_size;
      });

  bp::class_<Confirmation>("Confirmation")
      .add_property("order", bp::make_function(
                                 +[](const Confirmation &c) { return c.order; },
                                 bp::return_internal_reference<>()))
      .def_readonly("exec_id", &Confirmation::exec_id)
      .def_readonly("transaction_time", &Confirmation::transaction_time)
      .def_readonly("order_id", &Confirmation::order_id)
      .def_readonly("text", &Confirmation::text)
      .def_readonly("exec_type", &Confirmation::exec_type)
      .def_readonly("exec_trans_type", &Confirmation::exec_trans_type)
      .def_readonly("last_shares", &Confirmation::last_shares)
      .def_readonly("last_px", &Confirmation::last_px);

  bp::class_<Order, bp::bases<Contract>>("Order")
      .add_property("instrument",
                    bp::make_function(+[](const Order &o) { return o.inst; },
                                      bp::return_internal_reference<>()))
      .def_readonly("status", &Order::status)
      .def_readonly("algo_id", &Order::algo_id)
      .def_readonly("id", &Order::id)
      .def_readonly("orig_id", &Order::orig_id)
      .def_readonly("avg_px", &Order::avg_px)
      .def_readonly("cum_qty", &Order::cum_qty)
      .def_readonly("leaves_qty", &Order::leaves_qty)
      .add_property("is_live", &Order::IsLive);

  bp::class_<Instrument>("Instrument",
                         bp::init<Algo *, const Security &, DataSrc>())
      .add_property("sec", bp::make_function(&Instrument::sec,
                                             bp::return_internal_reference<>()))
      .add_property("md", bp::make_function(&Instrument::md,
                                            bp::return_internal_reference<>()))
      .add_property("bought_qty", &Instrument::bought_qty)
      .add_property("bought_qty", &Instrument::bought_qty)
      .add_property("sold_qty", &Instrument::sold_qty)
      .add_property("outstanding_buy_qty", &Instrument::outstanding_buy_qty)
      .add_property("outstanding_sell_qty", &Instrument::outstanding_sell_qty)
      .add_property("net_outstanding_qty", &Instrument::net_outstanding_qty)
      .add_property("total_outstanding_qty", &Instrument::total_outstanding_qty)
      .add_property("total_exposure", &Instrument::total_exposure)
      .add_property("net_qty", &Instrument::net_qty)
      .add_property("total_qty", &Instrument::total_qty)
      .add_property("id", &Instrument::id)
      .add_property("active_orders", +[](const Instrument &inst) {
        auto &orders = inst.active_orders();
        bp::list out;
        for (auto o : orders) {
          out.append(bp::ptr(o));
        }
        return out;
      });

  bp::class_<Python>("Algo")
      .def("subscribe", &Algo::Subscribe, bp::return_internal_reference<>())
      .def("place", &Algo::Place, bp::return_internal_reference<>())
      .def("cancel",
           +[](Python &algo, const Order *ord) {
             if (ord) return algo.Cancel(*ord);
             return false;
           })
      .def("stop", &Algo::Stop)
      .def("log_info", &Python::log_info)
      .def("log_debug", &Python::log_debug)
      .def("log_warn", &Python::log_warn)
      .def("log_error", &Python::log_error)
      .def("set_timeout",
           +[](Python &algo, bp::object func, size_t milliseconds) {
             algo.SetTimeout(
                 [func]() {
                   LockGIL lock;
                   try {
                     func();
                   } catch (const bp::error_already_set &err) {
                     PrintPyError("set_timeout");
                   }
                 },
                 milliseconds);
           })
      .add_property("id", &Algo::id)
      .add_property("name", +[](const Python &algo) { return algo.name(); })
      .add_property("is_active", &Algo::is_active);
}

#if PY_MAJOR_VERSION >= 3
#define INIT_MODULE PyInit_opentrade
extern "C" PyObject *INIT_MODULE();
#else
#define INIT_MODULE initopentrade
extern "C" void INIT_MODULE();
#endif

void PrintPyError(const char *from) {
  PyObject *ptype, *pvalue, *ptraceback;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
  bp::object type(bp::handle<>(bp::borrowed(ptype)));
  bp::object value(bp::handle<>(bp::borrowed(pvalue)));
  std::string result;
  if (ptraceback) {
    bp::object tb(bp::handle<>(bp::borrowed(ptraceback)));
    if (ptraceback) {
      bp::object lines =
          bp::import("traceback").attr("format_exception")(type, value, tb);
      for (auto i = 0; i < bp::len(lines); ++i)
        result += bp::extract<std::string>(lines[i])();
    }
  } else {
    std::string errstr = bp::extract<std::string>(bp::str(value));
    std::string typestr =
        bp::extract<std::string>(bp::str(type.attr("__name__")));
    try {
      std::string text = bp::extract<std::string>(value.attr("text"));
      int64_t offset = bp::extract<int64_t>(value.attr("offset"));
      char space[offset + 1] = {};
      for (auto i = 0; i < offset - 1; ++i) space[i] = ' ';
      errstr += "\n" + text + space + "^";
    } catch (const bp::error_already_set &err) {
      PyErr_Clear();
    }
    result = typestr + ": " + errstr;
  }
  PyErr_Restore(ptype, pvalue, ptraceback);
  PyErr_Clear();
  LOG_ERROR(from << "\n" << result);
}

static inline double GetDouble(const bp::object &obj) {
  auto ptr = obj.ptr();
  if (PyFloat_Check(ptr)) return PyFloat_AsDouble(ptr);
  if (PyLong_Check(ptr)) return PyLong_AsLong(ptr);
#if PY_MAJOR_VERSION < 3
  if (PyInt_Check(ptr)) return PyInt_AsLong(ptr);
#endif
  return 0;
}

static inline bp::object GetCallable(const bp::object &m, const char *name) {
  if (!PyObject_HasAttrString(m.ptr(), name)) return {};
  bp::object func = m.attr(name);
  if (!PyCallable_Check(func.ptr())) {
    return {};
  }
  LOG_INFO("Loaded python function " << name);
  return func;
}

void InitalizePy() {
  auto tmp = getenv("PYTHONPATH");
  std::string path = tmp ? tmp : "";
  setenv("PYTHONPATH", ("./algos:./algos/revisions:" + path).c_str(), 1);
  PyImport_AppendInittab(const_cast<char *>("opentrade"), INIT_MODULE);
  Py_InitializeEx(0);  // no signal registration
  if (!PyEval_ThreadsInitialized()) PyEval_InitThreads();
  LockGIL lock;
  kOpentrade = bp::import("opentrade");
  LOG_INFO("Python initialized");
  LOG_INFO("Python PATH: " << getenv("PYTHONPATH"));
}

PyModule LoadPyModule(const std::string &module_name) {
  LockGIL locker;
  bp::object m;
  try {
    m = bp::import(module_name.c_str());
  } catch (const bp::error_already_set &err) {
    PrintPyError("load python");
    return {};
  }
  LOG_INFO(module_name + " loaded");
  auto func = GetCallable(m, "get_param_defs");
  if (!func.ptr()) {
    LOG_ERROR("Can not find function \"get_param_defs\" in " << module_name);
    return {};
  }
  PyModule out;
  out.get_param_defs = func;
  out.on_start = GetCallable(m, "on_start");
  out.on_stop = GetCallable(m, "on_stop");
  out.on_market_trade = GetCallable(m, "on_market_trade");
  out.on_market_quote = GetCallable(m, "on_market_quote");
  out.on_confirmation = GetCallable(m, "on_confirmation");
  return out;
}

template <typename T>
static inline bool GetValueScalar(const bp::object &value, T *out) {
  auto ptr = value.ptr();
  if (PyFloat_Check(ptr)) {
    *out = PyFloat_AsDouble(ptr);
  } else if (PyLong_Check(ptr)) {
    *out = PyLong_AsLong(ptr);
#if PY_MAJOR_VERSION < 3
  } else if (PyInt_Check(ptr)) {
    *out = PyInt_AsLong(ptr);
#endif
  } else if (ptr == Py_True) {
    *out = true;
  } else if (ptr == Py_False) {
    *out = false;
  } else {
    try {
      *out = bp::extract<std::string>(value);
      return true;
    } catch (const bp::error_already_set &err) {
      PyErr_Clear();
      try {
        *out = bp::extract<SecurityTuple &>(value);
        return true;
      } catch (const bp::error_already_set &err) {
        PyErr_Clear();
        return false;
      }
    }
  }
  return true;
}

static inline bool ParseParamDef(const bp::object &item, ParamDef *out) {
  // (name, default_value, required, min_value, max_value, precision)
  ParamDef::ValueVector value_vector;
  auto n2 = PyTuple_Size(item.ptr());
  if (n2 < 2) return false;
  bp::object tmp;
  tmp = item[0];
  out->name = bp::extract<std::string>(tmp);
  bp::object value = item[1];
  if (PyTuple_Check(value.ptr()) || PyList_Check(value.ptr())) {
    auto n3 = PyTuple_Size(value.ptr());
    value_vector.resize(n3);
    for (auto i = 0; i < n3; ++i) {
      tmp = value[i];
      if (!GetValueScalar(tmp, &value_vector[i])) return false;
    }
    out->default_value = value_vector;
  } else {
    if (!GetValueScalar(value, &out->default_value)) return false;
  }
  if (n2 == 2) return true;
  tmp = item[2];
  out->required = PyObject_IsTrue(tmp.ptr());
  if (n2 == 3) return true;
  tmp = item[3];
  out->min_value = GetDouble(tmp);
  if (n2 == 4) return true;
  tmp = item[4];
  out->max_value = GetDouble(tmp);
  if (n2 == 5) return true;
  tmp = item[5];
  out->precision = GetDouble(tmp);
  return true;
}

static ParamDefs ParseParamDefs(const bp::object &func) {
  LockGIL locker;
  try {
    auto out = func();
    if (!out || !PyTuple_Check(out.ptr())) {
      LOG_ERROR("get_param_defs must return tuple");
      return {};
    }
    auto n = PyTuple_Size(out.ptr());
    ParamDefs defs;
    defs.resize(n);
    for (auto i = 0; i < n; ++i) {
      bp::object item = out[i];
      if (!PyTuple_Check(item.ptr()) || !ParseParamDef(item, &defs[i])) {
        LOG_ERROR("Invalid param definition \""
                  << bp::extract<const char *>(bp::str(item)) << "\"");
        return {};
      }
    }
    return defs;
  } catch (const bp::error_already_set &err) {
    PrintPyError("parse param defs");
    return {};
  }
}

static bp::dict CreateParamsDict(const Algo::ParamMap &params) {
  bp::dict obj;
  for (auto &pair : params) {
    if (auto pval = std::get_if<bool>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<int32_t>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<int64_t>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<const char *>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<std::string>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<double>(&pair.second)) {
      obj[pair.first] = *pval;
    } else if (auto pval = std::get_if<SecurityTuple>(&pair.second)) {
      obj[pair.first] = *pval;
    }
  }
  return obj;
}

static bp::object CreateAlgo(Python *algo) {
  LockGIL lock;
  return bp::object(bp::ptr(algo));
}

Python *Python::Load(const std::string &module_name) {
  auto p = new Python;
  auto m = LoadPyModule(module_name);
  if (!m.get_param_defs) return nullptr;
  p->def_ = ParseParamDefs(m.get_param_defs);
  if (p->def_.empty()) return nullptr;
  p->create_func_ = [m]() {
    auto p2 = new Python;
    p2->py_ = m;
    p2->obj_ = CreateAlgo(p2);
    return p2;
  };
  return p;
}

std::string Python::OnStart(const ParamMap &params) noexcept {
  if (!py_.on_start) return {};
  LockGIL locker;
  auto tmp = CreateParamsDict(params);
  try {
    auto out = py_.on_start(obj_, tmp);
    try {
      return bp::extract<std::string>(out);
    } catch (const bp::error_already_set &err) {
      PyErr_Clear();
    }
  } catch (const bp::error_already_set &err) {
    PrintPyError("on_start");
    return {};
  }
  return {};
}

void Python::OnStop() noexcept {
  if (!py_.on_stop) return;
  LockGIL locker;
  try {
    py_.on_stop(obj_);
  } catch (const bp::error_already_set &err) {
    PrintPyError("on_stop");
  }
}

void Python::OnMarketTrade(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_trade) return;
  LockGIL locker;
  try {
    py_.on_market_trade(obj_, bp::ptr(&inst));
  } catch (const bp::error_already_set &err) {
    PrintPyError("on_market_trade");
  }
}

void Python::OnMarketQuote(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_quote) return;
  LockGIL locker;
  try {
    py_.on_market_quote(obj_, bp::ptr(&inst));
  } catch (const bp::error_already_set &err) {
    PrintPyError("on_market_quote");
  }
}

void Python::OnConfirmation(const Confirmation &cm) noexcept {
  if (!py_.on_confirmation) return;
  LockGIL locker;
  try {
    py_.on_confirmation(obj_, bp::ptr(&cm));
  } catch (const bp::error_already_set &err) {
    PrintPyError("on_confirmation");
  }
}

const ParamDefs &Python::GetParamDefs() noexcept { return def_; }

}  // namespace opentrade
