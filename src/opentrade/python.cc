#include "python.h"

#include <Python.h>

#include "logger.h"

namespace opentrade {

struct SecurityTupleWrapper : SecurityTuple {
  const DataSrc GetSrc() const { return std::get<0>(*this); }

  const Security *GetSec() const { return std::get<1>(*this); }

  const SubAccount *GetAcc() const { return std::get<2>(*this); }

  const OrderSide GetSide() const { return std::get<3>(*this); }

  const double GetQty() const { return std::get<4>(*this); }
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
      .def_readonly("odd_lot_allowed", &Exchange::odd_lot_allowed);

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
      .add_property("exchange",
                    bp::make_function(&Security::GetExchange,
                                      bp::return_internal_reference<>()))
      .add_property("underlying",
                    bp::make_function(&Security::GetUnderlying,
                                      bp::return_internal_reference<>()))
      .def("is_in_trade_period", &Security::IsInTradePeriod)
      .def_readonly("local_symbol", &Security::local_symbol);

  bp::class_<SecurityTupleWrapper>("SecurityTuple")
      .add_property("sec", bp::make_function(&SecurityTupleWrapper::GetSec,
                                             bp::return_internal_reference<>()))
      .add_property("acc", bp::make_function(&SecurityTupleWrapper::GetAcc,
                                             bp::return_internal_reference<>()))
      .add_property("side", &SecurityTupleWrapper::GetSide)
      .add_property("qty", &SecurityTupleWrapper::GetQty)
      .add_property("src", &SecurityTupleWrapper::GetSrc);

  bp::class_<Contract>("Contract")
      .def("is_buy", &Contract::IsBuy)
      .def_readwrite("sec", &Contract::sec)
      .def_readwrite("qty", &Contract::qty)
      .def_readwrite("price", &Contract::price)
      .def_readwrite("stop_price", &Contract::stop_price)
      .def_readwrite("side", &Contract::side)
      .def_readwrite("tif", &Contract::tif)
      .def_readwrite("type", &Contract::type);

  bp::class_<Python>("Algo").def("subscribe", &Algo::Subscribe,
                                 bp::return_internal_reference<>());
}

void PrintPyError() {
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
    }
    result = typestr + ": " + errstr;
  }
  PyErr_Restore(ptype, pvalue, ptraceback);
  LOG_ERROR("\n" << result);
}

static bp::object kOpentrade;

static inline double GetDouble(const bp::object &obj) {
  auto ptr = obj.ptr();
  if (PyFloat_Check(ptr)) return PyFloat_AsDouble(ptr);
  if (PyLong_Check(ptr)) return PyLong_AsLong(ptr);
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

struct LockGIL {
  LockGIL() { m.lock(); }
  ~LockGIL() { m.unlock(); }
  static inline std::mutex m;
};

void InitalizePy() {
  auto tmp = getenv("PYTHONPATH");
  std::string path = tmp ? tmp : "";
  setenv("PYTHONPATH", ("./algos:./algos/revisions:" + path).c_str(), 1);
  PyImport_AppendInittab(const_cast<char *>("opentrade"), PyInit_opentrade);
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
    PrintPyError();
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
  } else if (ptr == Py_True) {
    *out = true;
  } else if (ptr == Py_False) {
    *out = false;
  } else if (PyUnicode_Check(ptr)) {
    *out = bp::extract<std::string>(value);
  } else {
    try {
      *out = bp::extract<SecurityTupleWrapper>(value);
      return true;
    } catch (const bp::error_already_set &err) {
      return false;
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
    PrintPyError();
    return {};
  }
}

static bp::dict CreateParamsDict(const Algo::ParamMap &params) {
  bp::dict obj;
  for (auto &pair : params) {
    if (auto pval = std::get_if<bool>(&pair.second)) {
      obj[pair.first] = *pval;
      continue;
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
      SecurityTupleWrapper tmp;
      (SecurityTuple &)tmp = *pval;
      obj[pair.first] = tmp;
    }
  }
  return obj;
}

static bp::object CreateAlgo(Python *algo) {
  LockGIL lock;
  return bp::object(algo);
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
    }
  } catch (const bp::error_already_set &err) {
    PrintPyError();
    return {};
  }
  return {};
}

void Python::OnStop() noexcept {
  if (!py_.on_stop) return;
  LockGIL locker;
  py_.on_stop(obj_);
}

void Python::OnMarketTrade(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_trade) return;
  LockGIL locker;
}

void Python::OnMarketQuote(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_quote) return;
  LockGIL locker;
}

void Python::OnConfirmation(const Confirmation &cm) noexcept {
  if (!py_.on_confirmation) return;
  LockGIL locker;
}

const ParamDefs &Python::GetParamDefs() noexcept { return def_; }

}  // namespace opentrade
