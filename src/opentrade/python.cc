#include "python.h"

#include "logger.h"

namespace opentrade {

static PyObject *kConstants;
static PyObject *kCreateObjectPyFunc;
static PyObject *kSecurityTuple;
static const char *kRawPy = R"(# do not modify me
class Object: pass
def create_object(): return Object())";

static inline std::string GetString(PyObject *obj, bool convert2str = false) {
  if (convert2str) {
    auto tmp = PyObject_Str(obj);
    auto out = GetString(tmp);
    Py_XDECREF(tmp);
    return out;
  }
  if (!obj) return "";
  Py_ssize_t size;
  auto tmp = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!tmp) return "";
  return std::string(tmp, size);
}

static inline bool IsType(PyObject *obj, const char *type) {
  if (!PyObject_HasAttrString(obj, "__type__")) return false;
  auto p = PyObject_GetAttrString(obj, "__type__");
  auto out = GetString(p) == type;
  Py_XDECREF(p);
  return out;
}

static inline double GetDouble(PyObject *obj) {
  if (PyFloat_Check(obj)) return PyFloat_AsDouble(obj);
  if (PyLong_Check(obj)) return PyLong_AsLong(obj);
  return 0;
}

static inline std::string GetAttrString(PyObject *obj, const char **names,
                                        size_t n) {
  if (!PyObject_HasAttrString(obj, names[0])) return {};
  auto tmp = PyObject_GetAttrString(obj, names[0]);
  if (n > 1) {
    auto out = GetAttrString(tmp, names + 1, n - 1);
    Py_XDECREF(tmp);
    return out;
  }
  auto out = GetString(tmp, true);
  Py_XDECREF(tmp);
  return out;
}

static bool PrintPyError() {
  if (!PyErr_Occurred()) return false;
  PyObject *ptype = nullptr;
  PyObject *pvalue = nullptr;
  PyObject *ptraceback = nullptr;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  assert(pvalue);
  std::stringstream str;
  auto text = GetString(pvalue);
  std::stringstream ss;
  if (ptraceback) {
    const char *lineno_names[] = {"tb_lineno"};
    auto lineno = GetAttrString(ptraceback, lineno_names,
                                sizeof(lineno_names) / sizeof(*lineno_names));
    const char *filename_names[] = {"tb_frame", "f_code", "co_filename"};
    auto filename =
        GetAttrString(ptraceback, filename_names,
                      sizeof(filename_names) / sizeof(*filename_names));
    ss << " on line " << lineno << " of file " << filename;
  }
  PyErr_Restore(ptype, pvalue, ptraceback);  // does not increase ref
  LOG_ERROR(text << ss.str());
  PyErr_Print();
  return true;
}

static void RegisterFunc(PyMethodDef *def, PyObject *obj) {
  PyErr_Clear();
  auto pfunc = PyCFunction_New(def, obj);
  PrintPyError();
  PyObject_SetAttrString(obj, def->ml_name, pfunc);
  PrintPyError();
  Py_XDECREF(pfunc);
}

static inline PyObject *GetCallable(PyObject *m, const char *name) {
  if (!PyObject_HasAttrString(m, name)) return nullptr;
  auto pfunc = PyObject_GetAttrString(m, name);
  if (!PyCallable_Check(pfunc)) {
    Py_XDECREF(pfunc);
    return nullptr;
  }
  LOG_INFO("Loaded python function " << name);
  return pfunc;
}

template <typename T>
void SetValue(const char *name, T v, PyObject *obj) {
  auto pValue = PyLong_FromLong((int64_t)v);
  PyObject_SetAttrString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetValue(const char *name, const void *v, PyObject *obj) {
  auto pValue = PyLong_FromVoidPtr(const_cast<void *>(v));
  PyObject_SetAttrString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetValue(const char *name, double v, PyObject *obj) {
  auto pValue = PyFloat_FromDouble(v);
  PyObject_SetAttrString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetValue(const char *name, float v, PyObject *obj) {
  SetValue(name, static_cast<double>(v), obj);
}

template <>
void SetValue(const char *name, const char *v, PyObject *obj) {
  auto pValue = PyUnicode_DecodeFSDefault(v);
  PyObject_SetAttrString(obj, name, pValue);
  Py_XDECREF(pValue);
}

static inline PyObject *CreateObject(const void *native = nullptr,
                                     const char *type = nullptr) {
  auto out = PyObject_CallObject(kCreateObjectPyFunc, nullptr);
  PrintPyError();
  if (native) {
    SetValue("__native__", native, out);
  }
  if (type) {
    SetValue("__type__", type, out);
  }
  return out;
}

struct LockGIL {
  LockGIL() { m.lock(); }
  ~LockGIL() { m.unlock(); }
  std::mutex m;
};

void InitalizePy() {
  auto tmp = getenv("PYTHONPATH");
  std::string path = tmp ? tmp : "";
  setenv("PYTHONPATH", ("./algos:./algos/revisions:" + path).c_str(), 1);
  Py_InitializeEx(0);  // no signal registration
  if (!PyEval_ThreadsInitialized()) PyEval_InitThreads();
  LockGIL lock;
  std::ofstream of("./algos/__create_object__.py");
  if (!of.good()) {
    LOG_ERROR("Failed to write ./algos/__create_object__.py");
  }
  of << kRawPy;
  of.close();
  auto pname = PyUnicode_DecodeFSDefault("__create_object__");
  auto pmodule = PyImport_Import(pname);
  Py_XDECREF(pname);
  PrintPyError();
  kCreateObjectPyFunc = GetCallable(pmodule, "create_object");
  Py_XDECREF(pmodule);
  kSecurityTuple = CreateObject(nullptr, "security_tuple");
  PrintPyError();
  LOG_INFO("Python initialized");
  LOG_INFO("Python PATH: " << getenv("PYTHONPATH"));
  if (kCreateObjectPyFunc) {
    kConstants = CreateObject();
    PyObject_SetAttrString(kConstants, "security_tuple", kSecurityTuple);
    SetValue("order_side_buy", kBuy, kConstants);
    SetValue("order_side_sell", kSell, kConstants);
    SetValue("order_side_short", kShort, kConstants);
    SetValue("order_type_market", kMarket, kConstants);
    SetValue("order_type_limit", kLimit, kConstants);
    SetValue("order_type_stop", kStop, kConstants);
    SetValue("order_type_stop_limit", kStopLimit, kConstants);
    SetValue("order_type_otc", kOTC, kConstants);
    SetValue("order_status_new", kNew, kConstants);
    SetValue("order_status_partially_filled", kPartiallyFilled, kConstants);
    SetValue("order_status_filled", kFilled, kConstants);
    SetValue("order_status_done_for_day", kDoneForDay, kConstants);
    SetValue("order_status_canceled", kCanceled, kConstants);
    SetValue("order_status_replaced", kReplaced, kConstants);
    SetValue("order_status_stopped", kStopped, kConstants);
    SetValue("order_status_rejected", kRejected, kConstants);
    SetValue("order_status_suspended", kSuspended, kConstants);
    SetValue("order_status_pending_new", kPendingNew, kConstants);
    SetValue("order_status_calculated", kCalculated, kConstants);
    SetValue("order_status_expired", kExpired, kConstants);
    SetValue("order_status_pending_replace", kPendingReplace, kConstants);
    SetValue("order_status_risk_rejected", kRiskRejected, kConstants);
    SetValue("order_status_cancel_rejected", kCancelRejected, kConstants);
    SetValue("order_status_unconfirmed_new", kUnconfirmedNew, kConstants);
    SetValue("order_status_unconfirmed_cancel", kUnconfirmedCancel, kConstants);
    SetValue("order_status_unconfirmed_replace", kUnconfirmedReplace,
             kConstants);
    SetValue("tif_day", kDay, kConstants);
    SetValue("tif_gtc", kGoodTillCancel, kConstants);
    SetValue("tif_opg", kAtTheOpening, kConstants);
    SetValue("tif_ioc", kImmediateOrCancel, kConstants);
    SetValue("tif_fok", kFillOrKill, kConstants);
    SetValue("tif_gtx", kGoodTillCrossing, kConstants);
    SetValue("tif_GoodTillDate", kGoodTillDate, kConstants);
    SetValue("exec_trans_type_new", kTransNew, kConstants);
    SetValue("exec_trans_type_cancel", kTransCancel, kConstants);
    SetValue("exec_trans_type_correct", kTransCorrect, kConstants);
    SetValue("exec_trans_type_status", kTransStatus, kConstants);
    SetValue("security_type_stock", kStock.c_str(), kConstants);
    SetValue("security_type_commdity", kCommodity.c_str(), kConstants);
    SetValue("security_type_fx", kForexPair.c_str(), kConstants);
    SetValue("security_type_future", kFuture.c_str(), kConstants);
    SetValue("security_type_option", kOption.c_str(), kConstants);
    SetValue("security_type_index", kIndex.c_str(), kConstants);
    SetValue("security_type_future_option", kFutureOption.c_str(), kConstants);
    SetValue("security_type_combo", kCombo.c_str(), kConstants);
    SetValue("security_type_warrant", kWarrant.c_str(), kConstants);
    SetValue("security_type_bond", kBond.c_str(), kConstants);
  }
}

PyModule LoadPyModule(const std::string &module_name) {
  PyModule m;
  if (!kCreateObjectPyFunc) {
    LOG_ERROR("create_object function not loaded");
    return m;
  }
  LockGIL locker;
  PyErr_Clear();
  auto pname = PyUnicode_DecodeFSDefault(module_name.c_str());
  auto pmodule = PyImport_Import(pname);
  Py_XDECREF(pname);
  if (!pmodule) {
    PrintPyError();
    LOG_ERROR("Can not load " << module_name);
    Py_XDECREF(pmodule);
    return m;
  } else {
    LOG_INFO(module_name + " loaded");
    auto pfunc = GetCallable(pmodule, "get_param_defs");
    if (!pfunc) {
      LOG_ERROR("Can not find function \"get_param_defs\" in " << module_name);
      Py_XDECREF(pmodule);
      return m;
    }
    m.get_param_defs = pfunc;
    m.on_start = GetCallable(pmodule, "on_start");
    m.on_stop = GetCallable(pmodule, "on_stop");
    m.on_market_trade = GetCallable(pmodule, "on_market_trade");
    m.on_market_quote = GetCallable(pmodule, "on_market_quote");
    m.on_confirmation = GetCallable(pmodule, "on_confirmation");
  }
  Py_XDECREF(pmodule);
  return m;
}

template <typename T>
static inline bool GetValueScalar(PyObject *pvalue, T *out) {
  if (IsType(pvalue, "security_tuple")) {
    *out = SecurityTuple{};
  } else if (PyFloat_Check(pvalue)) {
    *out = PyFloat_AsDouble(pvalue);
  } else if (PyLong_Check(pvalue)) {
    *out = PyLong_AsLong(pvalue);
  } else if (pvalue == Py_True) {
    *out = true;
  } else if (pvalue == Py_False) {
    *out = false;
  } else if (PyUnicode_Check(pvalue)) {
    *out = GetString(pvalue);
  } else {
    PyErr_Clear();
    return false;
  }
  return true;
}

static inline bool ParseParamDef(PyObject *item, ParamDef *out) {
  // (name, default_value, required, min_value, max_value, precision)
  PyObject *pvalue = nullptr;
  PyObject *tmp = nullptr;
  ParamDef::ValueVector value_vector;
  auto n2 = PyTuple_Size(item);
  size_t n3;
  size_t i;
  if (n2 < 2) goto fail;
  tmp = PyTuple_GetItem(item, 0);
  if (!PyUnicode_Check(tmp)) goto fail;
  out->name = GetString(tmp);
  Py_XDECREF(tmp);
  pvalue = PyTuple_GetItem(item, 1);
  if (!pvalue) goto fail;
  if (PyTuple_Check(pvalue)) {
    n3 = PyTuple_Size(pvalue);
    value_vector.resize(n3);
    for (i = 0; i < n3; ++i) {
      tmp = PyTuple_GetItem(pvalue, i);
      if (!GetValueScalar(tmp, &value_vector[i])) goto fail;
      Py_XDECREF(tmp);
    }
    out->default_value = value_vector;
  } else {
    if (!GetValueScalar(pvalue, &out->default_value)) goto fail;
  }
  Py_XDECREF(pvalue);
  if (n2 == 2) return true;
  tmp = PyTuple_GetItem(item, 2);
  out->required = PyObject_IsTrue(tmp);
  Py_XDECREF(tmp);
  if (n2 == 3) return true;
  tmp = PyTuple_GetItem(item, 3);
  out->min_value = GetDouble(tmp);
  Py_XDECREF(tmp);
  if (n2 == 4) return true;
  tmp = PyTuple_GetItem(item, 4);
  out->max_value = GetDouble(tmp);
  Py_XDECREF(tmp);
  if (n2 == 5) return true;
  tmp = PyTuple_GetItem(item, 5);
  out->precision = GetDouble(tmp);
  Py_XDECREF(tmp);
  return true;

fail:
  Py_XDECREF(pvalue);
  Py_XDECREF(tmp);
  return false;
}

static ParamDefs ParseParamDefs(PyObject *pyfunc) {
  LockGIL locker;
  PyErr_Clear();
  auto pargs = PyTuple_New(1);
  PyTuple_SetItem(pargs, 0, kConstants);
  Py_XINCREF(kConstants);
  auto out = PyObject_CallObject(pyfunc, pargs);
  Py_XDECREF(pargs);
  if (PrintPyError()) {
    return {};
  }
  if (!out || !PyTuple_Check(out)) {
    Py_XDECREF(out);
    LOG_ERROR("get_param_defs must return tuple");
    return {};
  }
  auto n = PyTuple_Size(out);
  ParamDefs defs;
  defs.resize(n);
  for (auto i = 0; i < n; ++i) {
    auto item = PyTuple_GetItem(out, i);
    if (!PyTuple_Check(item) || !ParseParamDef(item, &defs[i])) {
      PrintPyError();
      LOG_ERROR("Invalid param definition \"" << GetString(item, true) << "\"");
      Py_XDECREF(item);
      Py_XDECREF(out);
      return {};
    }
    Py_XDECREF(item);
  }
  Py_XDECREF(out);
  return defs;
}

static inline PyObject *CreateSubAccount(const SubAccount *acc) {
  auto tmp = CreateObject(acc, "account");
  SetValue("name", acc->name, tmp);
  SetValue("id", acc->id, tmp);
  return tmp;
}

void *GetNativePtr(PyObject *self, const char *name = "__native__") {
  if (PyObject_HasAttrString(self, name)) return nullptr;
  auto native = PyObject_GetAttrString(self, name);
  if (!PyLong_Check(native)) {
    return nullptr;
  }
  auto algo = PyLong_AsVoidPtr(native);
  if (PrintPyError()) {
    return nullptr;
  }
  return algo;
}

Python *GetNative(PyObject *self) {
  return reinterpret_cast<Python *>(GetNativePtr(self));
}

namespace security_methods {

static PyObject *get_tick_size(PyObject *self, PyObject *args) {
  PyErr_Clear();
  double px;
  if (!PyArg_ParseTuple(args, "d", &px)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto sec = reinterpret_cast<Security *>(GetNativePtr(self));
  return PyFloat_FromDouble(sec->GetTickSize(px));
}

static PyMethodDef get_tick_size_def = {"get_tick_size", get_tick_size,
                                        METH_VARARGS, "get_tick_size(px)"};

static PyObject *is_in_trade_period(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto sec = reinterpret_cast<Security *>(GetNativePtr(self));
  if (sec->IsInTradePeriod())
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

static PyMethodDef is_in_trade_period_def = {"is_in_trade_period",
                                             is_in_trade_period, METH_VARARGS,
                                             "is_in_trade_period()"};

static PyObject *get_lot_size(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto sec = reinterpret_cast<Security *>(GetNativePtr(self));
  return PyLong_FromLong(sec->lot_size);
}

static PyMethodDef get_lot_size_def = {"get_lot_size", get_lot_size,
                                       METH_VARARGS, "get_lot_size()"};

}  // namespace security_methods

static PyObject *CreateSecurity(const Security *sec) {
  auto obj = CreateObject(sec, "security");
  SetValue("id", sec->id, obj);
  SetValue("symbol", sec->symbol, obj);
  SetValue("local_symbol", sec->local_symbol, obj);
  SetValue("type", sec->type, obj);
  SetValue("currency", sec->currency, obj);
  SetValue("bbgid", sec->bbgid, obj);
  SetValue("cusip", sec->cusip, obj);
  SetValue("isin", sec->isin, obj);
  SetValue("sedol", sec->sedol, obj);
  SetValue("exchange_name", sec->exchange->name, obj);
  SetValue("odd_lot_allowed", sec->exchange->odd_lot_allowed, obj);
  RegisterFunc(&security_methods::get_tick_size_def, obj);
  RegisterFunc(&security_methods::get_lot_size_def, obj);
  RegisterFunc(&security_methods::is_in_trade_period_def, obj);
  return obj;
}

static PyObject *CreateParamsDict(const Algo::ParamMap &params) {
  auto obj = PyDict_New();
  for (auto &pair : params) {
    PyObject *tmp = nullptr;
    if (auto pval = std::get_if<bool>(&pair.second)) {
      PyObject_SetAttrString(obj, pair.first.c_str(),
                             *pval ? Py_True : Py_False);
      continue;
    } else if (auto pval = std::get_if<int32_t>(&pair.second)) {
      tmp = PyLong_FromLong(*pval);
    } else if (auto pval = std::get_if<int64_t>(&pair.second)) {
      tmp = PyLong_FromLong(*pval);
    } else if (auto pval = std::get_if<const char *>(&pair.second)) {
      tmp = PyUnicode_DecodeFSDefault(*pval);
    } else if (auto pval = std::get_if<std::string>(&pair.second)) {
      tmp = PyUnicode_DecodeFSDefault(pval->c_str());
    } else if (auto pval = std::get_if<double>(&pair.second)) {
      tmp = PyFloat_FromDouble(*pval);
    } else if (auto pval = std::get_if<SecurityTuple>(&pair.second)) {
      auto &st = *pval;
      tmp = CreateObject();
      auto src = std::get<0>(st);
      SetValue("src", DataSrc::GetStr(src), tmp);
      auto sec = std::get<1>(st);
      if (sec) {
        auto obj = CreateSecurity(sec);
        PyObject_SetAttrString(tmp, "security", obj);
        Py_XDECREF(obj);
      }
      auto acc = std::get<2>(st);
      if (acc) {
        auto obj = CreateSubAccount(acc);
        PyObject_SetAttrString(tmp, "account", obj);
        Py_XDECREF(obj);
      }
      auto side = std::get<3>(st);
      SetValue("side", side, tmp);
      auto qty = std::get<4>(st);
      SetValue("qty", qty, tmp);
    }
    if (tmp) {
      PyObject_SetAttrString(obj, pair.first.c_str(), tmp);
      Py_XDECREF(tmp);
    }
  }
  return obj;
}

template <typename T>
void SetItem(const char *name, T v, PyObject *obj) {
  auto pValue = PyLong_FromLong((int64_t)v);
  PyDict_SetItemString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetItem(const char *name, double v, PyObject *obj) {
  auto pValue = PyFloat_FromDouble(v);
  PyDict_SetItemString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetItem(const char *name, float v, PyObject *obj) {
  SetItem(name, static_cast<double>(v), obj);
}

template <>
void SetItem(const char *name, const char *v, PyObject *obj) {
  auto pValue = PyUnicode_DecodeFSDefault(v);
  PyDict_SetItemString(obj, name, pValue);
  Py_XDECREF(pValue);
}

namespace md_methods {

static PyObject *open(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.open);
}

static PyMethodDef open_def = {"get_open", open, METH_VARARGS, "get_open()"};

static PyObject *high(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.high);
}

static PyMethodDef high_def = {"get_high", high, METH_VARARGS, "get_high()"};

static PyObject *low(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.low);
}

static PyMethodDef low_def = {"get_low", low, METH_VARARGS, "get_low()"};

static PyObject *close(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.close);
}

static PyMethodDef close_def = {"get_close", close, METH_VARARGS,
                                "get_close()"};

static PyObject *qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.qty);
}

static PyMethodDef qty_def = {"get_qty", qty, METH_VARARGS, "get_qty()"};

static PyObject *vwap(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.vwap);
}

static PyMethodDef vwap_def = {"get_vwap", vwap, METH_VARARGS, "get_vwap()"};

static PyObject *volume(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(md->trade.volume);
}

static PyMethodDef volume_def = {"get_volume", volume, METH_VARARGS,
                                 "get_volume()"};

static PyObject *ask_price(PyObject *self, PyObject *args) {
  PyErr_Clear();
  size_t i = 0;
  if (!PyArg_ParseTuple(args, "|l", &i)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(
      md->depth[std::max(i, MarketData::kDepthSize)].ask_price);
}

static PyMethodDef ask_price_def = {"get_ask_price", ask_price, METH_VARARGS,
                                    "get_ask_price(level=0)"};

static PyObject *ask_size(PyObject *self, PyObject *args) {
  PyErr_Clear();
  size_t i = 0;
  if (!PyArg_ParseTuple(args, "|l", &i)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(
      md->depth[std::max(i, MarketData::kDepthSize)].ask_size);
}

static PyMethodDef ask_size_def = {"get_ask_size", ask_size, METH_VARARGS,
                                   "get_ask_size(level=0)"};

static PyObject *bid_price(PyObject *self, PyObject *args) {
  PyErr_Clear();
  size_t i = 0;
  if (!PyArg_ParseTuple(args, "|l", &i)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(
      md->depth[std::max(i, MarketData::kDepthSize)].bid_price);
}

static PyMethodDef bid_price_def = {"get_bid_price", bid_price, METH_VARARGS,
                                    "get_bid_price(level=0)"};

static PyObject *bid_size(PyObject *self, PyObject *args) {
  PyErr_Clear();
  size_t i = 0;
  if (!PyArg_ParseTuple(args, "|l", &i)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto md = reinterpret_cast<MarketData *>(GetNativePtr(self));
  return PyFloat_FromDouble(
      md->depth[std::max(i, MarketData::kDepthSize)].bid_size);
}

static PyMethodDef bid_size_def = {"get_bid_size", bid_size, METH_VARARGS,
                                   "get_bid_size(level=0)"};

}  // namespace md_methods

static PyObject *CreateMd(const MarketData *md) {
  auto obj = CreateObject(md, "md");
  RegisterFunc(&md_methods::ask_price_def, obj);
  RegisterFunc(&md_methods::bid_price_def, obj);
  RegisterFunc(&md_methods::ask_size_def, obj);
  RegisterFunc(&md_methods::bid_size_def, obj);
  RegisterFunc(&md_methods::open_def, obj);
  RegisterFunc(&md_methods::high_def, obj);
  RegisterFunc(&md_methods::low_def, obj);
  RegisterFunc(&md_methods::close_def, obj);
  RegisterFunc(&md_methods::qty_def, obj);
  RegisterFunc(&md_methods::volume_def, obj);
  RegisterFunc(&md_methods::vwap_def, obj);
  return obj;
}

namespace order_methods {

static PyObject *cancel(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto order = reinterpret_cast<Order *>(GetNativePtr(self));
  auto out = Algo::Cancel(*order);
  if (out)
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

static PyMethodDef cancel_def = {"cancel", cancel, METH_VARARGS, "cancel()"};

static PyObject *is_live(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto order = reinterpret_cast<Order *>(GetNativePtr(self));
  auto out = order->IsLive();
  if (out)
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

static PyMethodDef is_live_def = {"is_live", is_live, METH_VARARGS,
                                  "is_live()"};

static PyObject *avg_px(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto order = reinterpret_cast<Order *>(GetNativePtr(self));
  return PyFloat_FromDouble(order->avg_px);
}

static PyMethodDef avg_px_def = {"get_avg_px", avg_px, METH_VARARGS,
                                 "get_avg_px()"};

static PyObject *cum_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto order = reinterpret_cast<Order *>(GetNativePtr(self));
  return PyFloat_FromDouble(order->cum_qty);
}

static PyMethodDef cum_qty_def = {"get_cum_qty", cum_qty, METH_VARARGS,
                                  "get_cum_qty()"};

static PyObject *status(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto order = reinterpret_cast<Order *>(GetNativePtr(self));
  return PyLong_FromLong(order->status);
}

static PyMethodDef status_def = {"get_status", status, METH_VARARGS,
                                 "get_status()"};

}  // namespace order_methods

static PyObject *CreateOrder(Order *order, PyObject *instrument,
                             PyObject *account) {
  auto obj = CreateObject(order, "order");
  PyObject_SetAttrString(obj, "instrument", instrument);
  PyObject_SetAttrString(obj, "account", account);
  SetValue("qty", order->qty, obj);
  SetValue("price", order->price, obj);
  SetValue("type", order->type, obj);
  SetValue("side", order->side, obj);
  SetValue("tif", order->tif, obj);
  SetValue("id", order->id, obj);
  SetValue("orig_id", order->orig_id, obj);
  RegisterFunc(&order_methods::cancel_def, obj);
  RegisterFunc(&order_methods::avg_px_def, obj);
  RegisterFunc(&order_methods::cum_qty_def, obj);
  RegisterFunc(&order_methods::is_live_def, obj);
  RegisterFunc(&order_methods::status_def, obj);
  return obj;
}

namespace instrument_methods {

static PyObject *place(PyObject *self, PyObject *args) {
  PyErr_Clear();
  PyObject *account;
  double qty;
  double px;
  int64_t side;
  int64_t type = kLimit;
  int64_t tif = kDay;
  if (!PyArg_ParseTuple(args, "oddl|ll", &account, &qty, &px, &side, &type,
                        &tif)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = reinterpret_cast<Python *>(GetNativePtr(self, "__native_algo__"));
  if (!algo) Py_RETURN_NONE;
  auto instrument = reinterpret_cast<Instrument *>(GetNativePtr(self));
  if (!instrument) Py_RETURN_NONE;
  Contract c;
  c.side = static_cast<OrderSide>(side);
  c.qty = qty;
  c.price = px;
  c.type = static_cast<OrderType>(type);
  c.tif = static_cast<TimeInForce>(tif);
  if (!IsType(account, "account")) {
    LOG_ERROR("place requires account object as the first argument");
    Py_RETURN_NONE;
  }
  c.sub_account = reinterpret_cast<SubAccount *>(GetNativePtr(account));
  auto order = algo->Place(c, instrument);
  if (!order) Py_RETURN_NONE;
  auto obj = CreateOrder(order, self, account);
  algo->AddObj(order, obj);
  Py_XINCREF(obj);
  return obj;
}

static PyMethodDef place_def = {
    "place", place, METH_VARARGS,
    "place(account, qty, px, side, type=order_type_limit, tif=tif_day)"};

static PyObject *total_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->total_qty());
}

static PyMethodDef total_qty_def = {"get_total_qty", total_qty, METH_VARARGS,
                                    "get_total_qty()"};

static PyObject *total_outstanding_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->total_outstanding_qty());
}

static PyMethodDef total_outstanding_qty_def = {
    "get_total_outstanding", total_outstanding_qty, METH_VARARGS,
    "get_total_outstanding()"};

static PyObject *total_exposure(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->total_exposure());
}

static PyMethodDef total_exposure_def = {"get_total_exposure", total_exposure,
                                         METH_VARARGS, "get_total_exposure()"};

static PyObject *bought_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->bought_qty());
}

static PyMethodDef bought_qty_def = {"get_bought", bought_qty, METH_VARARGS,
                                     "get_bought()"};

static PyObject *sold_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->sold_qty());
}

static PyMethodDef sold_qty_def = {"get_sold", sold_qty, METH_VARARGS,
                                   "get_sold()"};

static PyObject *outstanding_buy_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->outstanding_buy_qty());
}

static PyMethodDef outstanding_buy_qty_def = {"get_outstanding_buy",
                                              outstanding_buy_qty, METH_VARARGS,
                                              "get_outstanding_buy()"};

static PyObject *outstanding_sell_qty(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  return PyFloat_FromDouble(inst->outstanding_sell_qty());
}

static PyMethodDef outstanding_sell_qty_def = {
    "get_outstanding_sell", outstanding_sell_qty, METH_VARARGS,
    "get_outstanding_sell()"};

static PyObject *get_active_orders(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = reinterpret_cast<Python *>(GetNativePtr(self, "__native_algo__"));
  if (!algo) Py_RETURN_NONE;
  auto inst = reinterpret_cast<Instrument *>(GetNativePtr(self));
  if (inst) {
    auto &orders = inst->active_orders();
    auto obj = PyTuple_New(orders.size());
    auto i = 0;
    for (auto o : orders) {
      auto obj2 = algo->GetObj(o);
      PyTuple_SetItem(obj, i++, obj2);
      Py_XINCREF(obj2);
    }
    return obj;
  }
  Py_RETURN_NONE;
}

static PyMethodDef get_active_orders_def = {"get_active_orders",
                                            get_active_orders, METH_VARARGS,
                                            "get_active_orders()"};

}  // namespace instrument_methods

static PyObject *CreateInstrument(Algo *algo, const Instrument *inst,
                                  PyObject *sec, const char *src) {
  auto obj = CreateObject(inst, "instrument");
  auto md = CreateMd(&inst->md());
  PyObject_SetAttrString(obj, "md", md);
  Py_XDECREF(md);
  PyObject_SetAttrString(obj, "security", sec);
  SetValue("__native_algo__", algo, obj);
  SetValue("src", src, obj);
  RegisterFunc(&instrument_methods::total_qty_def, obj);
  RegisterFunc(&instrument_methods::total_outstanding_qty_def, obj);
  RegisterFunc(&instrument_methods::total_exposure_def, obj);
  RegisterFunc(&instrument_methods::bought_qty_def, obj);
  RegisterFunc(&instrument_methods::sold_qty_def, obj);
  RegisterFunc(&instrument_methods::outstanding_buy_qty_def, obj);
  RegisterFunc(&instrument_methods::outstanding_sell_qty_def, obj);
  RegisterFunc(&instrument_methods::place_def, obj);
  RegisterFunc(&instrument_methods::get_active_orders_def, obj);
  return obj;
}

namespace algo_methods {

static PyObject *log_info(PyObject *self, PyObject *args) {
  PyErr_Clear();
  const char *s;
  if (PyArg_ParseTuple(args, "s", &s)) {
    LOG_INFO(s);
  } else {
    PrintPyError();
  }
  Py_RETURN_NONE;
}

static PyMethodDef log_info_def = {"log_info", log_info, METH_VARARGS,
                                   "log_info(msg)"};

static PyObject *log_debug(PyObject *self, PyObject *args) {
  PyErr_Clear();
  const char *s;
  if (PyArg_ParseTuple(args, "s", &s)) {
    LOG_DEBUG(s);
  } else {
    PrintPyError();
  }
  Py_RETURN_NONE;
}

static PyMethodDef log_debug_def = {"log_debug", log_debug, METH_VARARGS,
                                    "log_debug(msg)"};

static PyObject *log_error(PyObject *self, PyObject *args) {
  PyErr_Clear();
  const char *s;
  if (PyArg_ParseTuple(args, "s", &s)) {
    LOG_ERROR(s);
  } else {
    PrintPyError();
  }
  Py_RETURN_NONE;
}

static PyMethodDef log_error_def = {"log_error", log_error, METH_VARARGS,
                                    "log_error(msg)"};

static PyObject *log_warn(PyObject *self, PyObject *args) {
  PyErr_Clear();
  const char *s;
  if (PyArg_ParseTuple(args, "s", &s)) {
    LOG_WARN(s);
  } else {
    PrintPyError();
  }
  Py_RETURN_NONE;
}

static PyMethodDef log_warn_def = {"log_warn", log_warn, METH_VARARGS,
                                   "log_warn(msg)"};

static PyObject *is_active(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = GetNative(self);
  if (algo) {
    if (algo->is_active())
      Py_RETURN_TRUE;
    else
      Py_RETURN_FALSE;
  }
  Py_RETURN_NONE;
}

static PyMethodDef is_active_def = {"is_active", is_active, METH_VARARGS,
                                    "is_active()"};

static PyObject *stop(PyObject *self, PyObject *args) {
  PyErr_Clear();
  if (!PyArg_ParseTuple(args, "")) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = GetNative(self);
  if (algo) algo->Stop();
  Py_RETURN_NONE;
}

static PyMethodDef stop_def = {"stop", stop, METH_VARARGS, "stop()"};

static PyObject *set_timeout(PyObject *self, PyObject *args) {
  PyErr_Clear();
  PyObject *pfunc;
  auto ms = 0l;
  if (!PyArg_ParseTuple(args, "ol", &pfunc, &ms)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  if (!PyCallable_Check(pfunc)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  Py_XINCREF(pfunc);
  auto algo = GetNative(self);
  if (algo) {
    algo->SetTimeout(
        [pfunc]() {
          LockGIL lock;
          Py_XDECREF(PyObject_CallObject(pfunc, nullptr));
          PrintPyError();
          Py_XDECREF(pfunc);
        },
        ms);
  }
  Py_RETURN_NONE;
}

static PyMethodDef set_timeout_def = {"set_timeout", set_timeout, METH_VARARGS,
                                      "set_timeout(callable, milliseconds)"};

static PyObject *subscribe(PyObject *self, PyObject *args) {
  PyErr_Clear();
  PyObject *sec;
  const char *src = "";
  if (!PyArg_ParseTuple(args, "o|s", &sec, src)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = GetNative(self);
  if (!IsType(sec, "security")) {
    LOG_ERROR("subscribe requires security object as the first argument")
    Py_RETURN_NONE;
  }
  if (algo) {
    auto inst = algo->Subscribe(
        *reinterpret_cast<Security *>(GetNativePtr(sec)), DataSrc::GetId(src));
    if (!inst) {
      Py_RETURN_NONE;
    }
    auto obj = CreateInstrument(algo, inst, sec, src);
    algo->AddObj(inst, obj);
    Py_XINCREF(obj);
    return obj;
  }
  Py_RETURN_NONE;
}

static PyMethodDef subscribe_def = {"subscribe", subscribe, METH_VARARGS,
                                    "subscribe(security, src=None)"};

}  // namespace algo_methods

static PyObject *CreateAlgo(Algo *algo) {
  LockGIL lock;
  auto obj = CreateObject(algo, "algo");
  PyObject_SetAttrString(obj, "constants", kConstants);
  RegisterFunc(&algo_methods::stop_def, obj);
  RegisterFunc(&algo_methods::is_active_def, obj);
  RegisterFunc(&algo_methods::subscribe_def, obj);
  RegisterFunc(&algo_methods::set_timeout_def, obj);
  RegisterFunc(&algo_methods::log_debug_def, obj);
  RegisterFunc(&algo_methods::log_info_def, obj);
  RegisterFunc(&algo_methods::log_warn_def, obj);
  RegisterFunc(&algo_methods::log_error_def, obj);
  SetValue("name", algo->name().c_str(), obj);
  SetValue("id", algo->id(), obj);
  return obj;
}

Python::Python() {}

Python::~Python() {
  Py_XDECREF(obj_);
  for (auto &pair : objs_) {
    Py_XDECREF(pair.second);
  }
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
  PyErr_Clear();
  auto pargs = PyTuple_New(2);
  PyTuple_SetItem(pargs, 0, obj_);
  Py_XINCREF(obj_);
  PyTuple_SetItem(pargs, 1, CreateParamsDict(params));
  auto out = PyObject_CallObject(py_.on_start, pargs);
  PrintPyError();
  Py_XDECREF(pargs);
  if (out && PyUnicode_Check(out)) {
    Py_XDECREF(out);
    return GetString(out);
  }
  Py_XDECREF(out);
  return {};
}

void Python::OnStop() noexcept {
  if (!py_.on_stop) return;
  LockGIL locker;
  PyErr_Clear();
  auto pargs = PyTuple_New(1);
  PyTuple_SetItem(pargs, 0, obj_);
  Py_XINCREF(obj_);
  Py_XDECREF(PyObject_CallObject(py_.on_stop, pargs));
  PrintPyError();
  Py_XDECREF(pargs);
}

void Python::OnMarketTrade(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_trade) return;
  LockGIL locker;
  PyErr_Clear();
  auto pargs = PyTuple_New(2);
  PyTuple_SetItem(pargs, 0, obj_);
  Py_XINCREF(obj_);
  auto obj = GetObj(&inst);
  PyTuple_SetItem(pargs, 1, obj);
  Py_XINCREF(obj);
  Py_XDECREF(PyObject_CallObject(py_.on_market_trade, pargs));
  PrintPyError();
  Py_XDECREF(pargs);
}

void Python::OnMarketQuote(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {
  if (!py_.on_market_quote) return;
  LockGIL locker;
  PyErr_Clear();
  auto pargs = PyTuple_New(2);
  PyTuple_SetItem(pargs, 0, obj_);
  Py_XINCREF(obj_);
  auto obj = GetObj(&inst);
  PyTuple_SetItem(pargs, 1, obj);
  Py_XINCREF(obj);
  Py_XDECREF(PyObject_CallObject(py_.on_market_quote, pargs));
  PrintPyError();
  Py_XDECREF(pargs);
}

void Python::OnConfirmation(const Confirmation &cm) noexcept {
  if (!py_.on_confirmation) return;
  LockGIL locker;
  PyErr_Clear();
  auto pargs = PyTuple_New(2);
  PyTuple_SetItem(pargs, 0, obj_);
  Py_XINCREF(obj_);
  auto dict = PyDict_New();
  PyObject_SetAttrString(dict, "order", GetObj(cm.order));
  SetItem("order_id", cm.order_id.c_str(), dict);
  SetItem("exec_id", cm.exec_id.c_str(), dict);
  SetItem("text", cm.text.c_str(), dict);
  SetItem("exec_type", cm.exec_type, dict);
  SetItem("exec_trans_type", cm.exec_trans_type, dict);
  SetItem("last_px", cm.last_px, dict);
  SetItem("last_shares", cm.last_shares, dict);
  SetItem("transaction_time", cm.transaction_time, dict);
  PyTuple_SetItem(pargs, 1, dict);
  Py_XDECREF(PyObject_CallObject(py_.on_confirmation, pargs));
  PrintPyError();
  Py_XDECREF(pargs);
}

const ParamDefs &Python::GetParamDefs() noexcept { return def_; }

}  // namespace opentrade
