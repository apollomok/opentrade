#include "python.h"

#include "logger.h"

namespace opentrade {

static PyObject *kCreateObjectPyFunc;
static PyObject *kSecurityTuple;
static PyObject *kEmptyArgs;
static const char *kRawPy = R"(# do not modify me
class Object: pass
def create_object(): return Object())";

static inline PyObject *CreateObject() {
  return PyObject_CallObject(kCreateObjectPyFunc, kEmptyArgs);
}

static inline std::string GetString(PyObject *obj) {
  if (!obj) return "";
  Py_ssize_t size;
  auto tmp = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!tmp) return "";
  return std::string(tmp, size);
}

static inline PyObject *GetCallable(PyObject *m, const char *name) {
  auto pfunc = PyObject_GetAttrString(m, name);
  if (!pfunc || !PyCallable_Check(pfunc)) {
    Py_XDECREF(pfunc);
    return NULL;
  }
  LOG_INFO("Loaded callback " << name);
  return pfunc;
}

static void PrintPyError() {
  if (!PyErr_Occurred()) return;
  PyObject *ptype = NULL;
  PyObject *pvalue = NULL;
  PyObject *ptraceback = NULL;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  if (!pvalue) return;
  std::stringstream str;
  auto text = GetString(pvalue);
  PyErr_Restore(ptype, pvalue, ptraceback);  // does not increase ref
  LOG_ERROR(text);
  PyErr_Print();
}

void InitalizePy() {
  auto tmp = getenv("PYTHONPATH");
  std::string path = tmp ? tmp : "";
  setenv("PYTHONPATH", (".:" + path).c_str(), 1);
  Py_InitializeEx(0);  // no signal registration
  std::ofstream of("./__create_object__.py");
  if (!of.good()) {
    LOG_ERROR("Failed to write ./__create_object__.py");
  }
  of << kRawPy;
  of.close();
  auto pname = PyUnicode_DecodeFSDefault("__create_object__");
  auto pmodule = PyImport_Import(pname);
  Py_XDECREF(pname);
  PrintPyError();
  kCreateObjectPyFunc = GetCallable(pmodule, "create_object");
  kSecurityTuple = PyTuple_New(0);
  kEmptyArgs = PyTuple_New(0);
  PrintPyError();
  LOG_INFO("Python initialized");
  LOG_INFO("Python PATH: " << getenv("PYTHONPATH"));
}

struct LockGIL {
  LockGIL() { kGILMutex.lock(); }
  ~LockGIL() { kGILMutex.unlock(); }
  static inline std::mutex kGILMutex;
};

PyModule LoadPyModule(const std::string &fn) {
  PyModule m;
  if (!kCreateObjectPyFunc) {
    LOG_ERROR("create_object function not loaded");
    return m;
  }
  if (!std::ifstream("./" + fn).good()) {
    LOG_ERROR("Can not find " << fn);
    return m;
  }
  LockGIL locker;
  auto pname = PyUnicode_DecodeFSDefault(fn.substr(0, fn.length() - 3).c_str());
  auto pmodule = PyImport_Import(pname);
  Py_XDECREF(pname);
  if (!pmodule) {
    PrintPyError();
    LOG_ERROR("Can not load " << fn);
    Py_XDECREF(pmodule);
    return m;
  } else {
    LOG_INFO(fn + " loaded");
    auto pfunc = GetCallable(pmodule, "get_param_defs");
    if (!pfunc) {
      LOG_ERROR("Can not find function \"get_param_defs\" in " << fn);
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

static void RegisterFunc(PyMethodDef *def, PyObject *obj) {
  PyErr_Clear();
  auto pfunc = PyCFunction_New(def, obj);
  PrintPyError();
  PyObject_SetAttrString(obj, def->ml_name, pfunc);
  PrintPyError();
  Py_XDECREF(pfunc);
}

template <typename T>
void SetValue(const char *name, T v, PyObject *obj) {
  auto pValue = PyLong_FromLong((int64_t)v);
  PyObject_SetAttrString(obj, name, pValue);
  Py_XDECREF(pValue);
}

template <>
void SetValue(const char *name, void *v, PyObject *obj) {
  auto pValue = PyLong_FromVoidPtr(v);
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

void *GetNativePtr(PyObject *self, const char *name = "__native__") {
  auto native = PyObject_GetAttrString(self, name);
  if (!native) {
    PrintPyError();
    return NULL;
  }
  auto algo = PyLong_AsVoidPtr(native);
  if (PyErr_Occurred()) {
    PrintPyError();
    return NULL;
  }
  return algo;
}

Algo *GetNative(PyObject *self) {
  return reinterpret_cast<Algo *>(GetNativePtr(self));
}

namespace security_methods {}

static PyObject *CreateSecurity(Security *sec) {
  auto obj = CreateObject();
  SetValue("__native__", sec, obj);
  return obj;
}

namespace instrument_methods {}

static PyObject *CreateInstrument(Instrument *inst) {
  auto obj = CreateObject();
  SetValue("__native__", inst, obj);
  return obj;
}

namespace algo_methods {

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

static PyObject *subscribe(PyObject *self, PyObject *args) {
  PyErr_Clear();
  PyObject *sec;
  auto src = 0l;
  if (!PyArg_ParseTuple(args, "o|l", &sec, src)) {
    PrintPyError();
    Py_RETURN_NONE;
  }
  auto algo = GetNative(self);
  if (algo) {
    auto inst =
        algo->Subscribe(*reinterpret_cast<Security *>(GetNativePtr(sec)), src);
    if (!inst) {
      Py_RETURN_NONE;
    }
    return CreateInstrument(inst);
  }
  Py_RETURN_NONE;
}

static PyMethodDef subscribe_def = {"subscribe", subscribe, METH_VARARGS,
                                    "subscribe(security, src=None)"};

}  // namespace algo_methods

static PyObject *CreateAlgo(Algo *algo) {
  auto obj = CreateObject();
  SetValue("__native__", algo, obj);
  RegisterFunc(&algo_methods::stop_def, obj);
  RegisterFunc(&algo_methods::subscribe_def, obj);
  return obj;
}

Python *Python::Load(const std::string &fn) {
  auto p = new Python;
  auto m = LoadPyModule(fn);
  if (!m.get_param_defs) return nullptr;
  p->create_func_ = [p]() {
    auto p2 = new Python;
    p2->py_ = p->py_;
    return p2;
  };
  return p;
}

std::string Python::OnStart(const ParamMap &params) noexcept { return ""; }

void Python::OnStop() noexcept {}

void Python::OnMarketTrade(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {}

void Python::OnMarketQuote(const Instrument &inst, const MarketData &md,
                           const MarketData &md0) noexcept {}

void Python::OnConfirmation(const Confirmation &cm) noexcept {}

const ParamDefs &Python::GetParamDefs() noexcept {
  static ParamDefs def;
  return def;
}

}  // namespace opentrade
