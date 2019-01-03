#ifndef OPENTRADE_ADAPTER_H_
#define OPENTRADE_ADAPTER_H_

#include <tbb/atomic.h>
#include <memory>
#include <string>
#include <unordered_map>

#include "utility.h"

namespace opentrade {

static const char* kApiVersion = "1";

class Adapter {
 public:
  virtual ~Adapter();
  typedef std::unordered_map<std::string, std::string> StrMap;
  const std::string& name() const { return name_; }
  void set_name(const std::string& name) { name_ = name; }
  void set_config(const StrMap& config) { config_ = config; }
  std::string GetVersion() const {
#ifdef BACKTEST
    return std::string("backtest_") + kApiVersion;
#endif
    return kApiVersion;
  }
  typedef Adapter* (*CFunc)();
  typedef std::function<Adapter*()> Func;
  Adapter* Clone() {
    auto inst = create_func_();
    inst->set_name(name());
    inst->set_config(config());
    return inst;
  }
  const StrMap& config() const { return config_; }
  std::string config(const std::string& name) const {
    return FindInMap(config_, name);
  }
  static Adapter* Load(const std::string& sofile);
  virtual void Start() noexcept = 0;

 protected:
  std::string name_;
  StrMap config_;
  Func create_func_;
};

class NetworkAdapter : public Adapter {
 public:
  virtual void Reconnect() noexcept {}
  virtual bool connected() const noexcept { return 1 == connected_; }

 protected:
  tbb::atomic<int> connected_ = 0;
};

template <typename T>
class AdapterManager {
 public:
  typedef std::unordered_map<std::string, T*> AdapterMap;
  void Add(T* adapter) { adapters_[adapter->name()] = adapter; }
  T* GetAdapter(const std::string& name) { return FindInMap(adapters_, name); }
  const AdapterMap& adapters() { return adapters_; }

 private:
  AdapterMap adapters_;
};

}  // namespace opentrade

#endif  // OPENTRADE_ADAPTER_H_
