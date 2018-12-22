#ifndef OPENTRADE_COMMON_H_
#define OPENTRADE_COMMON_H_

#include <boost/filesystem.hpp>
#include <memory>
#include <string>
#include <unordered_map>

#include "task_pool.h"
#include "utility.h"

namespace opentrade {

static inline const std::string kEmptyStr;
namespace fs = boost::filesystem;
static inline const fs::path kAlgoPath = fs::path(".") / "algos";
static inline const fs::path kStorePath = fs::path(".") / "store";

struct ParamsBase {
  typedef std::unordered_map<std::string, std::string> StrMap;
  typedef std::shared_ptr<const StrMap> StrMapPtr;

  std::string GetParam(const std::string& k) const {
    return FindInMap(params_, k);
  }

  StrMapPtr params() const { return params_; }

  std::string set_params(const std::string& params) {
    auto tmp = std::make_shared<StrMap>();
    for (auto& str : Split(params, ",;\n")) {
      char k[str.size()];
      char v[str.size()];
      if (sscanf(str.c_str(), "%s=%s", k, v) != 2) {
        return "Invalid params format, expect <name>=<value>[,;\n]...'";
      }
      tmp->emplace((const char*)k, (const char*)v);
    }
    std::atomic_thread_fence(std::memory_order_release);
    params_ = tmp;
    return {};
  }

  std::string GetParamsString() const {
    std::string out;
    for (auto& pair : *params_) {
      if (!out.empty()) out += "\n";
      out += pair.first + "=" + pair.second;
    }
    return out;
  }

 private:
  StrMapPtr params_ = std::make_shared<const StrMap>();
};

template <typename V>
class Singleton {
 public:
  static V& Instance() {
    static V kInstance;
    return kInstance;
  }

 protected:
  Singleton() {}
};

inline TaskPool kSharedTaskPool;
inline TaskPool kWriteTaskPool;
}  // namespace opentrade

#endif  // OPENTRADE_COMMON_H_
