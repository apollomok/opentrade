#ifndef OPENTRADE_COMMON_H_
#define OPENTRADE_COMMON_H_

#include <boost/filesystem.hpp>
#include <string>

#include "task_pool.h"

namespace opentrade {

static inline const std::string kEmptyStr;
namespace fs = boost::filesystem;
static inline const fs::path kAlgoPath = fs::path(".") / "algos";
static inline const fs::path kStorePath = fs::path(".") / "store";

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
