#ifndef OPENTRADE_DATABASE_H_
#define OPENTRADE_DATABASE_H_

#include <soci.h>
#include <boost/type_index.hpp>
#include <cstring>
#include <memory>
#include <string>
#include <typeinfo>

#include "logger.h"

namespace opentrade {

class Database {
 public:
  static void Initialize(const std::string& url, uint8_t pool_size,
                         bool create_tables);
  static auto Session() { return std::make_unique<soci::session>(*pool_); }

  template <typename T>
  static T Get(soci::row const& row, int index) {
    try {
      return row.get<T>(index);
    } catch (const std::bad_cast& e) {
      auto type = "unknown";
      switch (row.get_properties(index).get_data_type()) {
        case soci::dt_string:
          type = "soci::dt_string";
          break;
        case soci::dt_date:
          type = "soci::dt_date";
          break;
        case soci::dt_double:
          type = "soci::dt_double";
          break;
        case soci::dt_integer:
          type = "soci::dt_integer";
          break;
        case soci::dt_long_long:
          type = "soci::dt_long_long";
          break;
        case soci::dt_unsigned_long_long:
          type = "soci::dt_unsigned_long_long";
          break;
      }
      LOG_ERROR("Failed to cast '"
                << row.get_properties(index).get_name() << "', expected '"
                << type << "' compatible, but you set '"
                << boost::typeindex::type_id<T>().pretty_name() << "'");
      throw e;
    }
  }

  template <typename T>
  static T GetValue(soci::row const& row, int index, T default_value) {
    if (row.get_indicator(index) != soci::i_null)
      return Get<T>(row, index);
    else
      return default_value;
  }

  static const char* GetValue(soci::row const& row, int index,
                              const char* default_value) {
    if (row.get_indicator(index) != soci::i_null)
      return strdup(Get<std::string>(row, index).c_str());
    else
      return default_value;
  }

 private:
  inline static soci::connection_pool* pool_ = nullptr;
};

}  // namespace opentrade

#endif  // OPENTRADE_DATABASE_H_
