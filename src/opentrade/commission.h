#ifndef OPENTRADE_COMMISSION_H_
#define OPENTRADE_COMMISSION_H_

#include "adapter.h"

namespace opentrade {

struct Commission {
  struct {
    double per_share = 0;
    double per_value = 0;
  } buy;
  struct {
    double per_share = 0;
    double per_value = 0;
  } sell;
};

struct CommissionAdapter : public Adapter {};

struct CommissionManager : public AdapterManager<CommissionAdapter, kCmPrefix>,
                           public Singleton<CommissionManager> {};

}  // namespace opentrade

#endif  // OPENTRADE_COMMISSION_H_
