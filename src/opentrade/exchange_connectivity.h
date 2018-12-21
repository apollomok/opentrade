#ifndef OPENTRADE_EXCHANGE_CONNECTIVITY_H_
#define OPENTRADE_EXCHANGE_CONNECTIVITY_H_

#include "adapter.h"
#include "order.h"

namespace opentrade {

class ExchangeConnectivityAdapter : public virtual NetworkAdapter {
 public:
  virtual std::string Place(const Order& ord) noexcept = 0;
  virtual std::string Cancel(const Order& ord) noexcept = 0;
  void HandleNew(Order::IdType id, const std::string& order_id,
                 int64_t transaction_time = 0);
  void HandleSuspended(Order::IdType id, const std::string& order_id,
                       int64_t transaction_time = 0);
  void HandlePendingNew(Order::IdType id, const std::string& text,
                        int64_t transaction_time = 0);
  void HandlePendingCancel(Order::IdType id, Order::IdType orig_id,
                           int64_t transaction_time = 0);
  void HandleFill(Order::IdType id, double qty, double price,
                  const std::string& exec_id, int64_t transaction_time = 0,
                  bool is_partial = false,
                  ExecTransType exec_trans_type = kTransNew,
                  Confirmation::StrMap* misc = nullptr);
  void HandleCanceled(Order::IdType id, Order::IdType orig_id,
                      const std::string& text, int64_t transaction_time = 0);
  void HandleNewRejected(Order::IdType id, const std::string& text,
                         int64_t transaction_time = 0);
  void HandleCancelRejected(Order::IdType id, Order::IdType orig_id,
                            const std::string& text,
                            int64_t transaction_time = 0);
  void HandleOthers(Order::IdType id, OrderStatus exec_type,
                    const std::string& text, int64_t transaction_time = 0);
};

class ExchangeConnectivityManager
    : public AdapterManager<ExchangeConnectivityAdapter>,
      public Singleton<ExchangeConnectivityManager> {
 public:
  bool Place(Order* ord);
  bool Cancel(const Order& orig_ord);
  ExchangeConnectivityAdapter* Get(const std::string& name) {
    auto out = GetAdapter(name);
    if (out) return out;
    return GetAdapter("ec_" + name);
  }
};

}  // namespace opentrade

#endif  // OPENTRADE_EXCHANGE_CONNECTIVITY_H_
