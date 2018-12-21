#include "exchange_connectivity.h"

#include <boost/date_time/posix_time/posix_time.hpp>

#include "logger.h"
#include "risk.h"

namespace opentrade {

static inline void UpdateThrottle(const Order& ord) {
  auto tm = GetTime();
  const_cast<SubAccount*>(ord.sub_account)->throttle_in_sec.Update(tm);
  const_cast<BrokerAccount*>(ord.broker_account)->throttle_in_sec.Update(tm);
  const_cast<User*>(ord.user)->throttle_in_sec.Update(tm);
  if (ord.sub_account->limits.msg_rate_per_security > 0)
    const_cast<SubAccount*>(ord.sub_account)
        ->throttle_per_security_in_sec[ord.sec->id]
        .Update(tm);
  if (ord.broker_account->limits.msg_rate_per_security > 0)
    const_cast<BrokerAccount*>(ord.broker_account)
        ->throttle_per_security_in_sec[ord.sec->id]
        .Update(tm);
  if (ord.user->limits.msg_rate_per_security > 0)
    const_cast<User*>(ord.user)
        ->throttle_per_security_in_sec[ord.sec->id]
        .Update(tm);
}

static inline void HandleConfirmation(Order* ord, OrderStatus exec_type,
                                      const std::string& text = "",
                                      int64_t tm = 0) {
  auto cm = std::make_shared<Confirmation>();
  cm->order = ord;
  cm->exec_type = exec_type;
  if (exec_type == kNew)
    cm->order_id = text;
  else
    cm->text = text;
  cm->transaction_time = tm ? tm : NowUtcInMicro();
  GlobalOrderBook::Instance().Handle(cm);
}

template <typename T>
static inline void Handle(const std::string& name, Order::IdType id, T desc,
                          OrderStatus exec_type, const std::string& text,
                          int64_t transaction_time) {
  auto ord = GlobalOrderBook::Instance().Get(id);
  if (!ord) {
    LOG_DEBUG(name << ": Unknown ClOrdId of " << desc << " confirmation: " << id
                   << ", ignored");
    return;
  }
  HandleConfirmation(ord, exec_type, text, transaction_time);
}

template <typename T>
static inline void Handle(const std::string& name, Order::IdType id,
                          Order::IdType orig_id, T desc, OrderStatus exec_type,
                          const std::string& text, int64_t transaction_time) {
  if (!orig_id) {
    auto ord = GlobalOrderBook::Instance().Get(id);
    if (!ord) {
      LOG_DEBUG(name << ": Unknown ClOrdId of " << desc
                     << " confirmation: " << id << ", ignored");
      return;
    }
    if (ord->orig_id)
      orig_id = ord->orig_id;
    else
      orig_id = id;
  }
  Handle(name, orig_id, desc, exec_type, text, transaction_time);
}

static inline void HandleConfirmation(Order* ord, double qty, double price,
                                      const std::string& exec_id, int64_t tm,
                                      bool is_partial,
                                      ExecTransType exec_trans_type,
                                      Confirmation::StrMap* misc = nullptr) {
  auto cm = std::make_shared<Confirmation>();
  cm->order = ord;
  cm->exec_type = is_partial ? kPartiallyFilled : kFilled;
  cm->last_shares = qty;
  cm->last_px = price;
  cm->exec_id = exec_id;
  cm->exec_trans_type = exec_trans_type;
  cm->transaction_time = tm ? tm : NowUtcInMicro();
  cm->misc = misc;
  GlobalOrderBook::Instance().Handle(cm);
}

static inline bool CheckAdapter(ExchangeConnectivityAdapter* adapter,
                                const char* name) {
  if (!adapter) {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "Exchange connectivity adapter '%s' is not started", name);
    kRiskError = buf;
    return false;
  }
  if (!adapter->connected()) {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "Exchange connectivity adapter '%s' is disconnected", name);
    kRiskError = buf;
    return false;
  }
  return true;
}

bool ExchangeConnectivityManager::Place(Order* ord) {
  assert(ord->qty > 0);
  kRiskError.clear();
  assert(ord->sub_account);
  assert(ord->sec);
  assert(ord->user);
  if (!ord->sub_account) return false;
  if (!ord->sec) return false;
  if (!ord->user) return false;
  if (!ord->user->GetSubAccount(ord->sub_account->id)) {
    char buf[256];
    snprintf(buf, sizeof(buf), "Not permissioned to trade with sub account: %s",
             ord->sub_account->name);
    kRiskError = buf;
    HandleConfirmation(ord, kRiskRejected, kRiskError);
    return false;
  }
  auto exchange = ord->sec->exchange;
  auto broker = ord->sub_account->GetBrokerAccount(exchange->id);
  if (!broker) {
    char buf[256];
    snprintf(buf, sizeof(buf), "Not permissioned to trade on exchange: %s",
             exchange->name);
    kRiskError = buf;
    HandleConfirmation(ord, kRiskRejected, kRiskError);
    return false;
  }
  ord->broker_account = broker;
  if (ord->type == kOTC) {
    ord->id = GlobalOrderBook::Instance().NewOrderId();
    ord->leaves_qty = ord->qty;
    HandleConfirmation(ord, kUnconfirmedNew);
    HandleConfirmation(ord, ord->qty, ord->price,
                       "OTC-" + std::to_string(ord->id), NowUtcInMicro(), false,
                       kTransNew);
    return true;
  }
  auto adapter = ord->broker_account->adapter;
  auto name = ord->broker_account->adapter_name;
  if (!CheckAdapter(adapter, name)) {
    HandleConfirmation(ord, kRiskRejected, kRiskError);
    return false;
  }
  if (ord->type == kMarket || ord->type == kStop) {
    if (ord->price <= 0) {
      ord->price = ord->sec->CurrentPrice();
      if (ord->price <= 0) {
        kRiskError = "Can not find last price for this security";
        HandleConfirmation(ord, kRiskRejected, kRiskError);
        return false;
      }
    }
  } else if (ord->price <= 0) {
    kRiskError = "Price can not be empty for limit order";
    HandleConfirmation(ord, kRiskRejected, kRiskError);
    return false;
  }
  if (!RiskManager::Instance().Check(*ord)) {
    HandleConfirmation(ord, kRiskRejected, kRiskError);
    return false;
  }
  ord->leaves_qty = ord->qty;
  ord->id = GlobalOrderBook::Instance().NewOrderId();
  ord->tm = NowUtcInMicro();
  HandleConfirmation(ord, kUnconfirmedNew, "", ord->tm);
  kRiskError = adapter->Place(*ord);
  auto ok = kRiskError.empty();
  if (!ok)
    HandleConfirmation(ord, kRiskRejected, kRiskError);
  else
    UpdateThrottle(*ord);
  return ok;
}

static inline bool Cancel(Order* cancel_order) {
  kRiskError.clear();
  cancel_order->tm = NowUtcInMicro();
  if (!RiskManager::Instance().CheckMsgRate(*cancel_order)) {
    HandleConfirmation(cancel_order, kRiskRejected, kRiskError);
    static uint32_t seed;
    kSharedTaskPool.AddTask(
        [cancel_order]() { Cancel(cancel_order); },
        boost::posix_time::milliseconds(1000 + rand_r(&seed) % 1000));
    return false;
  }
  auto adapter = cancel_order->broker_account->adapter;
  auto name = cancel_order->broker_account->adapter_name;
  if (!CheckAdapter(adapter, name)) {
    HandleConfirmation(cancel_order, kRiskRejected, kRiskError);
    return false;
  }
  HandleConfirmation(cancel_order, kUnconfirmedCancel, "", cancel_order->tm);
  cancel_order->id = GlobalOrderBook::Instance().NewOrderId();
  kRiskError = adapter->Cancel(*cancel_order);
  auto ok = kRiskError.empty();
  if (!ok)
    HandleConfirmation(cancel_order, kRiskRejected, kRiskError);
  else
    UpdateThrottle(*cancel_order);
  return ok;
}

bool ExchangeConnectivityManager::Cancel(const Order& orig_ord) {
  assert(orig_ord.sub_account);
  assert(orig_ord.sec);
  assert(orig_ord.user);
  assert(orig_ord.broker_account);
  if (!orig_ord.IsLive()) return false;
  if (!orig_ord.sub_account) return false;
  if (!orig_ord.sec) return false;
  if (!orig_ord.user) return false;
  if (!orig_ord.broker_account) return false;
  auto cancel_order = new Order(orig_ord);
  cancel_order->orig_id = orig_ord.id;
  cancel_order->status = kUnconfirmedCancel;
  return opentrade::Cancel(cancel_order);
}

void ExchangeConnectivityAdapter::HandleNew(Order::IdType id,
                                            const std::string& order_id,
                                            int64_t transaction_time) {
  Handle(name(), id, "new", kNew, order_id, transaction_time);
}

void ExchangeConnectivityAdapter::HandleSuspended(Order::IdType id,
                                                  const std::string& order_id,
                                                  int64_t transaction_time) {
  Handle(name(), id, "suspended", kSuspended, order_id, transaction_time);
}

void ExchangeConnectivityAdapter::HandlePendingNew(Order::IdType id,
                                                   const std::string& text,
                                                   int64_t transaction_time) {
  Handle(name(), id, "pending new", kPendingNew, text, transaction_time);
}

void ExchangeConnectivityAdapter::HandleFill(
    Order::IdType id, double qty, double price, const std::string& exec_id,
    int64_t transaction_time, bool is_partial, ExecTransType exec_trans_type,
    Confirmation::StrMap* misc) {
  if (GlobalOrderBook::Instance().IsDupExecId(id, exec_id)) {
    LOG_DEBUG(name() << ": Duplicate exec id: " << exec_id << ", ignored");
    return;
  }
  auto ord = GlobalOrderBook::Instance().Get(id);
  if (!ord) {
    LOG_DEBUG(name() << ": Unknown ClOrdId of fill confirmation: " << id
                     << ", ignored");
    return;
  }
  if (qty <= 0 || price <= 0) {
    LOG_DEBUG(name() << ": Invalid fill confirmation: " << id << ", qty=" << qty
                     << ", price=" << price << ", ignored");
    return;
  }
  HandleConfirmation(ord, qty, price, exec_id, transaction_time, is_partial,
                     exec_trans_type, misc);
}

void ExchangeConnectivityAdapter::HandleCanceled(Order::IdType id,
                                                 Order::IdType orig_id,
                                                 const std::string& text,
                                                 int64_t transaction_time) {
  Handle(name(), id, orig_id, "canceled", kCanceled, text, transaction_time);
}

void ExchangeConnectivityAdapter::HandleNewRejected(Order::IdType id,
                                                    const std::string& text,
                                                    int64_t transaction_time) {
  Handle(name(), id, "rejected", kRejected, text, transaction_time);
}

void ExchangeConnectivityAdapter::HandleCancelRejected(
    Order::IdType id, Order::IdType orig_id, const std::string& text,
    int64_t transaction_time) {
  Handle(name(), id, orig_id, "cancel rejected", kCancelRejected, text,
         transaction_time);
}

void ExchangeConnectivityAdapter::HandlePendingCancel(
    Order::IdType id, Order::IdType orig_id, int64_t transaction_time) {
  Handle(name(), id, orig_id, "pending cancel", kPendingCancel, "",
         transaction_time);
}

void ExchangeConnectivityAdapter::HandleOthers(Order::IdType id,
                                               OrderStatus exec_type,
                                               const std::string& text,
                                               int64_t transaction_time) {
  Handle(name(), id, exec_type, exec_type, text, transaction_time);
}

}  // namespace opentrade
