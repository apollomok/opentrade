#ifndef FIX_APPLICATION_H_
#define FIX_APPLICATION_H_

#include <tbb/concurrent_unordered_map.h>
#include <fstream>
#include <memory>
#define throw(...)
#include <quickfix/MessageCracker.h>
#include <quickfix/NullStore.h>
#include <quickfix/Session.h>
#include <quickfix/ThreadedSocketAcceptor.h>
#include <quickfix/ThreadedSocketInitiator.h>
#include <quickfix/fix42/ExecutionReport.h>
#include <quickfix/fix42/MarketDataIncrementalRefresh.h>
#include <quickfix/fix42/MarketDataRequest.h>
#include <quickfix/fix42/MarketDataRequestReject.h>
#include <quickfix/fix42/MarketDataSnapshotFullRefresh.h>
#include <quickfix/fix42/NewOrderSingle.h>
#include <quickfix/fix42/OrderCancelReject.h>
#include <quickfix/fix42/OrderCancelReplaceRequest.h>
#include <quickfix/fix42/OrderCancelRequest.h>
#include <quickfix/fix42/OrderStatusRequest.h>
#include <quickfix/fix42/Reject.h>
#include <quickfix/fix42/TradingSessionStatus.h>
#include <quickfix/fix44/ExecutionReport.h>
#include <quickfix/fix44/MarketDataIncrementalRefresh.h>
#include <quickfix/fix44/MarketDataRequest.h>
#include <quickfix/fix44/MarketDataRequestReject.h>
#include <quickfix/fix44/MarketDataSnapshotFullRefresh.h>
#include <quickfix/fix44/NewOrderSingle.h>
#include <quickfix/fix44/OrderCancelReject.h>
#include <quickfix/fix44/OrderCancelReplaceRequest.h>
#include <quickfix/fix44/OrderCancelRequest.h>
#include <quickfix/fix44/OrderStatusRequest.h>
#include <quickfix/fix44/Reject.h>
#include <quickfix/fix44/TradingSessionStatus.h>
#undef throw

#include "filelog.h"
#include "filestore.h"

namespace opentrade {

class Application : public FIX::Application {
 public:
  void onCreate(const FIX::SessionID& session_id) override {
    if (!session_) session_ = FIX::Session::lookupSession(session_id);
  }
  void fromApp(const FIX::Message& msg,
               const FIX::SessionID& session_id) override {}
  void onLogon(const FIX::SessionID& session_id) override {}
  void onLogout(const FIX::SessionID& session_id) override {}
  void toApp(FIX::Message& msg, const FIX::SessionID& session_id) override {}
  void toAdmin(FIX::Message& msg, const FIX::SessionID& id) override {}
  void fromAdmin(const FIX::Message&, const FIX::SessionID&) override {}

 protected:
  std::unique_ptr<FIX::SessionSettings> fix_settings_;
  std::unique_ptr<FIX::MessageStoreFactory> fix_store_factory_;
  std::unique_ptr<FIX::LogFactory> fix_log_factory_;
  std::unique_ptr<FIX::ThreadedSocketAcceptor> threaded_socket_acceptor_;
  std::unique_ptr<FIX::ThreadedSocketInitiator> threaded_socket_initiator_;
  FIX::Session* session_ = nullptr;
};

}  // namespace opentrade

#endif  // FIX_APPLICATION_H_
