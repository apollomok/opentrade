#ifndef FIX_FIX_H_
#define FIX_FIX_H_

#include <tbb/concurrent_unordered_map.h>
#include <fstream>
#include <memory>
#include <optional>
#define throw(...)
#include <quickfix/MessageCracker.h>
#include <quickfix/NullStore.h>
#include <quickfix/Session.h>
#include <quickfix/ThreadedSocketInitiator.h>
#include <quickfix/fix42/Allocation.h>
#include <quickfix/fix42/ExecutionReport.h>
#include <quickfix/fix42/NewOrderSingle.h>
#include <quickfix/fix42/OrderCancelReject.h>
#include <quickfix/fix42/OrderCancelReplaceRequest.h>
#include <quickfix/fix42/OrderCancelRequest.h>
#include <quickfix/fix42/OrderStatusRequest.h>
#include <quickfix/fix42/Reject.h>
#include <quickfix/fix42/SettlementInstructions.h>
#include "quickfix/fix42/MarketDataIncrementalRefresh.h"
#include "quickfix/fix42/MarketDataRequest.h"
#include "quickfix/fix42/MarketDataRequestReject.h"
#include "quickfix/fix42/MarketDataSnapshotFullRefresh.h"
#include "quickfix/fix42/TradingSessionStatus.h"
#undef throw

#include "filelog.h"
#include "filestore.h"
#include "opentrade/consolidation.h"
#include "opentrade/exchange_connectivity.h"
#include "opentrade/logger.h"
#include "opentrade/market_data.h"
#include "opentrade/utility.h"

namespace opentrade {

static inline const std::string kRemoveTag = "<remove>";
static inline const std::string kTagPrefix = "tag";

class Fix : public FIX::Application,
            public FIX::MessageCracker,
            public ExchangeConnectivityAdapter,
            public MarketDataAdapter {
 public:
  void Start() noexcept override {
    CreatePriceSources();
    auto config_file = config("config_file");
    if (config_file.empty()) LOG_FATAL(name() << ": config_file not given");
    if (!std::ifstream(config_file.c_str()).good())
      LOG_FATAL(name() << ": Faield to open: " << config_file);

    fix_settings_.reset(new FIX::SessionSettings(config_file));
    if (empty_store_)
      fix_store_factory_.reset(new FIX::NullStoreFactory());
    else
      fix_store_factory_.reset(new FIX::AsyncFileStoreFactory(*fix_settings_));
    fix_log_factory_.reset(new FIX::AsyncFileLogFactory(*fix_settings_));
    threaded_socket_initiator_.reset(new FIX::ThreadedSocketInitiator(
        *this, *fix_store_factory_, *fix_settings_, *fix_log_factory_));
    threaded_socket_initiator_->start();
  }

  void Stop() noexcept override { threaded_socket_initiator_->stop(); }

  void onCreate(const FIX::SessionID& session_id) override {
    if (!session_) session_ = FIX::Session::lookupSession(session_id);
  }

  void onLogon(const FIX::SessionID& session_id) override {
    if (session_ != FIX::Session::lookupSession(session_id)) return;
    connected_ = -1;
    // in case frequently reconnected, e.g. seqnum mismatch,
    // OnLogout is called immediately after OnLogon
    tp_.AddTask(
        [=]() {
          if (-1 == connected_) {
            connected_ = 1;
            ReSubscribeAll();
            LOG_INFO(name() << ": Logged-in to " << session_id.toString());
          }
        },
        boost::posix_time::seconds(1));
  }

  void onLogout(const FIX::SessionID& session_id) override {
    if (session_ != FIX::Session::lookupSession(session_id)) return;
    if (connected())
      LOG_INFO(name() << ": Logged-out from " << session_id.toString());
    connected_ = 0;
  }

  void toApp(FIX::Message& msg, const FIX::SessionID& session_id) override {
    if (msg.isSetField(FIX::FIELD::PossDupFlag)) {
      FIX::PossDupFlag flag;
      msg.getHeader().getField(flag);
      if (flag) throw FIX::DoNotSend();
    }
  }

  void fromApp(const FIX::Message& msg,
               const FIX::SessionID& session_id) override {
    crack(msg, session_id);
  }

  void fromAdmin(const FIX::Message&, const FIX::SessionID&) override {}

  void toAdmin(FIX::Message& msg, const FIX::SessionID& id) override {
    FIX::MsgType msg_type;
    msg.getHeader().getField(msg_type);
    if (msg_type == FIX::MsgType_Logon) {
      try {
        auto username = fix_settings_->get(id).getString("Username");
        if (!username.empty())
          msg.getHeader().setField(FIX::Username(username));
      } catch (...) {
      }
      try {
        auto password = fix_settings_->get(id).getString("Password");
        if (!password.empty())
          msg.getHeader().setField(FIX::Password(password));
      } catch (...) {
      }
    }
  }

  void UpdateTm(const FIX::Message& msg) {
    if (msg.isSetField(FIX::FIELD::TransactTime)) {
      auto t = FIX::UtcTimeStampConvertor::convert(
          (msg.getField(FIX::FIELD::TransactTime)));
      transact_time_ = t.getTimeT() * 1000000l + t.getMillisecond() * 1000;
    } else {
      transact_time_ = NowUtcInMicro();
    }
  }

  void OnExecutionReport(const FIX::Message& msg,
                         const FIX::SessionID& session_id) {
    UpdateTm(msg);
    std::string text;
    if (msg.isSetField(FIX::FIELD::Text)) text = msg.getField(FIX::FIELD::Text);
    auto exec_type = msg.getField(FIX::FIELD::ExecType)[0];
    switch (exec_type) {
      case FIX::ExecType_PENDING_NEW:
        OnPendingNew(msg, text);
        break;
      case FIX::ExecType_PENDING_CANCEL:
        OnPendingCancel(msg);
        break;
      case FIX::ExecType_NEW:
        OnNew(msg);
        break;
      case FIX::ExecType_PARTIAL_FILL:
      case FIX::ExecType_FILL:
      case FIX::ExecType_TRADE:
        OnFilled(msg, exec_type, exec_type == FIX::ExecType_PARTIAL_FILL);
        break;
      case FIX::ExecType_PENDING_REPLACE:
        break;
      case FIX::ExecType_CANCELED:
        OnCanceled(msg, text);
        break;
      case FIX::ExecType_REPLACED:
        OnReplaced(msg, text);
        break;
      case FIX::ExecType_REJECTED:
        OnRejected(msg, text);
        break;
      case FIX::ExecType_SUSPENDED:
        OnSuspended(msg);
        break;
      case FIX::ExecType_RESTATED:
        break;
      case FIX::ExecType_TRADE_CANCEL:
        // FIX4.4 to-do
        break;
      case FIX::ExecType_TRADE_CORRECT:
        // FIX4.4 to-do
        break;
      default:
        break;
    }
  }

  void OnNew(const FIX::Message& msg) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    std::string order_id;
    if (msg.isSetField(FIX::FIELD::OrderID))
      order_id = msg.getField(FIX::FIELD::OrderID);
    HandleNew(clordid, order_id, transact_time_);
  }

  void OnSuspended(const FIX::Message& msg) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    std::string order_id;
    if (msg.isSetField(FIX::FIELD::OrderID))
      order_id = msg.getField(FIX::FIELD::OrderID);
    HandleSuspended(clordid, order_id, transact_time_);
  }

  void OnPendingNew(const FIX::Message& msg, const std::string& text) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandlePendingNew(clordid, text, transact_time_);
  }

  void OnFilled(const FIX::Message& msg, char exec_type, bool is_partial) {
    char exec_trans_type = msg.getField(FIX::FIELD::ExecTransType)[0];
    if (exec_trans_type == FIX::ExecTransType_CORRECT) {
      LOG_WARN(name() << ": Ignoring FIX::ExecTransType_CORRECT");
      return;
    }
    auto exec_id = msg.getField(FIX::FIELD::ExecID);
    auto last_shares = atoll(msg.getField(FIX::FIELD::LastShares).c_str());
    auto last_px = atof(msg.getField(FIX::FIELD::LastPx).c_str());
    auto clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleFill(clordid, last_shares, last_px, exec_id, transact_time_,
               is_partial, static_cast<ExecTransType>(exec_trans_type));
  }

  void OnCanceled(const FIX::Message& msg, const std::string& text) {
    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleCanceled(clordid, orig_id, text, transact_time_);
  }

  void OnPendingCancel(const FIX::Message& msg) {
    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandlePendingCancel(clordid, orig_id, transact_time_);
  }

  template <typename MDEntry>
  void OnMarketData(const FIX::Message& msg) {
    auto req_id = atoi(msg.getField(FIX::FIELD::MDReqID).c_str());
    auto req = reqs_.find(req_id);
    if (req == reqs_.end()) return;
    auto no_md_entries = atoi(msg.getField(FIX::FIELD::NoMDEntries).c_str());
    for (auto i = 1; i <= no_md_entries; i++) {
      MDEntry md_entry;
      msg.getGroup(i, md_entry);
      double price = 0.;
      if (msg.isSetField(FIX::FIELD::MDEntryPx))
        price = atof(msg.getField(FIX::FIELD::MDEntryPx).c_str());
      int64_t size = 0;
      if (msg.isSetField(FIX::FIELD::MDEntrySize))
        size = atoll(msg.getField(FIX::FIELD::MDEntrySize).c_str());
      if (msg.isSetField(FIX::FIELD::MDUpdateAction)) {
        auto action = *msg.getField(FIX::FIELD::MDUpdateAction).c_str();
        if (action == FIX::MDUpdateAction_DELETE) {
          price = 0;
          size = 0;
        }
      }
      auto level = GetPriceLevel(md_entry);
      auto type = *msg.getField(FIX::FIELD::MDEntryType).c_str();
      auto md = req->second.first;
      auto sec = req->second.second->id;
      if (type == FIX::MDEntryType_BID) {
        md->Update(sec, price, size, true, level);
      } else if (type == FIX::MDEntryType_OFFER) {
        md->Update(sec, price, size, false, level);
      }
    }
  }

  virtual int GetPriceLevel(const FIX::Group& msg) noexcept { return 0; }
  virtual int GetDepth() noexcept { return 0; /* 0 is full depth, 1 is bbo */ }
  virtual void SetRelatedSymbol(const Security& sec, DataSrc src,
                                FIX::Message* msg) noexcept = 0;

  void OnReplaced(const FIX::Message& msg, const std::string& text) {
    // to-do
  }

  void OnRejected(const FIX::Message& msg, const std::string& text) {
    Order::IdType clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    HandleNewRejected(clordid, text, transact_time_);
  }

  // Cancel or Replace msg got rejected
  void OnCancelRejected(const FIX::Message& msg, const FIX::SessionID&) {
    FIX::CxlRejResponseTo rejResponse;
    msg.getField(rejResponse);
    switch (rejResponse) {
      case FIX::CxlRejResponseTo_ORDER_CANCEL_REQUEST:
        break;
      case FIX::CxlRejResponseTo_ORDER_CANCEL_REPLACE_REQUEST:
      default:
        return;  // to-do: replace rejected
        break;
    }

    Order::IdType orig_id = 0;
    if (msg.isSetField(FIX::FIELD::OrigClOrdID))
      orig_id = atol(msg.getField(FIX::FIELD::OrigClOrdID).c_str());
    Order::IdType clordid = 0;
    if (msg.isSetField(FIX::FIELD::ClOrdID))
      clordid = atol(msg.getField(FIX::FIELD::ClOrdID).c_str());
    UpdateTm(msg);
    std::string text;
    if (msg.isSetField(FIX::FIELD::Text)) text = msg.getField(FIX::FIELD::Text);
    HandleCancelRejected(clordid, orig_id, text, transact_time_);
  }

  virtual void SetExtraTags(const Order& ord, FIX::Message* msg) {}

  void SetTags(const Order& ord, FIX::Message* msg) {
    if (!ord.orig_id) {  // not cancel
      if (ord.type != kMarket && ord.type != kStop) {
        msg->setField(FIX::Price(ord.price));
      }
      if (ord.stop_price) msg->setField(FIX::StopPx(ord.stop_price));
      msg->setField(FIX::TimeInForce(ord.tif));
    } else {
      msg->setField(FIX::OrigClOrdID(std::to_string(ord.orig_id)));
    }

    msg->setField(FIX::HandlInst('1'));
    msg->setField(FIX::OrderQty(ord.qty));
    msg->setField(FIX::ClOrdID(std::to_string(ord.id)));
    msg->setField(FIX::Side(ord.side));
    if (ord.side == kShort) msg->setField(FIX::LocateReqd(false));
    msg->setField(FIX::TransactTime());
    msg->setField(FIX::OrdType(ord.type));

    auto type = ord.sec->type;
    if (type == kOption) {
      msg->setField(FIX::PutOrCall(ord.sec->put_or_call));
      msg->setField(FIX::OptAttribute('A'));
      msg->setField(FIX::StrikePrice(ord.sec->strike_price));
      msg->setField(FIX::SecurityType(FIX::SecurityType_OPTION));
      auto d = ord.sec->maturity_date;
      msg->setField(FIX::MaturityMonthYear(std::to_string(d / 100)));
      msg->setField(FIX::MaturityDay(std::to_string(d % 100)));
    } else if (type == kStock) {
      msg->setField(FIX::SecurityType(FIX::SecurityType_COMMON_STOCK));
    } else if (type == kFuture || type == kCommodity) {
      msg->setField(FIX::SecurityType(FIX::SecurityType_FUTURE));
    } else if (type == kForexPair) {
      msg->setField(FIX::Product(FIX::Product_CURRENCY));
      msg->setField(FIX::Currency(std::string(
          ord.sec->symbol, std::min(3lu, strlen(ord.sec->symbol)))));
    }

    msg->setField(FIX::Symbol(ord.sec->symbol));
    msg->setField(FIX::ExDestination(ord.sec->exchange->name));
  }

  inline void Set(const std::string& key, const std::string& value,
                  FIX::Message* msg) {
    if (key.find(kTagPrefix) == 0) {
      auto tag = atoi(key.c_str() + kTagPrefix.length());
      if (!tag) return;
      if (msg->isHeaderField(tag)) {
        msg->getHeader().setField(tag, value);
        if (value == kRemoveTag) msg->getHeader().removeField(tag);
      } else {
        msg->setField(tag, value);
        if (value == kRemoveTag) msg->removeField(tag);
      }
    }
  }

  void SetBrokerTags(const Order& ord, FIX::Message* msg) {
    auto params = ord.broker_account->params();
    for (auto& pair : *params) Set(pair.first, pair.second, msg);
    if (ord.optional) {
      for (auto& pair : *ord.optional)
        Set(pair.first, ToString(pair.second), msg);
    }
  }

  bool Send(FIX::Message* msg) { return session_->send(*msg); }

  virtual std::string SetAndSend(const opentrade::Order& ord,
                                 FIX::Message* msg) {
    SetTags(ord, msg);
    SetBrokerTags(ord, msg);
    SetExtraTags(ord, msg);
    if (Send(msg))
      return {};
    else
      return "Failed in FIX::Session::send()";
  }

  void CreatePriceSources() {
    auto srcs = Split(config("srcs"), ",");
    for (auto& src : srcs) srcs_.push_back(new DummyFeed(src));
    if (srcs_.empty()) srcs_.push_back(this);
  }

 protected:
  std::unique_ptr<FIX::SessionSettings> fix_settings_;
  std::unique_ptr<FIX::MessageStoreFactory> fix_store_factory_;
  std::unique_ptr<FIX::LogFactory> fix_log_factory_;
  std::unique_ptr<FIX::ThreadedSocketInitiator> threaded_socket_initiator_;
  FIX::Session* session_ = nullptr;

  int64_t transact_time_ = 0;
  bool empty_store_ = true;
  std::vector<MarketDataAdapter*> srcs_;
  tbb::concurrent_unordered_map<int,
                                std::pair<MarketDataAdapter*, const Security*>>
      reqs_;
};

class Fix42 : public opentrade::Fix {
 public:
  void onMessage(const FIX42::ExecutionReport& msg, const FIX::SessionID& id) {
    OnExecutionReport(msg, id);
  }

  void onMessage(const FIX42::TradingSessionStatus& status,
                 const FIX::SessionID& session) override {
    LOG_INFO(name() << status);
  }

  void onMessage(const FIX42::OrderCancelReject& msg,
                 const FIX::SessionID& id) {
    OnCancelRejected(msg, id);
  }

  std::string Place(const opentrade::Order& ord) noexcept override {
    FIX42::NewOrderSingle msg;
    return SetAndSend(ord, &msg);
  }

  std::string Cancel(const opentrade::Order& ord) noexcept override {
    FIX42::OrderCancelRequest msg;
    return SetAndSend(ord, &msg);
  }

  void onMessage(const FIX42::MarketDataSnapshotFullRefresh& depth,
                 const FIX::SessionID& session) override {
    OnMarketData<FIX42::MarketDataSnapshotFullRefresh::NoMDEntries>(depth);
  }

  void onMessage(const FIX42::MarketDataIncrementalRefresh& depth_refresh,
                 const FIX::SessionID& session) override {
    OnMarketData<FIX42::MarketDataIncrementalRefresh::NoMDEntries>(
        depth_refresh);
  }

  void onMessage(const FIX42::MarketDataRequestReject& reject,
                 const FIX::SessionID& session) override {
    auto req_id = atoi(reject.getField(FIX::FIELD::MDReqID).c_str());
    LOG_WARN(name() << ": #" << req_id << " subscription rejected");
    // to-do
  }

  void SubscribeSync(const Security& sec) noexcept override {
    for (auto& src : srcs_) {
      FIX::SubscriptionRequestType sub_type(
          FIX::SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES);
      FIX::MarketDepth mkt_depth(GetDepth());
      auto n = ++request_counter_;
      FIX::MDReqID md_req_id(std::to_string(n));
      FIX42::MarketDataRequest req(md_req_id, sub_type, mkt_depth);
      req.set(FIX::MDUpdateType(FIX::MDUpdateType_INCREMENTAL_REFRESH));
      SetRelatedSymbol(sec, DataSrc(src->src()), &req);
      Send(&req);
      reqs_[n] = std::make_pair(src, &sec);
    }
  }

  void SetRelatedSymbol(const Security& sec, DataSrc src,
                        FIX::Message* msg) noexcept override {
    FIX42::MarketDataRequest::NoRelatedSym symbol_group;
    symbol_group.set(FIX::Symbol(sec.symbol));
    if (src.value) symbol_group.set(FIX::SecurityExchange(src.str()));
    msg->addGroup(symbol_group);
  }
};

}  // namespace opentrade

#endif  // FIX_FIX_H_
