#include "opentick.h"

#include "logger.h"

namespace opentrade {

struct OpenTickLogger : public opentick::Logger {
  void Info(const std::string& msg) noexcept override { LOG_INFO(msg); }
  void Error(const std::string& msg) noexcept override { LOG_ERROR(msg); }
};

void OpenTick::Initialize(const std::string& url) {
  auto strs1 = Split(url, "@");
  if (strs1.size() < 1) {
    LOG_FATAL("Invalid opentick url '" << url << "'");
  }
  std::string dbname = "opentrade";
  if (strs1.size() > 1) dbname = strs1[1];
  auto strs2 = Split(strs1[0], ":");
  if (strs2.size() < 1) {
    LOG_FATAL("Invalid opentick url '" << url << "'");
  }
  auto port = 1116;
  if (strs2.size() > 2) port = atol(strs2[1].c_str());
  auto host = strs2[0];
  conn_ = opentick::Connection::Create(host, port, dbname);
  conn_->SetLogger(std::make_shared<OpenTickLogger>());
  conn_->SetAutoReconnect(3);
  conn_->Start();
}

opentick::ResultSet OpenTick::Request(Security::IdType sec, int interval,
                                      time_t start_time, time_t end_time,
                                      const std::string& tbl,
                                      opentick::Callback callback) {
  if (!conn_ || !conn_->IsConnected()) {
    if (callback) callback({}, "OpenTick not connected");
    return {};
  }
  try {
    auto fut = conn_->ExecuteAsync(
        "select time, open, high, low, close, volume from " + tbl +
            " where sec=? and interval=? and time>=? and time<?",
        opentick::Args{sec, interval, start_time, end_time}, callback);
    if (!callback) return fut->Get();
  } catch (std::exception& e) {
    if (callback) callback({}, e.what());
  }
  return {};
}

}  // namespace opentrade
