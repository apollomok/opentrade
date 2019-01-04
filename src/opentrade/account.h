#ifndef OPENTRADE_ACCOUNT_H_
#define OPENTRADE_ACCOUNT_H_

#include <tbb/concurrent_unordered_map.h>
#include <string>
#include <unordered_map>

#include "position_value.h"
#include "risk.h"
#include "security.h"
#include "utility.h"

namespace opentrade {

class ExchangeConnectivityAdapter;

struct AccountBase {
  Limits limits;
  Throttle throttle_in_sec;
  tbb::concurrent_unordered_map<Security::IdType, Throttle>
      throttle_per_security_in_sec;
  PositionValue position_value;
};

struct BrokerAccount : public AccountBase, public ParamsBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  const char* adapter_name = "";
  ExchangeConnectivityAdapter* adapter = nullptr;
};

struct SubAccount : public AccountBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  typedef std::unordered_map<Exchange::IdType, const BrokerAccount*>
      BrokerAccountMap;
  typedef std::shared_ptr<const BrokerAccountMap> BrokerAccountMapPtr;
  BrokerAccountMapPtr broker_accounts() const { return broker_accounts_; }
  void set_broker_accounts(BrokerAccountMapPtr accs) {
    broker_accounts_ = accs;
  }
  const BrokerAccount* GetBrokerAccount(Exchange::IdType id) const {
    assert(id);
    auto tmp = FindInMap(broker_accounts(), id);
    if (!tmp && id) tmp = FindInMap(broker_accounts(), 0);
    return tmp;
  }

 private:
  BrokerAccountMapPtr broker_accounts_ = std::make_shared<BrokerAccountMap>();
};

struct User : public AccountBase {
  typedef uint16_t IdType;
  IdType id = 0;
  const char* name = "";
  const char* password = "";
  bool is_admin = false;
  bool is_disabled = false;
  typedef std::unordered_map<SubAccount::IdType, const SubAccount*>
      SubAccountMap;
  typedef std::shared_ptr<const SubAccountMap> SubAccountMapPtr;
  const SubAccount* GetSubAccount(SubAccount::IdType id) const {
    return FindInMap(sub_accounts_, id);
  }
  SubAccountMapPtr sub_accounts() const { return sub_accounts_; }
  void set_sub_accounts(SubAccountMapPtr accs) { sub_accounts_ = accs; }

 private:
  SubAccountMapPtr sub_accounts_ = std::make_shared<SubAccountMap>();
};

class AccountManager : public Singleton<AccountManager> {
 public:
  static void Initialize();
  const User* GetUser(const std::string& name) const {
    return FindInMap(user_of_name_, name);
  }
  const User* GetUser(User::IdType id) const { return FindInMap(users_, id); }
  const SubAccount* GetSubAccount(SubAccount::IdType id) const {
    return FindInMap(sub_accounts_, id);
  }
  const SubAccount* GetSubAccount(const std::string& name) const {
    return FindInMap(sub_account_of_name_, name);
  }
  const BrokerAccount* GetBrokerAccount(BrokerAccount::IdType id) const {
    return FindInMap(broker_accounts_, id);
  }
  const BrokerAccount* GetBrokerAccount(const std::string& name) const {
    return FindInMap(broker_account_of_name_, name);
  }

 private:
  tbb::concurrent_unordered_map<User::IdType, User*> users_;
  tbb::concurrent_unordered_map<std::string, User*> user_of_name_;
  tbb::concurrent_unordered_map<SubAccount::IdType, SubAccount*> sub_accounts_;
  tbb::concurrent_unordered_map<std::string, SubAccount*> sub_account_of_name_;
  tbb::concurrent_unordered_map<BrokerAccount::IdType, BrokerAccount*>
      broker_accounts_;
  tbb::concurrent_unordered_map<std::string, BrokerAccount*>
      broker_account_of_name_;
  friend class Connection;
  friend class Backtest;
  friend class PositionManager;
};

}  // namespace opentrade

#endif  // OPENTRADE_ACCOUNT_H_
