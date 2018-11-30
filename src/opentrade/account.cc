#include "account.h"

#include <memory>
#include <vector>

#include "database.h"
#include "exchange_connectivity.h"
#include "utility.h"

namespace opentrade {

void AccountManager::Initialize() {
  auto& self = Instance();
  auto sql = Database::Session();

  auto query = R"(
    select id, "name", password, is_admin, is_disabled, limits from "user"
  )";
  soci::rowset<soci::row> st = sql->prepare << query;
  for (auto it = st.begin(); it != st.end(); ++it) {
    auto u = new User();
    auto i = 0;
    u->id = Database::GetValue(*it, i++, 0);
    u->name = Database::GetValue(*it, i++, "");
    u->password = Database::GetValue(*it, i++, "");
    u->is_admin = Database::GetValue(*it, i++, 0);
    u->is_disabled = Database::GetValue(*it, i++, 0);
    u->limits.FromString(Database::GetValue(*it, i++, kEmptyStr));
    self.users_.emplace(u->id, u);
    self.user_of_name_.emplace(u->name, u);
  }

  query = R"(
    select id, "name", limits from sub_account 
  )";
  st = sql->prepare << query;
  for (auto it = st.begin(); it != st.end(); ++it) {
    auto s = new SubAccount();
    auto i = 0;
    s->id = Database::GetValue(*it, i++, 0);
    s->name = Database::GetValue(*it, i++, "");
    s->limits.FromString(Database::GetValue(*it, i++, kEmptyStr));
    self.sub_accounts_.emplace(s->id, s);
    self.sub_account_of_name_.emplace(s->name, s);
  }

  query = R"(
    select id, "name", adapter, params, limits from broker_account 
  )";
  st = sql->prepare << query;
  for (auto it = st.begin(); it != st.end(); ++it) {
    auto b = new BrokerAccount();
    auto i = 0;
    b->id = Database::GetValue(*it, i++, 0);
    b->name = Database::GetValue(*it, i++, "");
    b->adapter_name = Database::GetValue(*it, i++, "");
    b->adapter = ExchangeConnectivityManager::Instance().Get(b->adapter_name);
    b->set_params(Database::GetValue(*it, i++, kEmptyStr));
    b->limits.FromString(Database::GetValue(*it, i++, kEmptyStr));
    self.broker_accounts_.emplace(b->id, b);
    self.broker_account_of_name_.emplace(b->name, b);
  }

  query = R"(
    select user_id, sub_account_id from user_sub_account_map
  )";
  st = sql->prepare << query;
  std::unordered_map<User*, User::SubAccountMap> user_sub_account_map;
  for (auto it = st.begin(); it != st.end(); ++it) {
    auto i = 0;
    auto user_id = Database::GetValue(*it, i++, 0);
    auto sub_account_id = Database::GetValue(*it, i++, 0);
    auto it1 = self.users_.find(user_id);
    auto it2 = self.sub_accounts_.find(sub_account_id);
    if (it1 != self.users_.end() && it2 != self.sub_accounts_.end()) {
      user_sub_account_map[it1->second].emplace(it2->second->id, it2->second);
    }
  }
  for (auto& pair : user_sub_account_map) {
    pair.first->set_sub_accounts(User::SubAccountMapPtr(
        new decltype(pair.second)(std::move(pair.second))));
  }

  query = R"(
    select sub_account_id, exchange_id, broker_account_id from sub_account_broker_account_map 
  )";
  st = sql->prepare << query;
  std::unordered_map<SubAccount*, SubAccount::BrokerAccountMap>
      sub_account_broker_account_map;
  for (auto it = st.begin(); it != st.end(); ++it) {
    auto i = 0;
    auto sub_account_id = Database::GetValue(*it, i++, 0);
    auto exchange_id = Database::GetValue(*it, i++, 0);
    auto broker_account_id = Database::GetValue(*it, i++, 0);
    auto it1 = self.sub_accounts_.find(sub_account_id);
    auto it2 = self.broker_accounts_.find(broker_account_id);
    if (it1 != self.sub_accounts_.end() && it2 != self.broker_accounts_.end()) {
      sub_account_broker_account_map[it1->second].emplace(exchange_id,
                                                          it2->second);
    }
  }
  for (auto& pair : sub_account_broker_account_map) {
    pair.first->set_broker_accounts(SubAccount::BrokerAccountMapPtr(
        new decltype(pair.second)(std::move(pair.second))));
  }
}

}  // namespace opentrade
