# OpenTrade

![OpenTrade Logo](https://github.com/opentradesolutions/opentrade/blob/master/web/img/ot.png)

OpenTrade is an open source OEMS, and algorithmic trading platform, designed for simplicity, flexibility and performance. 

[**Demo**](http://demo.opentradesolutions.com)

---

# Features:
* Built on C++17
* Strictly follows [Google C++ Style Guild](https://google.github.io/styleguide/cppguide.html)
* Multi-level account functionality
* Super simple API interfaces for market data adapter (C++), exchange connectivity (C++) and execution/alpha algo (C++ and Python)
* **REST** and web socket interface
* Multi-source market data support, e.g., different FX pricing sources
* Pre-trade risk limits on multi-level accounts
* Post-trade risk integrated with [OpenRisk](https://github.com/opentradesolutions/openrisk)
* Edit and test **Python** algo online
* **Backtest**
* [Multi-theme web frontend](http://demo.opentradesolutions.com)
* Fully thread-safe design, everything can be modified during runtime, e.g., reload symbol list, modify tick size table, lot-size, exchange timezone and trading/break period etc.
* Built-in execution simulator
* Simple configuration

---

[![Algo Editor](https://github.com/opentradesolutions/opentrade/blob/master/imgs/algo-editor.png)](http://opentradesolutions.com/images/algo-editor.png)

---

# Steps to run on Ubuntu 18.04
* **Compile**
  * Prepare dev environment.
  ```bash
  sudo apt update \
  && sudo apt install -y \
    g++  \
    make \
    cmake \
    clang-format \
    clang \
    python \
    python3-dev \
    vim \
    exuberant-ctags \
    git \
    wget \
    libssl-dev \
    libboost-program-options-dev \
    libboost-system-dev \
    libboost-date-time-dev \
    libboost-filesystem-dev \
    libboost-iostreams-dev \
    libboost-python-dev \
    libsoci-dev \
    libpq-dev \
    libquickfix-dev \
    libtbb-dev \
    liblog4cxx-dev
  ```
  * Build
  ```bash
  git clone https://github.com/opentradesolutions/opentrade
  cd opentrade
  make debug
  ```
  
 * **Setup database**
   ```bash
   sudo apt remove --purge postgres*
   sudo apt autoremove
   sudo apt install -y postgresql-10 postgresql-contrib postgresql-client
   sudo su postgres;
   cd;
   wget https://github.com/opentradesolutions/data/raw/master/opentrade-pg_dumpall.sql
   psql -f opentrade-pg_dumpall.sql 
   ```
 
 * **Run opentrade**
   * Download tick data files
   ```bash
   cd opentrade
   wget https://raw.githubusercontent.com/opentradesolutions/data/master/bbgids.txt
   wget https://github.com/opentradesolutions/data/raw/master/ticks.txt.xz.part1
   wget https://github.com/opentradesolutions/data/raw/master/ticks.txt.xz.part2
   cat ticks.txt.xz.part1 ticks.txt.xz.part2 > ticks.txt.xz
   xz -d ticks.txt.xz
   ```
   * Run
   ```Bash
   cp opentrade.conf-example opentrade.conf
   ./opentrade
   ```
   
 * **Open Web UI**
   ```
   # username/password: test/test
   http://localhost:9111
   ```
   
# Backtest
  * Only BBO support currently, full orderbook support will come soon
  * It is up to you to generate report
  ```
  make args=-j backtest-debug
  wget -O ticks.tgz https://www.dropbox.com/s/maikrn2qbz8hxba/ticks.tgz?dl=1; tar xzf ticks.tgz
  ./build/backtest-debug/opentrade/opentrade -b scripts/backtest.py -t ticks/%Y%m%d -s 20170701 -e 20181115
  ```

# The other OS system
  we prepared [Dockfile-dev](https://raw.githubusercontent.com/opentradesolutions/opentrade/master/Dockfile-dev) for you.
