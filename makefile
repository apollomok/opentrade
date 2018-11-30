release:
	mkdir -p build/release; cd build/release; cmake ../../src -DCMAKE_BUILD_TYPE=Release; make ${args}; cd -;

debug:
	mkdir -p build/debug; cd build/debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug; make ${args}; cd -;

backtest-debug:
	mkdir -p build/backtest-debug; cd build/backtest-debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug -DBACKTEST=1; make ${args}; cd -;

backtest-release:
	mkdir -p build/backtest-release; cd build/backtest-release; cmake ../../src -DCMAKE_BUILD_TYPE=Debug -DBACKTEST=1; make ${args}; cd -;

lint:
	./scripts/cpplint.py src/*/*h src/*/*cc src/adapters/*/*h src/adapters/*/*cc src/algo/*/*h src/algo/*/*cc

fmt:
	clang-format -style=Google -i src/*/*h src/*/*cc src/adapters/*/*h src/adapters/*/*cc src/algo/*/*h src/algo/*/*cc

clean:
	rm -rf build;
