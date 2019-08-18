release:
	mkdir -p build/release; cd build/release; cmake ../../src -DCMAKE_BUILD_TYPE=Release; make ${args}; cd -;

debug:
	mkdir -p build/debug; cd build/debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug; make ${args}; cd -;

backtest-debug:
	mkdir -p build/backtest-debug; cd build/backtest-debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug -DBACKTEST=1; make ${args}; cd -;

backtest-release:
	mkdir -p build/backtest-release; cd build/backtest-release; cmake ../../src -DCMAKE_BUILD_TYPE=Release -DBACKTEST=1; make ${args}; cd -;

unit-test-debug: 
	mkdir -p build/unit_test_debug; cd build/unit_test_debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug -DUNIT_TEST=1; make ${args}; cd -;
	build/unit_test_debug/unit_test/unit_test

unit-test-release: 
	mkdir -p build/unit_test_release; cd build/unit_test_release; cmake ../../src -DCMAKE_BUILD_TYPE=Release -DUNIT_TEST=1; make ${args}; cd -;
	build/unit_test_release/unit_test/unit_test

lint:
	./scripts/cpplint.py src/*/*h src/*/*cc src/adapters/*/*h src/adapters/*/*cc src/algos/*/*h src/algos/*/*cc

fmt:
	clang-format -style=Google -i src/*/*h src/*/*cc src/adapters/*/*h src/adapters/*/*cc src/algos/*/*h src/algos/*/*cc

clean:
	rm -rf build;
