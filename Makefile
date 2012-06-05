TMP_DIR=tmp
BIN_DIR=bin

MapReduce: main.o Logger.o MapReduce.o ProcessInfo.o
	g++ $(TMP_DIR)/main.o $(TMP_DIR)/Logger.o $(TMP_DIR)/MapReduce.o $(TMP_DIR)/ProcessInfo.o -o MapReduce
	mv MapReduce $(BIN_DIR)

main.o: main.cpp
	g++ -c main.cpp -o main.o
	mv main.o $(TMP_DIR)

Logger.o: src/Logger.cpp
	g++ -c src/Logger.cpp -o Logger.o
	mv Logger.o $(TMP_DIR)

MapReduce.o: src/MapReduce.cpp
	g++ -c src/MapReduce.cpp -o MapReduce.o
	mv MapReduce.o $(TMP_DIR)

ProcessInfo.o: src/ProcessInfo.cpp
	g++ -c src/ProcessInfo.cpp -o ProcessInfo.o
	mv ProcessInfo.o $(TMP_DIR)

clean:
	rm $(BIN_DIR)/MapReduce $(TMP_DIR)/*.o
