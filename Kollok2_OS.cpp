#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <cmath>
#include <cassert>
#include <string>

using namespace std;

class Operation
{
public:
    virtual ~Operation() = default;
    virtual double execute(const vector<double>& numbers) const = 0;
};

class Addition : public Operation {
public:
    double execute(const vector<double>& numbers) const override {
        double sum = 0;
        for (double number : numbers) {
            sum += number;
        }
        return sum;
    }
};

class Multiplication : public Operation {
public:
    double execute(const vector<double>& numbers) const override {
        double product = 1;
        for (double number : numbers) {
            product *= number;
        }
        return product;
    }
};

class SumOfSquares : public Operation {
public:
    double execute(const vector<double>& numbers) const override {
        double sum = 0;
        for (double number : numbers) {
            sum += number * number;
        }
        return sum;
    }
};


class FileProcessor {
private:
    string directory;
    int threadsCount;
    mutex mutex;
    queue<string> inputFiles;
    double totalResult;

    void processFile() {
        string inputFile = getNextFile();

        while (!inputFile.empty()) {
            ifstream in(directory + "/" + inputFile);
            if (in.is_open()) {
                int action;
                in >> action;

                Operation* operation = nullptr;
                switch (action) {
                    case 1:
                      operation = new Addition();
                        break;
                    case 2:
                        operation = new Multiplication();
                        break;
                    case 3:
                        operation = new SumOfSquares();
                        break;
                    default:
                        throw runtime_error("Неверный номер действия в файле " + inputFile);
                }

                vector<double> numbers;
                double number;
                while (in >> number) {
                    numbers.push_back(number);
                }

                in.close();
                double result = operation->execute(numbers);
                delete operation;
                updateTotalResult(result);
            }
            else
            {
                throw runtime_error("Не удалось открыть файл " + inputFile + " для чтения");
            }

            inputFile = getNextFile();
        }
    }

    string getNextFile() {
        lock_guard<std::mutex> lock(mutex);

        if (inputFiles.empty()) {
            return "";
        }

        string inputFile = inputFiles.front();
        inputFiles.pop();
        return inputFile;
    }

    void updateTotalResult(double result) {
        lock_guard<std::mutex> lock(mutex);
        totalResult += result;
    }

public:
    FileProcessor(const string& directory, int threadsCount) {
        if (directory.empty()) {
            throw invalid_argument("Директория не может быть пустой");
        }

        if (threadsCount <= 0) {
            throw invalid_argument("Количество потоков должно быть положительным");
        }

        this->directory = directory;
        this->threadsCount = threadsCount;
        totalResult = 0;
    }

    void run() {
        for (int i = 1; i <= 10; i++) {
            inputFiles.push("in_" + to_string(i) + ".dat");
        }

        vector<std::thread> threads;

        for (int i = 0; i < threadsCount; i++) {
            threads.emplace_back(&FileProcessor::processFile, this);
        }

        for (std::thread& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        ofstream out(directory + "/out.dat");
        if (out.is_open()) {
            out << totalResult << endl;
            out.close();
        }
        else {
            throw runtime_error("Не удалось открыть файл out.dat для записи");
        }
    }
};

void checkResult(const string& directory) {
    ifstream in(directory + "/out.dat");
    if (in.is_open()) {
        double totalResult;
        in >> totalResult;
        in.close();
        double expected = 0;
        for (int i = 1; i <= 100; i++) {
            ifstream in(directory + "/in_" + to_string(i) + ".dat");
            if (in.is_open()) {
                int action;
                in >> action;
                Operation* operation = nullptr;
                switch (action) {
                    case 1:
                        operation = new Addition();
                        break;
                    case 2:
                        operation = new Multiplication();
                        break;
                    case 3:
                        operation = new SumOfSquares();
                        break;
                    default:
                        throw runtime_error("Неверный номер действия в файле in_" + to_string(i) + ".dat");
                }

                vector<double> numbers;
                double number;
                while (in >> number) {
                    numbers.push_back(number);
                }

                in.close();
                double result = operation->execute(numbers);
                delete operation;
                expected += result;
            }
            else {
                throw runtime_error("Не удалось открыть файл in_" + to_string(i) + ".dat для чтения");
            }
        }

        double epsilon = 1e-6;
        assert(abs(totalResult - expected) < epsilon);
    }
    else {
        throw runtime_error("Не удалось открыть файл out.dat для чтения");
    }
}

int main(int argc, char* argv[]) {

    setlocale(LC_ALL, "RUSSIAN");

    if (argc != 3) {
        cerr << "Неверное количество аргументов" << endl;
        return 1;
    }

    string directory = argv[1];
    int threadsCount = stoi(argv[2]);

    FileProcessor fileProcessor(directory, threadsCount);
    fileProcessor.run();
    checkResult(directory);
    cout << "Приложение успешно выполнено" << endl;

    return 0;
}
