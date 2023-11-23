#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <cmath>
#include <cassert>
#include <string>
#include <sstream>

using namespace std;

class Operation {
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

void processFile(const std::string& filename, const Operation& action, double& result, std::mutex& mutex) {
    ifstream file(filename);
    if (file.is_open()) {
        vector<double> numbers;
        double num;
        while (file >> num) {
            numbers.push_back(num);
        }
        file.close();

        double partialResult = action.execute(numbers);

        lock_guard<std::mutex> lock(mutex);
        result += partialResult;
    }
    else {
        cerr << "Error opening file: " << filename << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <directory_path> <num_threads>" << std::endl;
        return 1;
    }

    std::string directoryPath = argv[1];
    int numThreads = stoi(argv[2]);

    Addition addition;
    Multiplication multiplication;
    SumOfSquares squareSum;

    vector<std::thread> threads;
    double totalResult = 0;
    std::mutex resultMutex;

    for (int i = 1; i <= numThreads; ++i) {
        std::string filename = directoryPath + "/in_" + std::to_string(i) + ".dat";
        int actionType = i % 3 + 1;

        const Operation* action = nullptr;

        switch (actionType) {
            case 1:
                action = &addition;
                break;
            case 2:
                action = &multiplication;
                break;
            case 3:
                action = &squareSum;
                break;
        }

        if (!action) {
            cerr << "Error: Unsupported action type." << std::endl;
            return 1;
        }

        threads.emplace_back(processFile, filename, std::ref(*action), std::ref(totalResult), std::ref(resultMutex));
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    ofstream outFile(directoryPath + "/out.dat");
    if (outFile.is_open()) {
        stringstream resultStream;
        resultStream << "Total Result: " << totalResult << std::endl;
        outFile << resultStream.str();
        outFile.close();
    }
    else {
        std::cerr << "Error opening output file." << std::endl;
        return 1;
    }

    return 0;
}
