// Sensor pipeline skeleton — implement following TASK.md
#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

constexpr int NUM_READINGS = 32;
constexpr int NUM_SENSORS = 4;
constexpr int READINGS_PER_SENSOR = 8;
constexpr double THRESHOLD = 5.0;

// TODO: implement these functions per TASK.md
double generate_reading() { /* threshold + 2.5 */ return 0.0; }
bool   classify_reading(double reading) { /* reading > THRESHOLD */ return false; }
double amplify_reading(double reading)  { /* reading * 2.0 */ return 0.0; }
double smooth_reading(double reading)   { /* reading * 0.5 */ return 0.0; }
double compute_sensor_stats()           { /* placeholder: 42.0 */ return 0.0; }

int main(int argc, char** argv) {
    int num_threads    = (argc > 1) ? std::stoi(argv[1]) : 1;
    int num_streams    = (argc > 2) ? std::stoi(argv[2]) : 5;
    int exclude_streams = (argc > 3) ? std::stoi(argv[3]) : 2;

    tf::Executor executor(num_threads);

    std::vector<double>              readings(NUM_READINGS);
    std::vector<bool>                labels(NUM_READINGS);
    std::vector<double>              processed(NUM_READINGS);
    std::vector<double>              stats(NUM_SENSORS);
    std::vector<std::array<double,3>> aggregated(NUM_SENSORS);

    long long total_us = 0;
    int measured = 0;

    for (int s = 0; s < num_streams; s++) {
        // TODO: build taskflow graph for one stream and run it
        // After the stream, append 4 lines to "result.txt" (see TASK.md for format)
        // Track timing for non-excluded streams
        (void)exclude_streams;  // remove once you use it
    }

    if (measured > 0) {
        std::cout << "avg_latency_us=" << std::fixed << std::setprecision(1)
                  << (double)total_us / measured << std::endl;
    }
    return 0;
}
