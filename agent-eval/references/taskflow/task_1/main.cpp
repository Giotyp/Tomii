// Oracle Taskflow reference implementation of sensor pipeline (task_1).
// Uses static partitioner and full thread count for best throughput.
#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>
#include <array>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

constexpr int NUM_READINGS = 256;
constexpr int NUM_SENSORS = 4;
constexpr int READINGS_PER_SENSOR = 64;
constexpr double THRESHOLD = 5.0;

static inline double calibration_load() {
    volatile double s = 0.0;
    for (int i = 0; i < 100000; i++) s += std::sqrt((double)i);
    return s;
}
static inline double generate_reading()          { calibration_load(); return THRESHOLD + 2.5; }
static inline bool   classify_reading(double r)  { return r > THRESHOLD; }
static inline double amplify_reading(double r)   { calibration_load(); return r * 2.0; }
static inline double smooth_reading(double r)    { calibration_load(); return r * 0.5; }
static inline double compute_sensor_stats()      { return 42.0; }

static void write_stream(
    std::ofstream& f,
    const std::vector<std::array<double, 3>>& agg)
{
    for (int s = 0; s < NUM_SENSORS; s++) {
        f << "Sensor-" << s << ": ["
          << std::fixed << std::setprecision(2)
          << agg[s][0] << ", " << agg[s][1] << ", " << agg[s][2]
          << "]\n";
    }
}

int main(int argc, char** argv) {
    int num_threads     = (argc > 1) ? std::stoi(argv[1]) : 4;
    int num_streams     = (argc > 2) ? std::stoi(argv[2]) : 7;
    int exclude_streams = (argc > 3) ? std::stoi(argv[3]) : 3;

    tf::Executor executor(num_threads);

    std::vector<double> readings(NUM_READINGS);
    std::vector<bool>   labels(NUM_READINGS);
    std::vector<double> processed(NUM_READINGS);
    std::vector<double> stats(NUM_SENSORS);
    std::vector<std::array<double, 3>> aggregated(NUM_SENSORS);

    tf::Taskflow taskflow;
    tf::StaticPartitioner part;

    auto gen = taskflow.for_each_index(0, NUM_READINGS, 1,
        [&](int i) { readings[i] = generate_reading(); }, part);

    auto cls = taskflow.for_each_index(0, NUM_READINGS, 1,
        [&](int i) { labels[i] = classify_reading(readings[i]); }, part);
    cls.succeed(gen);

    auto branch = taskflow.for_each_index(0, NUM_READINGS, 1,
        [&](int i) {
            processed[i] = labels[i] ? amplify_reading(readings[i])
                                     : smooth_reading(readings[i]);
        }, part);
    branch.succeed(cls);

    auto cstats = taskflow.for_each_index(0, NUM_SENSORS, 1,
        [&](int sg) { stats[sg] = compute_sensor_stats(); (void)sg; }, part);
    cstats.succeed(branch);

    auto agg = taskflow.for_each_index(0, NUM_SENSORS, 1,
        [&](int sg) {
            aggregated[sg] = {stats[sg] - 0.5, stats[sg], stats[sg] + 0.5};
        }, part);
    agg.succeed(cstats);

    // Write task added after running (outside the fixed taskflow)

    long long total_us = 0;
    int measured = 0;
    std::ofstream outf("result.txt", std::ios::app);

    for (int s = 0; s < num_streams; s++) {
        auto t0 = std::chrono::high_resolution_clock::now();
        executor.run(taskflow).wait();
        auto t1 = std::chrono::high_resolution_clock::now();

        write_stream(outf, aggregated);
        if (s >= exclude_streams) {
            total_us += std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            measured++;
        }
    }

    if (measured > 0) {
        std::cout << "avg_latency_us=" << std::fixed << std::setprecision(1)
                  << (double)total_us / measured << "\n";
    }
    return 0;
}
