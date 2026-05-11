#include <chrono>
#include <cstdio>
#include <vector>

#include "helpers.hpp"
#include <taskflow/algorithm/for_each.hpp>

int main(int argc, char **argv)
{
    Cli cli = parse_args(argc, argv);

    const int N = cli.n;
    const int W = cli.workers;
    const int ITERS = cli.iterations;
    const int WARM = cli.warmup;

    std::printf("Taskflow Wavefront N=%d workers=%d partitioner=%s\n",
                N, W, cli.partitioner.c_str());

    std::vector<double> grid(static_cast<std::size_t>(N) * N, 0.0);
    for (int j = 0; j < N; ++j)
        grid[j] = static_cast<double>(j + 1);
    for (int i = 1; i < N; ++i)
        grid[i * N] = static_cast<double>(i + 1);

    tf::Executor executor(W);
    if (cli.pin)
        pin_workers(executor, 1);

    tf::Taskflow taskflow;
    double *grid_ptr = grid.data();

    if (cli.partitioner == "static")
        build_wavefront_dag(taskflow, N, grid_ptr, tf::StaticPartitioner{});
    else if (cli.partitioner == "dynamic")
        build_wavefront_dag(taskflow, N, grid_ptr, tf::DynamicPartitioner{});
    else if (cli.partitioner == "guided")
        build_wavefront_dag(taskflow, N, grid_ptr, tf::GuidedPartitioner{});
    else
        build_wavefront_dag(taskflow, N, grid_ptr, tf::RandomPartitioner{});

    auto run_sweep = [&]() { executor.run(taskflow).get(); };

    for (int w = 0; w < WARM; ++w)
        run_sweep();

    double total_ms = 0.0;
    for (int iter = 0; iter < ITERS; ++iter)
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        run_sweep();
        auto t1 = std::chrono::high_resolution_clock::now();
        double elapsed_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;
        total_ms += elapsed_ms;
        std::printf("  iter %2d: %.4fms\n", iter + 1, elapsed_ms);
    }

    double ms_per_iter = total_ms / ITERS;
    std::string system_label = (cli.pin ? "taskflow_pinned" : "taskflow");
    system_label += "_" + cli.partitioner;
    std::printf(
        "%s Wavefront | n=%d | workers=%d | iters=%d | total=%.4fms | %.4fms/iter\n",
        system_label.c_str(), N, W, ITERS, total_ms, ms_per_iter);

    append_wavefront_csv(cli.output, system_label, N, W, ITERS, total_ms, ms_per_iter);
    return 0;
}
