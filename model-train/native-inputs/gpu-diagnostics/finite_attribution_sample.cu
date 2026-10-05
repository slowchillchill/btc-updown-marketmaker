// #181 有限定点诊断；3654 次状态更新全部在同一个 kernel 内完成。
#include "../../../contracts/gpu-diagnostics/finite_attribution.cuh"
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace finite_attribution_gpu_sample;
// 固定 CPU 参考的案例数量，不定义生产容量。
constexpr uint32_t round_count = 6;
constexpr uint32_t split_count = 6;
constexpr uint32_t capacity_count = 4;
constexpr uint32_t capacity_slots = 3;
constexpr uint32_t checkpoint_count = 6;
struct RoundInput { Memory magnitude, factor; uint32_t negative, scale; };
struct SplitInput { Memory total, part, whole, factor; uint32_t scale; };
struct CapacityInput {
  Memory total, capacities[capacity_slots], factor;
  uint32_t count, scale;
};
struct GrowthInput {
  Memory initial, added, part, whole, factor, decimal_denominator;
  uint32_t iterations, scale, checkpoints[checkpoint_count];
};
struct Input {
  RoundInput rounding[round_count];
  SplitInput splits[split_count];
  CapacityInput capacities[capacity_count];
  GrowthInput growth;
};
struct SplitOutput { Memory allocated, remaining; };
struct Checkpoint { Memory cost, cumulative; };
struct Output {
  Memory rounding[round_count];
  uint32_t rounding_negative[round_count];
  SplitOutput splits[split_count];
  Memory capacities[capacity_count][capacity_slots];
  Memory initial, final_cost, cumulative, original_total;
  Checkpoint checkpoints[checkpoint_count];
  uint32_t max_cost_denominator_bits, arithmetic_fault;
};

__device__ uint32_t denominator_bits(Env &env, const Number &value, const Number &denominator) {
  Number divisor, reduced;
  cgbn_gcd(env, divisor, value, denominator);
  cgbn_div(env, reduced, denominator, divisor);
  return bits - cgbn_clz(env, reduced);
}

__global__ void finite_kernel(const Input *input, Output *output, cgbn_error_report_t *errors) {
  if (threadIdx.x >= threads_per_integer) return;
  Context context(cgbn_report_monitor, errors, 0);
  Env env(context.env<Env>());
  uint32_t fault = 0;
  Number a, b, whole, factor, allocated, remaining;
  for (uint32_t i = 0; i < round_count; ++i) {
    load(env, a, input->rounding[i].magnitude);
    load(env, factor, input->rounding[i].factor);
    round_scale12(env, allocated, a, factor, fault);
    cgbn_store(env, &output->rounding[i], allocated);
    const uint32_t negative = cgbn_compare_ui32(env, allocated, 0) == 0 ? 0 : input->rounding[i].negative;
    if (threadIdx.x == 0)
      output->rounding_negative[i] = negative;
  }
  for (uint32_t i = 0; i < split_count; ++i) {
    load(env, a, input->splits[i].total);
    load(env, b, input->splits[i].part);
    load(env, whole, input->splits[i].whole);
    load(env, factor, input->splits[i].factor);
    split_scale12(env, allocated, remaining, a, b, whole, factor, fault);
    cgbn_store(env, &output->splits[i].allocated, allocated);
    cgbn_store(env, &output->splits[i].remaining, remaining);
  }
  for (uint32_t i = 0; i < capacity_count; ++i) {
    load(env, remaining, input->capacities[i].total);
    load(env, factor, input->capacities[i].factor);
    cgbn_set_ui32(env, whole, 0);
    for (uint32_t j = 0; j < input->capacities[i].count; ++j) {
      load(env, b, input->capacities[i].capacities[j]);
      add(env, whole, whole, b, fault);
    }
    fault |= cgbn_compare(env, remaining, whole) > 0;
    for (uint32_t j = 0; j < input->capacities[i].count; ++j) {
      load(env, b, input->capacities[i].capacities[j]);
      allocate_capacity(env, allocated, remaining, whole, b, factor,
                        j + 1 == input->capacities[i].count, fault);
      cgbn_store(env, &output->capacities[i][j], allocated);
    }
    fault |= cgbn_compare_ui32(env, remaining, 0) != 0;
  }

  Number cost, added, cumulative, original_total, denominator, restored;
  load(env, cost, input->growth.initial);
  load(env, added, input->growth.added);
  load(env, b, input->growth.part);
  load(env, whole, input->growth.whole);
  load(env, factor, input->growth.factor);
  load(env, denominator, input->growth.decimal_denominator);
  cgbn_set(env, original_total, cost);
  cgbn_set_ui32(env, cumulative, 0);
  cgbn_store(env, &output->initial, cost);
  uint32_t maximum = denominator_bits(env, cost, denominator);
  uint32_t next_checkpoint = 0;
  for (uint32_t iteration = 1; iteration <= input->growth.iterations; ++iteration) {
    add(env, a, cost, added, fault);
    split_scale12(env, allocated, remaining, a, b, whole, factor, fault);
    cgbn_set(env, cost, remaining);
    add(env, cumulative, cumulative, allocated, fault);
    add(env, original_total, original_total, added, fault);
    add(env, restored, cost, cumulative, fault);
    fault |= cgbn_compare(env, restored, original_total) != 0;
    const uint32_t current = denominator_bits(env, cost, denominator);
    if (current > maximum) maximum = current;
    if (next_checkpoint < checkpoint_count && iteration == input->growth.checkpoints[next_checkpoint]) {
      cgbn_store(env, &output->checkpoints[next_checkpoint].cost, cost);
      cgbn_store(env, &output->checkpoints[next_checkpoint].cumulative, cumulative);
      ++next_checkpoint;
    }
  }
  fault |= next_checkpoint != checkpoint_count;
  cgbn_store(env, &output->final_cost, cost);
  cgbn_store(env, &output->cumulative, cumulative);
  cgbn_store(env, &output->original_total, original_total);
  if (threadIdx.x == 0) {
    output->max_cost_denominator_bits = maximum;
    output->arithmetic_fault = fault;
  }
}

static void check(cudaError_t result, const char *operation) {
  if (result != cudaSuccess)
    throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(result));
}
static Memory read_integer(std::istream &input) {
  std::string text;
  if (!(input >> text)) throw std::runtime_error("整数材料不足");
  mpz_t number;
  mpz_init(number);
  if (mpz_set_str(number, text.c_str(), 10) != 0 || mpz_sgn(number) < 0 || mpz_sizeinbase(number, 2) > bits) {
    mpz_clear(number);
    throw std::runtime_error("材料不是本诊断的非负256位整数");
  }
  Memory output{};
  size_t count = 0;
  mpz_export(output._limbs, &count, -1, sizeof(uint32_t), 0, 0, number);
  mpz_clear(number);
  return output;
}
static uint32_t read_u32(std::istream &input) {
  uint64_t value;
  if (!(input >> value) || value > UINT32_MAX) throw std::runtime_error("计数或尺度材料无效");
  return uint32_t(value);
}
static void print_amount(const Memory &input, uint32_t scale, bool negative = false) {
  mpz_t number;
  mpz_init(number);
  mpz_import(number, bits / 32, -1, sizeof(uint32_t), 0, 0, input._limbs);
  if (negative && mpz_sgn(number)) mpz_neg(number, number);
  char text[128];
  mpz_get_str(text, 10, number);
  mpz_clear(number);
  std::cout << "{\"coefficient\":\"" << text << "\",\"scale\":" << scale << "}";
}
static void launch(const Input *input, Output *output, cgbn_error_report_t *errors) {
  finite_kernel<<<1, 32>>>(input, output, errors);
  check(cudaGetLastError(), "launch");
}

int main(int argc, char **argv) {
  try {
    if (argc != 5) throw std::runtime_error("用法：sample input.txt device warmups iterations");
    const int device = std::stoi(argv[2]), warmups = std::stoi(argv[3]), iterations = std::stoi(argv[4]);
    if (device < 0 || warmups < 0 || iterations < 1) throw std::runtime_error("运行参数越界");
    std::ifstream source(argv[1]);
    Input input{};
    for (auto &row : input.rounding) {
      row.magnitude = read_integer(source); row.factor = read_integer(source);
      row.negative = read_u32(source); row.scale = read_u32(source);
      if (row.negative > 1) throw std::runtime_error("符号材料无效");
    }
    for (auto &row : input.splits) {
      row.total = read_integer(source); row.part = read_integer(source); row.whole = read_integer(source);
      row.factor = read_integer(source); row.scale = read_u32(source);
    }
    for (auto &row : input.capacities) {
      row.total = read_integer(source); row.factor = read_integer(source);
      row.scale = read_u32(source); row.count = read_u32(source);
      if (row.count == 0 || row.count > capacity_slots) throw std::runtime_error("容量案例长度与编译材料不符");
      for (uint32_t i = 0; i < row.count; ++i) row.capacities[i] = read_integer(source);
    }
    auto &growth = input.growth;
    growth.initial = read_integer(source); growth.added = read_integer(source);
    growth.part = read_integer(source); growth.whole = read_integer(source);
    growth.factor = read_integer(source); growth.decimal_denominator = read_integer(source);
    growth.scale = read_u32(source); growth.iterations = read_u32(source);
    if (growth.iterations != 3654) throw std::runtime_error("本轮诊断须执行3654次更新");
    for (auto &value : growth.checkpoints) value = read_u32(source);
    std::string extra;
    if (source >> extra) throw std::runtime_error("输入数量超出编译时案例格式");

    check(cudaSetDevice(device), "cudaSetDevice");
    cudaDeviceProp properties{};
    check(cudaGetDeviceProperties(&properties, device), "cudaGetDeviceProperties");
    int driver = 0, runtime = 0;
    check(cudaDriverGetVersion(&driver), "cudaDriverGetVersion");
    check(cudaRuntimeGetVersion(&runtime), "cudaRuntimeGetVersion");
    const auto start = std::chrono::steady_clock::now();
    Input *gpu_input = nullptr;
    Output *gpu_output = nullptr;
    cgbn_error_report_t *gpu_errors = nullptr;
    check(cudaMalloc(&gpu_input, sizeof(Input)), "allocate input");
    check(cudaMalloc(&gpu_output, sizeof(Output)), "allocate output");
    check(cudaMalloc(&gpu_errors, sizeof(cgbn_error_report_t)), "allocate errors");
    check(cudaMemset(gpu_output, 0, sizeof(Output)), "clear output");
    check(cudaMemset(gpu_errors, 0, sizeof(cgbn_error_report_t)), "clear errors");
    check(cudaMemcpy(gpu_input, &input, sizeof(Input), cudaMemcpyHostToDevice), "input H2D");
    cudaEvent_t before, after;
    check(cudaEventCreate(&before), "create before"); check(cudaEventCreate(&after), "create after");
    std::cerr << "GPU预热：" << warmups << "轮，每轮包含3654次连续状态更新\n";
    check(cudaEventRecord(before), "warmup start");
    for (int i = 0; i < warmups; ++i) launch(gpu_input, gpu_output, gpu_errors);
    check(cudaEventRecord(after), "warmup end"); check(cudaEventSynchronize(after), "warmup synchronize");
    float warmup_ms = 0;
    check(cudaEventElapsedTime(&warmup_ms, before, after), "warmup elapsed");
    std::cerr << "GPU稳态：" << iterations << "轮，中间状态不回CPU\n";
    check(cudaEventRecord(before), "steady start");
    for (int i = 0; i < iterations; ++i) launch(gpu_input, gpu_output, gpu_errors);
    check(cudaEventRecord(after), "steady end"); check(cudaEventSynchronize(after), "steady synchronize");
    float steady_ms = 0;
    check(cudaEventElapsedTime(&steady_ms, before, after), "steady elapsed");
    Output output{}; cgbn_error_report_t errors{};
    check(cudaMemcpy(&output, gpu_output, sizeof(Output), cudaMemcpyDeviceToHost), "output D2H");
    check(cudaMemcpy(&errors, gpu_errors, sizeof(errors), cudaMemcpyDeviceToHost), "errors D2H");
    const double wall_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    if (cgbn_error_report_check(&errors)) throw std::runtime_error(cgbn_error_string(&errors));
    if (output.arithmetic_fault) throw std::runtime_error("诊断容量、前提或逐步守恒检查失败");

    std::cout << std::setprecision(9)
      << "{\"schema\":\"gpu_finite_attribution_sample_v1\",\"device\":" << device
      << ",\"device_name\":" << std::quoted(properties.name)
      << ",\"compute_capability\":[" << properties.major << "," << properties.minor
      << "],\"cuda_driver\":" << driver << ",\"cuda_runtime\":" << runtime
      << ",\"bits\":" << bits << ",\"wide_bits\":" << 2 * bits
      << ",\"threads_per_integer\":" << threads_per_integer << ",\"block_threads\":32"
      << ",\"warmups\":" << warmups << ",\"iterations\":" << iterations
      << ",\"kernels_per_iteration\":1,\"growth_steps_per_iteration\":3654"
      << ",\"warmup_gpu_ms\":" << warmup_ms << ",\"steady_gpu_ms\":" << steady_ms
      << ",\"steady_iteration_ms\":" << steady_ms / iterations
      << ",\"allocation_transfer_warmup_steady_wall_ms\":" << wall_ms
      << ",\"owned_device_allocation_bytes\":" << sizeof(Input) + sizeof(Output) + sizeof(errors)
      << ",\"h2d_calls\":1,\"h2d_bytes\":" << sizeof(Input)
      << ",\"d2h_calls\":2,\"d2h_bytes\":" << sizeof(Output) + sizeof(errors)
      << ",\"device_memset_bytes\":" << sizeof(Output) + sizeof(errors)
      << ",\"arithmetic_fault\":" << output.arithmetic_fault << ",\"rounding\":[";
    for (uint32_t i = 0; i < round_count; ++i) {
      if (i) std::cout << ",";
      print_amount(output.rounding[i], input.rounding[i].scale, output.rounding_negative[i]);
    }
    std::cout << "],\"splits\":[";
    for (uint32_t i = 0; i < split_count; ++i) {
      if (i) std::cout << ",";
      std::cout << "{\"allocated\":"; print_amount(output.splits[i].allocated, input.splits[i].scale);
      std::cout << ",\"remaining\":"; print_amount(output.splits[i].remaining, input.splits[i].scale);
      std::cout << "}";
    }
    std::cout << "],\"capacity_allocations_candidate\":[";
    for (uint32_t i = 0; i < capacity_count; ++i) {
      if (i) std::cout << ",";
      std::cout << "[";
      for (uint32_t j = 0; j < input.capacities[i].count; ++j) {
        if (j) std::cout << ",";
        print_amount(output.capacities[i][j], input.capacities[i].scale);
      }
      std::cout << "]";
    }
    std::cout << "],\"growth\":{\"iterations\":" << growth.iterations << ",\"initial_cost\":";
    print_amount(output.initial, growth.scale);
    std::cout << ",\"final_cost\":"; print_amount(output.final_cost, growth.scale);
    std::cout << ",\"cumulative_allocated\":"; print_amount(output.cumulative, growth.scale);
    std::cout << ",\"original_total_cost\":"; print_amount(output.original_total, growth.scale);
    std::cout << ",\"max_persisted_denominator_bits\":" << output.max_cost_denominator_bits
              << ",\"checkpoints\":[";
    for (uint32_t i = 0; i < checkpoint_count; ++i) {
      if (i) std::cout << ",";
      std::cout << "{\"iteration\":" << growth.checkpoints[i] << ",\"cost\":";
      print_amount(output.checkpoints[i].cost, growth.scale);
      std::cout << ",\"cumulative_allocated\":";
      print_amount(output.checkpoints[i].cumulative, growth.scale);
      std::cout << "}";
    }
    std::cout << "]}}\n";
    check(cudaEventDestroy(before), "destroy before"); check(cudaEventDestroy(after), "destroy after");
    check(cudaFree(gpu_input), "free input"); check(cudaFree(gpu_output), "free output");
    check(cudaFree(gpu_errors), "free errors");
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "有限定点 GPU 诊断失败：" << error.what() << "\n";
    return 1;
  }
}
