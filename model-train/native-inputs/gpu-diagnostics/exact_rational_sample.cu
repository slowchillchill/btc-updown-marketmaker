// #181 有限精确数值诊断；CPU 只准备整数材料并在结束后读取结果。
#include "../../../contracts/gpu-diagnostics/exact_rational.cuh"
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace rational_gpu_sample;
struct Input {
  Stored normalization[6], comparisons[3][2], cancellation[2], division[2];
  Stored midpoints[3], divisor, rounding[6];
};
struct Output {
  Stored normalization[6], cancellation, division;
  Stored differences[2], squares[2], sums[2], mean;
  int32_t comparisons[3];
  uint32_t rounded_bits[6], arithmetic_fault;
};

__global__ void rational_kernel(const Input *input, Output *output,
                                cgbn_error_report_t *errors) {
  // CUDA block 保留完整 warp；仅首个 TPI8 整数组执行，scalar 由线程 0 写回。
  if (threadIdx.x >= threads_per_integer) return;
  Context context(cgbn_report_monitor, errors, 0);
  Env env(context.env<Env>());
  uint32_t fault = 0;
  Rational a, b, value;
  for (uint32_t i = 0; i < 6; ++i) {
    from_stored(env, value, input->normalization[i], fault);
    store(env, output->normalization[i], value);
  }
  for (uint32_t i = 0; i < 3; ++i) {
    from_stored(env, a, input->comparisons[i][0], fault);
    from_stored(env, b, input->comparisons[i][1], fault);
    const int order = compare(env, a, b, fault);
    if (threadIdx.x == 0) output->comparisons[i] = order;
  }
  from_stored(env, a, input->cancellation[0], fault);
  from_stored(env, b, input->cancellation[1], fault);
  add_sub(env, value, a, b, false, fault);
  store(env, output->cancellation, value);
  from_stored(env, a, input->division[0], fault);
  from_stored(env, b, input->division[1], fault);
  mul_div(env, value, a, b, true, fault);
  store(env, output->division, value);

  Rational sum, previous, current, difference, square;
  cgbn_set_ui32(env, sum.numerator, 0);
  cgbn_set_ui32(env, sum.denominator, 1);
  sum.negative = 0;
  from_stored(env, previous, input->midpoints[0], fault);
  for (uint32_t i = 0; i < 2; ++i) {
    from_stored(env, current, input->midpoints[i + 1], fault);
    add_sub(env, difference, current, previous, true, fault);
    mul_div(env, square, difference, difference, false, fault);
    add_sub(env, sum, sum, square, false, fault);
    store(env, output->differences[i], difference);
    store(env, output->squares[i], square);
    store(env, output->sums[i], sum);
    previous = current;
  }
  from_stored(env, b, input->divisor, fault);
  mul_div(env, value, sum, b, true, fault);
  store(env, output->mean, value);
  for (uint32_t i = 0; i < 6; ++i) {
    from_stored(env, value, input->rounding[i], fault);
    const uint32_t result = round_f32(env, value, fault);
    if (threadIdx.x == 0) output->rounded_bits[i] = result;
  }
  if (threadIdx.x == 0) output->arithmetic_fault = fault;
}

static void check(cudaError_t result, const char *operation) {
  if (result != cudaSuccess)
    throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(result));
}
static uint32_t read_integer(std::istream &input, Memory &output) {
  std::string text;
  if (!(input >> text)) throw std::runtime_error("输入整数数量不足");
  mpz_t number;
  mpz_init(number);
  if (mpz_set_str(number, text.c_str(), 10) != 0 ||
      mpz_sizeinbase(number, 2) > bits) {
    mpz_clear(number);
    throw std::runtime_error("诊断输入不是有效的有界十进制整数");
  }
  const uint32_t negative = mpz_sgn(number) < 0;
  std::memset(&output, 0, sizeof(output));
  size_t count = 0;
  mpz_export(output._limbs, &count, -1, sizeof(uint32_t), 0, 0, number);
  mpz_clear(number);
  return negative;
}
static Stored read_rational(std::istream &input) {
  Stored value{};
  value.numerator_negative = read_integer(input, value.numerator);
  value.denominator_negative = read_integer(input, value.denominator);
  return value;
}
static std::string decimal(const Memory &input, uint32_t negative) {
  mpz_t number;
  mpz_init(number);
  mpz_import(number, bits / 32, -1, sizeof(uint32_t), 0, 0, input._limbs);
  if (negative) mpz_neg(number, number);
  char text[128];
  mpz_get_str(text, 10, number);
  mpz_clear(number);
  return text;
}
static void print_rational(const Stored &value) {
  std::cout << "{\"numerator\":\""
            << decimal(value.numerator, value.numerator_negative)
            << "\",\"denominator\":\""
            << decimal(value.denominator, value.denominator_negative) << "\"}";
}
template <size_t count> static void print_array(const Stored (&values)[count]) {
  std::cout << "[";
  for (size_t i = 0; i < count; ++i) {
    if (i) std::cout << ",";
    print_rational(values[i]);
  }
  std::cout << "]";
}
static void launch(const Input *input, Output *output, cgbn_error_report_t *errors) {
  rational_kernel<<<1, 32>>>(input, output, errors);
  check(cudaGetLastError(), "launch");
}

int main(int argc, char **argv) {
  try {
    if (argc != 5) throw std::runtime_error("用法：sample input.txt device warmups iterations");
    const int device = std::stoi(argv[2]), warmups = std::stoi(argv[3]);
    const int iterations = std::stoi(argv[4]);
    if (device < 0 || warmups < 0 || iterations < 1)
      throw std::runtime_error("device/warmups/iterations 参数越界");
    std::ifstream source(argv[1]);
    Input input{};
    for (auto &value : input.normalization) value = read_rational(source);
    for (auto &pair : input.comparisons)
      for (auto &value : pair) value = read_rational(source);
    for (auto &value : input.cancellation) value = read_rational(source);
    for (auto &value : input.division) value = read_rational(source);
    for (auto &value : input.midpoints) value = read_rational(source);
    input.divisor = read_rational(source);
    for (auto &value : input.rounding) value = read_rational(source);
    std::string extra;
    if (source >> extra) throw std::runtime_error("输入整数数量超出固定样本格式");

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
    check(cudaMalloc(&gpu_input, sizeof(Input)), "cudaMalloc input");
    check(cudaMalloc(&gpu_output, sizeof(Output)), "cudaMalloc output");
    check(cudaMalloc(&gpu_errors, sizeof(cgbn_error_report_t)), "cudaMalloc errors");
    check(cudaMemset(gpu_output, 0, sizeof(Output)), "cudaMemset output");
    check(cudaMemset(gpu_errors, 0, sizeof(cgbn_error_report_t)), "cudaMemset errors");
    check(cudaMemcpy(gpu_input, &input, sizeof(Input), cudaMemcpyHostToDevice), "input H2D");
    cudaEvent_t before, after;
    check(cudaEventCreate(&before), "event create before");
    check(cudaEventCreate(&after), "event create after");
    std::cerr << "GPU预热：" << warmups << "轮，每轮1个连续有理数kernel\n";
    check(cudaEventRecord(before), "warmup event start");
    for (int i = 0; i < warmups; ++i) launch(gpu_input, gpu_output, gpu_errors);
    check(cudaEventRecord(after), "warmup event end");
    check(cudaEventSynchronize(after), "warmup synchronize");
    float warmup_ms = 0;
    check(cudaEventElapsedTime(&warmup_ms, before, after), "warmup elapsed");
    std::cerr << "GPU稳态：" << iterations << "轮；循环中无输入输出回传\n";
    check(cudaEventRecord(before), "steady event start");
    for (int i = 0; i < iterations; ++i) launch(gpu_input, gpu_output, gpu_errors);
    check(cudaEventRecord(after), "steady event end");
    check(cudaEventSynchronize(after), "steady synchronize");
    float steady_ms = 0;
    check(cudaEventElapsedTime(&steady_ms, before, after), "steady elapsed");
    Output output{};
    cgbn_error_report_t errors{};
    check(cudaMemcpy(&output, gpu_output, sizeof(Output), cudaMemcpyDeviceToHost), "output D2H");
    check(cudaMemcpy(&errors, gpu_errors, sizeof(errors), cudaMemcpyDeviceToHost), "errors D2H");
    const double wall_ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - start).count();
    if (cgbn_error_report_check(&errors))
      throw std::runtime_error(cgbn_error_string(&errors));
    if (output.arithmetic_fault)
      throw std::runtime_error("诊断位宽/除零检查失败");

    std::cout << std::setprecision(9)
              << "{\"schema\":\"gpu_exact_rational_sample_v1\",\"device\":" << device
              << ",\"device_name\":" << std::quoted(properties.name)
              << ",\"compute_capability\":[" << properties.major << "," << properties.minor
              << "],\"cuda_driver\":" << driver << ",\"cuda_runtime\":" << runtime
              << ",\"bits\":" << bits << ",\"wide_bits\":" << 2 * bits
              << ",\"threads_per_integer\":" << threads_per_integer
              << ",\"block_threads\":32,\"warmups\":" << warmups
              << ",\"iterations\":" << iterations << ",\"warmup_gpu_ms\":" << warmup_ms
              << ",\"kernels_per_iteration\":1"
              << ",\"steady_gpu_ms\":" << steady_ms
              << ",\"steady_iteration_ms\":" << steady_ms / iterations
              << ",\"allocation_transfer_warmup_steady_wall_ms\":" << wall_ms
              << ",\"owned_device_allocation_bytes\":"
              << sizeof(Input) + sizeof(Output) + sizeof(cgbn_error_report_t)
              << ",\"h2d_calls\":1,\"h2d_bytes\":" << sizeof(Input)
              << ",\"d2h_calls\":2,\"d2h_bytes\":" << sizeof(Output) + sizeof(errors)
              << ",\"device_memset_bytes\":" << sizeof(Output) + sizeof(errors)
              << ",\"normalization\":";
    print_array(output.normalization);
    std::cout << ",\"comparisons\":[";
    for (int i = 0; i < 3; ++i) { if (i) std::cout << ","; std::cout << output.comparisons[i]; }
    std::cout << "],\"cancellation\":"; print_rational(output.cancellation);
    std::cout << ",\"signed_division\":"; print_rational(output.division);
    std::cout << ",\"rms\":{\"differences\":"; print_array(output.differences);
    std::cout << ",\"squares\":"; print_array(output.squares);
    std::cout << ",\"sums\":"; print_array(output.sums);
    std::cout << ",\"mean\":"; print_rational(output.mean);
    std::cout << "},\"round_f32_bits\":[";
    for (int i = 0; i < 6; ++i) { if (i) std::cout << ","; std::cout << output.rounded_bits[i]; }
    std::cout << "]}\n";
    check(cudaEventDestroy(before), "event destroy before");
    check(cudaEventDestroy(after), "event destroy after");
    check(cudaFree(gpu_input), "free input");
    check(cudaFree(gpu_output), "free output");
    check(cudaFree(gpu_errors), "free errors");
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "精确有理数 GPU 诊断失败：" << error.what() << "\n";
    return 1;
  }
}
