// #181 标准CUDA末端特征有限诊断。输入整数与比值转换都在设备端衔接。
#include "../../../contracts/gpu-diagnostics/standard_projection.cuh"
#include <cuda_runtime.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using standard_projection_sample::scaled_ratio;
constexpr uint32_t block_threads = 32;
struct InputRow {
  uint32_t operation, negative, numerator_offset, numerator_count, denominator_offset, denominator_count;
};
struct OutputRow {
  uint64_t ratio_f64_bits, sqrt_bits, transform_bits;
  uint32_t input_f32_bits, result_bits;
};
struct Timing { float warmup_ms, steady_ms; };

template <bool fp32>
__global__ void projection_kernel(const InputRow *input, const uint32_t *words, OutputRow *output, uint32_t sample_count) {
  const uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;
  if (index >= sample_count) return;
  const auto row = input[index];
  const double ratio = scaled_ratio(words + row.numerator_offset, row.numerator_count,
                                    words + row.denominator_offset, row.denominator_count, row.negative != 0);
  OutputRow result{};
  result.ratio_f64_bits = static_cast<uint64_t>(__double_as_longlong(ratio));
  if constexpr (fp32) {
    const float x = __double2float_rn(ratio);
    result.input_f32_bits = __float_as_uint(x);
    float transformed;
    if (row.operation == 0) {
      transformed = copysignf(log1pf(fabsf(x)), x);
    } else {
      const float root = sqrtf(x);
      result.sqrt_bits = __float_as_uint(root);
      transformed = log1pf(root);
    }
    result.transform_bits = __float_as_uint(transformed);
    result.result_bits = __float_as_uint(transformed);
  } else {
    double transformed;
    if (row.operation == 0) {
      transformed = copysign(log1p(fabs(ratio)), ratio);
    } else {
      const double root = sqrt(ratio);
      result.sqrt_bits = static_cast<uint64_t>(__double_as_longlong(root));
      transformed = log1p(root);
    }
    result.transform_bits = static_cast<uint64_t>(__double_as_longlong(transformed));
    result.result_bits = __float_as_uint(__double2float_rn(transformed));
  }
  output[index] = result;
}

void check(cudaError_t status, const char *operation) {
  if (status != cudaSuccess) throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
}

uint32_t read_u32(std::istream &source) {
  uint64_t value;
  if (!(source >> value) || value > UINT32_MAX) throw std::runtime_error("无效的诊断uint32输入");
  return static_cast<uint32_t>(value);
}

template <bool fp32>
Timing measure(const InputRow *input, const uint32_t *words, OutputRow *output, uint32_t sample_count,
               int warmups, int iterations) {
  const char *label = fp32 ? "FP32对照" : "FP64转FP32候选";
  cudaEvent_t before, after;
  check(cudaEventCreate(&before), "create before");
  check(cudaEventCreate(&after), "create after");
  auto launches = [&](int count) {
    for (int i = 0; i < count; ++i) {
      projection_kernel<fp32><<<(sample_count + block_threads - 1) / block_threads, block_threads>>>(input, words, output, sample_count);
      check(cudaGetLastError(), "projection kernel launch");
    }
  };
  Timing timing{};
  std::cerr << label << "预热：" << warmups << "轮，每轮" << sample_count << "项\n";
  check(cudaEventRecord(before), "warmup start");
  launches(warmups);
  check(cudaEventRecord(after), "warmup end");
  check(cudaEventSynchronize(after), "warmup synchronize");
  check(cudaEventElapsedTime(&timing.warmup_ms, before, after), "warmup elapsed");
  std::cerr << label << "稳态：" << iterations << "轮\n";
  check(cudaEventRecord(before), "steady start");
  launches(iterations);
  check(cudaEventRecord(after), "steady end");
  check(cudaEventSynchronize(after), "steady synchronize");
  check(cudaEventElapsedTime(&timing.steady_ms, before, after), "steady elapsed");
  check(cudaEventDestroy(before), "destroy before");
  check(cudaEventDestroy(after), "destroy after");
  return timing;
}

void print_method(const OutputRow *rows, uint32_t sample_count, const Timing &timing, int iterations) {
  std::cout << "{\"warmup_gpu_ms\":" << timing.warmup_ms
            << ",\"steady_gpu_ms\":" << timing.steady_ms
            << ",\"steady_iteration_ms\":" << timing.steady_ms / iterations << ",\"rows\":[";
  for (uint32_t i = 0; i < sample_count; ++i) {
    if (i) std::cout << ',';
    const auto &r = rows[i];
    std::cout << "{\"index\":" << i << ",\"ratio_f64_bits\":" << r.ratio_f64_bits
              << ",\"input_f32_bits\":" << r.input_f32_bits << ",\"sqrt_bits\":" << r.sqrt_bits
              << ",\"transform_bits\":" << r.transform_bits << ",\"result_bits\":" << r.result_bits << '}';
  }
  std::cout << "]}";
}

int main(int argc, char **argv) {
  try {
    if (argc != 5) throw std::runtime_error("用法：sample input.txt device warmups iterations");
    const int device = std::stoi(argv[2]), warmups = std::stoi(argv[3]), iterations = std::stoi(argv[4]);
    if (device < 0 || warmups < 1 || iterations < 1) throw std::runtime_error("诊断运行参数越界");
    std::ifstream source(argv[1]);
    const uint32_t sample_count = read_u32(source);
    if (sample_count == 0) throw std::runtime_error("有限诊断输入为空");
    std::vector<InputRow> input(sample_count);
    std::vector<uint32_t> words;
    for (auto &row : input) {
      row.operation = read_u32(source); row.negative = read_u32(source);
      row.numerator_count = read_u32(source); row.denominator_count = read_u32(source);
      // 本诊断输入只有已选有限材料；此容量检查不是生产合法值规则。
      if (row.operation > 1 || row.negative > 1 || (row.operation == 1 && row.negative)
          || row.numerator_count == 0 || row.denominator_count == 0
          || row.numerator_count > 40 || row.denominator_count > 40)
        throw std::runtime_error("输入不符合有限诊断布局");
      row.numerator_offset = static_cast<uint32_t>(words.size());
      for (uint32_t i = 0; i < row.numerator_count; ++i) words.push_back(read_u32(source));
      if (row.numerator_count > 1 && words.back() == 0) throw std::runtime_error("分子存在多余高位零");
      row.denominator_offset = static_cast<uint32_t>(words.size());
      for (uint32_t i = 0; i < row.denominator_count; ++i) words.push_back(read_u32(source));
      if (words.back() == 0) throw std::runtime_error("分母为零或存在多余高位零");
    }
    std::string extra;
    if (source >> extra) throw std::runtime_error("输入包含多余材料");
    check(cudaSetDevice(device), "cudaSetDevice");
    cudaDeviceProp properties{};
    check(cudaGetDeviceProperties(&properties, device), "cudaGetDeviceProperties");
    int driver = 0, runtime = 0;
    check(cudaDriverGetVersion(&driver), "cudaDriverGetVersion");
    check(cudaRuntimeGetVersion(&runtime), "cudaRuntimeGetVersion");
    const auto started = std::chrono::steady_clock::now();
    InputRow *gpu_input = nullptr;
    uint32_t *gpu_words = nullptr;
    OutputRow *gpu_fp64 = nullptr, *gpu_fp32 = nullptr;
    const size_t input_bytes = input.size() * sizeof(InputRow);
    const size_t word_bytes = words.size() * sizeof(uint32_t), result_bytes = sample_count * sizeof(OutputRow);
    check(cudaMalloc(&gpu_input, input_bytes), "allocate metadata");
    check(cudaMalloc(&gpu_words, word_bytes), "allocate integer words");
    check(cudaMalloc(&gpu_fp64, result_bytes), "allocate fp64 output");
    check(cudaMalloc(&gpu_fp32, result_bytes), "allocate fp32 output");
    check(cudaMemcpy(gpu_input, input.data(), input_bytes, cudaMemcpyHostToDevice), "metadata H2D");
    check(cudaMemcpy(gpu_words, words.data(), word_bytes, cudaMemcpyHostToDevice), "integer words H2D");
    const auto timing64 = measure<false>(gpu_input, gpu_words, gpu_fp64, sample_count, warmups, iterations);
    const auto timing32 = measure<true>(gpu_input, gpu_words, gpu_fp32, sample_count, warmups, iterations);
    std::vector<OutputRow> output64(sample_count), output32(sample_count);
    check(cudaMemcpy(output64.data(), gpu_fp64, result_bytes, cudaMemcpyDeviceToHost), "fp64 output D2H");
    check(cudaMemcpy(output32.data(), gpu_fp32, result_bytes, cudaMemcpyDeviceToHost), "fp32 output D2H");
    const double wall_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
    std::cout << std::setprecision(17)
              << "{\"schema\":\"gpu_standard_projection_sample_v1\",\"device\":" << device
              << ",\"device_name\":" << std::quoted(properties.name)
              << ",\"compute_capability\":[" << properties.major << ',' << properties.minor
              << "],\"cuda_driver\":" << driver << ",\"cuda_runtime\":" << runtime
              << ",\"sample_count\":" << sample_count << ",\"block_threads\":" << block_threads
              << ",\"warmups\":" << warmups << ",\"iterations\":" << iterations
              << ",\"kernels_per_method_iteration\":1,\"owned_device_allocation_bytes\":"
              << input_bytes + word_bytes + 2 * result_bytes
              << ",\"h2d_calls\":2,\"h2d_bytes\":" << input_bytes + word_bytes
              << ",\"d2h_calls\":2,\"d2h_bytes\":" << 2 * result_bytes
              << ",\"device_memset_bytes\":0,\"allocation_transfer_warmup_steady_wall_ms\":" << wall_ms
              << ",\"fp64_then_fp32\":";
    print_method(output64.data(), sample_count, timing64, iterations);
    std::cout << ",\"fp32_control\":";
    print_method(output32.data(), sample_count, timing32, iterations);
    std::cout << "}\n";
    check(cudaFree(gpu_input), "free input"); check(cudaFree(gpu_words), "free words");
    check(cudaFree(gpu_fp64), "free fp64 output"); check(cudaFree(gpu_fp32), "free fp32 output");
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "标准数学函数GPU诊断失败：" << error.what() << '\n';
    return 1;
  }
}
