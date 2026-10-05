// #181 技术诊断：固定 CPU 参考输入；所有费用→支持集→选定意图依赖在同一卡上。
#include "../../../sim-system/gpu-diagnostics/exact_buy_cost.cuh"
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace sim_gpu_sample;
struct Input {
  CostInput costs[4];
  Memory cash_micro, minimum_fok_micro;
  Memory add_a, add_b, multiply_a, multiply_b, divisor, round_n, round_d;
  Memory tokens[2];
  uint32_t choices[2];
  char condition[67];
};
struct Output {
  CostOutput costs[4];
  uint32_t mask[999], support_fault[999];
  Memory quantity, spend_b, tokens[2];
  uint32_t prices[2], variants[2], post_only, selection_fault;
  char condition[67];
  Memory sum, product_low, product_high, quotient, remainder, rounded;
  uint32_t carry, probe_fault;
};
__global__ void costs_kernel(cgbn_error_report_t *errors, const Input *input, Output *output) {
  const uint32_t id = threadIdx.x / threads_per_integer;
  if (id >= 4) return;
  Context context(cgbn_report_monitor, errors, id);
  Env env(context.env<Env>());
  Number principal, fee;
  uint32_t fault = 0;
  limit_buy_cost_e1(env, input->costs[id], principal, fee, fault);
  cgbn_store(env, &output->costs[id].principal_micro, principal);
  cgbn_store(env, &output->costs[id].fee_1e5_units, fee);
  if (threadIdx.x % threads_per_integer == 0) output->costs[id].arithmetic_fault = fault;
}
__device__ void fok_minimum(Env &env, const CostInput &input, const Memory &minimum,
                            Number &step, Number &minimum_cost, uint32_t &fault) {
  Number quantity, scaled, tick, k, remainder, required;
  load_read_only(env, quantity, input.quantity_centishares);
  scale(env, scaled, quantity, 10000, fault);
  cgbn_set_ui32(env, tick, input.tick_denominator);
  cgbn_div(env, step, scaled, tick);
  load_read_only(env, required, minimum);
  cgbn_div_rem(env, k, remainder, required, step);
  if (cgbn_compare_ui32(env, remainder, 0) != 0)
    fault |= cgbn_add_ui32(env, k, k, 1) != 0;
  fault |= cgbn_compare_ui32(env, k, input.tick_denominator) >= 0;
  multiply(env, minimum_cost, k, step, fault);
}
__global__ void support_kernel(cgbn_error_report_t *errors, const Input *input, Output *output) {
  const uint32_t id = (blockIdx.x * blockDim.x + threadIdx.x) / threads_per_integer;
  if (id >= 999) return;
  Context context(cgbn_report_monitor, errors, id + 4);
  Env env(context.env<Env>());
  uint32_t fault = 0, valid = 0;
  CostInput candidate = input->costs[0];
  candidate.price_index = id + 1;
  if (candidate.price_index < candidate.tick_denominator) {
    Number principal, fee, fee_micro, minimum_b, step, total, cash;
    limit_buy_cost_e1(env, candidate, principal, fee, fault);
    fok_minimum(env, candidate, input->minimum_fok_micro, step, minimum_b, fault);
    scale(env, fee_micro, fee, 10, fault);
    fault |= cgbn_add(env, total, principal, fee_micro) != 0;
    fault |= cgbn_add(env, total, total, minimum_b) != 0;
    load_read_only(env, cash, input->cash_micro);
    valid = cgbn_compare(env, total, cash) <= 0;
  }
  if (threadIdx.x % threads_per_integer == 0) {
    output->mask[id] = valid;
    output->support_fault[id] = fault;
  }
}
__global__ void select_kernel(cgbn_error_report_t *errors, const Input *input, Output *output) {
  // block 保持完整 warp；本诊断只使用首个完整 TPI 组，避免重复写入。
  if (threadIdx.x >= threads_per_integer) return;
  Context context(cgbn_report_monitor, errors, 1003);
  Env env(context.env<Env>());
  uint32_t fault = !output->mask[input->choices[0]];
  CostInput candidate = input->costs[0];
  candidate.price_index = input->choices[0] + 1;
  Number principal, fee, fee_micro, step, minimum_b, spend, total, cash, required, value;
  limit_buy_cost_e1(env, candidate, principal, fee, fault);
  fok_minimum(env, candidate, input->minimum_fok_micro, step, minimum_b, fault);
  scale(env, spend, step, input->choices[1] + 1, fault);
  load_read_only(env, required, input->minimum_fok_micro);
  fault |= cgbn_compare(env, spend, required) < 0;
  fault |= input->choices[1] + 1 >= candidate.tick_denominator;
  scale(env, fee_micro, fee, 10, fault);
  fault |= cgbn_add(env, total, principal, fee_micro) != 0;
  fault |= cgbn_add(env, total, total, spend) != 0;
  load_read_only(env, cash, input->cash_micro);
  fault |= cgbn_compare(env, total, cash) > 0;
  cgbn_store(env, &output->spend_b, spend);
  load_read_only(env, value, candidate.quantity_centishares);
  cgbn_store(env, &output->quantity, value);
  for (uint32_t i = 0; i < 2; ++i) {
    load_read_only(env, value, input->tokens[i]);
    cgbn_store(env, &output->tokens[i], value);
  }
  if (threadIdx.x == 0) {
    output->prices[0] = input->choices[0] + 1;
    output->prices[1] = input->choices[1] + 1;
    output->variants[0] = 0; // 已固定的普通 GTC。
    output->variants[1] = 1; // 已固定的 FOK。
    output->post_only = 0;
    for (uint32_t i = 0; i < sizeof(output->condition); ++i)
      output->condition[i] = input->condition[i];
    output->selection_fault = fault;
  }
}
__global__ void probes_kernel(cgbn_error_report_t *errors, const Input *input, Output *output) {
  if (threadIdx.x >= threads_per_integer) return;
  Context context(cgbn_report_monitor, errors, 1004);
  Env env(context.env<Env>());
  Number a, b, value, quotient, remainder, denominator;
  Wide product;
  load_read_only(env, a, input->add_a);
  load_read_only(env, b, input->add_b);
  const uint32_t carry = cgbn_add(env, value, a, b);
  cgbn_store(env, &output->sum, value);
  load_read_only(env, a, input->multiply_a);
  load_read_only(env, b, input->multiply_b);
  cgbn_mul_wide(env, product, a, b);
  cgbn_store(env, &output->product_low, product._low);
  cgbn_store(env, &output->product_high, product._high);
  load_read_only(env, denominator, input->divisor);
  uint32_t fault = cgbn_compare(env, product._high, denominator) >= 0;
  if (!fault) {
    cgbn_div_rem_wide(env, quotient, remainder, product, denominator);
    cgbn_store(env, &output->quotient, quotient);
    cgbn_store(env, &output->remainder, remainder);
  }
  load_read_only(env, a, input->round_n);
  load_read_only(env, denominator, input->round_d);
  half_up(env, value, a, denominator, fault);
  cgbn_store(env, &output->rounded, value);
  if (threadIdx.x == 0) { output->carry = carry; output->probe_fault = fault; }
}

static void check(cudaError_t status) {
  if (status != cudaSuccess) throw std::runtime_error(cudaGetErrorString(status));
}
static void read_number(std::istream &stream, Memory &value) {
  std::string text;
  if (!(stream >> text)) throw std::runtime_error("输入整数缺失");
  mpz_t number;
  mpz_init(number);
  if (mpz_set_str(number, text.c_str(), 10) != 0 || mpz_sgn(number) < 0 ||
      mpz_sizeinbase(number, 2) > bits) {
    mpz_clear(number); throw std::runtime_error("诊断输入不属于256位非负表示");
  }
  std::memset(&value, 0, sizeof(value));
  mpz_export(value._limbs, nullptr, -1, sizeof(uint32_t), 0, 0, number);
  mpz_clear(number);
}
static std::string decimal(const Memory &value) {
  mpz_t number;
  mpz_init(number);
  mpz_import(number, bits / 32, -1, sizeof(uint32_t), 0, 0, value._limbs);
  char text[128]; // 256 位无符号数至多78位十进制；这里只做输出格式化。
  mpz_get_str(text, 10, number);
  mpz_clear(number);
  return text;
}
static void launch(cgbn_error_report_t *errors, const Input *input, Output *output) {
  costs_kernel<<<1, 32>>>(errors, input, output);
  support_kernel<<<(999 + 15) / 16, 128>>>(errors, input, output);
  select_kernel<<<1, 32>>>(errors, input, output);
  probes_kernel<<<1, 32>>>(errors, input, output);
  check(cudaGetLastError());
}
int main(int argc, char **argv) {
  try {
    if (argc != 5) throw std::runtime_error("参数：输入文件 设备号 预热次数 稳态次数");
    const int device = std::stoi(argv[2]), warmups = std::stoi(argv[3]), iterations = std::stoi(argv[4]);
    if (warmups < 1 || iterations < 1) throw std::runtime_error("诊断次数必须为正");
    std::ifstream stream(argv[1]);
    Input input{};
    for (auto &cost : input.costs) {
      read_number(stream, cost.quantity_centishares);
      read_number(stream, cost.rate_numerator);
      read_number(stream, cost.rate_denominator);
      stream >> cost.tick_denominator >> cost.price_index;
    }
    read_number(stream, input.cash_micro); read_number(stream, input.minimum_fok_micro);
    for (auto value : {&input.add_a, &input.add_b, &input.multiply_a, &input.multiply_b,
                       &input.divisor, &input.round_n, &input.round_d, &input.tokens[0], &input.tokens[1]})
      read_number(stream, *value);
    std::string condition;
    stream >> input.choices[0] >> input.choices[1] >> condition;
    if (!stream || condition.size() != 66 || input.choices[0] >= 999 || input.choices[1] >= 999)
      throw std::runtime_error("诊断输入形状错误");
    std::memcpy(input.condition, condition.c_str(), 67);
    check(cudaSetDevice(device));
    cudaDeviceProp prop{}; check(cudaGetDeviceProperties(&prop, device));
    int driver = 0, runtime = 0; check(cudaDriverGetVersion(&driver)); check(cudaRuntimeGetVersion(&runtime));
    const auto start = std::chrono::steady_clock::now();
    Input *gpu_input; Output *gpu_output; cgbn_error_report_t *errors;
    check(cudaMalloc((void **)&gpu_input, sizeof(Input)));
    check(cudaMalloc((void **)&gpu_output, sizeof(Output)));
    check(cudaMalloc((void **)&errors, sizeof(cgbn_error_report_t)));
    check(cudaMemset(gpu_output, 0, sizeof(Output))); check(cudaMemset(errors, 0, sizeof(cgbn_error_report_t)));
    check(cudaMemcpy(gpu_input, &input, sizeof(Input), cudaMemcpyHostToDevice));
    cudaEvent_t begin, end; check(cudaEventCreate(&begin)); check(cudaEventCreate(&end));
    std::cerr << "GPU预热：" << warmups << "轮，每轮4个kernel\n";
    check(cudaEventRecord(begin));
    for (int i = 0; i < warmups; ++i) launch(errors, gpu_input, gpu_output);
    check(cudaEventRecord(end)); check(cudaEventSynchronize(end));
    float warmup_ms; check(cudaEventElapsedTime(&warmup_ms, begin, end));
    std::cerr << "GPU稳态：" << iterations << "轮；循环中无输入输出回传\n";
    check(cudaEventRecord(begin));
    for (int i = 0; i < iterations; ++i) launch(errors, gpu_input, gpu_output);
    check(cudaEventRecord(end)); check(cudaEventSynchronize(end));
    float steady_ms; check(cudaEventElapsedTime(&steady_ms, begin, end));
    Output output{}; cgbn_error_report_t error{};
    check(cudaMemcpy(&output, gpu_output, sizeof(Output), cudaMemcpyDeviceToHost));
    check(cudaMemcpy(&error, errors, sizeof(error), cudaMemcpyDeviceToHost));
    const double wall_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    if (cgbn_error_report_check(&error)) throw std::runtime_error(cgbn_error_string(&error));
    uint32_t faults = output.selection_fault | output.probe_fault;
    for (auto &cost : output.costs) faults |= cost.arithmetic_fault;
    for (auto fault : output.support_fault) faults |= fault;
    if (faults) throw std::runtime_error("样板精确位宽或固定选择条件失败；不输出成功结果");
    std::cout << "{\"schema\":\"gpu_exact_sample_v1\",\"device\":" << device
      << ",\"name\":" << std::quoted(prop.name) << ",\"compute_capability\":[" << prop.major << ',' << prop.minor
      << "],\"cuda_driver\":" << driver << ",\"cuda_runtime\":" << runtime
      << ",\"bits\":256,\"wide_bits\":512,\"threads_per_integer\":8,\"warmups\":" << warmups
      << ",\"iterations\":" << iterations << ",\"kernels_per_iteration\":4,\"warmup_gpu_ms\":" << warmup_ms
      << ",\"steady_gpu_ms\":" << steady_ms << ",\"steady_iteration_ms\":" << steady_ms / iterations
      << ",\"allocation_transfer_warmup_steady_wall_ms\":" << wall_ms
      << ",\"owned_device_allocation_bytes\":" << sizeof(Input) + sizeof(Output) + sizeof(error)
      << ",\"h2d_calls\":1,\"h2d_bytes\":" << sizeof(Input)
      << ",\"d2h_calls\":2,\"d2h_bytes\":" << sizeof(Output) + sizeof(error)
      << ",\"device_memset_bytes\":" << sizeof(Output) + sizeof(error) << ",\"costs\":[";
    for (int i = 0; i < 4; ++i) {
      if (i) std::cout << ',';
      std::cout << "{\"principal_micro\":\"" << decimal(output.costs[i].principal_micro)
        << "\",\"fee_1e5_units\":\"" << decimal(output.costs[i].fee_1e5_units) << "\"}";
    }
    std::cout << "],\"mask\":[";
    for (int i = 0; i < 999; ++i) { if (i) std::cout << ','; std::cout << (output.mask[i] ? "true" : "false"); }
    std::cout << "],\"selected\":{\"quantity_centishares\":\"" << decimal(output.quantity)
      << "\",\"spend_b_micro\":\"" << decimal(output.spend_b) << "\",\"price_indices\":["
      << output.prices[0] << ',' << output.prices[1] << "],\"variants\":[" << output.variants[0] << ','
      << output.variants[1] << "],\"post_only\":" << (output.post_only ? "true" : "false")
      << ",\"tokens\":[\"" << decimal(output.tokens[0]) << "\",\"" << decimal(output.tokens[1])
      << "\"],\"condition_id\":" << std::quoted(output.condition) << "},\"probes\":{\"sum\":\""
      << decimal(output.sum) << "\",\"carry\":" << output.carry << ",\"product_low\":\""
      << decimal(output.product_low) << "\",\"product_high\":\"" << decimal(output.product_high)
      << "\",\"quotient\":\"" << decimal(output.quotient) << "\",\"remainder\":\""
      << decimal(output.remainder) << "\",\"rounded\":\"" << decimal(output.rounded) << "\"}}\n";
    check(cudaEventDestroy(begin)); check(cudaEventDestroy(end));
    check(cudaFree(errors)); check(cudaFree(gpu_output)); check(cudaFree(gpu_input));
    return 0;
  } catch (const std::exception &error) { std::cerr << "诊断失败：" << error.what() << '\n'; return 1; }
}
