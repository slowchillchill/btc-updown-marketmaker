#include "../../contracts/gpu/exact.cuh"
using namespace public_gpu;

// 每根仅扫描当前原快照的ragged范围。无Snapshot人口的根立即返回，
// 不展开全页字典或levels×全页盘口；临时积使用完整输入宽度的精确交叉乘积。
__global__ void snapshot_gather_kernel(const int64_t* index, const int64_t* asset,
    const bool* enabled, const int64_t* sources, const int64_t* owners,
    const int64_t* level_assets, const int64_t* sides, const int64_t* offsets,
    const int64_t* snapshot, const uint32_t* pn, int64_t wpn, const int8_t* ps,
    const uint32_t* pd, int64_t wpd, const int32_t* pf,
    const uint32_t* dn, int64_t wdn, const int8_t* ds,
    const uint32_t* dd, int64_t wdd, const int32_t* df, const bool* closed,
    int64_t* ids, int64_t* counts, int32_t* faults, uint32_t* work,
    int64_t cap, int64_t roots, int64_t levels) {
  int64_t root = int64_t(blockIdx.x)*blockDim.x + threadIdx.x;
  if (root >= roots || !enabled[root]) return;
  auto* left = work + root*2*cap;
  auto* right = left+cap;
  int32_t fault = 0;
  for (int64_t step=offsets[index[root]]; step<offsets[index[root]+1]; ++step) {
    const auto* entry = snapshot+step*3;
    int64_t price = entry[1], hits = 0;
    for (int64_t level=0; level<levels; ++level) {
      int64_t runtime_asset = level_assets[level];
      if (owners[runtime_asset] != root || sources[runtime_asset] != asset[root]
          || closed[runtime_asset] || sides[level] != entry[0]) continue;
      fault |= pf[level] | df[price];
      if (pf[level] || df[price]) continue;
      auto* a = pn+level*wpn; auto* b = dn+price*wdn;
      auto* ad = pd+level*wpd; auto* bd = dd+price*wdd;
      if (!length(ad,wpd) || !length(bd,wdd)) { fault |= 1; continue; }
      int sa = length(a,wpn) ? ps[level] : 0;
      int sb = length(b,wdn) ? ds[price] : 0;
      if (sa != sb) continue;
      multiply(left,cap,a,length(a,wpn),bd,length(bd,wdd));
      multiply(right,cap,b,length(b,wdn),ad,length(ad,wpd));
      if (compare(left,cap,right,cap) != 0) continue;
      ++hits; ++counts[level]; ids[level] = entry[2];
    }
    if (hits != 1) fault |= 1;
  }
  faults[root] = fault;
}

extern "C" void sim_gpu_snapshot_gather(const int64_t* index, const int64_t* asset,
    const bool* enabled, const int64_t* sources, const int64_t* owners,
    const int64_t* level_assets, const int64_t* sides, const int64_t* offsets,
    const int64_t* snapshot, const uint32_t* pn, int64_t wpn, const int8_t* ps,
    const uint32_t* pd, int64_t wpd, const int32_t* pf,
    const uint32_t* dn, int64_t wdn, const int8_t* ds,
    const uint32_t* dd, int64_t wdd, const int32_t* df, const bool* closed,
    int64_t* ids, int64_t* counts, int32_t* faults, uint32_t* work,
    int64_t cap, int64_t roots, int64_t levels, cudaStream_t stream) {
  if (roots) snapshot_gather_kernel<<<(roots+127)/128,128,0,stream>>>(index,asset,enabled,
      sources,owners,level_assets,sides,offsets,snapshot,pn,wpn,ps,pd,wpd,pf,dn,wdn,ds,
      dd,wdd,df,closed,ids,counts,faults,work,cap,roots,levels);
}
