#include "metrics.h"

namespace lsm {

// 全局指标指针
Metrics* g_metrics = nullptr;

// 初始化指标
void InitMetrics() {
  if (g_metrics == nullptr) {
    g_metrics = new Metrics();
  }
}

// 释放指标
void DestroyMetrics() {
  if (g_metrics != nullptr) {
    delete g_metrics;
    g_metrics = nullptr;
  }
}

}  // namespace lsm
