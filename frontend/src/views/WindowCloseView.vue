<template>
  <section class="page" data-route="window-close">
    <header class="page-head">
      <div>
        <h2>关窗结果单核对台</h2>
        <p class="page-desc">
          面向值班员核对管道实际接收、空窗签收、迟到真实零值更正与下一笔补交安排。
        </p>
      </div>
      <RouterLink class="btn" to="/aggregate_sink">返回聚合分发</RouterLink>
    </header>
    <WindowClosePanel />
  </section>
</template>

<script setup lang="ts">
import WindowClosePanel from '@/components/WindowClosePanel.vue'
</script>
