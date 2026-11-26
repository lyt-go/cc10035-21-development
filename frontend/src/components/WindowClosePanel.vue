<template>
  <section
    class="window-panel"
    data-testid="window-close-panel"
    data-feature="window-close-order"
    aria-label="管道关窗结果单"
  >
    <header class="window-head">
      <div>
        <h3>管道关窗结果单</h3>
        <p>
          服务端按管道实际接收状态判定窗口资格：未收完仍为「待关闭」；确认无样本只保留可查询的历史空窗记录、不写占位
          0；真实 0 是合法读数，重复投递只计一次。
        </p>
      </div>
      <div class="window-toolbar">
        <button class="btn" data-testid="wc-seed-scenario" type="button" :disabled="busy" @click="seedScenario(false)">
          载入两窗场景
        </button>
        <button class="btn" data-testid="wc-demo" type="button" :disabled="busy" @click="runDemo">
          一键复现零值冲突
        </button>
        <button class="btn primary" data-testid="wc-new-toggle" type="button" @click="showCreate = !showCreate">
          办理关窗申请
        </button>
        <button class="btn ghost" data-testid="wc-refresh" type="button" @click="reload()">刷新核对</button>
      </div>
    </header>

    <form v-if="showCreate" class="window-create-form" data-testid="wc-create-form" @submit.prevent="createOrder">
      <label>
        <span>管道编号</span>
        <input v-model="createForm.pipeline_id" data-testid="wc-create-pipeline" required />
      </label>
      <label>
        <span>窗口编号</span>
        <input v-model="createForm.window_id" data-testid="wc-create-window" placeholder="例如 W3" required />
      </label>
      <label>
        <span>位置/测点</span>
        <input v-model="createForm.position" data-testid="wc-create-position" />
      </label>
      <label>
        <span>窗口开始</span>
        <input v-model.number="createForm.window_start" data-testid="wc-create-start" type="number" />
      </label>
      <label>
        <span>窗口结束</span>
        <input v-model.number="createForm.window_end" data-testid="wc-create-end" type="number" />
      </label>
      <label>
        <span>聚合任务编号</span>
        <input v-model="createForm.aggregate_task_id" data-testid="wc-create-task" />
      </label>
      <label class="check-label">
        <input v-model="createForm.receive_complete" type="checkbox" data-testid="wc-create-complete" />
        <span>管道已收齐</span>
      </label>
      <button class="btn primary" data-testid="wc-create-submit" type="submit" :disabled="busy">创建结果单</button>
    </form>

    <p v-if="message" class="window-message" :class="{ error: !lastOk }" data-testid="wc-message" role="status">
      {{ message }}
    </p>

    <div class="window-grid" data-testid="wc-order-list">
      <article
        v-for="row in orders"
        :key="row.id"
        class="window-card"
        :data-testid="`wc-order-${windowKey(row)}`"
        :data-order-id="row.id"
        :data-window-id="row.窗口编号"
        :data-status="row.当前状态 || row.status"
        :data-empty="row.is_empty_window ? 'true' : 'false'"
        :data-has-zero="hasZero(row) ? 'true' : 'false'"
      >
        <div class="window-card-head">
          <div>
            <strong data-testid="wc-order-title">{{ row.窗口名称 || row.窗口编号 }}（{{ row.窗口编号 }}）</strong>
            <span class="order-no">{{ row.关窗结果单号 }}</span>
          </div>
          <span class="status-pill" :data-status="row.当前状态 || row.status">{{ row.当前状态 || row.status }}</span>
        </div>

        <dl class="window-facts">
          <div><dt>管道</dt><dd>{{ row.管道编号 || '—' }}</dd></div>
          <div><dt>位置</dt><dd>{{ row.位置 || '—' }}</dd></div>
          <div><dt>接收状态</dt><dd data-testid="wc-receive-status">{{ row.接收状态 || '—' }}</dd></div>
          <div><dt>真实样本</dt><dd data-testid="wc-sample-count">{{ Number(row.有效样本数 ?? 0) }} 个</dd></div>
          <div><dt>重复报告</dt><dd data-testid="wc-duplicate-count">{{ Number(row.重复样本数 ?? 0) }} 次</dd></div>
          <div>
            <dt>聚合结果</dt>
            <dd class="aggregate-value" data-testid="wc-aggregate-value" :data-empty="isEmptyResult(row)">
              {{ aggregateText(row) }}
            </dd>
          </div>
        </dl>

        <div class="sign-state" data-testid="wc-sign-state">
          <span>第一次签收：<b data-testid="wc-first-status">{{ row.首次签收状态 || '尚未签收' }}</b></span>
          <span>随后更正：<b data-testid="wc-corrected-status">{{ row.更正后状态 || row.更正前状态 || '暂无更正' }}</b></span>
        </div>

        <div class="sample-form">
          <label>
            <span>样本编号/幂等键</span>
            <input
              v-model="sampleForms[row.id].sample_id"
              :data-testid="`wc-sample-id-${windowKey(row)}`"
              placeholder="同一编号重投只计一次"
            />
          </label>
          <label class="value-field">
            <span>读数</span>
            <input
              v-model="sampleForms[row.id].value"
              :data-testid="`wc-sample-value-${windowKey(row)}`"
              inputmode="decimal"
            />
          </label>
          <label class="time-field">
            <span>数据时间</span>
            <input
              v-model="sampleForms[row.id].sample_time"
              :data-testid="`wc-sample-time-${windowKey(row)}`"
              inputmode="decimal"
              placeholder="例如 10"
            />
          </label>
          <button
            class="btn"
            :data-testid="`wc-report-${windowKey(row)}`"
            data-action="report-sample"
            type="button"
            :disabled="busy"
            @click="reportSample(row)"
          >
            报送样本/迟到零值
          </button>
        </div>

        <div class="card-actions">
          <button
            class="btn primary"
            :data-testid="`wc-confirm-empty-${windowKey(row)}`"
            data-action="confirm-empty"
            type="button"
            :disabled="busy"
            :title="emptyButtonTitle(row)"
            @click="confirmEmpty(row)"
          >
            确认无样本并签收空窗
          </button>
          <button
            class="btn"
            :data-testid="`wc-correct-${windowKey(row)}`"
            data-action="correct"
            type="button"
            :disabled="busy || Number(row.有效样本数 ?? 0) === 0"
            @click="correct(row)"
          >
            核对并应用更正
          </button>
          <button
            class="btn ghost"
            :data-testid="`wc-supplement-${windowKey(row)}`"
            data-action="supplement"
            type="button"
            :disabled="busy"
            @click="supplement(row)"
          >
            安排下一笔补交
          </button>
          <button class="link" :data-testid="`wc-review-${windowKey(row)}`" type="button" @click="selectedId = row.id">
            核对签收记录
          </button>
        </div>

        <div v-if="selectedId === row.id" class="window-detail" data-testid="wc-detail">
          <section>
            <h4>实际样本（去重后）</h4>
            <ul data-testid="wc-sample-list">
              <li v-for="sample in row.samples ?? []" :key="String(sample.fingerprint || sample.sample_id)">
                <span>编号 {{ String(sample.sample_id) }}</span>
                <strong>值 {{ String(sample.value) }}</strong>
                <span>位置 {{ String(sample.位置 || sample.position || '—') }}</span>
                <span>入口 {{ String(sample.source || '—') }}</span>
                <span>重报 {{ Number((sample.reports as unknown[] | undefined)?.length ?? 1) - 1 }} 次</span>
              </li>
              <li v-if="!(row.samples ?? []).length" class="muted">无真实样本；这不是占位 0。</li>
            </ul>
          </section>
          <section>
            <h4>前后状态与冲突记录</h4>
            <ol class="history-list" data-testid="wc-history">
              <li v-for="item in row.history ?? []" :key="Number(item.seq)" :data-event="String(item.type)">
                <time>{{ String(item.at) }}</time>
                <span>{{ String(item.message) }}</span>
                <em>状态：{{ String(item.status) }}；样本：{{ String(item.sample_count) }}；聚合：{{ item.aggregate_value === null || item.aggregate_value === undefined ? '无值' : String(item.aggregate_value) }}</em>
              </li>
            </ol>
          </section>
          <section v-if="(row.supplement_orders ?? []).length">
            <h4>补交安排</h4>
            <ul data-testid="wc-supplement-list">
              <li v-for="item in row.supplement_orders ?? []" :key="String(item.id)">
                {{ String(item.补交单号) }}：{{ String(item.补交安排 || item.next_action) }}（依据状态 {{ String(item.based_on_status) }}）
              </li>
            </ul>
          </section>
        </div>
      </article>
    </div>

    <p v-if="!orders.length && !loading" class="window-empty" data-testid="wc-empty">
      暂无关窗结果单，可「载入两窗场景」或「办理管道关窗」。
    </p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  confirmWindowEmpty,
  correctWindow,
  createWindowOrder,
  listWindowOrders,
  reportWindowSample,
  runWindowDemo,
  scheduleSupplement,
  seedWindowScenario,
  type WindowOrder,
} from '@/api/windowClose'

const orders = ref<WindowOrder[]>([])
const loading = ref(false)
const busy = ref(false)
const message = ref('')
const lastOk = ref(true)
const selectedId = ref<number | null>(null)
const showCreate = ref(false)

const createForm = reactive({
  pipeline_id: 'DATA-WINDOW-01',
  window_id: '',
  position: 'POINT-01',
  window_start: 0 as number | string,
  window_end: 60 as number | string,
  aggregate_task_id: '',
  receive_complete: true,
})

const sampleForms = reactive<Record<number, { sample_id: string; value: string; sample_time: string }>>({})

function windowKey(row: WindowOrder): string {
  return String(row.窗口编号 || row.window_id || row.id)
}

function ensureSampleForm(row: WindowOrder) {
  if (!sampleForms[row.id]) {
    sampleForms[row.id] = {
      sample_id: `DUP-${windowKey(row)}-0`,
      value: '0',
      sample_time: '',
    }
  }
  return sampleForms[row.id]
}

function openCreate(taskId = '', windowId = '', pipelineId = '') {
  createForm.aggregate_task_id = taskId
  if (windowId) createForm.window_id = windowId
  if (pipelineId) createForm.pipeline_id = pipelineId
  else if (!createForm.pipeline_id) createForm.pipeline_id = 'DATA-WINDOW-01'
  showCreate.value = true
  message.value = ''
}

defineExpose({ openCreate, reload })

function hasZero(row: WindowOrder): boolean {
  return (row.samples ?? []).some((sample) => sample.value === 0 || sample.数值 === 0)
}

function isEmptyResult(row: WindowOrder): boolean {
  const result = row.aggregate_result as { status?: string } | undefined
  return Number(row.有效样本数 ?? 0) === 0 && (row.is_empty_window === true || result?.status === 'empty' || Boolean(row.首次签收状态))
}

function aggregateText(row: WindowOrder): string {
  if (Number(row.有效样本数 ?? 0) > 0) return String(row.聚合值 ?? row.aggregated_value ?? '—')
  if (isEmptyResult(row)) return '空窗 · 无值（不写 0）'
  return '尚未聚合'
}

function emptyButtonTitle(row: WindowOrder): string {
  if (row.接收状态 !== '已收齐') return '管道实际尚未收完'
  if (Number(row.有效样本数 ?? 0) > 0) return '已有真实样本，0 也是合法读数'
  return '确认管道无样本并正式签收'
}

function notify(ok: boolean, text: string) {
  lastOk.value = ok
  message.value = text
}

async function reload(selectId?: number) {
  loading.value = true
  try {
    orders.value = await listWindowOrders()
    for (const row of orders.value) ensureSampleForm(row)
    if (selectId !== undefined) selectedId.value = selectId
    else if (selectedId.value === null && orders.value.length) selectedId.value = orders.value[0].id
  } catch (error) {
    notify(false, error instanceof Error ? error.message : '关窗结果单读取失败')
  } finally {
    loading.value = false
  }
}

async function seedScenario(reset: boolean) {
  busy.value = true
  try {
    const result = await seedWindowScenario(reset)
    notify(result.ok, result.message || '场景已载入')
    await reload()
  } finally {
    busy.value = false
  }
}

async function runDemo() {
  busy.value = true
  try {
    const result = await runWindowDemo()
    notify(Boolean(result.ok), result.message || '冲突场景已处理')
    await reload()
    const w1 = result.window1 as WindowOrder | undefined
    if (w1) selectedId.value = w1.id
  } finally {
    busy.value = false
  }
}

async function createOrder() {
  busy.value = true
  try {
    const payload: Record<string, unknown> = { ...createForm }
    if (createForm.aggregate_task_id.trim() === '') delete payload.aggregate_task_id
    const result = await createWindowOrder(payload)
    notify(result.ok, result.message || '关窗结果单已创建')
    if (result.ok) {
      showCreate.value = false
      await reload((result.order as WindowOrder | undefined)?.id)
    }
  } finally {
    busy.value = false
  }
}

async function reportSample(row: WindowOrder) {
  busy.value = true
  try {
    const form = ensureSampleForm(row)
    const uiSource = window.location.pathname.includes('data_stream')
      ? 'data_stream_workbench'
      : 'aggregate_workbench'
    const payload: Record<string, unknown> = {
      sample_id: form.sample_id,
      value: form.value,
      source: uiSource,
      auto_confirm: true,
    }
    if (form.sample_time !== '') payload.sample_time = Number(form.sample_time)
    const result = await reportWindowSample(row.id, payload)
    notify(Boolean(result.ok), result.message || '样本已报送')
    await reload(row.id)
  } finally {
    busy.value = false
  }
}

async function confirmEmpty(row: WindowOrder) {
  busy.value = true
  try {
    const result = await confirmWindowEmpty(row.id)
    notify(Boolean(result.ok), result.message || '空窗已签收')
    await reload(row.id)
  } finally {
    busy.value = false
  }
}

async function correct(row: WindowOrder) {
  busy.value = true
  try {
    const result = await correctWindow(row.id)
    notify(Boolean(result.ok), result.message || '更正已应用')
    await reload(row.id)
  } finally {
    busy.value = false
  }
}

async function supplement(row: WindowOrder) {
  busy.value = true
  try {
    const result = await scheduleSupplement(row.id)
    notify(Boolean(result.ok), result.message || '补交已安排')
    await reload(row.id)
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  await reload()
  // 全新环境没有任何关窗单时，直接准备提示中的 W1/W2 初始态，方便值班员核对。
  if (!orders.value.length) await seedScenario(false)
})
</script>

<style scoped>
.window-panel { margin: 0 0 16px; padding: 14px; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; }
.window-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.window-head h3 { margin: 0 0 6px; font-size: 16px; }
.window-head p { margin: 0; color: var(--muted); font-size: 13px; line-height: 1.6; }
.window-toolbar { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.window-create-form { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 10px; margin: 12px 0; padding: 12px; background: #fff; border: 1px solid var(--border); border-radius: 8px; }
.window-create-form label, .sample-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.window-create-form input, .sample-form input { padding: 7px 8px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; color: #1f2937; }
.check-label { flex-direction: row !important; align-items: center; justify-content: center; gap: 6px; color: #1f2937 !important; }
.window-message { padding: 8px 10px; border-radius: 6px; background: #ecfdf3; color: #027a48; font-size: 13px; }
.window-message.error { background: #fef3f2; color: #b42318; }
.window-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 12px; margin-top: 12px; }
.window-card { background: #fff; border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
.window-card-head { display: flex; justify-content: space-between; gap: 8px; align-items: center; }
.order-no { display: block; color: var(--muted); font-size: 12px; margin-top: 2px; }
.status-pill { padding: 3px 8px; border-radius: 999px; background: #e0e7ff; color: #3730a3; font-size: 12px; white-space: nowrap; }
.status-pill[data-status='已更正'], .status-pill[data-status='已关闭'] { background: #dcfce7; color: #166534; }
.status-pill[data-status='冲突待核对'], .status-pill[data-status='读数待核对'] { background: #fef3c7; color: #92400e; }
.window-facts { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 12px 0; }
.window-facts div { background: #f8fafc; border-radius: 6px; padding: 7px; }
.window-facts dt { font-size: 11px; color: var(--muted); }
.window-facts dd { margin: 2px 0 0; font-size: 13px; }
.aggregate-value { font-weight: 700; }
.aggregate-value[data-empty='true'] { color: #b45309; font-weight: 600; }
.sign-state { display: flex; flex-wrap: wrap; gap: 12px; padding: 8px; border-left: 3px solid var(--brand); background: #eff6ff; font-size: 12px; margin-bottom: 10px; }
.sample-form { display: grid; grid-template-columns: 1.4fr .6fr .7fr auto; gap: 8px; align-items: end; margin-bottom: 10px; }
.card-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.card-actions .btn[disabled] { opacity: .5; cursor: not-allowed; }
.window-detail { margin-top: 12px; border-top: 1px dashed var(--border); padding-top: 10px; }
.window-detail h4 { margin: 8px 0; font-size: 13px; }
.window-detail ul, .window-detail ol { margin: 0; padding-left: 18px; font-size: 12px; }
.window-detail li { margin-bottom: 6px; }
.window-detail li > span, .window-detail li > strong { margin-right: 10px; }
.history-list { padding-left: 0 !important; list-style: none; }
.history-list li { display: grid; gap: 2px; padding: 7px; background: #f8fafc; border-radius: 6px; }
.history-list time { color: var(--muted); }
.history-list em { color: #475569; font-style: normal; }
.muted, .window-empty { color: var(--muted); font-size: 13px; }
@media (max-width: 900px) {
  .window-head { flex-direction: column; }
  .window-toolbar { justify-content: flex-start; }
  .window-create-form { grid-template-columns: 1fr 1fr; }
  .window-grid { grid-template-columns: 1fr; }
  .sample-form { grid-template-columns: 1fr 1fr; }
}
</style>
