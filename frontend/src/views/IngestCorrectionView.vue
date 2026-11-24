<template>
  <section class="page" data-testid="ingest-correction">
    <header class="page-head">
      <div>
        <h2>拒收数据批量补交</h2>
        <p class="page-desc">
          按分区偏移批量读取拒收行并重放，生成拒收纠正数据记录；逐行确认与整批确认都会写入确认边界。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/stream_ingest" data-testid="back-to-ingest">返回流式摄入</RouterLink>
        <button class="btn" data-testid="refresh-all" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <nav class="tab-bar">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        :data-testid="`tab-${tab.key}`"
        type="button"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </nav>

    <p v-if="message" class="notice" :class="{ error: messageError }" data-testid="correction-message">
      {{ message }}
    </p>

    <!-- 补交批次 -->
    <section v-show="activeTab === 'batches'" class="panel" data-testid="panel-batches">
      <article class="panel-card">
        <h3>按分区偏移批量读取</h3>
        <p class="hint">
          同一批偏移共用幂等键，并发提交只生效一次；同一原偏移重复重放只累计重复次数，不再新建纠正结果。
        </p>
        <form class="inline-form" @submit.prevent="runReplay">
          <label>
            <span>p1 偏移（逗号分隔）</span>
            <input v-model="replayForm.p1" data-testid="replay-p1" placeholder="101" />
          </label>
          <label>
            <span>p2 偏移（逗号分隔）</span>
            <input v-model="replayForm.p2" data-testid="replay-p2" placeholder="205" />
          </label>
          <label>
            <span>幂等键</span>
            <input v-model="replayForm.token" data-testid="replay-token" placeholder="留空按偏移生成" />
          </label>
          <button class="btn primary" data-testid="replay-submit" type="submit" :disabled="busy">
            {{ busy ? '处理中…' : '批量读取并重放' }}
          </button>
          <button class="btn ghost" data-testid="add-version" type="button" :disabled="busy" @click="addVersion">
            登记 p2 同业务键新版本
          </button>
          <button class="btn ghost" data-testid="scenario-row" type="button" :disabled="busy" @click="runScenario('逐行确认')">
            跑逐行确认场景
          </button>
          <button class="btn ghost" data-testid="scenario-all" type="button" :disabled="busy" @click="runScenario('整批确认')">
            跑整批确认场景
          </button>
        </form>
      </article>

      <article v-for="batch in batches" :key="batch.id" class="panel-card" :data-testid="`batch-${batch.id}`">
        <header class="batch-head">
          <div>
            <strong>{{ batch.batch_no }}</strong>
            <span class="pill" :data-status="batch.status">{{ batch.status }}</span>
            <span class="muted">确认方式：{{ batch.confirmation_strategy }}</span>
          </div>
          <div class="page-actions">
            <button
              class="btn"
              :data-testid="`confirm-all-${batch.id}`"
              type="button"
              :disabled="busy"
              @click="doConfirmAll(batch.id)"
            >
              整批确认
            </button>
          </div>
        </header>
        <p class="hint">
          生效提交 {{ batch.effective_submissions }} 次，重复提交 {{ batch.duplicate_submissions }} 次，
          整批确认冲突 {{ batch.batch_confirm_attempts }} 次，重复行数 {{ batch.duplicate_count }}。
        </p>
        <table class="data-table">
          <thead>
            <tr>
              <th>分区</th>
              <th>原偏移</th>
              <th>业务键</th>
              <th>缺失字段</th>
              <th>合并后数据</th>
              <th>处置</th>
              <th>确认方式</th>
              <th>重复次数</th>
              <th>逐行操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="item in batch.items"
              :key="item.id"
              :data-testid="`item-${item.offset_key}`"
              :data-status="item.disposition"
            >
              <td>{{ item.partition }}</td>
              <td>{{ item.offset }}</td>
              <td>{{ item.business_key }}</td>
              <td>{{ item.missing_fields.join('、') || '—' }}</td>
              <td class="value-cell">{{ formatValues(item.merged_values) }}</td>
              <td>{{ item.disposition }}</td>
              <td>{{ item.confirmation_mode || '—' }}</td>
              <td>{{ item.duplicate_count }}</td>
              <td class="row-actions">
                <button
                  class="link"
                  :data-testid="`fix-${item.offset_key}`"
                  type="button"
                  :disabled="item.disposition === '已入湖'"
                  @click="openFix(item)"
                >
                  补齐字段
                </button>
                <button
                  class="link"
                  :data-testid="`confirm-row-${item.offset_key}`"
                  type="button"
                  :disabled="item.disposition === '已入湖'"
                  @click="doConfirmRow(item)"
                >
                  逐行确认
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="panel-card">
        <header class="batch-head">
          <h3>质量纠正待办</h3>
          <button class="btn ghost" data-testid="refresh-todos" type="button" @click="refreshTodoList">
            刷新待办
          </button>
        </header>
        <p class="hint">
          每次补交、修正和确认后都会刷新；点“查看版本详情”进入分栏视图，核对已落湖行与仍被拒绝行。
        </p>
        <table class="data-table" data-testid="todo-table">
          <thead>
            <tr>
              <th>待办编号</th>
              <th>分区</th>
              <th>偏移</th>
              <th>业务键</th>
              <th>来源</th>
              <th>缺失字段</th>
              <th>状态</th>
              <th>重复次数</th>
              <th>说明</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="todo in todos"
              :key="todo.id"
              :data-testid="`todo-${todo.offset_key}`"
              :data-status="todo.status"
            >
              <td>{{ todo.todo_no }}</td>
              <td>{{ todo.partition }}</td>
              <td>{{ todo.offset }}</td>
              <td>{{ todo.business_key }}</td>
              <td>{{ todo.kind }}</td>
              <td>{{ todo.missing_fields.join('、') || '—' }}</td>
              <td>{{ todo.status }}</td>
              <td>{{ todo.duplicate_count }}</td>
              <td class="reason-cell">{{ todo.reason }}</td>
              <td>
                <RouterLink
                  class="link"
                  :to="`/stream_ingest/correction/versions/${todo.version_id}`"
                  :data-testid="`todo-version-${todo.offset_key}`"
                >
                  查看版本详情
                </RouterLink>
              </td>
            </tr>
            <tr v-if="!todos.length">
              <td colspan="10" class="empty-state">暂无质量纠正待办</td>
            </tr>
          </tbody>
        </table>
      </article>
    </section>

    <!-- 检查点恢复 -->
    <section v-show="activeTab === 'checkpoint'" class="panel" data-testid="panel-checkpoint">
      <article class="panel-card">
        <h3>检查点恢复</h3>
        <p class="hint">按逐行回执重排每个分区的下一次读取，已确认偏移不会因同业务键新版本而回退。</p>
        <div class="partition-grid">
          <article
            v-for="part in checkpoint?.partitions || []"
            :key="part.partition"
            class="partition-card"
            :data-testid="`checkpoint-${part.partition}`"
          >
            <header>
              <strong>{{ part.partition }}</strong>
              <span class="muted">下一次读取偏移：{{ part.next_read_offset }}</span>
            </header>
            <p class="muted">
              最近确认偏移 {{ part.last_confirmed_offset ?? '—' }}，待读取
              {{ part.pending_offsets.join('、') || '无' }}，重复 {{ part.duplicate_count }} 次
            </p>
            <table class="mini-table">
              <thead>
                <tr><th>偏移</th><th>业务键</th><th>类型</th><th>读取状态</th></tr>
              </thead>
              <tbody>
                <tr v-for="entry in part.ordered_offsets" :key="entry.offset" :data-offset="entry.offset">
                  <td>{{ entry.offset }}</td>
                  <td>{{ entry.business_key }}</td>
                  <td>{{ entry.kind }}</td>
                  <td>{{ entry.status }}</td>
                </tr>
              </tbody>
            </table>
            <p class="receipt-title">逐行回执</p>
            <ul class="receipt-list">
              <li v-for="receipt in part.receipts" :key="receipt.reject_id" :data-testid="`receipt-${receipt.offset_key}`">
                {{ receipt.offset_key }} · {{ receipt.business_key }} · {{ receipt.confirmation_mode }}
                · 重复 {{ receipt.duplicate_count }}
              </li>
              <li v-if="!part.receipts.length" class="muted">暂无已确认回执</li>
            </ul>
          </article>
        </div>
      </article>
    </section>

    <!-- 版本详情 -->
    <section v-show="activeTab === 'version'" class="panel" data-testid="panel-version">
      <article class="panel-card">
        <header class="batch-head">
          <div>
            <strong>版本 {{ versionDetail?.version?.label || '—' }}</strong>
            <span class="muted">{{ versionDetail?.version?.table }} · {{ versionDetail?.pipeline?.name }}</span>
          </div>
          <div class="page-actions">
            <RouterLink class="btn ghost" to="/stream_ingest/correction" data-testid="back-to-batches">
              返回补交列表
            </RouterLink>
          </div>
        </header>
        <div class="stat-row">
          <article class="stat-card">
            <span class="stat-label">已落湖行</span>
            <strong class="stat-value" data-testid="accepted-count">{{ versionDetail?.counts.accepted ?? 0 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">仍被拒绝行</span>
            <strong class="stat-value" data-testid="rejected-count">{{ versionDetail?.counts.rejected ?? 0 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">纠正结果</span>
            <strong class="stat-value">{{ versionDetail?.counts.corrections ?? 0 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">重复次数</span>
            <strong class="stat-value" data-testid="version-duplicate-count">{{ versionDetail?.counts.duplicates ?? 0 }}</strong>
          </article>
        </div>

        <div class="split-view">
          <section class="split-col" data-testid="accepted-column">
            <h3>已落湖的行</h3>
            <table class="data-table">
              <thead>
                <tr>
                  <th>偏移</th><th>业务键</th><th>数据</th><th>确认方式</th><th>重复次数</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in versionDetail?.accepted || []"
                  :key="row.reject_id"
                  :data-testid="`accepted-${row.offset_key}`"
                >
                  <td>{{ row.offset_key }}</td>
                  <td>{{ row.business_key }}</td>
                  <td class="value-cell">{{ formatValues(row.values) }}</td>
                  <td>{{ row.confirmation_mode }}</td>
                  <td>{{ row.duplicate_count }}</td>
                </tr>
                <tr v-if="!(versionDetail?.accepted || []).length">
                  <td colspan="5" class="empty-state">该版本还没有落湖行</td>
                </tr>
              </tbody>
            </table>
          </section>
          <section class="split-col" data-testid="rejected-column">
            <h3>仍被拒绝的行</h3>
            <table class="data-table">
              <thead>
                <tr>
                  <th>偏移</th><th>业务键</th><th>缺失字段</th><th>状态</th><th>说明</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in versionDetail?.rejected || []"
                  :key="row.reject_id"
                  :data-testid="`rejected-${row.offset_key}`"
                >
                  <td>{{ row.offset_key }}</td>
                  <td>{{ row.business_key }}</td>
                  <td>{{ row.missing_fields.join('、') || '—' }}</td>
                  <td>{{ row.disposition }}</td>
                  <td class="reason-cell">{{ row.reason }}</td>
                </tr>
                <tr v-if="!(versionDetail?.rejected || []).length">
                  <td colspan="5" class="empty-state">该版本没有仍被拒绝的行</td>
                </tr>
              </tbody>
            </table>
          </section>
        </div>

        <h3>确认边界</h3>
        <table class="data-table" data-testid="boundary-table">
          <thead>
            <tr><th>序号</th><th>批次</th><th>方式</th><th>偏移</th><th>说明</th><th>时间</th></tr>
          </thead>
          <tbody>
            <tr v-for="boundary in versionDetail?.boundaries || []" :key="boundary.seq" :data-testid="`boundary-${boundary.seq}`">
              <td>{{ boundary.seq }}</td>
              <td>{{ boundary.batch_no }}</td>
              <td>{{ boundary.mode }}</td>
              <td>{{ boundary.offsets.join('、') }}</td>
              <td class="reason-cell">{{ boundary.note }}</td>
              <td>{{ boundary.at }}</td>
            </tr>
            <tr v-if="!(versionDetail?.boundaries || []).length">
              <td colspan="6" class="empty-state">还没有确认边界</td>
            </tr>
          </tbody>
        </table>
      </article>
    </section>

    <div v-if="fixTarget" class="modal-backdrop" data-testid="fix-modal">
      <form class="modal" @submit.prevent="submitFix">
        <h3>补齐 {{ fixTarget.offset_key }} 缺失字段</h3>
        <p class="hint">业务键 {{ fixTarget.business_key }}；缺失字段：{{ fixTarget.missing_fields.join('、') }}</p>
        <label v-for="field in fixFields" :key="field.key" class="create-field">
          <span>{{ field.label }}</span>
          <input
            v-model="fixValues[field.key]"
            :data-testid="`fix-input-${field.key}`"
            :type="field.key === 'amount' ? 'number' : 'text'"
            step="0.01"
            required
          />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="fixTarget = null">取消</button>
          <button class="btn primary" data-testid="fix-submit" type="submit" :disabled="busy">保存修正值</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>质量纠正待办 {{ todos.length }} 条 · 检查点 {{ checkpoint?.partitions.length || 0 }} 个分区</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import {
  addNewVersion,
  confirmAll,
  confirmRow,
  fixCorrection,
  loadCheckpoint,
  loadTodos,
  loadVersionDetail,
  refreshTodos,
  replayRejects,
  runScenario as runScenarioApi,
  type CheckpointResult,
  type CorrectionItem,
  type TodoItem,
  type VersionDetail,
} from '@/api/ingest-correction'

type Tab = 'batches' | 'checkpoint' | 'version'

const route = useRoute()
const busy = ref(false)
const message = ref('')
const messageError = ref(false)
const errorMessage = ref('')
const todos = ref<TodoItem[]>([])
const versionDetail = ref<VersionDetail | null>(null)
const checkpoint = ref<CheckpointResult | null>(null)
const activeTab = ref<Tab>('batches')

const replayForm = reactive({ p1: '101', p2: '205', token: '' })
const fixTarget = ref<CorrectionItem | null>(null)
const fixValues = reactive<Record<string, string>>({})

const tabs: { key: Tab; label: string }[] = [
  { key: 'batches', label: '批量补交' },
  { key: 'checkpoint', label: '检查点恢复' },
  { key: 'version', label: '版本详情' },
]

const versionId = computed(() => Number(route.params.versionId || 7))
const batches = computed(() => versionDetail.value?.batches || [])

const fixFields = computed(() => {
  const labels = fixTarget.value?.missing_fields || []
  const fields: { key: string; label: string }[] = []
  if (labels.includes('金额')) fields.push({ key: 'amount', label: '金额' })
  if (labels.includes('日期')) fields.push({ key: 'order_date', label: '日期' })
  if (!fields.length) fields.push({ key: 'amount', label: '金额' })
  return fields
})

function flash(text: string, error = false) {
  message.value = text
  messageError.value = error
}

function formatValues(values: Record<string, string | number>) {
  return Object.entries(values)
    .filter(([key]) => key !== 'business_key')
    .map(([key, value]) => `${key}=${value ?? '—'}`)
    .join(' · ')
}

function parseOffsets(raw: string): number[] {
  return raw
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item))
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [todoResult, version, checkpointResult] = await Promise.all([
      loadTodos(),
      loadVersionDetail(versionId.value),
      loadCheckpoint(),
    ])
    todos.value = todoResult.items
    versionDetail.value = version.ok ? version : null
    checkpoint.value = checkpointResult
    if (!version.ok) errorMessage.value = version.message || '版本详情读取失败'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务数据读取失败'
  }
}

async function refreshAfter(result: { ok: boolean; message: string }) {
  flash(result.message, !result.ok)
  await reloadAll()
}

async function runReplay() {
  const reads: { partition: string; offsets: number[] }[] = []
  const p1 = parseOffsets(replayForm.p1)
  const p2 = parseOffsets(replayForm.p2)
  if (p1.length) reads.push({ partition: 'p1', offsets: p1 })
  if (p2.length) reads.push({ partition: 'p2', offsets: p2 })
  if (!reads.length) {
    flash('请先填写要读取的分区偏移', true)
    return
  }
  busy.value = true
  try {
    const result = await replayRejects({
      idempotency_key: replayForm.token.trim() || undefined,
      reads,
    })
    flash(result.message, !result.ok)
    if (result.ok) replayForm.token = ''
  } catch (error) {
    flash(error instanceof Error ? error.message : '批量读取失败', true)
  } finally {
    busy.value = false
    await reloadAll()
  }
}

async function runScenario(mode: '逐行确认' | '整批确认') {
  busy.value = true
  try {
    const result = await runScenarioApi(mode)
    flash(result.message, !result.ok)
  } catch (error) {
    flash(error instanceof Error ? error.message : '场景执行失败', true)
  } finally {
    busy.value = false
    await reloadAll()
  }
}

function openFix(item: CorrectionItem) {
  fixTarget.value = item
  for (const key of Object.keys(fixValues)) delete fixValues[key]
  if (item.missing_fields.includes('金额')) fixValues.amount = ''
  if (item.missing_fields.includes('日期')) fixValues.order_date = ''
}

async function submitFix() {
  if (!fixTarget.value) return
  busy.value = true
  try {
    const values: Record<string, string> = {}
    for (const field of fixFields.value) {
      if (fixValues[field.key]) values[field.key] = fixValues[field.key]
    }
    const result = await fixCorrection(fixTarget.value.batch_id, {
      correction_id: fixTarget.value.id,
      values,
    })
    if (result.ok) fixTarget.value = null
    await refreshAfter(result)
  } catch (error) {
    flash(error instanceof Error ? error.message : '修正保存失败', true)
  } finally {
    busy.value = false
  }
}

async function doConfirmRow(item: CorrectionItem) {
  busy.value = true
  try {
    const result = await confirmRow(item.batch_id, { correction_id: item.id })
    flash(result.message, !result.ok)
  } catch (error) {
    flash(error instanceof Error ? error.message : '逐行确认失败', true)
  } finally {
    busy.value = false
    await reloadAll()
  }
}

async function doConfirmAll(batchId: number) {
  busy.value = true
  try {
    const result = await confirmAll(batchId)
    flash(result.message, !result.ok)
  } catch (error) {
    flash(error instanceof Error ? error.message : '整批确认失败', true)
  } finally {
    busy.value = false
    await reloadAll()
  }
}

async function addVersion() {
  busy.value = true
  try {
    const result = await addNewVersion({
      partition: 'p2',
      offset: 206,
      business_key: 'ORD-1001',
      reject_id: 1,
      values: { order_id: 'ORD-1001', amount: 199, order_date: '2026-11-22' },
    })
    await refreshAfter(result)
  } catch (error) {
    flash(error instanceof Error ? error.message : '新版本登记失败', true)
  } finally {
    busy.value = false
  }
}

async function refreshTodoList() {
  try {
    const result = await refreshTodos()
    todos.value = result.items
    flash(result.message || '质量纠正待办已刷新')
  } catch (error) {
    flash(error instanceof Error ? error.message : '待办刷新失败', true)
  }
}

watch(
  () => route.fullPath,
  () => {
    if (route.params.versionId) activeTab.value = 'version'
    else if (activeTab.value === 'version') activeTab.value = 'batches'
    void reloadAll()
  },
)

onMounted(async () => {
  if (route.params.versionId) activeTab.value = 'version'
  await reloadAll()
})
</script>

