<template>
  <section class="page" data-module="partitioncheck">
    <header class="page-head">
      <div>
        <h2>分区检测管理</h2>
        <p class="page-desc">管理数据表分区的完整性检测规则，缺失分区受控交接为可执行补齐任务，并与运行实例分区处理状态分开跟踪。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记检测规则</button>
        <button class="btn" type="button" @click="exportRows">导出分区检测清单</button>
        <button class="btn" type="button" @click="downloadBundle">下载补齐任务文件</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="openDetection(row)">发起分区检测</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无分区检测数据，可先登记检测规则</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条分区检测记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <!-- 分区检测批次：每次检测固化缺失分区清单快照，与运行实例处理状态区分 -->
    <section class="sub-panel">
      <header class="sub-head">
        <h3>检测批次与缺失分区清单</h3>
        <button class="btn ghost" type="button" @click="loadDetections">刷新批次</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>检测批次号</th>
            <th>规则编号</th>
            <th>数据表名</th>
            <th>缺失分区数</th>
            <th>清单快照校验和</th>
            <th>检测时间</th>
            <th>批次状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in detections" :key="String(batch.id)">
            <td>{{ batch['检测批次号'] }}</td>
            <td>{{ batch['规则编号'] }}</td>
            <td>{{ batch['数据表名'] }}</td>
            <td>{{ batch['缺失分区数'] }}</td>
            <td><code>{{ batch['清单校验和'] }}</code></td>
            <td>{{ batch['检测时间'] }}</td>
            <td>{{ batch['批次状态'] }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="viewMissing(batch)">查看缺失分区</button>
              <button class="link" type="button" @click="handoff(batch)">受控交接补齐任务</button>
            </td>
          </tr>
          <tr v-if="!detections.length">
            <td colspan="8" class="empty-state">暂无检测批次，在上方规则行点击「发起分区检测」</td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 分区补齐任务：同一分区只有一份，任务状态与运行实例分区状态分列 -->
    <section class="sub-panel">
      <header class="sub-head">
        <h3>分区补齐任务（同一缺失分区仅一份）</h3>
        <div class="filter-bar inline">
          <label class="filter-item">
            <span>任务状态</span>
            <select v-model="taskStatusFilter" @change="loadTasks">
              <option value="">全部</option>
              <option v-for="s in taskStatuses" :key="s" :value="s">{{ s }}</option>
            </select>
          </label>
          <label class="filter-item">
            <span>仅看未补齐</span>
            <input type="checkbox" v-model="onlyUnbackfilled" @change="loadTasks" />
          </label>
          <button class="btn ghost" type="button" @click="loadTasks">刷新任务</button>
        </div>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>补齐任务编号</th>
            <th>数据表名</th>
            <th>分区标识</th>
            <th>优先级</th>
            <th>任务状态</th>
            <th>运行实例分区状态</th>
            <th>完整性结论</th>
            <th>采纳来源</th>
            <th>关联检测批次</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in backfillTasks" :key="String(task.id)">
            <td>{{ task['补齐任务编号'] }}</td>
            <td>{{ task['数据表名'] }}</td>
            <td>{{ task['分区标识'] }}</td>
            <td>{{ task['优先级'] }}</td>
            <td>{{ task['任务状态'] }}</td>
            <td>{{ task['运行实例分区状态'] }}</td>
            <td>{{ task['完整性结论'] }}</td>
            <td>{{ task['采纳来源'] ?? '—' }}</td>
            <td>{{ (task['关联检测批次'] ?? []).join('、') }}</td>
            <td class="row-actions">
              <button v-if="task['任务状态'] === '待交接'" class="link" type="button" @click="confirmTask(task)">人工确认</button>
              <button class="link" type="button" @click="downloadTask(task)">下载任务文件</button>
              <button class="link" type="button" @click="openResults(task)">处理记录</button>
              <button class="link" type="button" @click="openResultDialog(task, '自动补齐')">自动补齐结果</button>
              <button class="link" type="button" @click="openResultDialog(task, '人工处理')">人工处理结果</button>
              <button class="link" type="button" @click="openRuntimeDialog(task)">回写运行实例状态</button>
            </td>
          </tr>
          <tr v-if="!backfillTasks.length">
            <td colspan="10" class="empty-state">暂无补齐任务，先对检测批次执行受控交接</td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 发起检测对话框 -->
    <div v-if="detectionDialog.open" class="modal-mask" @click.self="detectionDialog.open = false">
      <div class="modal">
        <h3>发起分区检测 · {{ detectionDialog.rule?.['规则编号'] }}</h3>
        <p class="page-desc">每行一个分区标识，可附优先级（逗号分隔，如 dt=2026-11-15,高）。提交后生成检测批次与缺失分区清单快照。</p>
        <textarea v-model="detectionDialog.text" rows="7" placeholder="dt=2026-11-15,高&#10;dt=2026-11-16"></textarea>
        <div class="modal-actions">
          <button class="btn ghost" type="button" @click="detectionDialog.open = false">取消</button>
          <button class="btn primary" type="button" :disabled="detectionDialog.saving" @click="submitDetection">提交检测</button>
        </div>
      </div>
    </div>

    <!-- 处理结果对话框：自动补齐与人工处理冲突在此仲裁 -->
    <div v-if="resultDialog.open" class="modal-mask" @click.self="resultDialog.open = false">
      <div class="modal">
        <h3>提交{{ resultDialog.source }}结果 · {{ resultDialog.task?.['分区标识'] }}</h3>
        <p class="page-desc">人工处理优先于自动补齐；同来源较晚到达的结果覆盖较早结果。成功结果与分区完整性结论原子提交。</p>
        <label class="filter-item">
          <span>处理结论</span>
          <select v-model="resultDialog.success">
            <option :value="true">补齐成功</option>
            <option :value="false">补齐失败</option>
          </select>
        </label>
        <label class="filter-item">
          <span>备注</span>
          <input v-model="resultDialog.remark" placeholder="处理说明（可选）" />
        </label>
        <label class="filter-item">
          <span>快照校验和（留空则不校验）</span>
          <input v-model="resultDialog.checksum" :placeholder="resultDialog.task?.['快照校验和'] ?? ''" />
        </label>
        <div class="modal-actions">
          <button class="btn ghost" type="button" @click="resultDialog.open = false">取消</button>
          <button class="btn primary" type="button" :disabled="resultDialog.saving" @click="submitResult">提交结果</button>
        </div>
      </div>
    </div>

    <!-- 运行实例状态回写对话框 -->
    <div v-if="runtimeDialog.open" class="modal-mask" @click.self="runtimeDialog.open = false">
      <div class="modal">
        <h3>回写运行实例分区状态 · {{ runtimeDialog.task?.['分区标识'] }}</h3>
        <p class="page-desc">运行实例分区处理状态与补齐任务状态分字段存储，互不覆盖。</p>
        <label class="filter-item">
          <span>运行实例分区状态</span>
          <select v-model="runtimeDialog.status">
            <option v-for="s in runtimeStatuses" :key="s" :value="s">{{ s }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>关联运行记录</span>
          <input v-model="runtimeDialog.recordId" placeholder="如 RUNT-0002" />
        </label>
        <div class="modal-actions">
          <button class="btn ghost" type="button" @click="runtimeDialog.open = false">取消</button>
          <button class="btn primary" type="button" :disabled="runtimeDialog.saving" @click="submitRuntimeStatus">回写</button>
        </div>
      </div>
    </div>

    <!-- 处理记录留痕抽屉 -->
    <div v-if="recordsDialog.open" class="modal-mask" @click.self="recordsDialog.open = false">
      <div class="modal wide">
        <h3>处理记录 · {{ recordsDialog.task?.['分区标识'] }}</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>处理来源</th>
              <th>处理结论</th>
              <th>是否采纳</th>
              <th>驳回原因</th>
              <th>处理人</th>
              <th>到达时间</th>
              <th>备注</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in recordsDialog.items" :key="String(item.id)">
              <td>{{ item['处理来源'] }}</td>
              <td>{{ item['处理结论'] }}</td>
              <td>{{ item['是否采纳'] ? '采纳' : '驳回' }}</td>
              <td>{{ item['驳回原因'] || '—' }}</td>
              <td>{{ item['处理人'] }}</td>
              <td>{{ item['到达时间'] }}</td>
              <td>{{ item['备注'] || '—' }}</td>
            </tr>
            <tr v-if="!recordsDialog.items.length">
              <td colspan="7" class="empty-state">暂无处理记录</td>
            </tr>
          </tbody>
        </table>
        <div class="modal-actions">
          <button class="btn primary" type="button" @click="recordsDialog.open = false">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | boolean | string[] | null>
type Dict = Record<string, any>

const ENDPOINT = '/api/partitioncheck'
const BACKFILL = '/api/partitionbackfill'
const columns = ["规则编号", "数据表名", "期望分区数", "分区延迟容忍", "缺失分区告警", "检测间隔分钟", "自动补齐开关", "补齐优先级"]
const actions = ["提交规则评审", "启用规则", "暂停规则"]
const stats = ref([
  { label: "检测规则总数", value: 0 },
  { label: "检测批次数", value: 0 },
  { label: "待补齐分区", value: 0 },
  { label: "已补齐分区", value: 0 },
])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const detections = ref<Dict[]>([])
const backfillTasks = ref<Dict[]>([])
const taskStatuses = ["待交接", "待补齐", "补齐中", "已补齐", "补齐失败"]
const runtimeStatuses = ["未下发", "运行实例处理中", "运行实例已完成", "运行实例失败"]
const taskStatusFilter = ref('')
const onlyUnbackfilled = ref(false)

const detectionDialog = reactive({ open: false, saving: false, rule: null as Dict | null, text: '' })
const resultDialog = reactive({ open: false, saving: false, task: null as Dict | null, source: '自动补齐', success: true, remark: '', checksum: '' })
const runtimeDialog = reactive({ open: false, saving: false, task: null as Dict | null, status: '运行实例处理中', recordId: '' })
const recordsDialog = reactive({ open: false, task: null as Dict | null, items: [] as Dict[] })

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function downloadBundle() {
  window.open(`${BACKFILL}/tasks-file/bundle`, '_blank')
}

function openCreate() {
  errorMessage.value = '检测规则登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区检测操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('检测规则列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区检测列表读取失败'
  }
}

async function loadDetections() {
  try {
    const response = await request(`${BACKFILL}/detections?size=200`)
    const payload = await response.json()
    detections.value = payload.items ?? []
    await loadTasks()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检测批次读取失败'
  }
}

async function loadTasks() {
  try {
    const params = new URLSearchParams({ size: '200' })
    if (taskStatusFilter.value) params.set('status', taskStatusFilter.value)
    params.set('include_backfilled', String(!onlyUnbackfilled.value))
    const response = await request(`${BACKFILL}/tasks?${params.toString()}`)
    const payload = await response.json()
    backfillTasks.value = payload.items ?? []
    stats.value[1].value = detections.value.length
    stats.value[2].value = backfillTasks.value.filter((t) => t['任务状态'] !== '已补齐').length
    stats.value[3].value = backfillTasks.value.filter((t) => t['任务状态'] === '已补齐').length
    stats.value[0].value = rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补齐任务读取失败'
  }
}

function openDetection(row: Row) {
  detectionDialog.open = true
  detectionDialog.rule = row as Dict
  detectionDialog.text = ''
}

async function submitDetection() {
  if (!detectionDialog.rule) return
  const missing = detectionDialog.text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [partition, priority] = line.split(',').map((part) => part.trim())
      return priority ? { 分区标识: partition, 优先级: priority } : { 分区标识: partition }
    })
  if (!missing.length) {
    errorMessage.value = '请至少填写一个缺失分区标识'
    return
  }
  detectionDialog.saving = true
  try {
    const response = await request(`${BACKFILL}/detections`, {
      method: 'POST',
      body: JSON.stringify({ values: { 规则id: detectionDialog.rule.id, 缺失分区清单: missing } }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    detectionDialog.open = false
    await loadDetections()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区检测提交失败'
  } finally {
    detectionDialog.saving = false
  }
}

function viewMissing(batch: Dict) {
  const text = (batch['缺失分区清单'] as Dict[]).map((item) => `${item['分区标识']}（优先级：${item['优先级']}）`).join('\n')
  window.alert(`批次 ${batch['检测批次号']} 缺失分区：\n${text}`)
}

async function handoff(batch: Dict) {
  try {
    const response = await request(`${BACKFILL}/detections/${batch.id}/handoff`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    errorMessage.value = payload.message
    await Promise.all([loadDetections(), loadTasks()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '受控交接失败'
  }
}

async function confirmTask(task: Dict) {
  try {
    const response = await request(`${BACKFILL}/tasks/${task.id}/confirm`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    await loadTasks()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '人工确认失败'
  }
}

function downloadTask(task: Dict) {
  window.open(`${BACKFILL}/tasks/${task.id}/download`, '_blank')
}

function openResultDialog(task: Dict, source: string) {
  resultDialog.open = true
  resultDialog.task = task
  resultDialog.source = source
  resultDialog.success = true
  resultDialog.remark = ''
  resultDialog.checksum = ''
}

async function submitResult() {
  if (!resultDialog.task) return
  resultDialog.saving = true
  try {
    const values: Dict = {
      处理来源: resultDialog.source,
      处理成功: resultDialog.success,
      备注: resultDialog.remark,
    }
    if (resultDialog.checksum.trim()) values['快照校验和'] = resultDialog.checksum.trim()
    const response = await request(`${BACKFILL}/tasks/${resultDialog.task.id}/results`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    errorMessage.value = payload.message
    resultDialog.open = false
    await loadTasks()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '处理结果提交失败'
  } finally {
    resultDialog.saving = false
  }
}

function openRuntimeDialog(task: Dict) {
  runtimeDialog.open = true
  runtimeDialog.task = task
  runtimeDialog.status = task['运行实例分区状态'] ?? '运行实例处理中'
  runtimeDialog.recordId = task['关联运行记录'] ?? ''
}

async function submitRuntimeStatus() {
  if (!runtimeDialog.task) return
  runtimeDialog.saving = true
  try {
    const response = await request(`${BACKFILL}/tasks/${runtimeDialog.task.id}/runtime-status`, {
      method: 'POST',
      body: JSON.stringify({
        values: { 运行实例分区状态: runtimeDialog.status, 关联运行记录: runtimeDialog.recordId },
      }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    runtimeDialog.open = false
    await loadTasks()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例状态回写失败'
  } finally {
    runtimeDialog.saving = false
  }
}

async function openResults(task: Dict) {
  recordsDialog.open = true
  recordsDialog.task = task
  try {
    const response = await request(`${BACKFILL}/tasks/${task.id}/results`)
    const payload = await response.json()
    recordsDialog.items = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '处理记录读取失败'
    recordsDialog.items = []
  }
}

onMounted(async () => {
  await reload()
  await loadDetections()
})
</script>

<style scoped>
.sub-panel {
  margin-top: 24px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
}
.sub-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}
.sub-head h3 {
  margin: 0;
  font-size: 15px;
}
.filter-bar.inline {
  margin: 0;
}
.modal-mask {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 50;
}
.modal {
  width: 480px;
  background: #fff;
  border-radius: 10px;
  padding: 18px 20px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.modal.wide {
  width: 860px;
  max-height: 80vh;
  overflow: auto;
}
.modal h3 {
  margin: 0;
}
.modal textarea,
.modal input,
.modal select {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
}
.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 6px;
}
</style>
