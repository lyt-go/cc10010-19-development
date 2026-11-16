<template>
  <section class="page" data-module="rerun">
    <header class="page-head">
      <div>
        <h2>异常重跑管理</h2>
        <p class="page-desc">管理ETL作业的异常重跑任务；分区类重跑读取补齐任务数据快照，且只消费仍未补齐的数据表分区。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记重跑任务</button>
        <button class="btn" type="button" @click="exportRows">导出异常重跑清单</button>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无异常重跑数据，可先登记重跑任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条异常重跑记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <!-- 快照消费：异常重跑只读补齐任务快照，且只拿仍未补齐的分区 -->
    <section class="sub-panel">
      <header class="sub-head">
        <h3>分区补齐任务快照消费</h3>
        <div class="filter-bar inline">
          <label class="filter-item">
            <span>检测批次id（留空消费全部未补齐分区）</span>
            <input v-model="consumeForm.batchId" placeholder="如 1" style="width: 220px" />
          </label>
          <label class="filter-item">
            <span>快照校验和（留空不校验）</span>
            <input v-model="consumeForm.checksum" placeholder="16位校验和" style="width: 220px" />
          </label>
          <button class="btn primary" type="button" :disabled="consumeForm.saving" @click="consumeSnapshot">读取快照并生成重跑清单</button>
          <button class="btn ghost" type="button" @click="loadRerunItems">刷新清单</button>
        </div>
      </header>
      <p class="page-desc">已补齐分区不会进入重跑清单，仅作为跳过分区返回；快照校验和不匹配时拒绝消费，避免按旧检测批次重跑。</p>
      <table v-if="lastConsume" class="data-table">
        <thead>
          <tr><th>重跑清单编号</th><th>快照校验和</th><th>待重跑分区</th><th>跳过的已补齐分区</th></tr>
        </thead>
        <tbody>
          <tr>
            <td>{{ lastConsume['重跑清单编号'] }}</td>
            <td><code>{{ lastConsume['快照校验和'] }}</code></td>
            <td>
              <span v-for="part in lastConsume['待重跑分区']" :key="part['补齐任务编号']" class="tag">
                {{ part['分区标识'] }}（{{ part['优先级'] }}）
              </span>
            </td>
            <td>
              <span v-for="part in lastConsume['跳过分区']" :key="part['补齐任务编号']" class="tag muted">
                {{ part['分区标识'] }}
              </span>
              <span v-if="!lastConsume['跳过分区'].length">—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 仍未补齐的数据表分区：重跑协作业务面回写处理状态 -->
    <section class="sub-panel">
      <header class="sub-head">
        <h3>仍未补齐的数据表分区（异常重跑可消费范围）</h3>
        <button class="btn ghost" type="button" @click="loadPartitionTasks">刷新分区</button>
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
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in partitionTasks" :key="String(task.id)">
            <td>{{ task['补齐任务编号'] }}</td>
            <td>{{ task['数据表名'] }}</td>
            <td>{{ task['分区标识'] }}</td>
            <td>{{ task['优先级'] }}</td>
            <td>{{ task['任务状态'] }}</td>
            <td>{{ task['运行实例分区状态'] }}</td>
            <td>{{ task['完整性结论'] }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="openStatusDialog(task)">回写处理状态</button>
            </td>
          </tr>
          <tr v-if="!partitionTasks.length">
            <td colspan="8" class="empty-state">没有仍未补齐的分区，异常重跑无需消费</td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 异常重跑清单：业务结果落单并随分区检测重算 -->
    <section class="sub-panel">
      <header class="sub-head">
        <h3>异常重跑清单（业务结果与检测重算）</h3>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>重跑清单编号</th>
            <th>消费时间</th>
            <th>消费分区数</th>
            <th>跳过已补齐</th>
            <th>消费分区</th>
            <th>业务结果</th>
            <th>清单状态</th>
            <th>重算补齐分区</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in rerunItems" :key="String(item.id)">
            <td>{{ item['重跑清单编号'] }}</td>
            <td>{{ item['消费时间'] }}</td>
            <td>{{ item['消费分区数'] }}</td>
            <td>{{ item['跳过已补齐分区数'] }}</td>
            <td>{{ (item['消费分区'] ?? []).join('、') }}</td>
            <td>{{ item['业务结果'] || '—' }}</td>
            <td>{{ item['清单状态'] }}</td>
            <td>{{ (item['重算补齐分区'] ?? []).join('、') || '—' }}</td>
            <td class="row-actions">
              <button v-if="item['清单状态'] !== '已重算'" class="link" type="button" @click="openResultDialog(item)">回填业务结果</button>
            </td>
          </tr>
          <tr v-if="!rerunItems.length">
            <td colspan="9" class="empty-state">暂无重跑清单，先读取分区补齐快照</td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 处理状态回写对话框 -->
    <div v-if="statusDialog.open" class="modal-mask" @click.self="statusDialog.open = false">
      <div class="modal">
        <h3>回写重跑处理状态 · {{ statusDialog.task?.['分区标识'] }}</h3>
        <p class="page-desc">只允许写入未完成分区；已补齐分区在接口侧直接拒绝。</p>
        <label class="filter-item">
          <span>运行实例分区状态</span>
          <select v-model="statusDialog.status">
            <option v-for="s in runtimeStatuses" :key="s" :value="s">{{ s }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>重跑清单编号</span>
          <input v-model="statusDialog.rerunNo" placeholder="如 RR-PART-0001" />
        </label>
        <label class="filter-item">
          <span>备注</span>
          <input v-model="statusDialog.remark" />
        </label>
        <div class="modal-actions">
          <button class="btn ghost" type="button" @click="statusDialog.open = false">取消</button>
          <button class="btn primary" type="button" :disabled="statusDialog.saving" @click="submitStatus">回写</button>
        </div>
      </div>
    </div>

    <!-- 业务结果回填对话框 -->
    <div v-if="resultDialog.open" class="modal-mask" @click.self="resultDialog.open = false">
      <div class="modal">
        <h3>回填业务结果 · {{ resultDialog.item?.['重跑清单编号'] }}</h3>
        <p class="page-desc">业务结果落到异常重跑清单并随分区检测重算；勾选成功的分区会原子提交「补齐完成 + 分区完整」结论。</p>
        <label class="filter-item">
          <span>业务结果说明</span>
          <textarea v-model="resultDialog.businessResult" rows="3"></textarea>
        </label>
        <div class="filter-item">
          <span>重跑成功分区（原子提交完整性结论）</span>
          <label v-for="partition in resultDialog.item?.['消费分区'] ?? []" :key="partition" class="check-line">
            <input type="checkbox" :value="partition" v-model="resultDialog.successPartitions" />
            {{ partition }}
          </label>
        </div>
        <div class="modal-actions">
          <button class="btn ghost" type="button" @click="resultDialog.open = false">取消</button>
          <button class="btn primary" type="button" :disabled="resultDialog.saving" @click="submitRerunResult">提交重算</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Dict = Record<string, any>

const ENDPOINT = '/api/rerun'
const columns = ["任务编号", "原始运行编号", "重跑区间开始", "重跑区间结束", "重跑原因", "覆盖策略", "依赖级联", "重跑状态"]
const actions = ["提交重跑审批", "启动重跑", "取消重跑"]
const runtimeStatuses = ["未下发", "运行实例处理中", "运行实例已完成", "运行实例失败"]
const stats = ref([
  { label: "重跑任务总数", value: 0 },
  { label: "待补齐分区", value: 0 },
  { label: "重跑清单数", value: 0 },
  { label: "已重算清单", value: 0 },
])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const partitionTasks = ref<Dict[]>([])
const rerunItems = ref<Dict[]>([])
const lastConsume = ref<Dict | null>(null)
const consumeForm = reactive({ batchId: '', checksum: '', saving: false })
const statusDialog = reactive({ open: false, saving: false, task: null as Dict | null, status: '运行实例处理中', rerunNo: '', remark: '' })
const resultDialog = reactive({ open: false, saving: false, item: null as Dict | null, businessResult: '', successPartitions: [] as string[] })

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '重跑任务登记入口尚未接入审批流'
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
    errorMessage.value = error instanceof Error ? error.message : '异常重跑操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('重跑任务列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats.value[0].value = total.value
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常重跑列表读取失败'
  }
}

async function loadPartitionTasks() {
  try {
    const response = await request(`${ENDPOINT}/partition-tasks?size=200`)
    const payload = await response.json()
    partitionTasks.value = payload.items ?? []
    stats.value[1].value = payload.total ?? partitionTasks.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '未补齐分区读取失败'
  }
}

async function loadRerunItems() {
  try {
    const response = await request(`${ENDPOINT}/partition-rerun/items?size=200`)
    const payload = await response.json()
    rerunItems.value = payload.items ?? []
    stats.value[2].value = payload.total ?? 0
    stats.value[3].value = rerunItems.value.filter((item) => item['清单状态'] === '已重算').length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重跑清单读取失败'
  }
}

async function consumeSnapshot() {
  consumeForm.saving = true
  try {
    const values: Dict = {}
    if (consumeForm.batchId.trim()) values['检测批次id'] = consumeForm.batchId.trim()
    if (consumeForm.checksum.trim()) values['快照校验和'] = consumeForm.checksum.trim()
    const response = await request(`${ENDPOINT}/partition-snapshot/consume`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    lastConsume.value = payload.entry
    errorMessage.value = payload.message
    await Promise.all([loadPartitionTasks(), loadRerunItems()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照消费失败'
  } finally {
    consumeForm.saving = false
  }
}

function openStatusDialog(task: Dict) {
  statusDialog.open = true
  statusDialog.task = task
  statusDialog.status = task['运行实例分区状态'] ?? '运行实例处理中'
  statusDialog.rerunNo = ''
  statusDialog.remark = ''
}

async function submitStatus() {
  if (!statusDialog.task) return
  statusDialog.saving = true
  try {
    const response = await request(`${ENDPOINT}/partition-tasks/${statusDialog.task.id}/status`, {
      method: 'POST',
      body: JSON.stringify({
        values: {
          运行实例分区状态: statusDialog.status,
          重跑清单编号: statusDialog.rerunNo,
          备注: statusDialog.remark,
        },
      }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    statusDialog.open = false
    await loadPartitionTasks()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '处理状态回写失败'
  } finally {
    statusDialog.saving = false
  }
}

function openResultDialog(item: Dict) {
  resultDialog.open = true
  resultDialog.item = item
  resultDialog.businessResult = ''
  resultDialog.successPartitions = []
}

async function submitRerunResult() {
  if (!resultDialog.item) return
  resultDialog.saving = true
  try {
    const response = await request(
      `${ENDPOINT}/partition-rerun/items/${encodeURIComponent(resultDialog.item['重跑清单编号'])}/results`,
      {
        method: 'POST',
        body: JSON.stringify({
          values: {
            业务结果: resultDialog.businessResult || '重跑完成',
            成功分区: resultDialog.successPartitions,
          },
        }),
      },
    )
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    resultDialog.open = false
    errorMessage.value = payload.message
    await Promise.all([loadRerunItems(), loadPartitionTasks()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务结果回填失败'
  } finally {
    resultDialog.saving = false
  }
}

onMounted(async () => {
  await reload()
  await Promise.all([loadPartitionTasks(), loadRerunItems()])
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
.tag {
  display: inline-block;
  background: #eaf2ff;
  border: 1px solid #b9d4ff;
  border-radius: 999px;
  padding: 2px 8px;
  margin: 2px 4px 2px 0;
  font-size: 12px;
}
.tag.muted {
  background: #f1f5f9;
  border-color: var(--border);
  color: var(--muted);
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
  width: 520px;
  max-height: 82vh;
  overflow: auto;
  background: #fff;
  border-radius: 10px;
  padding: 18px 20px;
  display: flex;
  flex-direction: column;
  gap: 10px;
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
.check-line {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-right: 12px;
  font-size: 13px;
}
.check-line input {
  width: auto;
}
</style>
