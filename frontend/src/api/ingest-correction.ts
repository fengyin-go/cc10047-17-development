/** 拒收补交接口封装：批量读取重放、修正、确认、待办、版本详情与检查点。 */
import { request } from './client'

export type CorrectionItem = {
  id: number
  batch_id: number
  reject_id: number
  partition: string
  offset: number
  offset_key: string
  business_key: string
  kind: string
  missing_fields: string[]
  corrected_values: Record<string, string | number>
  merged_values: Record<string, string | number>
  fixed: boolean
  valid: boolean
  disposition: string
  duplicate_count: number
  replay_count: number
  confirmation_mode: string | null
  created_at: string
  updated_at: string
  confirmed_at: string | null
  version_id: number
}

export type CorrectionBatch = {
  id: number
  batch_no: string
  idempotency_key: string
  pipeline: string
  table: string
  status: string
  mode: string | null
  confirmation_strategy: string
  items: CorrectionItem[]
  item_count: number
  landed_count: number
  pending_count: number
  duplicate_count: number
  boundaries: ConfirmationBoundary[]
  effective_submissions: number
  duplicate_submissions: number
  batch_confirm_attempts: number
  last_conflict_at: string | null
  created_at: string
  updated_at: string
}

export type ConfirmationBoundary = {
  seq: number
  batch_id: number
  batch_no: string
  mode: string
  offsets: string[]
  partition: string | null
  offset: number | null
  count: number
  note: string
  at: string
}

export type ReplayResult = {
  ok: boolean
  effective: boolean
  duplicate: boolean
  message: string
  batch: CorrectionBatch | null
  created_corrections?: number
  duplicate_corrections?: number
  duplicate_submissions?: number
  not_found?: { partition: string; offset: number }[]
}

export type FixResult = {
  ok: boolean
  message: string
  batch: CorrectionBatch | null
  item: CorrectionItem | null
}

export type ConfirmResult = {
  ok: boolean
  effective: boolean
  duplicate?: boolean
  conflict?: boolean
  message: string
  item?: CorrectionItem
  items?: CorrectionItem[]
  boundary?: ConfirmationBoundary
  waiting_for?: {
    correction_id: number
    partition: string
    offset: number
    offset_key: string
    missing_fields: string[]
    disposition: string
  }[]
  batch: CorrectionBatch | null
  checkpoint?: CheckpointResult
  version?: VersionSummary
}

export type TodoItem = {
  id: number
  todo_no: string
  pipeline_id: string
  pipeline: string
  table: string
  partition: string
  offset: number
  offset_key: string
  business_key: string
  kind: string
  same_key_of: number | null
  missing_fields: string[]
  status: string
  reason: string
  duplicate_count: number
  correction_id: number | null
  batch_id: number | null
  version_id: number
  updated_at: string
}

export type TodoResult = {
  ok: boolean
  items: TodoItem[]
  total: number
  counts: Record<string, number>
  pipeline: string
  table: string
  version_id: number
  refreshed_at: string
  message?: string
}

export type VersionRow = {
  reject_id: number
  partition: string
  offset: number
  offset_key: string
  business_key: string
  kind: string
  same_key_of: number | null
  source_version: string
  values: Record<string, string | number>
  missing_fields: string[]
  duplicate_count: number
  correction_id: number | null
  batch_id: number | null
  reason: string
  disposition: string
  confirmation_mode: string | null
  confirmed_at: string | null
}

export type VersionSummary = {
  id: number
  label: string
  table: string
  accepted_count: number
  rejected_count: number
  accepted_offsets: string[]
  rejected_offsets: string[]
  duplicate_count: number
}

export type VersionDetail = {
  ok: boolean
  message?: string
  version: { id: number; label: string; table: string; pipeline: string; committed_at: string } | null
  pipeline: { id: string; name: string; source: string; target_table: string }
  accepted: VersionRow[]
  rejected: VersionRow[]
  counts: { accepted: number; rejected: number; duplicates: number; corrections: number }
  boundaries: ConfirmationBoundary[]
  batches: CorrectionBatch[]
  checkpoint: CheckpointResult
  refreshed_at: string
}

export type CheckpointPartition = {
  partition: string
  last_confirmed_offset: number | null
  next_offset: number
  next_read_offset: number
  ordered_offsets: {
    offset: number
    offset_key: string
    business_key: string
    kind: string
    status: string
  }[]
  receipts: {
    reject_id: number
    offset: number
    offset_key: string
    business_key: string
    status: string
    confirmation_mode: string | null
    confirmed_at: string | null
    duplicate_count: number
  }[]
  pending_offsets: number[]
  duplicate_count: number
}

export type CheckpointResult = {
  ok: boolean
  order_basis: string
  partitions: CheckpointPartition[]
  refreshed_at: string
}

export type ScenarioResult = {
  ok: boolean
  mode: string
  message: string
  trace: {
    step: string
    at: string
    ok: boolean
    effective: boolean
    message: string
    [key: string]: unknown
  }[]
  batch: CorrectionBatch
  version: VersionDetail
  checkpoint: CheckpointResult
  todos: TodoResult
  verification: {
    accepted_count: number
    rejected_count: number
    rows: {
      partition: string
      offset: number
      offset_key: string
      business_key: string
      landed: boolean
      disposition: string
      confirmation_mode: string | null
      checkpoint_next_offset: number | null
      duplicate_count: number
      values: Record<string, string | number>
    }[]
    new_version: {
      partition: string
      offset: number
      offset_key: string
      business_key: string
      same_key_of: number | null
      disposition: string
      replaced_original: boolean
    } | null
    boundaries: ConfirmationBoundary[]
    confirmation_strategy: string
    batch_effective_submissions: number
    duplicate_count: number
  }
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

async function get<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

export function replayRejects(payload: {
  idempotency_key?: string
  reads: { partition: string; offsets: number[] }[]
}): Promise<ReplayResult> {
  return post('/api/ingest-correction/replay', payload)
}

export function fixCorrection(
  batchId: number,
  payload: {
    correction_id?: number
    partition?: string
    offset?: number
    values: Record<string, string | number>
  },
): Promise<FixResult> {
  return post(`/api/ingest-correction/batches/${batchId}/fix`, payload)
}

export function confirmRow(
  batchId: number,
  payload: { correction_id?: number; partition?: string; offset?: number },
): Promise<ConfirmResult> {
  return post(`/api/ingest-correction/batches/${batchId}/confirm-row`, payload)
}

export function confirmAll(batchId: number): Promise<ConfirmResult> {
  return post(`/api/ingest-correction/batches/${batchId}/confirm-all`)
}

export function addNewVersion(payload: {
  partition: string
  offset: number
  business_key: string
  reject_id?: number
  source_version?: string
  values?: Record<string, string | number>
}): Promise<{
  ok: boolean
  created: boolean
  replaced: boolean
  message: string
  reject: { id: number; offset_key: string; kind: string; status: string; business_key: string }
  original: { id: number; offset_key: string; status: string } | null
  original_still_valid: boolean | null
}> {
  return post('/api/ingest-correction/rejects/new-version', payload)
}

export function loadTodos(): Promise<TodoResult> {
  return get('/api/ingest-correction/todos')
}

export function refreshTodos(): Promise<TodoResult> {
  return post('/api/ingest-correction/todos/refresh')
}

export function loadVersionDetail(versionId: number): Promise<VersionDetail> {
  return get(`/api/ingest-correction/versions/${versionId}`)
}

export function loadCheckpoint(): Promise<CheckpointResult> {
  return get('/api/ingest-correction/checkpoint')
}

export function runScenario(
  mode: '逐行确认' | '整批确认',
  newVersionOffset = 206,
): Promise<ScenarioResult> {
  return post(
    `/api/ingest-correction/scenario?mode=${encodeURIComponent(mode)}&new_version_offset=${newVersionOffset}`,
  )
}

export function resetCorrectionDemo(): Promise<{ ok: boolean; message: string }> {
  return post('/api/ingest-correction/reset')
}
