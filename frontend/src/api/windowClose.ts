import { request } from './client'

export type WindowOrder = {
  id: number
  关窗结果单号?: string
  窗口编号?: string
  窗口名称?: string
  管道编号?: string
  聚合任务编号?: string
  位置?: string
  接收状态?: string
  当前状态?: string
  status?: string
  有效样本数?: number
  重复样本数?: number
  聚合值?: number | null
  首次签收状态?: string
  更正前状态?: string
  更正后状态?: string
  history?: Array<Record<string, unknown>>
  samples?: Array<Record<string, unknown>>
  supplement_orders?: Array<Record<string, unknown>>
  [key: string]: unknown
}

type Envelope = {
  ok: boolean
  message?: string
  items?: WindowOrder[]
  orders?: WindowOrder[]
  window?: WindowOrder
  order?: WindowOrder
  total?: number
  [key: string]: unknown
}

async function post(path: string, payload: Record<string, unknown> = {}): Promise<Envelope> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(payload) })
  return (await response.json()) as Envelope
}

export async function listWindowOrders(): Promise<WindowOrder[]> {
  const response = await request('/api/window-close-orders?size=200')
  const data = (await response.json()) as Envelope
  return data.items ?? data.orders ?? []
}

export function seedWindowScenario(reset = false): Promise<Envelope> {
  return post(`/api/window-close/scenario${reset ? '?reset=true' : ''}`)
}

export function runWindowDemo(): Promise<Envelope> {
  return post('/api/window-close/demo?reset=true')
}

export function createWindowOrder(payload: Record<string, unknown>): Promise<Envelope> {
  return post('/api/window-close-orders', payload)
}

export function reportWindowSample(id: number, payload: Record<string, unknown>): Promise<Envelope> {
  return post(`/api/window-close-orders/${id}/samples`, payload)
}

export function confirmWindowEmpty(id: number): Promise<Envelope> {
  return post(`/api/window-close-orders/${id}/confirm-empty`, {})
}

export function correctWindow(id: number): Promise<Envelope> {
  return post(`/api/window-close-orders/${id}/correct`, {})
}

export function scheduleSupplement(id: number): Promise<Envelope> {
  return post(`/api/window-close-orders/${id}/supplement`, {})
}
