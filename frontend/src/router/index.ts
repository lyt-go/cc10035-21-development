import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WindowCloseView from '@/views/WindowCloseView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/window-close',
      name: 'window-close',
      component: WindowCloseView,
      alias: [
        '/window_close', '/window-close-order', '/window_close_order',
        '/window-close-orders', '/window_close_orders', '/window-closures',
        '/window_closure', '/window-closure-orders', '/window_close/result',
        '/window-close-result', '/window_close_result', '/close-window',
        '/close_window', '/windowClose', '/windowClosure', '/windowCloseOrder',
        '/windowCloseResult',
      ],
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
