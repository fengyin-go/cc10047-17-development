import { createRouter, createWebHistory } from 'vue-router'

import IngestCorrectionView from '@/views/IngestCorrectionView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 拒收补交比 /:moduleKey 更具体，必须排在动态模块路由之前。
    {
      path: '/stream_ingest/correction',
      name: 'ingest-correction',
      component: IngestCorrectionView,
    },
    {
      path: '/stream_ingest/correction/versions/:versionId',
      name: 'ingest-correction-version',
      component: IngestCorrectionView,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
