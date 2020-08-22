import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router)

/* Layout */
import Layout from '@/layout'

/**
 * Note: sub-menu only appear when route children.length >= 1
 * Detail see: https://panjiachen.github.io/vue-element-admin-site/guide/essentials/router-and-nav.html
 *
 * hidden: true                   if set true, item will not show in the sidebar(default is false)
 * alwaysShow: true               if set true, will always show the root menu
 *                                if not set alwaysShow, when item has more than one children route,
 *                                it will becomes nested mode, otherwise not show the root menu
 * redirect: noRedirect           if set noRedirect will no redirect in the breadcrumb
 * name:'router-name'             the name is used by <keep-alive> (must set!!!)
 * meta : {
    roles: ['admin','editor']    control the page roles (you can set multiple roles)
    title: 'title'               the name show in sidebar and breadcrumb (recommend set)
    icon: 'svg-name'             the icon show in the sidebar
    breadcrumb: false            if set false, the item will hidden in breadcrumb(default is true)
    activeMenu: '/example/list'  if set path, the sidebar will highlight the path you set
  }
 */

/**
 * constantRoutes
 * a base page that does not have permission requirements
 * all roles can be accessed
 */
export const constantRoutes = [
  {
    path: '/login',
    component: () => import('@/views/login/index'),
    hidden: true
  },

  {
    path: '/404',
    component: () => import('@/views/404'),
    hidden: true
  },

  {
    path: '/',
    component: Layout,
    redirect: '/dashboard',
    children: [{
      path: 'dashboard',
      name: '主页',
      component: () => import('@/views/dashboard/index'),
      meta: { title: '主页', icon: 'dashboard' }
    }],
    hidden: true
  },

  {
    path: '/sys',
    component: Layout,
    redirect: '/user',
    children: [{
      path: 'user',
      name: '用户信息',
      component: () => import('@/views/sys/UserInfo'),
      meta: { title: '用户信息' }
    }],
    hidden: true
  },

  {
    path: '/canalServer',
    component: Layout,
    redirect: '/canalServer/nodeServers',
    name: 'Canal Server',
    meta: { title: 'Canal Server', icon: 'example' },
    children: [
      {
        path: 'canalClusters',
        name: 'Canal 集群管理',
        component: () => import('@/views/canalServer/CanalCluster'),
        meta: { title: '集群管理', icon: 'tree' }
      },
      {
        path: 'nodeServers',
        name: 'Server 状态',
        component: () => import('@/views/canalServer/NodeServer'),
        meta: { title: 'Server 管理', icon: 'form' }
      },
      {
        path: 'nodeServer/config',
        name: 'Server 配置',
        component: () => import('@/views/canalServer/CanalConfig'),
        meta: { title: 'Server 配置' },
        hidden: true
      },
      {
        path: 'canalInstances',
        name: 'Instance 管理',
        component: () => import('@/views/canalServer/CanalInstance'),
        meta: { title: 'Instance 管理', icon: 'nested' }
      },
      {
        path: 'canalInstance/add',
        name: '新建Instance配置',
        component: () => import('@/views/canalServer/CanalInstanceAdd'),
        meta: { title: '新建Instance配置' },
        hidden: true
      },
      {
        path: 'canalInstance/modify',
        name: '修改Instance配置',
        component: () => import('@/views/canalServer/CanalInstanceUpdate'),
        meta: { title: '修改Instance配置' },
        hidden: true
      },
      {
        path: 'nodeServer/log',
        name: 'Server 日志',
        component: () => import('@/views/canalServer/CanalLogDetail'),
        meta: { title: 'Server 日志' },
        hidden: true
      },
      {
        path: 'canalInstance/log',
        name: 'Instance 日志',
        component: () => import('@/views/canalServer/CanalInstanceLogDetail'),
        meta: { title: 'Instance 日志' },
        hidden: true
      }
    ]
  },

  // 404 page must be placed at the end !!!
  { path: '*', redirect: '/404', hidden: true }
]

const createRouter = () => new Router({
  // mode: 'history', // require service support
  scrollBehavior: () => ({ y: 0 }),
  routes: constantRoutes
})

const router = createRouter()

// Detail see: https://github.com/vuejs/vue-router/issues/1234#issuecomment-357941465
export function resetRouter() {
  const newRouter = createRouter()
  router.matcher = newRouter.matcher // reset router
}

export default router
