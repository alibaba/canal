import { mount, createLocalVue } from '@vue/test-utils'
import VueRouter from 'vue-router'
import ElementUI from 'element-ui'
import Breadcrumb from '@/components/Breadcrumb/index.vue'

const localVue = createLocalVue()
localVue.use(VueRouter)
localVue.use(ElementUI)

const routes = [
  {
    path: '/',
    name: 'home',
    children: [{
      path: 'dashboard',
      name: 'dashboard'
    }]
  },
  {
    path: '/menu',
    name: 'menu',
    children: [{
      path: 'menu1',
      name: 'menu1',
      meta: { title: 'menu1' },
      children: [{
        path: 'menu1-1',
        name: 'menu1-1',
        meta: { title: 'menu1-1' }
      },
      {
        path: 'menu1-2',
        name: 'menu1-2',
        redirect: 'noredirect',
        meta: { title: 'menu1-2' },
        children: [{
          path: 'menu1-2-1',
          name: 'menu1-2-1',
          meta: { title: 'menu1-2-1' }
        },
        {
          path: 'menu1-2-2',
          name: 'menu1-2-2'
        }]
      }]
    }]
  }]

const router = new VueRouter({
  routes
})

describe('Breadcrumb.vue', () => {
  const wrapper = mount(Breadcrumb, {
    localVue,
    router
  })
  it('dashboard', () => {
    router.push('/dashboard')
    const len = wrapper.findAll('.el-breadcrumb__inner').length
    expect(len).toBe(1)
  })
  it('normal route', () => {
    router.push('/menu/menu1')
    const len = wrapper.findAll('.el-breadcrumb__inner').length
    expect(len).toBe(2)
  })
  it('nested route', () => {
    router.push('/menu/menu1/menu1-2/menu1-2-1')
    const len = wrapper.findAll('.el-breadcrumb__inner').length
    expect(len).toBe(4)
  })
  it('no meta.title', () => {
    router.push('/menu/menu1/menu1-2/menu1-2-2')
    const len = wrapper.findAll('.el-breadcrumb__inner').length
    expect(len).toBe(3)
  })
  // it('click link', () => {
  //   router.push('/menu/menu1/menu1-2/menu1-2-2')
  //   const breadcrumbArray = wrapper.findAll('.el-breadcrumb__inner')
  //   const second = breadcrumbArray.at(1)
  //   console.log(breadcrumbArray)
  //   const href = second.find('a').attributes().href
  //   expect(href).toBe('#/menu/menu1')
  // })
  // it('noRedirect', () => {
  //   router.push('/menu/menu1/menu1-2/menu1-2-1')
  //   const breadcrumbArray = wrapper.findAll('.el-breadcrumb__inner')
  //   const redirectBreadcrumb = breadcrumbArray.at(2)
  //   expect(redirectBreadcrumb.contains('a')).toBe(false)
  // })
  it('last breadcrumb', () => {
    router.push('/menu/menu1/menu1-2/menu1-2-1')
    const breadcrumbArray = wrapper.findAll('.el-breadcrumb__inner')
    const redirectBreadcrumb = breadcrumbArray.at(3)
    expect(redirectBreadcrumb.contains('a')).toBe(false)
  })
})
