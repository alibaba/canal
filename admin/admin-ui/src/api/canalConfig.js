import request from '@/utils/request'

export function getCanalConfig(clusterId, serverId) {
  return request({
    url: '/canal/config/' + clusterId + '/' + serverId,
    method: 'get'
  })
}

export function updateCanalConfig(data) {
  return request({
    url: '/canal/config',
    method: 'put',
    data
  })
}

export function getTemplateConfig() {
  return request({
    url: '/canal/config/template',
    method: 'get'
  })
}
