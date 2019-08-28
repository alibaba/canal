import request from '@/utils/request'

export function getCanalConfig(serverId) {
  return request({
    url: '/canal/config/' + serverId,
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
