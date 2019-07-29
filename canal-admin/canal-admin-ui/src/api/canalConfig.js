import request from '@/utils/request'

export function getCanalConfig() {
  return request({
    url: '/canal/config',
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
