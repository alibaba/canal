import request from '@/utils/request'

export function getCanalConfig() {
  return request({
    url: '/canal/config',
    method: 'get'
  })
}
