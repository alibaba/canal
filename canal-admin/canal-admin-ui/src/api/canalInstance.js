import request from '@/utils/request'

export function getCanalInstances(params) {
  return request({
    url: '/canal/instances',
    method: 'get',
    params: params
  })
}

export function canalInstanceDetail(id) {
  return request({
    url: '/canal/instance/' + id,
    method: 'get'
  })
}

export function updateCanalInstance(data) {
  return request({
    url: '/canal/instance',
    method: 'put',
    data
  })
}
