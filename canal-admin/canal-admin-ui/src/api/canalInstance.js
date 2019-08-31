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

export function addCanalInstance(data) {
  return request({
    url: '/canal/instance',
    method: 'post',
    data
  })
}

export function deleteCanalInstance(id) {
  return request({
    url: '/canal/instance/' + id,
    method: 'delete'
  })
}

export function startInstance(id, nodeId) {
  return request({
    url: '/canal/instance/start/' + id + '/' + nodeId,
    method: 'put'
  })
}

export function stopInstance(id, nodeId) {
  return request({
    url: '/canal/instance/stop/' + id + '/' + nodeId,
    method: 'put'
  })
}

export function instanceLog(id, nodeId) {
  return request({
    url: '/canal/instance/log/' + id + '/' + nodeId,
    method: 'get'
  })
}

export function instanceStatus(id, option) {
  return request({
    url: '/canal/instance/status/' + id + '?option=' + option,
    method: 'put'
  })
}

export function getActiveInstances(serverId) {
  return request({
    url: '/canal/active/instances/' + serverId,
    method: 'get'
  })
}

export function getTemplateInstance() {
  return request({
    url: '/canal/instance/template',
    method: 'get'
  })
}
