import request from '@/utils/request'

export function getNodeServers(params) {
  return request({
    url: '/nodeServers',
    method: 'get',
    params: params
  })
}

export function addNodeServer(data) {
  return request({
    url: '/nodeServer',
    method: 'post',
    data
  })
}

export function nodeServerDetail(id) {
  return request({
    url: '/nodeServer/' + id,
    method: 'get'
  })
}

export function updateNodeServer(data) {
  return request({
    url: '/nodeServer',
    method: 'put',
    data
  })
}

export function deleteNodeServer(id) {
  return request({
    url: '/nodeServer/' + id,
    method: 'delete'
  })
}

export function startNodeServer(id) {
  return request({
    url: '/nodeServer/start/' + id,
    method: 'put'
  })
}

export function stopNodeServer(id) {
  return request({
    url: '/nodeServer/stop/' + id,
    method: 'put'
  })
}

export function nodeServerLog(id) {
  return request({
    url: '/nodeServer/log/' + id,
    method: 'get'
  })
}
