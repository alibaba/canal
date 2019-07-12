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
