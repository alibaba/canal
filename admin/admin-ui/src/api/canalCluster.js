import request from '@/utils/request'

export function getCanalClusters(params) {
  return request({
    url: '/canal/clusters',
    method: 'get',
    params: params
  })
}

export function addCanalCluster(data) {
  return request({
    url: '/canal/cluster',
    method: 'post',
    data
  })
}

export function canalClusterDetail(id) {
  return request({
    url: '/canal/cluster/' + id,
    method: 'get'
  })
}

export function updateCanalCluster(data) {
  return request({
    url: '/canal/cluster',
    method: 'put',
    data
  })
}

export function deleteCanalCluster(id) {
  return request({
    url: '/canal/cluster/' + id,
    method: 'delete'
  })
}

export function getClustersAndServers() {
  return request({
    url: '/canal/clustersAndServers',
    method: 'get'
  })
}
