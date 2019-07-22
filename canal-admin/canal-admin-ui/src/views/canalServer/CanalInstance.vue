<template>
  <div class="app-container">
    <div class="filter-container">
      <el-input v-model="listQuery.name" placeholder="实例名称" style="width: 200px;" class="filter-item" />
      <el-button class="filter-item" type="primary" icon="el-icon-search" plain @click="fetchData()">查询</el-button>
      &nbsp;&nbsp;
      <el-button class="filter-item" type="primary" @click="handleCreate()">新建实例</el-button>
      <el-button class="filter-item" type="info" @click="fetchData()">刷新列表</el-button>
    </div>
    <el-table
      v-loading="listLoading"
      :data="list"
      element-loading-text="Loading"
      border
      fit
      highlight-current-row
    >
      <el-table-column label="实例名称" min-width="200" align="center">
        <template slot-scope="scope">
          {{ scope.row.name }}
        </template>
      </el-table-column>
      <el-table-column label="运行节点" min-width="200" align="center">
        <template slot-scope="scope">
          <span>{{ scope.row.nodeIp }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" min-width="200" align="center">
        <template slot-scope="scope">
          {{ scope.row.modifiedTime }}
        </template>
      </el-table-column>
      <el-table-column align="center" prop="created_at" label="操作" min-width="150">
        <template slot-scope="scope">
          <el-dropdown trigger="click">
            <el-button type="primary" size="mini">
              操作<i class="el-icon-arrow-down el-icon--right" />
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="handleUpdate(scope.row)">修改实例</el-dropdown-item>
              <el-dropdown-item @click.native="handleDelete(scope.row)">删除实例</el-dropdown-item>
              <el-dropdown-item @click.native="handleStart(scope.row)">启动服务</el-dropdown-item>
              <el-dropdown-item @click.native="handleStop(scope.row)">停止服务</el-dropdown-item>
              <el-dropdown-item @click.native="handleLog(scope.row)">日志详情</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog :visible.sync="dialogFormVisible" title="确定启动服务" width="400px">
      <el-form ref="dataForm" :rules="rules" :model="nodeModel" label-position="left" label-width="80px" style="width: 350px; margin-left:30px;">
        <el-form-item label="选择节点" prop="nodeId">
          <el-select v-model="nodeModel.id" placeholder="选择节点">
            <el-option v-for="item in nodeServices" :key="item.id" :label="item.name" :value="item.id" />
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取消</el-button>
        <el-button type="primary" @click="doStartInstance()">确定</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { getCanalInstances, deleteCanalInstance, startInstance, stopInstance } from '@/api/canalInstance'
import { getNodeServers } from '@/api/nodeServer'

export default {
  filters: {
    statusFilter(status) {
      const statusMap = {
        published: 'success',
        draft: 'gray',
        deleted: 'danger'
      }
      return statusMap[status]
    }
  },
  data() {
    return {
      list: null,
      listLoading: true,
      dialogFormVisible: false,
      nodeServices: [],
      listQuery: {
        name: ''
      },
      currentId: null,
      nodeModel: {
        id: null
      },
      rules: {
        id: [{ required: true, message: '请选择节点', trigger: 'change' }]
      }
    }
  },
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      this.listLoading = true
      getCanalInstances(this.listQuery).then(res => {
        this.list = res.data
        this.listLoading = false
      })
    },
    handleCreate() {
      this.$router.push('/canalServer/canalInstance/add')
    },
    handleUpdate(row) {
      this.$router.push('/canalServer/canalInstance/modify?id=' + row.id)
    },
    handleDelete(row) {
      this.$confirm('删除实例配置会导致Canal实例停止', '确定删除实例信息', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        deleteCanalInstance(row.id).then((res) => {
          if (res.data === 'success') {
            this.fetchData()
            this.$message({
              message: '删除实例信息成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '删除实例点信息失败',
              type: 'error'
            })
          }
        })
      })
    },
    handleStart(row) {
      if (row.nodeId !== null) {
        this.$message({ message: '当前实例不是停止状态，无法启动', type: 'error' })
        return
      }

      this.currentId = row.id
      this.nodeModel.id = null

      this.$nextTick(() => {
        this.$refs['dataForm'].clearValidate()
      })

      getNodeServers().then((res) => {
        this.nodeServices = res.data
        this.dialogFormVisible = true
      })
    },
    doStartInstance() {
      startInstance(this.currentId, this.nodeModel.id).then((res) => {
        if (res.data) {
          this.fetchData()
          this.$message({
            message: '启动成功',
            type: 'success'
          })
          this.dialogFormVisible = false
        } else {
          this.$message({
            message: '启动实例服务出现异常',
            type: 'error'
          })
        }
      })
    },
    handleStop(row) {
      if (row.nodeId === null) {
        this.$message({ message: '当前实例不是启动状态，无法停止', type: 'error' })
        return
      }
      this.$confirm('停止Canal Instance: ' + row.name, '确定停止实例服务', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        stopInstance(row.id, row.nodeId).then((res) => {
          if (res.data) {
            this.fetchData()
            this.$message({
              message: '停止成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '停止实例服务出现异常',
              type: 'error'
            })
          }
        })
      })
    },
    handleLog(row) {
      if (row.nodeId === null) {
        this.$message({ message: '当前实例不是启动状态，无法查看日志', type: 'warning' })
        return
      }
      this.$router.push('canalInstance/log?id=' + row.id + '&nodeId=' + row.nodeId)
    }
  }
}
</script>
