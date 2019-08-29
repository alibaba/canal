<template>
  <div class="app-container">
    <div class="filter-container">
      <!-- <el-input v-model="listQuery.name" placeholder="Server 名称" style="width: 200px;" class="filter-item" />
      <el-input v-model="listQuery.ip" placeholder="Server IP" style="width: 200px;" class="filter-item" />
      <el-button class="filter-item" type="primary" icon="el-icon-search" plain @click="fetchData()">查询</el-button> -->
      <el-button class="filter-item" type="primary" @click="handleCreate()">新建集群</el-button>
    </div>
    <el-table
      v-loading="listLoading"
      :data="list"
      element-loading-text="Loading"
      border
      fit
      highlight-current-row
    >
      <el-table-column label="集群名称" min-width="200" align="center">
        <template slot-scope="scope">
          {{ scope.row.name }}
        </template>
      </el-table-column>
      <el-table-column label="ZK地址" min-width="300" align="center">
        <template slot-scope="scope">
          <span>{{ scope.row.zkHosts }}</span>
        </template>
      </el-table-column>
      <el-table-column align="center" prop="created_at" label="操作" min-width="150">
        <template slot-scope="scope">
          <el-dropdown trigger="click">
            <el-button type="primary" size="mini">
              操作<i class="el-icon-arrow-down el-icon--right" />
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="handleConfig(scope.row)">主配置</el-dropdown-item>
              <el-dropdown-item @click.native="handleUpdate(scope.row)">修改集群</el-dropdown-item>
              <el-dropdown-item @click.native="handleDelete(scope.row)">删除集群</el-dropdown-item>
              <el-dropdown-item @click.native="handleView(scope.row)">查看Server</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog :visible.sync="dialogFormVisible" :title="textMap[dialogStatus]" width="600px">
      <el-form ref="dataForm" :rules="rules" :model="canalCluster" label-position="left" label-width="120px" style="width: 400px; margin-left:30px;">
        <el-form-item label="集群名称" prop="name">
          <el-input v-model="canalCluster.name" />
        </el-form-item>
        <el-form-item label="ZK地址" prop="zkHosts">
          <el-input v-model="canalCluster.zkHosts" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取消</el-button>
        <el-button type="primary" @click="dataOperation()">确定</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { addCanalCluster, getCanalClusters, updateCanalCluster, deleteCanalCluster } from '@/api/canalCluster'

export default {
  filters: {
    statusFilter(status) {
      const statusMap = {
        '1': 'success',
        '0': 'gray',
        '-1': 'danger'
      }
      return statusMap[status]
    },
    statusLabel(status) {
      const statusMap = {
        '1': '启动',
        '0': '停止',
        '-1': '断开'
      }
      return statusMap[status]
    }
  },
  data() {
    return {
      list: null,
      listLoading: true,
      listQuery: {
        name: '',
        ip: ''
      },
      dialogFormVisible: false,
      textMap: {
        create: '新建集群信息',
        update: '修改集群信息'
      },
      canalCluster: {
        id: null,
        name: null,
        zkHosts: null
      },
      rules: {
        name: [{ required: true, message: '集群名称不能为空', trigger: 'change' }],
        zkHosts: [{ required: true, message: 'zk地址不能为空', trigger: 'change' }]
      },
      dialogStatus: 'create'
    }
  },
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      this.listLoading = true
      getCanalClusters(this.listQuery).then(res => {
        this.list = res.data
      }).finally(() => {
        this.listLoading = false
      })
    },
    resetModel() {
      this.canalCluster = {
        id: null,
        name: null,
        zkHosts: null
      }
    },
    handleCreate() {
      this.resetModel()
      this.dialogStatus = 'create'
      this.dialogFormVisible = true
      this.$nextTick(() => {
        this.$refs['dataForm'].clearValidate()
      })
    },
    dataOperation() {
      this.$refs['dataForm'].validate((valid) => {
        if (valid) {
          if (this.dialogStatus === 'create') {
            addCanalCluster(this.canalCluster).then(res => {
              this.operationRes(res)
            })
          }
          if (this.dialogStatus === 'update') {
            updateCanalCluster(this.canalCluster).then(res => {
              this.operationRes(res)
            })
          }
        }
      })
    },
    operationRes(res) {
      if (res.data === 'success') {
        this.fetchData()
        this.dialogFormVisible = false
        this.$message({
          message: this.textMap[this.dialogStatus] + '成功',
          type: 'success'
        })
      } else {
        this.$message({
          message: this.textMap[this.dialogStatus] + '失败',
          type: 'error'
        })
      }
    },
    handleView(row) {
      this.$router.push('/canalServer/nodeServers?clusterId=' + row.id)
    },
    handleConfig(row) {
      this.$router.push('/canalServer/nodeServer/config?clusterId=' + row.id)
    },
    handleUpdate(row) {
      this.resetModel()
      this.canalCluster = Object.assign({}, row)
      this.dialogStatus = 'update'
      this.dialogFormVisible = true
      this.$nextTick(() => {
        this.$refs['dataForm'].clearValidate()
      })
    },
    handleDelete(row) {
      this.$confirm('删除集群信息会导致服务停止', '确定删除集群信息', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        deleteCanalCluster(row.id).then((res) => {
          if (res.data === 'success') {
            this.fetchData()
            this.$message({
              message: '删除集群信息成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '删除集群信息失败',
              type: 'error'
            })
          }
        })
      })
    }
  }
}
</script>
