<template>
  <div class="app-container">
    <div class="filter-container">
      <!-- <el-input v-model="listQuery.name" placeholder="节点名称" style="width: 200px;" class="filter-item" />
      <el-input v-model="listQuery.ip" placeholder="节点IP" style="width: 200px;" class="filter-item" />
      <el-button class="filter-item" type="primary" icon="el-icon-search" plain @click="fetchData()">查询</el-button> -->
      <el-button class="filter-item" type="primary" @click="handleCreate()">新建节点</el-button>
      <el-button class="filter-item" type="info" @click="fetchData()">刷新节点</el-button>
    </div>
    <el-table
      v-loading="listLoading"
      :data="list"
      element-loading-text="Loading"
      border
      fit
      highlight-current-row
    >
      <el-table-column label="节点名称" min-width="200" align="center">
        <template slot-scope="scope">
          {{ scope.row.name }}
        </template>
      </el-table-column>
      <el-table-column label="IP" min-width="200" align="center">
        <template slot-scope="scope">
          <span>{{ scope.row.ip }}</span>
        </template>
      </el-table-column>
      <el-table-column label="端口" min-width="100" align="center">
        <template slot-scope="scope">
          {{ scope.row.port }}
        </template>
      </el-table-column>
      <el-table-column label="监控端口" min-width="100" align="center">
        <template slot-scope="scope">
          {{ scope.row.port2 }}
        </template>
      </el-table-column>
      <el-table-column class-name="status-col" label="状态" min-width="150" align="center">
        <template slot-scope="scope">
          <el-tag :type="scope.row.status | statusFilter">{{ scope.row.status | statusLabel }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column align="center" prop="created_at" label="操作" min-width="150">
        <template slot-scope="scope">
          <el-dropdown trigger="click">
            <el-button type="primary" size="mini">
              操作<i class="el-icon-arrow-down el-icon--right" />
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="handleUpdate(scope.row)">修改节点</el-dropdown-item>
              <el-dropdown-item @click.native="handleDelete(scope.row)">删除节点</el-dropdown-item>
              <el-dropdown-item @click.native="handleStart(scope.row)">启动服务</el-dropdown-item>
              <el-dropdown-item @click.native="handleStop(scope.row)">停止服务</el-dropdown-item>
              <el-dropdown-item @click.native="handleLog(scope.row)">日志详情</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog :visible.sync="dialogFormVisible" title="新建节点信息" width="600px">
      <el-form ref="dataForm" :rules="rules" :model="nodeModel" label-position="left" label-width="80px" style="width: 350px; margin-left:80px;">
        <el-form-item label="节点名称" prop="name">
          <el-input v-model="nodeModel.name" />
        </el-form-item>
        <el-form-item label="节点IP" prop="ip">
          <el-input v-model="nodeModel.ip" />
        </el-form-item>
        <el-form-item label="节点端口" prop="port">
          <el-input v-model="nodeModel.port" placeholder="11113" type="number" />
        </el-form-item>
        <el-form-item label="监控端口" prop="port2">
          <el-input v-model="nodeModel.port2" type="number" />
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取消</el-button>
        <el-button type="primary" @click="createData()">确定</el-button>
      </div>
    </el-dialog>
    <el-dialog :visible.sync="dialogFormVisible" :title="textMap[dialogStatus]" width="600px">
      <el-form ref="dataForm" :rules="rules" :model="nodeModel" label-position="left" label-width="80px" style="width: 350px; margin-left:80px;">
        <el-form-item label="节点名称" prop="name">
          <el-input v-model="nodeModel.name" />
        </el-form-item>
        <el-form-item label="节点IP" prop="ip">
          <el-input v-model="nodeModel.ip" />
        </el-form-item>
        <el-form-item label="节点端口" prop="port">
          <el-input v-model="nodeModel.port" placeholder="11113" type="number" />
        </el-form-item>
        <el-form-item label="监控端口" prop="port2">
          <el-input v-model="nodeModel.port2" type="number" />
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
import { addNodeServer, getNodeServers, updateNodeServer, deleteNodeServer, startNodeServer, stopNodeServer } from '@/api/nodeServer'

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
        create: '新建节点信息',
        update: '修改节点信息'
      },
      nodeModel: {
        id: undefined,
        name: null,
        ip: null,
        port: 11113,
        port2: null
      },
      rules: {
        name: [{ required: true, message: '节点名称不能为空', trigger: 'change' }],
        ip: [{ required: true, message: '节点IP不能为空', trigger: 'change' }],
        port: [{ required: true, message: '节点端口不能为空', trigger: 'change' }]
      }
    }
  },
  // { min: 2, max: 5, message: '长度在 2 到 5 个字符', trigger: 'change' }
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      this.listLoading = true
      getNodeServers(this.listQuery).then(res => {
        this.list = res.data
        this.listLoading = false
      })
    },
    resetModel() {
      this.nodeModel = {
        id: undefined,
        name: null,
        ip: null,
        port: null,
        port2: null
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
            addNodeServer(this.nodeModel).then(res => {
              this.operationRes(res)
            })
          }
          if (this.dialogStatus === 'update') {
            updateNodeServer(this.nodeModel).then(res => {
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
    handleUpdate(row) {
      this.resetModel()
      this.nodeModel = Object.assign({}, row)
      this.dialogStatus = 'update'
      this.dialogFormVisible = true
      this.$nextTick(() => {
        this.$refs['dataForm'].clearValidate()
      })
    },
    handleDelete(row) {
      this.$confirm('删除节点信息并不会导致节点服务停止', '确定删除节点信息', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        deleteNodeServer(row.id).then((res) => {
          if (res.data === 'success') {
            this.fetchData()
            this.$message({
              message: '删除节点信息成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '删除节点信息失败',
              type: 'error'
            })
          }
        })
      })
    },
    handleStart(row) {
      if (row.status !== 0) {
        this.$message({ message: '当前节点不是停止状态，无法启动', type: 'error' })
        return
      }
      this.$confirm('启动节点 Canal Server 服务', '确定启动节点服务', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        startNodeServer(row.id).then((res) => {
          if (res.data) {
            this.fetchData()
            this.$message({
              message: '启动成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '启动节点服务出现异常',
              type: 'error'
            })
          }
        })
      })
    },
    handleStop(row) {
      if (row.status !== 1) {
        this.$message({ message: '当前节点不是启动状态，无法停止', type: 'error' })
        return
      }
      this.$confirm('停止节点 Canal Server 服务', '确定停止节点服务', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        stopNodeServer(row.id).then((res) => {
          if (res.data) {
            this.fetchData()
            this.$message({
              message: '停止成功',
              type: 'success'
            })
          } else {
            this.$message({
              message: '停止节点服务出现异常',
              type: 'error'
            })
          }
        })
      })
    },
    handleLog(row) {
      this.$router.push('nodeServer/log?id=' + row.id)
    }
  }
}
</script>
