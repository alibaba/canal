<template>
  <div>
    <el-form ref="form" :model="form">
      <div style="padding-left: 10px;padding-top: 20px;">
        <el-form-item>
          {{ form.name }}&nbsp;&nbsp;&nbsp;&nbsp;
          <el-select v-model="form.clusterServerId" placeholder="所属集群/主机" class="filter-item">
            <el-option-group v-for="group in options" :key="group.label" :label="group.label">
              <el-option v-for="item in group.options" :key="item.value" :label="item.label" :value="item.value" />
            </el-option-group>
          </el-select>
          <el-button type="primary" @click="onSubmit">修改</el-button>
          <el-button type="warning" @click="onCancel">重置</el-button>
          <el-button type="info" @click="onBack">返回</el-button>
        </el-form-item>
      </div>
      <editor v-model="form.content" lang="properties" theme="chrome" width="100%" :height="800" @init="editorInit" />
    </el-form>
  </div>
</template>

<script>
import { canalInstanceDetail, updateCanalInstance } from '@/api/canalInstance'
import { getClustersAndServers } from '@/api/canalCluster'

export default {
  components: {
    editor: require('vue2-ace-editor')
  },
  data() {
    return {
      options: [],
      form: {
        id: null,
        name: '',
        content: '',
        clusterServerId: ''
      }
    }
  },
  created() {
    this.loadCanalConfig()
    getClustersAndServers().then((res) => {
      this.options = res.data
    })
  },
  methods: {
    editorInit() {
      require('brace/ext/language_tools')
      require('brace/mode/html')
      require('brace/mode/yaml')
      require('brace/mode/properties')
      require('brace/mode/javascript')
      require('brace/mode/less')
      require('brace/theme/chrome')
      require('brace/snippets/javascript')
    },
    loadCanalConfig() {
      canalInstanceDetail(this.$route.query.id).then(response => {
        const data = response.data
        this.form.id = data.id
        this.form.name = data.name + '/instance.propertios'
        this.form.content = data.content
        this.form.clusterServerId = data.clusterServerId
      })
    },
    onSubmit() {
      this.$confirm(
        '修改Instance配置可能会导致重启，是否继续？',
        '确定修改',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }
      ).then(() => {
        updateCanalInstance(this.form).then(response => {
          if (response.data === 'success') {
            this.$message({
              message: '修改成功',
              type: 'success'
            })
            this.loadCanalConfig()
          } else {
            this.$message({
              message: '修改失败',
              type: 'error'
            })
          }
        })
      })
    },
    onCancel() {
      this.loadCanalConfig()
    },
    onBack() {
      history.go(-1)
    }
  }
}
</script>

<style scoped>
.line{
  text-align: center;
}
</style>

