<template>
  <div>
    <el-form ref="form" :model="form">
      <div style="padding-left: 10px;padding-top: 20px;">
        <el-form-item>
          {{ form.name }}&nbsp;&nbsp;&nbsp;&nbsp;
          <el-button type="primary" @click="onSubmit">保存</el-button>
          <el-button type="warning" @click="onCancel">重置</el-button>
          <el-button type="success" @click="onLoadTemplate">载入模板</el-button>
          <el-button type="info" @click="onBack">返回</el-button>
        </el-form-item>
      </div>
      <editor v-model="form.content" lang="properties" theme="chrome" width="100%" :height="800" @init="editorInit" />
    </el-form>
  </div>
</template>

<script>
import { getCanalConfig, updateCanalConfig, getTemplateConfig } from '@/api/canalConfig'

export default {
  components: {
    editor: require('vue2-ace-editor')
  },
  data() {
    return {
      form: {
        id: null,
        name: '',
        content: '',
        serverId: null,
        clusterId: null
      }
    }
  },
  created() {
    this.loadCanalConfig()
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
      let clusterId = 0
      let serverId = 0
      if (this.$route.query.clusterId) {
        clusterId = this.$route.query.clusterId
      } else if (this.$route.query.serverId) {
        serverId = this.$route.query.serverId
      }
      getCanalConfig(clusterId, serverId).then(response => {
        const data = response.data
        this.form.id = data.id
        this.form.name = data.name
        this.form.content = data.content
        this.form.serverId = this.$route.query.serverId
        this.form.clusterId = this.$route.query.clusterId
      })
    },
    onSubmit() {
      if (this.form.content === null || this.form.content === '') {
        this.$message({
          message: '配置内容不能为空',
          type: 'error'
        })
        return
      }
      this.$confirm(
        '修改主配置可能会导致Server重启，是否继续？',
        '确定修改',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }
      ).then(() => {
        updateCanalConfig(this.form).then(response => {
          if (response.data === 'success') {
            this.$message({
              message: '保存成功',
              type: 'success'
            })
            this.loadCanalConfig()
          } else {
            this.$message({
              message: '保存失败',
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
    },
    onLoadTemplate() {
      getTemplateConfig().then(res => {
        this.form.content = res.data
      })
    }
  }
}
</script>

<style scoped>
.line{
  text-align: center;
}
</style>

