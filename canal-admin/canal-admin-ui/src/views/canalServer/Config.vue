<template>
  <div>
    <el-form ref="form" :model="form">
      <div style="padding-left: 10px;padding-top: 20px;">
        <el-form-item>
          {{ form.name }}&nbsp;&nbsp;&nbsp;&nbsp;
          <el-button type="primary" @click="onSubmit">修改</el-button>
          <el-button type="warning" @click="onCancel">重置</el-button>
        </el-form-item>
      </div>
      <editor v-model="form.content" lang="properties" theme="chrome" width="100%" :height="800" @init="editorInit" />
    </el-form>
  </div>
</template>

<script>
import { getCanalConfig, updateCanalConfig } from '@/api/canalConfig'

export default {
  components: {
    editor: require('vue2-ace-editor')
  },
  data() {
    return {
      form: {
        id: null,
        name: '',
        content: ''
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
      getCanalConfig().then(response => {
        const data = response.data
        this.form.id = data.id
        this.form.name = data.name
        this.form.content = data.content
      })
    },
    onSubmit() {
      this.$confirm(
        '修改Server主配置可能会导致Server重启，是否继续？',
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
    }
  }
}
</script>

<style scoped>
.line{
  text-align: center;
}
</style>

