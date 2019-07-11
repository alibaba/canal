<template>
  <div>
    <el-form ref="form" :model="form">
      <div style="padding-left: 10px;padding-top: 20px;">
        <el-form-item>
          canal.properties&nbsp;&nbsp;&nbsp;&nbsp;
          <el-button type="primary" @click="onSubmit">修改</el-button>
          <el-button @click="onCancel">取消</el-button>
        </el-form-item>
      </div>
      <editor v-model="form.content" lang="properties" theme="chrome" width="100%" :height="800" @init="editorInit" />
    </el-form>
  </div>
</template>

<script>
import { getCanalConfig } from '@/api/canalConfig'

export default {
  components: {
    editor: require('vue2-ace-editor')
  },
  data() {
    return {
      form: {
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
        this.form.content = response.data.content
      })
    },
    onSubmit() {
      this.$message('submit!')
    },
    onCancel() {
      this.$message({
        message: 'cancel!',
        type: 'warning'
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

