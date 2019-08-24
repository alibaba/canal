<template>
  <div>
    <el-form ref="form" :model="form">
      <div class="filter-container" style="padding-left: 10px;padding-top: 20px;">
        <el-input v-model="form.name" placeholder="实例名称" style="width: 200px;" class="filter-item" />
        &nbsp;
        <el-button class="filter-item" type="primary" @click="onSubmit">新建</el-button>
        <el-button class="filter-item" type="info" @click="onBack">返回</el-button>
      </div>
      <editor v-model="form.content" lang="properties" theme="chrome" width="100%" :height="800" @init="editorInit" />
    </el-form>
  </div>
</template>

<script>
import { addCanalInstance } from '@/api/canalInstance'

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
    onSubmit() {
      if (this.form.name === '') {
        this.$message({
          message: '请输入实例名称',
          type: 'error'
        })
        return
      }
      this.$confirm(
        '确定新建',
        '确定新建',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }
      ).then(() => {
        addCanalInstance(this.form).then(response => {
          if (response.data === 'success') {
            this.$message({
              message: '新建成功',
              type: 'success'
            })
            this.$router.push('/canalServer/canalInstances')
          } else {
            this.$message({
              message: '新建失败',
              type: 'error'
            })
          }
        })
      })
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

