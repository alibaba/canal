<template>
  <div>
    <el-form ref="form" :model="form">
      <div style="padding-left: 10px;padding-right: 10px;padding-top: 20px;">
        <el-form-item>
          canal.log&nbsp;&nbsp;&nbsp;&nbsp;
          <el-button type="primary" @click="onRefresh">刷新</el-button>
          <el-button type="info" @click="onBack">返回</el-button>
        </el-form-item>
        <el-input v-model="form.desc" :rows="35" :readonly="'readonly'" type="textarea" />
      </div>
    </el-form>
  </div>
</template>

<script>
import { nodeServerLog } from '@/api/nodeServer'

export default {
  data() {
    return {
      form: {
        desc: ''
      }
    }
  },
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      nodeServerLog(this.$route.query.id).then(res => {
        this.form.desc = res.data
      })
    },
    onRefresh() {
      this.fetchData()
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

