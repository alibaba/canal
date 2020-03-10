<template>
  <div class="app-container" style="width: 600px;">
    <el-form ref="form" :rules="rules" :model="form" label-width="120px">
      <el-form-item label="用户名" prop="username">
        <el-input v-model="form.username" style="width: 200px;" />
      </el-form-item>
      <el-form-item label="旧密码" prop="oldPassword">
        <el-input v-model="form.oldPassword" type="password" style="width: 200px;" />
      </el-form-item>
      <el-form-item label="密码" prop="password">
        <el-input v-model="form.password" placeholder="空为不修改密码" type="password" style="width: 200px;" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="onSubmit">修改</el-button>
        <el-button @click="onCancel">取消</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import { getInfo, updateUser } from '@/api/user'
import { getToken } from '@/utils/auth'

export default {
  data() {
    return {
      form: {
        username: '',
        oldPassword: '',
        password: null
      },
      rules: {
        username: [{ required: true, message: '用户名能为空', trigger: 'change' }],
        oldPassword: [{ required: true, message: '旧密码不能为空', trigger: 'change' }]
      }
    }
  },
  created() {
    this.fetchUserInfo()
  },
  methods: {
    fetchUserInfo() {
      getInfo(getToken()).then(res => {
        this.form.username = res.data.username
      })
    },
    onSubmit() {
      this.$refs['form'].validate((valid) => {
        if (valid) {
          updateUser(this.form).then(res => {
            if (res.data === 'success') {
              this.form.oldPassword = ''
              this.form.password = null
              this.$nextTick(() => {
                this.$refs['form'].clearValidate()
              })
              this.$message({
                message: '修改用户信息成功',
                type: 'success'
              })
            } else {
              this.$message({
                message: '修改用户信息成功',
                type: 'error'
              })
            }
          })
        }
      })
    },
    onCancel() {
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

