import Cookies from 'js-cookie'

const TokenKey = 'canal_admin_token'

export function getToken() {
  return Cookies.get(TokenKey)
}

export function setToken(token) {
  return Cookies.set(TokenKey, token, { maxAge: 0 })
}

export function removeToken() {
  return Cookies.remove(TokenKey)
}
