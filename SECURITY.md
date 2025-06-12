# 漏洞奖励计划 
## 报告
如果您认为自己在本程序中发现了任何安全（技术）漏洞，欢迎您通过 https://security.alibaba.com 向我们提交漏洞报告。
如果您报告任何安全漏洞，请注意您可能包含以下信息（合格报告）：
* git程序URL地址，运行的环境
* 包含必要屏幕截图的详细说明
* 重现漏洞的步骤以及修复漏洞的建议。
* 其他有用信息

## 处理
ASRC（Alibaba Security Response Center阿里安全响应中心）将尽快审核并回复您的提交内容，并在我们努力修复您提交的漏洞时随时通知您。如有必要，我们可能会与您联系以获取更多信息。


## 条款和条件
1. 仅接受技术漏洞并对其进行评级
2. 出于安全原因，上报者同意与ASRC合作完成他/她提交的漏洞，不向任何第三方透露任何漏洞信息
3. 如果不止一个人报告相同的安全漏洞，奖励将给予完成合格报告的第一个人
4. 为了保护程序的用户，请在修复之前不要直接提交git的issue，也不要在社区讨论任何漏洞信息
5. 所有奖励和声誉积分将提供给仅向ASRC提交其安全漏洞的上报者
6. 安全漏洞奖励的解释权利归ASRC所有

## 收集范围
我们的主要收集漏洞类别是：
* 服务器端请求伪造（SSRF）
* SQL注入
* 拒绝服务攻击
* 远程执行代码（RCE）
* XML外部实体攻击（XXE）
* 访问控制问题（不安全的直接对象参考问题等）
* 敏感目录遍历问题
* 本地文件读取（LFD）
* 敏感信息泄露（密钥，Cookie，Session等）

## 奖励
* 可直接导致严重问题的每个漏洞奖励7000元人民币
* 存在限制及需要一定特殊环境下才能利用的问题将给予700-5600元人民币不等的奖励，比如需要用户主动点击才会触发的问题或需要admin权限
* 只有在指定环境下才可以运行的利用将有可能被收纳但不给予奖励，或直接被忽略，比如只在fastjson+linux特定版本才会出现的问题

## 不在收集范围的报告
* 影响过时浏览器或平台用户的漏洞
* Self-XSS
* 会话固定
* 内容欺骗
* 缺少cookie标记
* 混合内容警告
* SSL / TLS问题
* Clickjacking 
* 基于Flash的漏洞
* 反射文件下载攻击（RFD）
* 物理或社会工程攻击
* 未验证自动化工具或扫描仪的结果
* 登录/注销/未认证/低影响CSRF
* 需要MITM或物理访问用户设备的攻击
* 与网络协议或行业标准相关的问题
* 不能用于直接攻击的错误信息泄露
* 缺少与安全相关的HTTP标头等





# Vulnerability Reward Program
## Reporting
If you believe you have found any security (technical) vulnerability in the Program, you are welcomed to submit a vulnerability report to us at https://security.alibaba.com 
In case of reporting any security vulnerability, please be noted that you may including following information (Qualified Reporting):
* The git program URL and running version 
* A detailed description with necessary screenshots
* Steps to reappearance the vulnerability and your advice to fix it
* Other useful information


## Processing
ASRC (Alibaba Security Response Center) will review and respond as quickly as possible to your submission, and keep you informed as we work to fix the vulnerability you submitted. We may contact you for further information if necessary.


## Terms and Conditions
1. ONLY technical vulnerabilities will be accepted and rated.
2. With regarding to security reasons, reporters agree to cooperate with ASRC exclusively on the vulnerability he/she submitted and not disclose any information of vulnerability to any third-parties.
3. In the case that more than one person report the same security vulnerability, the reward will be given to the first person who accomplish a Qualified Reporting.
4. To protect users of the program, please do not directly submit issue on github or discuss anything with the community 
5. All Rewards and Reputation Credits are given to the reporters who submit his/her security vulnerabilities ONLY to ASRC.
6. All rights for the security vulnerability rewards are reserved by ASRC.

## Scope of Collecting
The main categories of vulnerabilities that we are sincerely looking for are:
* Server-Side Request Forgery (SSRF)
* SQL Injection
* Denial of Service Attack
* Remote Code Execution (RCE)
* XML External Entity Attacks (XXE)
* Access Control Issues (Insecure Direct Object Reference issues, etc.)
* Directory Traversal Issues
* Local File Disclosure (LFD)
* Sensitive Information Leakage (Key, Cookie, Session etc.)

## Reward
* $1,000 for one valid report
* $100-$800 for Vuls which is limited. For example, Vuls that need user interactions or administrator authority
* Vuls which only work on the special version will be accepted but no reward, or directly rejected. For example, Vul runs only on a special linux version

## Ineligible Reports
* Vulnerabilities affecting users of outdated browsers or platforms
* "Self" XSS
* Session fixation
* Content Spoofing
* Missing cookie flags
* Mixed content warnings
* SSL/TLS best practices
* Clickjacking/UI redressing
* Flash-based vulnerabilities
* Reflected file download attacks (RFD)
* Physical or social engineering attacks
* Unverified Results of automated tools or scanners
* Login/logout/unauthenticated/low-impact CSRF
* Attacks requiring MITM or physical access to a user's device
* Issues related to networking protocols or industry standards
* Error information disclosure that cannot be used to make a direct attack
* Missing security-related HTTP headers which do not lead directly to a vulnerability
